# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""System-level ACID tests through Chiral service entry points."""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from chiral.core.ingestion import ingest_data
from chiral.core.query_service import CreateExecutionValidationError, execute_json_request

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.asyncio


class TestAcidProperties:
    """Validate ACID properties through Chiral system APIs."""

    @staticmethod
    async def _fetch_scalar(engine: AsyncEngine, query: str, params: dict[str, object]) -> int:
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params)
            raw = result.scalar_one_or_none()
        return int(raw) if raw is not None else 0

    @staticmethod
    async def _seed_session_metadata(engine: AsyncEngine, session_id: str, schema: dict[str, object]) -> None:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO session_metadata (
                        session_id,
                        record_count,
                        schema_version,
                        drift_events,
                        safety_events,
                        migration_metrics,
                        schema_json
                    )
                    VALUES (
                        :sid,
                        0,
                        1,
                        '[]'::jsonb,
                        '[]'::jsonb,
                        '[]'::jsonb,
                        :schema_json
                    )
                    ON CONFLICT (session_id) DO UPDATE
                    SET schema_json = EXCLUDED.schema_json
                    """
                ),
                {"sid": session_id, "schema_json": json.dumps(schema)},
            )

    async def test_atomicity_ingest_rolls_back_staging_and_count_on_failure(self, acid_engine: AsyncEngine) -> None:
        """If ingest fails mid-flight, system should not leave partial staging/count writes."""
        session_id = "acid_system_atomicity"
        record = {"username": "atomic_user", "temperature": 30, "t_stamp": time.time()}

        with (
            patch("chiral.core.ingestion.MonotonicClock.get_sys_ingested_at", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await ingest_data(data=record, session_id=session_id)

        staging_count = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid",
            {"sid": session_id},
        )
        metadata_count = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM session_metadata WHERE session_id = :sid",
            {"sid": session_id},
        )
        record_count = await self._fetch_scalar(
            acid_engine,
            "SELECT COALESCE(MAX(record_count), 0) FROM session_metadata WHERE session_id = :sid",
            {"sid": session_id},
        )

        # Session row may be initialized first, but the ingest unit must not partially apply data.
        assert staging_count == 0
        assert metadata_count == 1
        assert record_count == 0

    async def test_atomicity_cross_backend_rollback(self, acid_engine: AsyncEngine) -> None:
        """If the dynamic child insert fails, the parent insert must roll back too."""
        session_id = "acid_atomicity_cross_backend"
        child_table = "chiral_data_atomic_comments"

        async with acid_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{child_table}" (
                        id SERIAL PRIMARY KEY,
                        chiral_data_id INTEGER NOT NULL,
                        session_id TEXT,
                        text TEXT,
                        overflow_data JSONB DEFAULT '{{}}'::jsonb
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        ALTER TABLE "{child_table}"
                        ADD CONSTRAINT fk_{child_table}_chiral_data
                        FOREIGN KEY (chiral_data_id) REFERENCES chiral_data(id) ON DELETE CASCADE;
                    EXCEPTION
                        WHEN duplicate_object THEN NULL;
                    END $$;
                    """
                )
            )
            await conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        ALTER TABLE "{child_table}"
                        ADD CONSTRAINT fk_{child_table}_session_metadata
                        FOREIGN KEY (session_id) REFERENCES session_metadata(session_id) ON DELETE CASCADE;
                    EXCEPTION
                        WHEN duplicate_object THEN NULL;
                    END $$;
                    """
                )
            )

        request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {
                "session_id": session_id,
                "username": "atomic_tester",
                "sys_ingested_at": 999.9,
                "t_stamp": 999.9,
                "comments": [{"text": "valid"}],
            },
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "atomic_comments",
                        "child_table": child_table,
                        "relationship": "one_to_many",
                        "child_columns": ["text"],
                        "child_column_types": {"text": "str"},
                    }
                ],
            },
        }

        with (
            patch("chiral.worker.migrator.materialize_decomposition_tables", return_value=None),
            patch("chiral.worker.migrator._insert_dynamic_row", side_effect=RuntimeError("Document Store Offline")),
            pytest.raises(RuntimeError, match="Document Store Offline"),
        ):
            await execute_json_request(request)

        parent_rows = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid",
            {"sid": session_id},
        )
        assert parent_rows == 0, "Atomicity failed: parent row persisted after child insert failure."

    async def test_consistency_logical_jsonb_update(self, acid_engine: AsyncEngine) -> None:
        """Logical updates should rewrite JSONB-backed fields without leaking schema details."""
        session_id = "acid_consistency_update"
        await self._seed_session_metadata(
            acid_engine,
            session_id,
            {
                "city": {"target": "jsonb", "type": "str"},
                "username": {"target": "sql", "type": "str"},
            },
        )

        async with acid_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO chiral_data (session_id, username, sys_ingested_at, t_stamp, overflow_data)
                    VALUES (:sid, :username, :sys_ingested_at, :t_stamp, CAST(:overflow_data AS jsonb))
                    """
                ),
                {
                    "sid": session_id,
                    "username": "update_tester",
                    "sys_ingested_at": 999.9,
                    "t_stamp": 999.9,
                    "overflow_data": '{"city": "Old City"}',
                },
            )

        await execute_json_request(
            {
                "operation": "update",
                "table": "chiral_data",
                "updates": {"city": "New City"},
                "filters": [{"field": "username", "op": "eq", "value": "update_tester"}],
                "session_id": session_id,
            }
        )

        async with acid_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT overflow_data->>'city' FROM chiral_data WHERE username = 'update_tester'")
            )
            updated_city = result.scalar_one()

        assert updated_city == "New City", "Consistency failed: logical update did not reach JSONB storage."

    async def test_consistency_cascading_deletes(self, acid_engine: AsyncEngine) -> None:
        """Deleting a parent should cascade to its materialized child rows."""
        session_id = "acid_consistency_cascade"
        decomposition_plan = {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [
                {
                    "source_field": "comments",
                    "child_table": "chiral_data_comments",
                    "relationship": "one_to_many",
                    "child_columns": ["text"],
                    "child_column_types": {"text": "str"},
                }
            ],
        }

        await execute_json_request(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {
                    "session_id": session_id,
                    "username": "cascade_user",
                    "sys_ingested_at": 999.9,
                    "t_stamp": 999.9,
                    "comments": [{"text": "comment 1"}, {"text": "comment 2"}],
                },
                "decomposition_plan": decomposition_plan,
            }
        )

        await execute_json_request(
            {
                "operation": "delete",
                "table": "chiral_data",
                "filters": [{"field": "username", "op": "eq", "value": "cascade_user"}],
                "session_id": session_id,
            }
        )

        async with acid_engine.connect() as conn:
            parent_count = await conn.scalar(text("SELECT COUNT(*) FROM chiral_data WHERE username = 'cascade_user'"))
            comments_count = await conn.scalar(
                text("SELECT COUNT(*) FROM chiral_data_comments WHERE session_id = :sid"),
                {"sid": session_id},
            )

        assert int(parent_count or 0) == 0
        assert int(comments_count or 0) == 0

    async def test_isolation_concurrent_crud_updates(self, acid_engine: AsyncEngine) -> None:
        """Concurrent logical updates should serialize cleanly without corrupting the row."""
        session_id = "acid_isolation_update"

        await execute_json_request(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {
                    "session_id": session_id,
                    "username": "race_condition_user",
                    "sys_ingested_at": 0.0,
                    "t_stamp": 0.0,
                },
            }
        )

        async def concurrent_update(val: int) -> None:
            await execute_json_request(
                {
                    "operation": "update",
                    "table": "chiral_data",
                    "updates": {"username": f"race_condition_user_{val}"},
                    "filters": [{"field": "session_id", "op": "eq", "value": session_id}],
                    "session_id": session_id,
                }
            )

        await asyncio.gather(*(concurrent_update(i) for i in range(1, 6)))

        async with acid_engine.connect() as conn:
            final_username = await conn.scalar(
                text("SELECT username FROM chiral_data WHERE session_id = :sid"),
                {"sid": session_id},
            )

        assert final_username in {f"race_condition_user_{i}" for i in range(1, 6)}

    async def test_consistency_referential_integrity(self, acid_engine: AsyncEngine) -> None:
        """Foreign keys on materialized child tables should reject orphan inserts."""
        session_id = "acid_consistency_fk"

        await execute_json_request(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {
                    "session_id": session_id,
                    "username": "fk_parent",
                    "sys_ingested_at": 111.1,
                    "t_stamp": 111.1,
                    "comments": [{"text": "seed"}],
                },
                "decomposition_plan": {
                    "version": 1,
                    "parent_table": "chiral_data",
                    "entities": [
                        {
                            "source_field": "comments",
                            "child_table": "chiral_data_comments",
                            "relationship": "one_to_many",
                            "child_columns": ["text"],
                            "child_column_types": {"text": "str"},
                        }
                    ],
                },
            }
        )

        with pytest.raises(IntegrityError):
            async with acid_engine.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO chiral_data_comments (chiral_data_id, session_id, text)
                        VALUES (-999, :sid, 'this should violate FK')
                        """
                    ),
                    {"sid": session_id},
                )

    async def test_consistency_create_requires_session_id_and_preserves_state(self, acid_engine: AsyncEngine) -> None:
        """System request validation should reject inconsistent create payloads without side effects."""
        bad_request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {
                "username": "missing_session",
                "sys_ingested_at": 1.0,
                "t_stamp": 1.0,
                "overflow_data": "{}",
            },
        }

        with pytest.raises(CreateExecutionValidationError, match="requires session_id"):
            await execute_json_request(bad_request)

        rows = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM chiral_data",
            {},
        )
        staging = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM staging_data",
            {},
        )
        assert rows == 0
        assert staging == 0

    async def test_isolation_concurrent_ingest_has_no_lost_updates(self, acid_engine: AsyncEngine) -> None:
        """Concurrent system ingests must serialize counter updates correctly."""
        session_id = "acid_system_isolation"
        total_writes = 4

        # Prime the session once to avoid concurrent init contention and then test steady-state isolation.
        await ingest_data(
            data={"username": "iso_prime", "temperature": -1, "t_stamp": time.time()},
            session_id=session_id,
        )

        async def one_ingest(idx: int) -> dict[str, object]:
            payload = {
                "username": f"iso_user_{idx}",
                "temperature": idx,
                "t_stamp": time.time() + idx,
            }
            attempts = 0
            while True:
                attempts += 1
                try:
                    return await ingest_data(data=payload, session_id=session_id)
                except Exception as exc:
                    if "deadlock detected" not in str(exc).lower() or attempts >= 8:
                        raise
                    await asyncio.sleep(0.03 * attempts)

        results = await asyncio.gather(*(one_ingest(i) for i in range(total_writes)))

        final_count = await self._fetch_scalar(
            acid_engine,
            "SELECT record_count FROM session_metadata WHERE session_id = :sid",
            {"sid": session_id},
        )
        staging_count = await self._fetch_scalar(
            acid_engine,
            "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid",
            {"sid": session_id},
        )

        assert len(results) == total_writes
        assert final_count == total_writes + 1
        assert staging_count == total_writes + 1

    async def test_atomicity_nested_create_rollback_on_child_failure(
        self, acid_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If a child entity fails to insert during a synchronous nested create, the parent MUST roll back."""
        from src.chiral.worker import migrator

        session_id = "acid_nested_atomicity"
        payload = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {
                "session_id": session_id,
                "username": "atomic_parent",
                "comments": [{"text": "this will fail"}],
            },
        }

        # We need a decomposition plan so the query engine attempts a synchronous multi-table insert
        plan = {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [{"source_field": "comments", "child_table": "chiral_data_comments"}],
        }
        payload["decomposition_plan"] = plan

        # Force the child insert to fail by mocking _insert_dynamic_row to throw a DB error
        original_insert_dynamic = migrator._insert_dynamic_row

        async def failing_child_insert(*args, **kwargs):
            if "chiral_data_comments" in kwargs.get("table_name", ""):
                raise Exception("Simulated database crash during child insert")
            return await original_insert_dynamic(*args, **kwargs)

        monkeypatch.setattr(migrator, "_insert_dynamic_row", failing_child_insert)

        # Execute the query. The exception will be caught and it will enqueue for async fallback.
        # BUT the synchronous transaction must have rolled back the parent.
        await execute_json_request(payload)

        # Verify the parent record was rolled back and DOES NOT exist in chiral_data
        parent_count = await self._fetch_scalar(
            acid_engine, "SELECT COUNT(*) FROM chiral_data WHERE username = :usr", {"usr": "atomic_parent"}
        )

        assert parent_count == 0, "Atomicity violated! Parent record persisted despite child insertion failure."

    async def test_isolation_concurrent_jsonb_updates_prevent_lost_updates(self, acid_engine: AsyncEngine) -> None:
        """Concurrent updates to different keys within the same JSONB document must not overwrite each other (No Lost Updates)."""
        session_id = "acid_jsonb_isolation"

        # 1. Create a parent record
        await execute_json_request(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "jsonb_user", "overflow_data": "{}"},
            }
        )

        # 2. Fire two concurrent UPDATE requests targeting different JSONB keys in overflow_data
        update_1 = execute_json_request(
            {
                "operation": "update",
                "table": "chiral_data",
                "updates": {"overflow_data.city": "Paris"},
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )

        update_2 = execute_json_request(
            {
                "operation": "update",
                "table": "chiral_data",
                "updates": {"overflow_data.device": "iPhone"},
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )

        await asyncio.gather(update_1, update_2)

        # 3. Read the record and verify BOTH updates survived in the JSONB document
        read_req = await execute_json_request(
            {
                "operation": "read",
                "table": "chiral_data",
                "select": ["overflow_data.city", "overflow_data.device"],
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )

        row = read_req["rows"][0]
        # If the DB lacked Isolation, one of these would be None/Null
        assert row["json_0_city"] == "Paris", "Lost Update: City was overwritten!"
        assert row["json_1_device"] == "iPhone", "Lost Update: Device was overwritten!"

    async def test_durability_create_visible_on_fresh_session(self, acid_engine: AsyncEngine) -> None:
        """Committed create through system API must persist and be readable from fresh DB sessions."""
        session_id = "acid_system_durability"
        create_request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {
                "session_id": session_id,
                "username": "durable_user",
                "sys_ingested_at": 123.45,
                "t_stamp": 123.45,
                "overflow_data": "{}",
            },
        }

        response = await execute_json_request(create_request)
        assert int(response.get("affected_rows", 0)) == 1

        async with acid_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid AND username = :username"),
                {"sid": session_id, "username": "durable_user"},
            )
            first_read = int(result.scalar_one())

        async with acid_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid AND username = :username"),
                {"sid": session_id, "username": "durable_user"},
            )
            second_read = int(result.scalar_one())

        assert first_read == 1
        assert second_read == 1

    async def test_consistency_cascading_deletes(self, acid_engine: AsyncEngine) -> None:
        """Prove that deleting a logical parent maintains system consistency by automatically purging physical child records."""
        session_id = "acid_consistency_cascade"

        # 1. Create a parent with nested child entities
        await execute_json_request({
            "operation": "create",
            "table": "chiral_data",
            "payload": {
                "session_id": session_id,
                "username": "cascade_user",
                "comments": [{"text": "comment 1"}, {"text": "comment 2"}]
            }
        })

        # 2. Execute logical delete on the parent
        await execute_json_request({
            "operation": "delete",
            "table": "chiral_data",
            "filters": [{"field": "username", "op": "eq", "value": "cascade_user"}]
        })

        # 3. Verify child records were purged consistently at the engine level
        async with acid_engine.connect() as conn:
            comments_count = await conn.scalar(
                text("SELECT COUNT(*) FROM chiral_data_comments WHERE session_id = :sid"),
                {"sid": session_id}
            )
            assert comments_count == 0, "Consistency failed: Orphaned child records remained after parent deletion."

    async def test_isolation_concurrent_crud_updates(self, acid_engine: AsyncEngine) -> None:
        """Prove that concurrent logical updates to the same record are safely isolated and serialized."""
        session_id = "acid_isolation_update"

        # 1. Create base record
        await execute_json_request({
            "operation": "create",
            "payload": {
                "session_id": session_id,
                "username": "race_condition_user",
                "temperature": 0
            }
        })

        # 2. Fire 5 concurrent updates trying to set temperature to different values
        async def concurrent_update(val: int) -> None:
            await execute_json_request({
                "operation": "update",
                "updates": {"temperature": val},
                "filters": [{"field": "username", "op": "eq", "value": "race_condition_user"}]
            })

        await asyncio.gather(*(concurrent_update(i) for i in range(1, 6)))

        # 3. Verify the database remained consistent and one of the updates won without corrupting state
        async with acid_engine.connect() as conn:
            final_temp = await conn.scalar(
                text("SELECT temperature FROM chiral_data WHERE username = 'race_condition_user'")
            )
            assert final_temp in [1, 2, 3, 4, 5], "Isolation failed: Concurrent updates corrupted the integer state."

    async def test_consistency_referential_integrity(self, acid_engine: AsyncEngine) -> None:
        """Prove the system enforces Consistency by rejecting operations that violate relational constraints."""
        from sqlalchemy.exc import IntegrityError

        # Attempt to insert a child record into chiral_data_comments pointing to a non-existent parent_id
        async with acid_engine.connect() as conn:
            with pytest.raises(IntegrityError):
                await conn.execute(
                    text("""
                        INSERT INTO chiral_data_comments (chiral_data_id, session_id, text)
                        VALUES (-999, 'fake_session', 'this should violate FK')
                    """)
                )
