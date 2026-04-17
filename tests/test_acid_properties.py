# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""System-level ACID tests through Chiral service entry points."""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from chiral.core.query_service import CreateExecutionValidationError
from chiral.worker import migrator

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from chiral.client import ChiralClient

pytestmark = pytest.mark.asyncio


class SimulatedDatabaseCrashError(Exception):
    """Raised to simulate a database crash during child insert."""


class TestAcidProperties:
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
                        session_id, record_count, schema_version, drift_events, safety_events, migration_metrics, schema_json
                    )
                    VALUES (:sid, 0, 1, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, :schema_json)
                    ON CONFLICT (session_id) DO UPDATE SET schema_json = EXCLUDED.schema_json
                    """
                ),
                {"sid": session_id, "schema_json": json.dumps(schema)},
            )

    async def test_atomicity_ingest_rolls_back_staging_and_count_on_failure(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        session_id = "acid_system_atomicity"
        record = {"username": "atomic_user", "temperature": 30, "t_stamp": time.time()}

        with (
            patch("chiral.core.ingestion.MonotonicClock.get_sys_ingested_at", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await acid_client.ingest(session_id=session_id, data=record)

        staging_count = await self._fetch_scalar(
            acid_engine, "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid", {"sid": session_id}
        )
        metadata_count = await self._fetch_scalar(
            acid_engine, "SELECT COUNT(*) FROM session_metadata WHERE session_id = :sid", {"sid": session_id}
        )
        record_count = await self._fetch_scalar(
            acid_engine,
            "SELECT COALESCE(MAX(record_count), 0) FROM session_metadata WHERE session_id = :sid",
            {"sid": session_id},
        )

        assert staging_count == 0
        assert metadata_count == 1
        assert record_count == 0

    async def test_atomicity_cross_backend_rollback(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_atomicity_cross_backend"
        child_table = "chiral_data_atomic_comments"

        async with acid_engine.begin() as conn:
            await conn.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS \"{child_table}\" (id SERIAL PRIMARY KEY, chiral_data_id INTEGER NOT NULL, session_id TEXT, text TEXT, overflow_data JSONB DEFAULT '{{}}'::jsonb)"
                )
            )
            await conn.execute(
                text(
                    f'DO $$ BEGIN ALTER TABLE "{child_table}" ADD CONSTRAINT fk_{child_table}_chiral_data FOREIGN KEY (chiral_data_id) REFERENCES chiral_data(id) ON DELETE CASCADE; EXCEPTION WHEN duplicate_object THEN NULL; END $$;'
                )
            )
            await conn.execute(
                text(
                    f'DO $$ BEGIN ALTER TABLE "{child_table}" ADD CONSTRAINT fk_{child_table}_session_metadata FOREIGN KEY (session_id) REFERENCES session_metadata(session_id) ON DELETE CASCADE; EXCEPTION WHEN duplicate_object THEN NULL; END $$;'
                )
            )

        request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {"session_id": session_id, "username": "atomic_tester", "comments": [{"text": "valid"}]},
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "atomic_comments",
                        "child_table": child_table,
                        "child_columns": ["text"],
                        "child_column_types": {"text": "str"},
                    }
                ],
            },
        }

        with (
            patch("chiral.worker.migrator.materialize_decomposition_tables", return_value=None),
            patch(
                "chiral.worker.migrator._insert_dynamic_row",
                side_effect=SimulatedDatabaseCrashError("Document Store Offline"),
            ),
            pytest.raises(SimulatedDatabaseCrashError, match="Document Store Offline"),
        ):
            await acid_client.query(request)

        parent_rows = await self._fetch_scalar(
            acid_engine, "SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid", {"sid": session_id}
        )
        assert parent_rows == 0

    async def test_consistency_logical_jsonb_update(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_consistency_update"
        await self._seed_session_metadata(
            acid_engine,
            session_id,
            {"city": {"target": "jsonb", "type": "str"}, "username": {"target": "sql", "type": "str"}},
        )

        async with acid_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO chiral_data (session_id, username, overflow_data) VALUES (:sid, :username, CAST(:overflow_data AS jsonb))"
                ),
                {"sid": session_id, "username": "update_tester", "overflow_data": '{"city": "Old City"}'},
            )

        await acid_client.query(
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
            assert result.scalar_one() == "New City"

    async def test_consistency_cascading_deletes(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_consistency_cascade"
        plan = {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [
                {
                    "source_field": "comments",
                    "child_table": "chiral_data_comments",
                    "child_columns": ["text"],
                    "child_column_types": {"text": "str"},
                }
            ],
        }

        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {
                    "session_id": session_id,
                    "username": "cascade_user",
                    "comments": [{"text": "1"}, {"text": "2"}],
                },
                "decomposition_plan": plan,
            }
        )
        await acid_client.query(
            {
                "operation": "delete",
                "table": "chiral_data",
                "filters": [{"field": "username", "op": "eq", "value": "cascade_user"}],
                "session_id": session_id,
            }
        )

        async with acid_engine.connect() as conn:
            assert await conn.scalar(text("SELECT COUNT(*) FROM chiral_data WHERE username = 'cascade_user'")) == 0
            assert (
                await conn.scalar(
                    text("SELECT COUNT(*) FROM chiral_data_comments WHERE session_id = :sid"), {"sid": session_id}
                )
                == 0
            )

    async def test_isolation_concurrent_crud_updates(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_isolation_update"
        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "race_condition_user"},
            }
        )

        async def concurrent_update(val: int) -> None:
            await acid_client.query(
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
                text("SELECT username FROM chiral_data WHERE session_id = :sid"), {"sid": session_id}
            )
        assert final_username in {f"race_condition_user_{i}" for i in range(1, 6)}

    async def test_consistency_referential_integrity(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_consistency_fk"
        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "fk_parent", "comments": [{"text": "seed"}]},
                "decomposition_plan": {
                    "version": 1,
                    "parent_table": "chiral_data",
                    "entities": [
                        {
                            "source_field": "comments",
                            "child_table": "chiral_data_comments",
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
                        "INSERT INTO chiral_data_comments (chiral_data_id, session_id, text) VALUES (-999, :sid, 'violates FK')"
                    ),
                    {"sid": session_id},
                )

    async def test_consistency_create_requires_session_id_and_preserves_state(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        bad_request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {"username": "missing_session", "overflow_data": "{}"},
        }
        with pytest.raises(CreateExecutionValidationError, match="requires session_id"):
            await acid_client.query(bad_request)

        assert await self._fetch_scalar(acid_engine, "SELECT COUNT(*) FROM chiral_data", {}) == 0
        assert await self._fetch_scalar(acid_engine, "SELECT COUNT(*) FROM staging_data", {}) == 0

    async def test_isolation_concurrent_ingest_has_no_lost_updates(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        session_id = "acid_system_isolation"
        total_writes = 4

        await acid_client.ingest(
            data={"username": "iso_prime", "temperature": -1, "t_stamp": time.time()}, session_id=session_id
        )

        async def one_ingest(idx: int) -> dict[str, object]:
            payload = {"username": f"iso_user_{idx}", "temperature": idx, "t_stamp": time.time() + idx}
            attempts = 0
            while True:
                attempts += 1
                try:
                    return await acid_client.ingest(data=payload, session_id=session_id)
                except Exception as exc:
                    if "deadlock detected" not in str(exc).lower() or attempts >= 8:
                        raise
                    await asyncio.sleep(0.03 * attempts)

        results = await asyncio.gather(*(one_ingest(i) for i in range(total_writes)))
        final_count = await self._fetch_scalar(
            acid_engine, "SELECT record_count FROM session_metadata WHERE session_id = :sid", {"sid": session_id}
        )
        assert len(results) == total_writes
        assert final_count == total_writes + 1

    async def test_atomicity_nested_create_rollback_on_child_failure(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        session_id = "acid_nested_atomicity"
        payload = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {"session_id": session_id, "username": "atomic_parent", "comments": [{"text": "fails"}]},
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [{"source_field": "comments", "child_table": "chiral_data_comments"}],
            },
        }

        original_insert_dynamic = migrator._insert_dynamic_row

        async def failing_child_insert(*args: Any, **kwargs: Any) -> Any:
            if "chiral_data_comments" in kwargs.get("table_name", ""):
                raise SimulatedDatabaseCrashError("Simulated database crash")
            return await original_insert_dynamic(*args, **kwargs)

        monkeypatch.setattr(migrator, "_insert_dynamic_row", failing_child_insert)

        # EXPECT the crash to bubble up after the database rollback happens!
        with pytest.raises(SimulatedDatabaseCrashError):
            await acid_client.query(payload)

        # Verify the parent record was rolled back and DOES NOT exist in chiral_data
        parent_count = await self._fetch_scalar(
            acid_engine, "SELECT COUNT(*) FROM chiral_data WHERE username = :usr", {"usr": "atomic_parent"}
        )
        assert parent_count == 0

    async def test_isolation_concurrent_jsonb_updates_prevent_lost_updates(self, acid_client: ChiralClient) -> None:
        session_id = "acid_jsonb_isolation"
        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "jsonb_user", "overflow_data": "{}"},
            }
        )

        await acid_client.flush(session_id)

        update_1 = acid_client.query(
            {
                "operation": "update",
                "table": "chiral_data",
                "updates": {"overflow_data.city": "Paris"},
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )
        update_2 = acid_client.query(
            {
                "operation": "update",
                "table": "chiral_data",
                "updates": {"overflow_data.device": "iPhone"},
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )

        await asyncio.gather(update_1, update_2)
        read_req = await acid_client.query(
            {
                "operation": "read",
                "table": "chiral_data",
                "select": ["overflow_data.city", "overflow_data.device"],
                "filters": [{"field": "username", "op": "eq", "value": "jsonb_user"}],
            }
        )

        row = read_req["rows"][0]
        assert row["json_0_city"] == "Paris"
        assert row["json_1_device"] == "iPhone"

    async def test_durability_create_visible_on_fresh_session(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        session_id = "acid_system_durability"
        create_request = {
            "operation": "create",
            "table": "chiral_data",
            "payload": {"session_id": session_id, "username": "durable_user", "overflow_data": "{}"},
        }
        await acid_client.query(create_request)

        async with acid_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid AND username = :username"),
                {"sid": session_id, "username": "durable_user"},
            )
            assert int(result.scalar_one()) == 1

    async def test_consistency_cascading_deletes1(self, acid_client: ChiralClient, acid_engine: AsyncEngine) -> None:
        session_id = "acid_consistency_cascade1"
        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "cascade_user", "comments": [{"text": "1"}]},
            }
        )

        await acid_client.flush(session_id)
        await acid_client.query(
            {
                "operation": "delete",
                "table": "chiral_data",
                "filters": [{"field": "username", "op": "eq", "value": "cascade_user"}],
            }
        )

        async with acid_engine.connect() as conn:
            assert (
                await conn.scalar(
                    text("SELECT COUNT(*) FROM chiral_data_comments WHERE session_id = :sid"), {"sid": session_id}
                )
                == 0
            )

    async def test_isolation_concurrent_crud_updates1(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        session_id = "acid_isolation_update1"

        # Explicitly seed schema and table column so concurrent updates can safely fire at it
        await self._seed_session_metadata(
            acid_engine,
            session_id,
            {"temperature": {"target": "sql", "type": "int"}, "username": {"target": "sql", "type": "str"}},
        )
        async with acid_engine.begin() as conn:
            await conn.execute(text("ALTER TABLE chiral_data ADD COLUMN IF NOT EXISTS temperature INTEGER"))

        await acid_client.query(
            {"operation": "create", "payload": {"session_id": session_id, "username": "race_user", "temperature": 0}}
        )

        async def concurrent_update(val: int) -> None:
            await acid_client.query(
                {
                    "operation": "update",
                    "updates": {"temperature": val},
                    "filters": [{"field": "username", "op": "eq", "value": "race_user"}],
                }
            )

        await asyncio.gather(*(concurrent_update(i) for i in range(1, 6)))

        async with acid_engine.connect() as conn:
            final_temp = await conn.scalar(text("SELECT temperature FROM chiral_data WHERE username = 'race_user'"))
            assert final_temp in [1, 2, 3, 4, 5]

    async def test_consistency_referential_integrity_parent(
        self, acid_client: ChiralClient, acid_engine: AsyncEngine
    ) -> None:
        session_id = "acid_consistency_fk_parent"
        await acid_client.query(
            {
                "operation": "create",
                "table": "chiral_data",
                "payload": {"session_id": session_id, "username": "fk_parent", "comments": [{"text": "seed"}]},
            }
        )

        await acid_client.flush(session_id)

        async with acid_engine.connect() as conn:
            with pytest.raises(IntegrityError):
                await conn.execute(
                    text(
                        "INSERT INTO chiral_data_comments (chiral_data_id, session_id, text) VALUES (-999, 'fake', 'violate')"
                    )
                )
