# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 6 CRUD query generation tests."""

import json
from typing import Any, cast

import pytest
from sqlalchemy.exc import IntegrityError

from src.chiral.core import query_service
from src.chiral.core.query_service import CreateExecutionValidationError, translate_json_request
from src.chiral.db.metadata_store import MetadataSnapshot
from src.chiral.db.query_builder import BuiltQuery, CrudQueryBuilder


def test_select_builder_with_sql_and_jsonb_filters() -> None:
    """Builder should produce SQL with mixed SQL + JSONB predicates."""
    builder = CrudQueryBuilder()
    built = builder.build_select(
        select_fields=["username", "overflow_data.city"],
        filters=[
            {"field": "session_id", "op": "eq", "value": "s1"},
            {"field": "overflow_data.city", "op": "eq", "value": "Paris"},
        ],
        limit=10,
    )

    assert 'FROM "chiral_data"' in built.sql
    assert '"session_id" = :p_0' in built.sql
    assert "overflow_data->>'city' = :p_1" in built.sql
    assert "LIMIT :limit" in built.sql
    assert built.params["p_0"] == "s1"
    assert built.params["p_1"] == "Paris"
    assert built.params["limit"] == 10


def test_update_builder_generates_set_and_where() -> None:
    """Update query should include SET bind params and where clauses."""
    builder = CrudQueryBuilder()
    built = builder.build_update(
        updates={"username": "alice"},
        filters=[{"field": "session_id", "op": "eq", "value": "s-01"}],
    )

    assert built.sql.startswith('UPDATE "chiral_data" SET')
    assert '"username" = :set_username' in built.sql
    assert '"session_id" = :p_0' in built.sql
    assert built.params["set_username"] == "alice"
    assert built.params["p_0"] == "s-01"


def test_translate_json_request_read() -> None:
    """Translation service should map read request to select query."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "overflow_data.device"],
            "filters": [{"field": "session_id", "op": "eq", "value": "abc"}],
            "limit": 5,
        }
    )

    assert built.sql.startswith("SELECT")
    assert built.params["p_0"] == "abc"
    assert built.params["limit"] == 5


def test_translate_json_request_create() -> None:
    """Translation service should map create request to insert query."""
    built = translate_json_request(
        {
            "operation": "create",
            "payload": {
                "session_id": "abc",
                "username": "bob",
                "overflow_data": "{}",
            },
        }
    )
    assert built.sql.startswith('INSERT INTO "chiral_data"')
    assert built.params["session_id"] == "abc"
    assert built.params["username"] == "bob"


def test_translate_json_request_delete() -> None:
    """Translation service should map delete request to delete query."""
    built = translate_json_request(
        {
            "operation": "delete",
            "filters": [{"field": "session_id", "op": "eq", "value": "abc"}],
        }
    )
    assert built.sql.startswith('DELETE FROM "chiral_data"')
    assert built.params["p_0"] == "abc"


def test_jsonb_numeric_range_filter_is_type_safe() -> None:
    """JSONB range filters should cast only numeric values and guard non-numeric text."""
    builder = CrudQueryBuilder()
    built = builder.build_select(
        select_fields=["*"],
        filters=[{"field": "overflow_data.value", "op": "gt", "value": 25}],
    )

    assert "overflow_data->>'value'" in built.sql
    assert " ~ " in built.sql
    assert "^-?" in built.sql
    assert "::double precision > :p_0" in built.sql
    assert built.params["p_0"] == 25.0


def test_jsonb_numeric_range_filter_rejects_non_numeric_value() -> None:
    """JSONB range filters should reject non-numeric filter values."""
    builder = CrudQueryBuilder()
    with pytest.raises(ValueError, match="requires numeric filter value"):
        builder.build_select(
            select_fields=["*"],
            filters=[{"field": "overflow_data.value", "op": "gt", "value": "25"}],
        )


def test_translate_json_request_infers_join_for_child_fields() -> None:
    """Query translation should infer LEFT JOIN from decomposition plan when child fields are referenced."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "comments.text"],
            "filters": [{"field": "comments.time", "op": "gt", "value": 120}],
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "child_columns": ["text", "time"],
                        "child_column_types": {"text": "str", "time": "int"},
                    }
                ],
            },
        }
    )

    assert 'FROM "chiral_data" LEFT JOIN "chiral_data_comments" AS "j_comments"' in built.sql
    assert '"j_comments"."chiral_data_id" = "chiral_data"."id"' in built.sql
    assert '"j_comments"."time" > :p_0' in built.sql
    assert built.params["p_0"] == 120


def test_translate_json_request_selects_bare_child_entity_as_joined_json() -> None:
    """Selecting a bare child entity name should project joined child row JSON."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "comments"],
            "filters": [
                {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
                {"field": "comments.score", "op": "gte", "value": "0.5"},
            ],
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "child_columns": ["score"],
                        "child_column_types": {"score": "float"},
                    }
                ],
            },
        }
    )

    assert 'row_to_json("j_comments") AS "comments"' in built.sql
    assert '"chiral_data"."comments"' not in built.sql
    assert built.params["p_1"] == 0.5


def test_translate_json_request_coerces_joined_child_filter_by_inferred_type() -> None:
    """Joined child filters should coerce bind values based on child_column_types metadata."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "comments.is_valid"],
            "filters": [
                {"field": "comments.time", "op": "gte", "value": "120"},
                {"field": "comments.is_valid", "op": "eq", "value": "true"},
            ],
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "child_columns": ["text", "time", "is_valid"],
                        "child_column_types": {"text": "str", "time": "int", "is_valid": "bool"},
                    }
                ],
            },
        }
    )

    assert '"j_comments"."time" >= :p_0' in built.sql
    assert '"j_comments"."is_valid" = :p_1' in built.sql
    assert built.params["p_0"] == 120
    assert built.params["p_1"] is True


def test_translate_json_request_rejects_invalid_joined_child_typed_filter_value() -> None:
    """Invalid typed filter values for joined child SQL columns should fail fast."""
    with pytest.raises(ValueError, match="Invalid filter value for inferred child type"):
        translate_json_request(
            {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", "comments.time"],
                "filters": [{"field": "comments.time", "op": "gt", "value": "not-int"}],
                "decomposition_plan": {
                    "version": 1,
                    "parent_table": "chiral_data",
                    "entities": [
                        {
                            "source_field": "comments",
                            "child_table": "chiral_data_comments",
                            "child_columns": ["time"],
                            "child_column_types": {"time": "int"},
                        }
                    ],
                },
            }
        )


def test_translate_json_request_joined_jsonb_range_filter_type_safe() -> None:
    """Joined child JSONB range filters should be guarded and cast safely."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["comments.overflow_data.sentiment"],
            "filters": [{"field": "comments.overflow_data.score", "op": "gte", "value": 0.5}],
            "decomposition_plan": {
                "version": 1,
                "parent_table": "chiral_data",
                "entities": [
                    {
                        "source_field": "comments",
                        "child_table": "chiral_data_comments",
                        "child_columns": ["text", "time"],
                    }
                ],
            },
        }
    )

    assert '"j_comments"."overflow_data"->>\'score\'' in built.sql
    assert "::double precision >= :p_0" in built.sql
    assert built.params["p_0"] == 0.5


def test_translate_json_request_uses_analysis_metadata_decomposition_plan() -> None:
    """Join inference should work when decomposition plan is nested under analysis_metadata."""
    built = translate_json_request(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "comments.text"],
            "filters": [{"field": "session_id", "op": "eq", "value": "s1"}],
            "analysis_metadata": {
                "decomposition_plan": {
                    "version": 1,
                    "parent_table": "chiral_data",
                    "entities": [
                        {
                            "source_field": "comments",
                            "child_table": "chiral_data_comments",
                            "child_columns": ["text", "time"],
                        }
                    ],
                }
            },
        }
    )

    assert 'LEFT JOIN "chiral_data_comments" AS "j_comments"' in built.sql


@pytest.mark.asyncio
async def test_execute_json_request_read_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Execution service should return rows for read operations."""

    class DummyResult:
        def mappings(self) -> "DummyResult":
            return self

        def all(self) -> list[dict[str, Any]]:
            return [{"username": "alice"}]

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> DummyResult:
            return DummyResult()

    monkeypatch.setattr(
        query_service,
        "translate_json_request",
        lambda _request: BuiltQuery(sql='SELECT "username" FROM "chiral_data"', params={}),
    )

    response = await query_service._execute_json_request_impl(
        {"operation": "read"},
        sql_session=cast("Any", DummySession()),
    )

    assert response["row_count"] == 1
    assert response["rows"][0]["username"] == "alice"


@pytest.mark.asyncio
async def test_hydrate_request_with_decomposition_plan_from_metadata() -> None:
    """Missing decomposition_plan should be auto-loaded from session metadata using session_id."""

    class DummyResult:
        def __init__(self, row: tuple[Any, ...] | None) -> None:
            self._row = row

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._row

    class DummySession:
        async def execute(self, _statement: str, params: dict[str, Any]) -> DummyResult:
            assert params["sid"] == "s1"
            schema_json = {
                "__analysis_metadata__": {
                    "decomposition_plan": {
                        "version": 1,
                        "parent_table": "chiral_data",
                        "entities": [
                            {
                                "source_field": "comments",
                                "child_table": "chiral_data_comments",
                                "child_columns": ["text", "time"],
                            }
                        ],
                    }
                }
            }
            return DummyResult((json.dumps(schema_json),))

    request = {
        "operation": "read",
        "table": "chiral_data",
        "select": ["username", "comments.text"],
        "filters": [{"field": "session_id", "op": "eq", "value": "s1"}],
    }

    hydrated = await query_service._hydrate_request_with_decomposition_plan(
        request,
        cast("Any", DummySession()),
    )

    plan = hydrated.get("decomposition_plan")
    assert isinstance(plan, dict)
    assert len(plan.get("entities", [])) == 1
    assert plan["entities"][0]["source_field"] == "comments"


@pytest.mark.asyncio
async def test_execute_json_request_impl_auto_hydrates_metadata_when_plan_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execute path should auto-hydrate decomposition plan before translation when request omits it."""

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            class DummyResult:
                def mappings(self) -> "DummyResult":
                    return self

                def all(self) -> list[dict[str, Any]]:
                    return [{"username": "alice"}]

            return DummyResult()

    async def fake_hydrate(request: dict[str, Any], _sql_session: Any) -> dict[str, Any]:
        hydrated = dict(request)
        hydrated["decomposition_plan"] = {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [
                {
                    "source_field": "comments",
                    "child_table": "chiral_data_comments",
                    "child_columns": ["text"],
                }
            ],
        }
        return hydrated

    captured: dict[str, Any] = {}

    def fake_translate(request: dict[str, Any]) -> BuiltQuery:
        captured.update(request)
        return BuiltQuery(sql='SELECT "username" FROM "chiral_data"', params={})

    monkeypatch.setattr(query_service, "_hydrate_request_with_decomposition_plan", fake_hydrate)
    monkeypatch.setattr(query_service, "translate_json_request", fake_translate)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "read",
            "table": "chiral_data",
            "select": ["username", "comments.text"],
            "filters": [{"field": "session_id", "op": "eq", "value": "s1"}],
        },
        sql_session=cast("Any", DummySession()),
    )

    assert "decomposition_plan" in captured
    assert response["row_count"] == 1


@pytest.mark.asyncio
async def test_execute_json_request_create_returns_phase1_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Create execution should return Phase 1 contract fields while preserving affected_rows."""

    class DummyResult:
        rowcount = 1

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> DummyResult:
            return DummyResult()

    async def no_op_init(_sql_session: Any, _session_id: str) -> None:
        return None

    async def no_op_lock(_sql_session: Any, _session_id: str) -> None:
        return None

    async def empty_plan(_sql_session: Any, _session_id: str) -> dict[str, Any]:
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    async def passthrough_resolve(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        table_name: str,
        current_plan: dict[str, Any],
    ) -> dict[str, Any]:
        assert session_id == "s1"
        assert table_name == "chiral_data"
        assert payload["username"] == "alice"
        return current_plan

    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", no_op_init)
    monkeypatch.setattr(query_service, "_lock_session_metadata_row", no_op_lock)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", empty_plan)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", passthrough_resolve)
    monkeypatch.setattr(
        query_service,
        "translate_json_request",
        lambda _request: BuiltQuery(
            sql='INSERT INTO "chiral_data" ("session_id", "username") VALUES (:session_id, :username)',
            params={"session_id": "s1", "username": "alice"},
        ),
    )

    response = await query_service._execute_json_request_impl(
        {"operation": "create", "session_id": "s1", "payload": {"username": "alice"}},
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "migrated_sync"
    assert response["affected_rows"] == 1
    assert response["parent_id"] is None
    assert response["child_insert_counts"] == {}


@pytest.mark.asyncio
async def test_execute_json_request_create_raises_typed_validation_error_on_invalid_payload() -> None:
    """Invalid create payloads should raise typed create validation error for API mapping."""

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            raise AssertionError("execute should not be called for invalid create payload")

    with pytest.raises(CreateExecutionValidationError, match="create operation requires object payload"):
        await query_service._execute_json_request_impl(
            {"operation": "create", "payload": ["not", "an", "object"]},
            sql_session=cast("Any", DummySession()),
        )


@pytest.mark.asyncio
async def test_execute_json_request_create_queues_nested_payload_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nested create payloads should be queued to ingest path until phase-3 sync decomposition is implemented."""

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            raise AssertionError("direct SQL execute should not run for queued_async path")

    async def no_op_init(_sql_session: Any, _session_id: str) -> None:
        return None

    async def no_op_lock(_sql_session: Any, _session_id: str) -> None:
        return None

    async def empty_plan(_sql_session: Any, _session_id: str) -> dict[str, Any]:
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    async def passthrough_resolve(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        table_name: str,
        current_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return current_plan

    async def fake_ingest(*, data: dict[str, Any], session_id: str) -> dict[str, Any]:
        data = data.get("payload", {})
        assert session_id == "s_nested"
        assert "comments" in data
        return {
            "status": "success",
            "session_id": session_id,
            "count": 1,
            "worker_triggered": False,
            "incremental": False,
        }

    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", no_op_init)
    monkeypatch.setattr(query_service, "_lock_session_metadata_row", no_op_lock)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", empty_plan)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", passthrough_resolve)
    monkeypatch.setattr(query_service, "ingest_data", fake_ingest)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "create",
            "session_id": "s_nested",
            "payload": {
                "session_id": "s_nested",
                "username": "alice",
                "comments": [{"text": "hello", "score": "0.7"}],
            },
        },
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "queued_async"
    assert response["affected_rows"] == 0
    assert response["staging_count"] == 1
    assert response["worker_triggered"] is False


@pytest.mark.asyncio
async def test_execute_json_request_create_nested_payload_migrates_sync_with_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nested create payload should migrate synchronously when decomposition entities are available."""

    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            class DummyResult:
                rowcount = 1

            return DummyResult()

    async def no_op_init(_sql_session: Any, _session_id: str) -> None:
        return None

    async def no_op_lock(_sql_session: Any, _session_id: str) -> None:
        return None

    async def plan_with_entities(_sql_session: Any, _session_id: str) -> dict[str, Any]:
        return {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [
                {
                    "source_field": "comments",
                    "child_table": "chiral_data_comments",
                    "child_columns": ["text", "score"],
                    "child_column_types": {"text": "str", "score": "float"},
                }
            ],
        }

    async def passthrough_resolve(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        table_name: str,
        current_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return current_plan

    async def fake_migrate_single_create_payload(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["session_id"] == "s_sync"
        assert kwargs["payload"]["comments"][0]["text"] == "hello"
        return {
            "parent_id": 42,
            "child_insert_counts": {"comments": 2},
            "safety_event_count": 0,
        }

    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", no_op_init)
    monkeypatch.setattr(query_service, "_lock_session_metadata_row", no_op_lock)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", plan_with_entities)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", passthrough_resolve)
    monkeypatch.setattr(query_service, "migrate_single_create_payload", fake_migrate_single_create_payload)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "create",
            "session_id": "s_sync",
            "payload": {
                "session_id": "s_sync",
                "username": "alice",
                "comments": [{"text": "hello", "score": "0.7"}, {"text": "bye", "score": "0.2"}],
            },
        },
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "migrated_sync"
    assert response["affected_rows"] == 1
    assert response["parent_id"] == 42
    assert response["child_insert_counts"] == {"comments": 2}


def test_merge_decomposition_plans_adds_new_source_fields() -> None:
    existing = {
        "version": 1,
        "parent_table": "chiral_data",
        "entities": [{"source_field": "comments", "child_table": "chiral_data_comments"}],
    }
    inferred = {
        "version": 1,
        "parent_table": "chiral_data",
        "entities": [{"source_field": "events", "child_table": "chiral_data_events"}],
    }

    merged = query_service._merge_decomposition_plans(existing, inferred, parent_table="chiral_data")
    source_fields = {entity["source_field"] for entity in merged["entities"]}
    assert source_fields == {"comments", "events"}


@pytest.mark.asyncio
async def test_resolve_create_metadata_and_plan_applies_drift_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummySession:
        def __init__(self) -> None:
            self.executed: list[dict[str, Any]] = []

        async def execute(self, _statement: Any, params: dict[str, Any]) -> Any:
            self.executed.append(params)

            class DummyResult:
                def fetchall(self) -> list[tuple[Any, ...]]:
                    return []

            return DummyResult()

    async def fake_snapshot(_sql_session: Any, _sid: str) -> MetadataSnapshot:
        return MetadataSnapshot(
            schema={
                "price": {"target": "sql", "type": "int"},
                "__analysis_metadata__": {
                    "decomposition_plan": {"version": 1, "parent_table": "chiral_data", "entities": []}
                },
            },
            schema_version=2,
            drift_events=[],
            safety_events=[],
            migration_metrics=[],
        )

    async def fake_infer(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        parent_table: str,
    ) -> dict[str, Any]:
        return {
            "version": 1,
            "parent_table": parent_table,
            "entities": [
                {
                    "source_field": "comments",
                    "child_table": "chiral_data_comments",
                    "child_columns": ["text"],
                }
            ],
        }

    monkeypatch.setattr(query_service, "load_metadata_snapshot", fake_snapshot)
    monkeypatch.setattr(query_service, "_infer_decomposition_plan_for_create", fake_infer)

    session = DummySession()
    resolved_plan = await query_service._resolve_create_metadata_and_plan(
        cast("Any", session),
        session_id="s1",
        payload={"price": "bad-int", "comments": [{"text": "hello"}]},
        table_name="chiral_data",
        current_plan={"version": 1, "parent_table": "chiral_data", "entities": []},
    )

    assert any(entity.get("source_field") == "comments" for entity in resolved_plan["entities"])
    assert len(session.executed) == 1
    assert session.executed[0]["schema_version"] >= 3


@pytest.mark.asyncio
async def test_execute_json_request_create_falls_back_to_async_on_analysis_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            class DummyResult:
                rowcount = 1

            return DummyResult()

    async def no_op_init(_sql_session: Any, _session_id: str) -> None:
        return None

    async def no_op_lock(_sql_session: Any, _session_id: str) -> None:
        return None

    async def empty_plan(_sql_session: Any, _session_id: str) -> dict[str, Any]:
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    async def timeout_resolve(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        table_name: str,
        current_plan: dict[str, Any],
    ) -> dict[str, Any]:
        raise TimeoutError("analysis timed out")

    async def fake_ingest(*, data: dict[str, Any], session_id: str) -> dict[str, Any]:
        return {
            "status": "success",
            "session_id": session_id,
            "count": 2,
            "worker_triggered": False,
            "incremental": False,
        }

    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", no_op_init)
    monkeypatch.setattr(query_service, "_lock_session_metadata_row", no_op_lock)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", empty_plan)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", timeout_resolve)
    monkeypatch.setattr(query_service, "ingest_data", fake_ingest)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "create",
            "session_id": "s_timeout",
            "payload": {"session_id": "s_timeout", "username": "alice"},
        },
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "queued_async"
    assert response["queue_reason"] == "analysis_timeout"
    assert response["fallback_trigger"] == "metadata_resolution"
    assert response["staging_count"] == 2


@pytest.mark.asyncio
async def test_execute_json_request_create_falls_back_to_async_on_sync_insert_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            class DummyResult:
                rowcount = 1

            return DummyResult()

    async def no_op_init(_sql_session: Any, _session_id: str) -> None:
        return None

    async def no_op_lock(_sql_session: Any, _session_id: str) -> None:
        return None

    async def plan_with_entities(_sql_session: Any, _session_id: str) -> dict[str, Any]:
        return {
            "version": 1,
            "parent_table": "chiral_data",
            "entities": [{"source_field": "comments", "child_table": "chiral_data_comments"}],
        }

    async def passthrough_resolve(
        _sql_session: Any,
        *,
        session_id: str,
        payload: dict[str, Any],
        table_name: str,
        current_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return current_plan

    async def conflicting_sync_migrate(**kwargs: Any) -> dict[str, Any]:
        msg = "duplicate key value"
        raise IntegrityError("INSERT INTO x", {}, Exception(msg))

    async def fake_ingest(*, data: dict[str, Any], session_id: str) -> dict[str, Any]:
        data = data.get("payload", {})
        return {
            "status": "success",
            "session_id": session_id,
            "count": 3,
            "worker_triggered": True,
            "incremental": True,
        }

    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", no_op_init)
    monkeypatch.setattr(query_service, "_lock_session_metadata_row", no_op_lock)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", plan_with_entities)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", passthrough_resolve)
    monkeypatch.setattr(query_service, "migrate_single_create_payload", conflicting_sync_migrate)
    monkeypatch.setattr(query_service, "ingest_data", fake_ingest)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "create",
            "session_id": "s_conflict",
            "payload": {
                "session_id": "s_conflict",
                "username": "alice",
                "comments": [{"text": "hello"}],
            },
        },
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "queued_async"
    assert response["queue_reason"] == "retriable_insert_conflict"
    assert response["fallback_trigger"] == "sync_migration"
    assert response["worker_triggered"] is True


@pytest.mark.asyncio
async def test_execute_json_request_create_uses_legacy_path_when_orchestration_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySession:
        async def execute(self, _statement: str, _params: dict[str, Any]) -> Any:
            class DummyResult:
                rowcount = 1

            return DummyResult()

    def should_not_call(*_args: Any, **_kwargs: Any) -> Any:
        error = "orchestration helper should not be called when feature flag is disabled"
        raise AssertionError(error)

    monkeypatch.setenv("CREATE_ORCHESTRATION_ENABLED", "false")
    monkeypatch.setattr(query_service, "_initialize_session_metadata_for_create", should_not_call)
    monkeypatch.setattr(query_service, "_resolve_create_metadata_and_plan", should_not_call)
    monkeypatch.setattr(query_service, "_load_decomposition_plan_from_metadata", should_not_call)

    response = await query_service._execute_json_request_impl(
        {
            "operation": "create",
            "session_id": "s_legacy",
            "payload": {
                "session_id": "s_legacy",
                "username": "alice",
                "overflow_data": "{}",
            },
        },
        sql_session=cast("Any", DummySession()),
    )

    assert response["mode"] == "migrated_sync"
    assert response["affected_rows"] == 1
    assert response["parent_id"] is None
    assert response["child_insert_counts"] == {}
