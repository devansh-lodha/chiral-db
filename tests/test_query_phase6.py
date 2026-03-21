# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Phase 6 CRUD query generation tests."""

from typing import Any, cast

import pytest

from src.chiral.core import query_service
from src.chiral.core.query_service import translate_json_request
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
                    }
                ],
            },
        }
    )

    assert 'FROM "chiral_data" LEFT JOIN "chiral_data_comments" AS "j_comments"' in built.sql
    assert '"j_comments"."chiral_data_id" = "chiral_data"."id"' in built.sql
    assert '"j_comments"."time" > :p_0' in built.sql
    assert built.params["p_0"] == 120


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
