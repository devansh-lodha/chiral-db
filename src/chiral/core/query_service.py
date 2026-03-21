# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""JSON request to SQL/JSONB query translation service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.query_builder import BuiltQuery, CrudQueryBuilder, InferredJoin
from chiral.db.sessions import session
from chiral.domain.key_policy import build_dynamic_child_key_spec


def _extract_decomposition_plan(request: dict[str, Any]) -> dict[str, Any]:
    direct_plan = request.get("decomposition_plan")
    if isinstance(direct_plan, dict):
        return direct_plan

    metadata = request.get("analysis_metadata")
    if isinstance(metadata, dict):
        nested = metadata.get("decomposition_plan")
        if isinstance(nested, dict):
            return nested

    return {"version": 1, "parent_table": "chiral_data", "entities": []}


def _build_inferred_joins_for_request(request: dict[str, Any], table_name: str) -> list[InferredJoin]:
    plan = _extract_decomposition_plan(request)
    entities = plan.get("entities", [])
    if not isinstance(entities, list) or not entities:
        return []

    referenced_prefixes: set[str] = set()
    select_fields = request.get("select", ["*"])
    filters = request.get("filters", [])

    if isinstance(select_fields, list):
        for field in select_fields:
            if isinstance(field, str) and "." in field:
                prefix = field.split(".", 1)[0]
                if prefix != "overflow_data":
                    referenced_prefixes.add(prefix)

    if isinstance(filters, list):
        for item in filters:
            if not isinstance(item, dict):
                continue
            field = item.get("field")
            if isinstance(field, str) and "." in field:
                prefix = field.split(".", 1)[0]
                if prefix != "overflow_data":
                    referenced_prefixes.add(prefix)

    inferred: list[InferredJoin] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue

        source_field = entity.get("source_field")
        child_table = entity.get("child_table")
        if not isinstance(source_field, str) or not isinstance(child_table, str):
            continue
        if source_field not in referenced_prefixes:
            continue

        key_spec = build_dynamic_child_key_spec(parent_table=table_name, source_field=source_field)
        parent_fk_column = key_spec.foreign_keys[0]["local_column"]
        inferred.append(
            InferredJoin(
                source_field=source_field,
                child_table=child_table,
                parent_fk_column=parent_fk_column,
            )
        )

    return inferred


def translate_json_request(request: dict[str, Any]) -> BuiltQuery:
    """Translate a user JSON CRUD request into a parameterized SQL query.

    Supported operation values: read, create, update, delete.
    """
    operation = str(request.get("operation", "")).lower()
    table_name = str(request.get("table", "chiral_data"))
    inferred_joins = _build_inferred_joins_for_request(request, table_name)
    builder = CrudQueryBuilder(table_name=table_name, inferred_joins=inferred_joins)

    if operation == "read":
        return builder.build_select(
            select_fields=request.get("select", ["*"]),
            filters=request.get("filters", []),
            limit=request.get("limit"),
            offset=request.get("offset"),
        )

    if operation == "create":
        payload = request.get("payload", {})
        if not isinstance(payload, dict):
            msg = "create operation requires object payload"
            raise ValueError(msg)
        return builder.build_insert(payload)

    if operation == "update":
        updates = request.get("updates", {})
        if not isinstance(updates, dict):
            msg = "update operation requires object updates"
            raise ValueError(msg)
        return builder.build_update(updates=updates, filters=request.get("filters", []))

    if operation == "delete":
        return builder.build_delete(filters=request.get("filters", []))

    msg = f"Unsupported operation: {operation}"
    raise ValueError(msg)


@session
async def execute_json_request(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Translate and execute a JSON CRUD request against SQL storage."""
    return await _execute_json_request_impl(request=request, sql_session=sql_session)


async def _execute_json_request_impl(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Execute translated requests (testable without session decorator)."""
    operation = str(request.get("operation", "")).lower()
    built = translate_json_request(request)

    result = await sql_session.execute(text(built.sql), built.params)

    if operation == "read":
        rows = [dict(row) for row in result.mappings().all()]
        return {
            "sql": built.sql,
            "params": built.params,
            "rows": rows,
            "row_count": len(rows),
        }

    affected_rows = int(getattr(result, "rowcount", 0) or 0)
    return {
        "sql": built.sql,
        "params": built.params,
        "affected_rows": affected_rows,
    }
