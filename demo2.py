# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Compact end-to-end showcase for normalized tables and join-aware querying."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib import error, request as urlrequest

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.chiral.config import get_settings

SESSION_ID = "session_assignment_1"
API_BASE_URL = os.getenv("CHIRAL_API_URL", "http://127.0.0.1:8000")


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


async def _fetchone_mapping(conn, query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    result = await conn.execute(text(query), params or {})
    row = result.mappings().first()
    return dict(row) if row else None


async def _fetch_scalar(conn, query: str, params: dict[str, Any] | None = None) -> Any:
    result = await conn.execute(text(query), params or {})
    return result.scalar()


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=30) as response:  # nosec B310
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


async def _execute_request_via_api(payload: dict[str, Any]) -> dict[str, Any]:
    endpoint = f"{API_BASE_URL.rstrip('/')}/query/execute"
    try:
        return await asyncio.to_thread(_post_json, endpoint, payload)
    except error.URLError as exc:
        return {
            "sql": "<api-unreachable>",
            "params": {},
            "rows": [],
            "row_count": 0,
            "error": str(exc),
        }


async def _load_schema_json(conn) -> dict[str, Any]:
    metadata = await _fetchone_mapping(
        conn,
        "SELECT schema_json FROM session_metadata WHERE session_id = :sid",
        {"sid": SESSION_ID},
    )
    if not metadata:
        return {}

    raw = metadata.get("schema_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    return raw if isinstance(raw, dict) else {}


def _extract_decomposition_plan(schema_json: dict[str, Any]) -> dict[str, Any]:
    meta = schema_json.get("__analysis_metadata__", {})
    if isinstance(meta, dict):
        plan = meta.get("decomposition_plan", {})
        if isinstance(plan, dict):
            return plan
    return {"version": 1, "parent_table": "chiral_data", "entities": []}


async def show_schema_summary(conn) -> dict[str, Any]:
    _print_header("DEMO2: SCHEMA SUMMARY")

    schema_json = await _load_schema_json(conn)
    if not schema_json:
        print(f"No metadata found for session_id={SESSION_ID}")
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    filtered_fields = []
    for name, meta in schema_json.items():
        if name == "__analysis_metadata__":
            continue
        if isinstance(meta, dict):
            filtered_fields.append(
                {
                    "attribute": name,
                    "type": meta.get("type", "unknown"),
                    "stored_in": meta.get("target", "unknown"),
                }
            )

    filtered_fields.sort(key=lambda item: str(item["attribute"]))

    print("Attributes (name, type, storage):")
    for item in filtered_fields:
        print(f"- {item['attribute']}: type={item['type']}, stored_in={item['stored_in']}")

    table_count = await _fetch_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        """,
    )
    print(f"\nTotal tables in public schema: {table_count}")

    decomposition_plan = _extract_decomposition_plan(schema_json)
    entities = decomposition_plan.get("entities", []) if isinstance(decomposition_plan, dict) else []
    child_tables = []
    if isinstance(entities, list):
        for entity in entities:
            if isinstance(entity, dict) and isinstance(entity.get("child_table"), str):
                child_tables.append(entity["child_table"])

    if child_tables:
        print(f"Detected child tables from decomposition: {', '.join(sorted(set(child_tables)))}")
    else:
        print("Detected child tables from decomposition: none")

    return decomposition_plan


async def show_example_queries(decomposition_plan: dict[str, Any]) -> None:
    _print_header("DEMO2: 5 EXAMPLE QUERIES (INCLUDES JOINS)")

    entities = decomposition_plan.get("entities", []) if isinstance(decomposition_plan, dict) else []
    first_entity = entities[0] if isinstance(entities, list) and entities and isinstance(entities[0], dict) else {}
    source_field = first_entity.get("source_field") if isinstance(first_entity.get("source_field"), str) else None

    child_columns = first_entity.get("child_columns") if isinstance(first_entity.get("child_columns"), list) else []
    first_child_col = None
    second_child_col = None
    for column in child_columns or []:
        if isinstance(column, str) and column:
            if first_child_col is None:
                first_child_col = column
            elif second_child_col is None:
                second_child_col = column
                break

    requests: list[dict[str, Any]] = [
        {
            "label": "Q1 Parent SQL filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", "sys_ingested_at"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 5,
            },
        },
        {
            "label": "Q2 Parent JSONB filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", "overflow_data.metadata"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 5,
            },
        },
    ]

    if source_field:
        requests.extend(
            [
                {
                    "label": "Q3 Inferred JOIN child SQL projection",
                    "request": {
                        "operation": "read",
                        "table": "chiral_data",
                        "select": ["username", f"{source_field}.{first_child_col or 'id'}"],
                        "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                        "decomposition_plan": decomposition_plan,
                        "limit": 5,
                    },
                },
                {
                    "label": "Q4 Inferred JOIN child SQL filter",
                    "request": {
                        "operation": "read",
                        "table": "chiral_data",
                        "select": ["username", f"{source_field}.{first_child_col or 'id'}"],
                        "filters": [
                            {"field": "session_id", "op": "eq", "value": SESSION_ID},
                            {
                                "field": f"{source_field}.{second_child_col or first_child_col or 'id'}",
                                "op": "ne",
                                "value": "",
                            },
                        ],
                        "decomposition_plan": decomposition_plan,
                        "limit": 5,
                    },
                },
                {
                    "label": "Q5 Inferred JOIN child JSONB projection",
                    "request": {
                        "operation": "read",
                        "table": "chiral_data",
                        "select": ["username", f"{source_field}.overflow_data.meta"],
                        "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                        "decomposition_plan": decomposition_plan,
                        "limit": 5,
                    },
                },
            ]
        )
    else:
        requests.extend(
            [
                {
                    "label": "Q3 Fallback SQL query",
                    "request": {
                        "operation": "read",
                        "table": "chiral_data",
                        "select": ["username", "t_stamp"],
                        "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                        "limit": 5,
                    },
                },
                {
                    "label": "Q4 Fallback JSONB numeric-safe range",
                    "request": {
                        "operation": "read",
                        "table": "chiral_data",
                        "select": ["username", "overflow_data.temperature"],
                        "filters": [
                            {"field": "session_id", "op": "eq", "value": SESSION_ID},
                            {"field": "overflow_data.temperature", "op": "gt", "value": 20},
                        ],
                        "limit": 5,
                    },
                },
                {
                    "label": "Q5 Metadata check",
                    "request": {
                        "operation": "read",
                        "table": "session_metadata",
                        "select": ["session_id", "status", "schema_version"],
                        "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                        "limit": 1,
                    },
                },
            ]
        )

    for item in requests[:5]:
        label = item["label"]
        payload = item["request"]
        result = await _execute_request_via_api(payload)
        rows = result.get("rows", []) if isinstance(result.get("rows"), list) else []
        sample_rows = rows[:3]

        print(f"\n{label}")
        print(f"- SQL: {result.get('sql', '<n/a>')}")
        print(f"- Params: {_pretty_json(result.get('params', {}))}")
        if "error" in result:
            print(f"- API execute error: {result['error']}")
        print(f"- Sample rows ({len(sample_rows)} shown): {_pretty_json(sample_rows)}")


async def wait_for_migration_completion(conn, timeout_seconds: int = 90, poll_interval: float = 2.0) -> None:
    print("\nWaiting for migration completion...")
    elapsed = 0.0
    while elapsed <= timeout_seconds:
        status_row = await _fetchone_mapping(
            conn,
            "SELECT status FROM session_metadata WHERE session_id = :sid",
            {"sid": SESSION_ID},
        )
        staging_rows = await _fetch_scalar(
            conn,
            "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid",
            {"sid": SESSION_ID},
        )
        chiral_rows = await _fetch_scalar(
            conn,
            "SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid",
            {"sid": SESSION_ID},
        )

        status = status_row.get("status") if status_row else None
        print(f"- t={int(elapsed):02d}s status={status} staging={staging_rows} chiral={chiral_rows}")

        if staging_rows == 0 and chiral_rows > 0 and status in {"migrated", "migrating_incremental"}:
            print("Migration is ready.")
            return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    print("Warning: migration did not fully settle before timeout; continuing.")


async def main() -> None:
    _print_header("DEMO2: NORMALIZATION + JOIN SHOWCASE")

    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)

    async with engine.connect() as conn:
        await wait_for_migration_completion(conn)

        total_rows = await _fetch_scalar(
            conn,
            "SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid",
            {"sid": SESSION_ID},
        )
        staging_rows = await _fetch_scalar(
            conn,
            "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid",
            {"sid": SESSION_ID},
        )

        print(f"Session: {SESSION_ID}")
        print(f"Rows in chiral_data: {total_rows}")
        print(f"Rows pending in staging_data: {staging_rows}")

        decomposition_plan = await show_schema_summary(conn)
        await show_example_queries(decomposition_plan)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
