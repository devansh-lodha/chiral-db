# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""TA-friendly end-to-end showcase for implemented phases.

Runs after ingestion to print formatted metadata and execute 10 example queries.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.chiral.config import get_settings
from src.chiral.core.query_service import translate_json_request

SESSION_ID = "session_assignment_1"


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


async def show_metadata(conn) -> None:
    _print_header("DEMO2: FORMATTED METADATA")

    metadata = await _fetchone_mapping(
        conn,
        """
        SELECT session_id, status, record_count, schema_version,
               schema_json, drift_events, safety_events, migration_metrics, created_at
        FROM session_metadata
        WHERE session_id = :sid
        """,
        {"sid": SESSION_ID},
    )

    if not metadata:
        print(f"No metadata found for session_id={SESSION_ID}")
        return

    schema_json = metadata.get("schema_json")
    if isinstance(schema_json, str):
        try:
            metadata["schema_json"] = json.loads(schema_json)
        except json.JSONDecodeError:
            pass

    print(_pretty_json(metadata))

    schema = metadata.get("schema_json") if isinstance(metadata.get("schema_json"), dict) else {}
    drift_events = metadata.get("drift_events") or []
    safety_events = metadata.get("safety_events") or []
    migration_metrics = metadata.get("migration_metrics") or []

    print("\nSummary:")
    print(f"- Learned fields: {len(schema)}")  # type: ignore
    print(f"- Drift events: {len(drift_events)}")
    print(f"- Safety events: {len(safety_events)}")
    print(f"- Metrics snapshots: {len(migration_metrics)}")


async def _discover_query_hints(conn) -> tuple[str | None, Any, str | None]:
    sample_key = await _fetch_scalar(
        conn,
        """
        SELECT key
        FROM chiral_data, jsonb_object_keys(overflow_data) AS key
        WHERE session_id = :sid AND overflow_data != '{}'::jsonb
        LIMIT 1
        """,
        {"sid": SESSION_ID},
    )

    sample_value = None
    if sample_key:
        sample_value = await _fetch_scalar(
            conn,
            """
            SELECT overflow_data->>:key
            FROM chiral_data
            WHERE session_id = :sid AND overflow_data ? :key
            LIMIT 1
            """,
            {"sid": SESSION_ID, "key": sample_key},
        )

    numeric_key = await _fetch_scalar(
        conn,
        """
        SELECT kv.key
        FROM chiral_data,
             jsonb_each_text(overflow_data) AS kv
        WHERE session_id = :sid
          AND kv.value ~ '^-?\\d+(?:\\.\\d+)?$'
        GROUP BY kv.key
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
        {"sid": SESSION_ID},
    )

    return sample_key, sample_value, numeric_key


async def show_example_queries(conn) -> None:
    _print_header("DEMO2: 10 EXAMPLE QUERIES (TRANSLATED + EXECUTED)")

    sample_key, sample_value, numeric_key = await _discover_query_hints(conn)

    requests: list[dict[str, Any]] = [
        {
            "label": "Q1 Basic rows",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["session_id", "username", "sys_ingested_at"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 5,
            },
        },
        {
            "label": "Q2 Username + timestamp",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", "t_stamp"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 10,
            },
        },
        {
            "label": "Q3 Latest ingested slice",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["sys_ingested_at", "username"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 10,
            },
        },
        {
            "label": "Q4 SQL range filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", "sys_ingested_at"],
                "filters": [
                    {"field": "session_id", "op": "eq", "value": SESSION_ID},
                    {"field": "sys_ingested_at", "op": "gt", "value": 0},
                ],
                "limit": 5,
            },
        },
        {
            "label": "Q5 JSONB key projection",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", f"overflow_data.{sample_key or 'fallback_key'}"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 5,
            },
        },
        {
            "label": "Q6 JSONB eq filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username"],
                "filters": [
                    {"field": "session_id", "op": "eq", "value": SESSION_ID},
                    {
                        "field": f"overflow_data.{sample_key or 'fallback_key'}",
                        "op": "eq",
                        "value": str(sample_value or "unknown"),
                    },
                ],
                "limit": 5,
            },
        },
        {
            "label": "Q7 JSONB numeric-safe gt filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", f"overflow_data.{numeric_key or 'fallback_numeric'}"],
                "filters": [
                    {"field": "session_id", "op": "eq", "value": SESSION_ID},
                    {"field": f"overflow_data.{numeric_key or 'fallback_numeric'}", "op": "gt", "value": 25},
                ],
                "limit": 5,
            },
        },
        {
            "label": "Q8 JSONB numeric-safe gte filter",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["username", f"overflow_data.{numeric_key or 'fallback_numeric'}"],
                "filters": [
                    {"field": "session_id", "op": "eq", "value": SESSION_ID},
                    {"field": f"overflow_data.{numeric_key or 'fallback_numeric'}", "op": "gte", "value": 0},
                ],
                "limit": 5,
            },
        },
        {
            "label": "Q9 Offset + pagination",
            "request": {
                "operation": "read",
                "table": "chiral_data",
                "select": ["session_id", "username"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 5,
                "offset": 5,
            },
        },
        {
            "label": "Q10 Metadata table query",
            "request": {
                "operation": "read",
                "table": "session_metadata",
                "select": ["session_id", "status", "schema_version", "record_count"],
                "filters": [{"field": "session_id", "op": "eq", "value": SESSION_ID}],
                "limit": 1,
            },
        },
    ]

    for item in requests:
        label = item["label"]
        request_payload = item["request"]
        built = translate_json_request(request_payload)

        result = await conn.execute(text(built.sql), built.params)
        rows = result.mappings().fetchmany(3)

        print(f"\n{label}")
        print(f"- Request: {_pretty_json(request_payload)}")
        print(f"- SQL: {built.sql}")
        print(f"- Params: {_pretty_json(built.params)}")
        print(f"- Sample rows ({len(rows)} shown): {_pretty_json([dict(r) for r in rows])}")


async def wait_for_migration_completion(conn, timeout_seconds: int = 90, poll_interval: float = 2.0) -> None:
    """Poll metadata/staging until migration completes or timeout is reached."""
    print("\nWaiting for migration completion...")
    elapsed = 0.0
    while elapsed <= timeout_seconds:
        status_row = await _fetchone_mapping(
            conn,
            "SELECT status, record_count FROM session_metadata WHERE session_id = :sid",
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
        print(
            f"- t={int(elapsed):02d}s status={status} staging={staging_rows} chiral={chiral_rows}",
        )

        if staging_rows == 0 and chiral_rows > 0 and status in {"migrated", "migrating_incremental"}:
            print("Migration is ready.")
            return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    print("Warning: migration did not fully settle before timeout; continuing with current state.")


async def main() -> None:
    _print_header("DEMO2: END-TO-END SHOWCASE")
    print("Features covered: phases 1-8 (routing, normalization, keys/FKs, JSONB strategy,")
    print("metadata versioning, CRUD translation, performance, observability).")

    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)

    async with engine.connect() as conn:
        await wait_for_migration_completion(conn)

        total_rows = await _fetch_scalar(
            conn, "SELECT COUNT(*) FROM chiral_data WHERE session_id = :sid", {"sid": SESSION_ID}
        )
        staging_rows = await _fetch_scalar(
            conn, "SELECT COUNT(*) FROM staging_data WHERE session_id = :sid", {"sid": SESSION_ID}
        )
        print(f"Session: {SESSION_ID}")
        print(f"Rows in chiral_data: {total_rows}")
        print(f"Rows pending in staging_data: {staging_rows}")

        await show_metadata(conn)
        await show_example_queries(conn)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
