# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Data Migration Logic."""

import json
import logging
import os
import re
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.metadata_store import apply_drift_to_metadata, bounded_append_events, load_metadata_snapshot
from chiral.db.observability import (
    build_guardrail_event,
    build_migration_metrics,
    should_guardrail_route_to_jsonb,
)
from chiral.db.performance import calculate_rows_per_second, chunked
from chiral.db.sessions import session
from chiral.domain.key_policy import KeyPolicy
from chiral.domain.routing import is_sql_target

logger = logging.getLogger(__name__)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def cast_value(value: object, expected_type: str) -> object:
    """Attempt to cast value to expected type."""
    if value is None:
        return None
    if expected_type == "int":
        return int(value)  # type: ignore[arg-type]
    if expected_type == "float":
        return float(value)  # type: ignore[arg-type]
    if expected_type == "bool":
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    return str(value)


def _ensure_system_columns(existing_columns: list[str]) -> tuple[list[str], list[str]]:
    """Ensure system columns exist and return columns to add and valid SQL columns.

    Returns:
        Tuple of (columns_to_add, valid_sql_cols)

    """
    columns_to_add = []
    valid_sql_cols = []

    if "username" not in existing_columns:
        columns_to_add.append('ADD COLUMN IF NOT EXISTS "username" TEXT')
    valid_sql_cols.append("username")

    if "sys_ingested_at" not in existing_columns:
        columns_to_add.append('ADD COLUMN IF NOT EXISTS "sys_ingested_at" FLOAT UNIQUE')
    valid_sql_cols.append("sys_ingested_at")

    if "t_stamp" not in existing_columns:
        columns_to_add.append('ADD COLUMN IF NOT EXISTS "t_stamp" FLOAT')
    valid_sql_cols.append("t_stamp")

    # Ensure overflow_data JSONB column exists
    if "overflow_data" not in existing_columns:
        columns_to_add.append("ADD COLUMN IF NOT EXISTS \"overflow_data\" JSONB DEFAULT '{}'::jsonb")

    return columns_to_add, valid_sql_cols


def _build_schema_columns(
    analysis: dict[str, Any],
    existing_columns: list[str],
    valid_sql_cols: list[str],
    key_policy: KeyPolicy | None = None,
) -> list[str]:
    """Build schema columns from analysis using key policy for UNIQUE constraints.

    Returns:
        List of columns to add

    """
    if key_policy is None:
        key_policy = KeyPolicy()

    columns_to_add = []

    for col, meta in analysis.items():
        if col in ["session_id", "sys_ingested_at", "t_stamp", "username"]:
            continue

        if is_sql_target(meta["target"]):
            if col not in existing_columns:
                sql_type = "TEXT"
                if meta["type"] == "int":
                    sql_type = "INTEGER"
                elif meta["type"] == "float":
                    sql_type = "FLOAT"
                elif meta["type"] == "bool":
                    sql_type = "BOOLEAN"

                unique_confidence = meta.get("unique_confidence", 0.0)
                should_add_constraint = key_policy.should_enforce_unique_constraint(
                    field_unique=meta.get("unique", False),
                    unique_confidence=unique_confidence,
                    threshold=key_policy.unique_confidence_threshold,
                )
                constraint = "UNIQUE" if should_add_constraint else ""
                columns_to_add.append(f'ADD COLUMN IF NOT EXISTS "{col}" {sql_type} {constraint}')
                valid_sql_cols.append(col)
            elif col in existing_columns:
                valid_sql_cols.append(col)

    return columns_to_add


def _process_document(
    doc: dict[str, Any],
    session_id: str,
    analysis: dict[str, Any],
    *,
    max_field_bytes: int,
    max_nesting_depth: int,
    safety_events: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Process a document for migration, splitting data between SQL columns and JSONB overflow.

    Returns:
        Tuple of (sql_row, overflow_data)

    """
    username = doc.get("username", "unknown")
    sys_ingested_at = doc.get("sys_ingested_at")
    t_stamp = doc.get("t_stamp")

    sql_row = {"session_id": session_id, "username": username}
    overflow = {}

    if sys_ingested_at is not None:
        sql_row["sys_ingested_at"] = sys_ingested_at
    if t_stamp is not None:
        sql_row["t_stamp"] = t_stamp

    for key, value in doc.items():
        if key in ["_id", "session_id", "username", "sys_ingested_at", "t_stamp"]:
            continue

        route_to_jsonb, size_bytes, nesting_depth, reason = should_guardrail_route_to_jsonb(
            value,
            max_bytes=max_field_bytes,
            max_depth=max_nesting_depth,
        )
        if route_to_jsonb and reason:
            overflow[key] = value
            if safety_events is not None:
                safety_events.append(
                    build_guardrail_event(
                        column=key,
                        reason=reason,
                        size_bytes=size_bytes,
                        nesting_depth=nesting_depth,
                    )
                )
            continue

        meta = analysis.get(key)
        target_overflow = True

        if meta and is_sql_target(meta["target"]):
            try:
                casted_val = cast_value(value, meta["type"])
                sql_row[key] = casted_val
                target_overflow = False
            except (ValueError, TypeError):
                target_overflow = True

        if target_overflow:
            overflow[key] = value

    return sql_row, overflow


async def _insert_sql_row(
    sql_row: dict[str, Any],
    overflow: dict[str, Any],
    valid_sql_cols: list[str],
    table_name: str,
    sql_session: AsyncSession,
) -> None:
    """Insert SQL row with overflow_data JSONB column."""
    insert_keys = [k for k in sql_row if k in valid_sql_cols or k == "session_id"]

    if len(insert_keys) == 0:
        return

    # Always include overflow_data
    insert_keys.append("overflow_data")
    sql_row["overflow_data"] = json.dumps(overflow) if overflow else "{}"

    col_list = ", ".join([f'"{k}"' for k in insert_keys])
    bind_list = ", ".join([f":{k}" for k in insert_keys])
    insert_stmt = text('INSERT INTO "' + table_name + '" (' + col_list + ") VALUES (" + bind_list + ")")

    try:
        await sql_session.execute(insert_stmt, sql_row)
    except IntegrityError as e:
        await sql_session.rollback()
        err_msg = str(e.orig) if hasattr(e, "orig") else str(e)
        match = re.search(r"Key \((.*?)\)=", err_msg)

        if match:
            col_culprit = match.group(1).replace('"', "")
            logger.info("Unique constraint violation on column '%s', removing constraint", col_culprit)
            await remove_unique_constraint(sql_session, table_name, col_culprit)

            try:
                await sql_session.execute(insert_stmt, sql_row)
                logger.info("Successfully inserted after removing unique constraint on '%s'", col_culprit)
            except SQLAlchemyError:
                logger.exception("Failed to insert even after removing constraint")
        else:
            logger.warning("Integrity error without identifiable column: %s", err_msg)
    except SQLAlchemyError:
        logger.exception("Unexpected error inserting into SQL")


def _build_insert_payload(
    sql_row: dict[str, Any],
    overflow: dict[str, Any],
    valid_sql_cols: list[str],
) -> dict[str, Any] | None:
    """Build normalized insert payload with overflow_data included."""
    insert_keys = [key for key in sql_row if key in valid_sql_cols or key == "session_id"]
    if not insert_keys:
        return None

    payload = {key: sql_row[key] for key in insert_keys}
    payload["overflow_data"] = json.dumps(overflow) if overflow else "{}"
    return payload


async def _bulk_insert_sql_rows(
    rows: list[dict[str, Any]],
    table_name: str,
    sql_session: AsyncSession,
    batch_size: int,
) -> int:
    """Bulk insert rows in batches using executemany-style execution."""
    if not rows:
        return 0

    # Normalize sparse rows to a unified key-set so executemany has all binds.
    insert_keys: list[str] = []
    seen_keys: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen_keys:
                seen_keys.add(key)
                insert_keys.append(key)

    normalized_rows = [{key: row.get(key) for key in insert_keys} for row in rows]

    col_list = ", ".join([f'"{key}"' for key in insert_keys])
    bind_list = ", ".join([f":{key}" for key in insert_keys])
    insert_stmt = text('INSERT INTO "' + table_name + '" (' + col_list + ") VALUES (" + bind_list + ")")

    inserted = 0
    for batch in chunked(normalized_rows, batch_size):
        try:
            await sql_session.execute(insert_stmt, batch)
            inserted += len(batch)
        except IntegrityError:
            await sql_session.rollback()
            for row in batch:
                overflow_data = row.get("overflow_data", "{}")
                sql_row = dict(row)
                sql_row.pop("overflow_data", None)
                overflow = json.loads(overflow_data) if isinstance(overflow_data, str) else {}
                await _insert_sql_row(sql_row, overflow, list(sql_row.keys()), table_name, sql_session)
                inserted += 1

    return inserted


async def remove_unique_constraint(session: AsyncSession, table_name: str, column_name: str) -> None:
    """Remove UNIQUE constraint from a specific column."""
    find_constraint_sql = text("""
        SELECT tc.constraint_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.constraint_column_usage AS ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE constraint_type = 'UNIQUE'
          AND tc.table_name = :table_name
          AND ccu.column_name = :column_name
    """)

    result = await session.execute(find_constraint_sql, {"table_name": table_name, "column_name": column_name})
    rows = result.fetchall()

    for row in rows:
        constraint_name = row[0]
        logger.info("Dropping unique constraint %s on %s.%s", constraint_name, table_name, column_name)
        drop_sql = text('ALTER TABLE "' + table_name + '" DROP CONSTRAINT "' + constraint_name + '"')
        await session.execute(drop_sql)

    await session.commit()


class _IncrementalMigrationContext:
    """Context for incremental migration to reduce function arguments."""

    def __init__(
        self,
        session_id: str,
        table_name: str,
        sql_session: AsyncSession,
    ) -> None:
        self.session_id = session_id
        self.table_name = table_name
        self.sql_session = sql_session


async def _process_document_incremental(
    doc: dict[str, Any],
    analysis: dict[str, Any],
    ctx: _IncrementalMigrationContext,
    *,
    max_field_bytes: int,
    max_nesting_depth: int,
    safety_events: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Process document for incremental migration with type drift detection.

    Returns:
        Tuple of (sql_row, overflow, updated_analysis)

    """
    username = doc.get("username", "unknown")
    sys_ingested_at = doc.get("sys_ingested_at")
    t_stamp = doc.get("t_stamp")

    sql_row = {"session_id": ctx.session_id, "username": username}
    overflow = {}

    if sys_ingested_at is not None:
        sql_row["sys_ingested_at"] = sys_ingested_at
    if t_stamp is not None:
        sql_row["t_stamp"] = t_stamp

    for key, value in doc.items():
        if key in ["_id", "session_id", "username", "sys_ingested_at", "t_stamp"]:
            continue

        route_to_jsonb, size_bytes, nesting_depth, reason = should_guardrail_route_to_jsonb(
            value,
            max_bytes=max_field_bytes,
            max_depth=max_nesting_depth,
        )
        if route_to_jsonb and reason:
            overflow[key] = value
            if safety_events is not None:
                safety_events.append(
                    build_guardrail_event(
                        column=key,
                        reason=reason,
                        size_bytes=size_bytes,
                        nesting_depth=nesting_depth,
                    )
                )
            continue

        meta = analysis.get(key)
        target_overflow = True

        if meta and is_sql_target(meta["target"]):
            try:
                casted_val = cast_value(value, meta["type"])
                sql_row[key] = casted_val
                target_overflow = False
            except (ValueError, TypeError):
                # Type drift detected — migrate column to JSONB overflow
                logger.warning(
                    "Type drift detected for column '%s': cannot cast value to %s",
                    key,
                    meta["type"],
                )
                logger.info("Triggering column migration: '%s' from SQL to JSONB overflow", key)

                await migrate_column_to_jsonb(
                    session_id=ctx.session_id,
                    column_name=key,
                    table_name=ctx.table_name,
                    sql_session=ctx.sql_session,
                )

                # Reload updated schema deterministically from metadata snapshot
                snapshot = await load_metadata_snapshot(ctx.sql_session, ctx.session_id)
                if snapshot:
                    analysis = snapshot.schema

                target_overflow = True

        if target_overflow:
            overflow[key] = value

    return sql_row, overflow, analysis


async def migrate_column_to_jsonb(
    session_id: str,
    column_name: str,
    table_name: str,
    sql_session: AsyncSession,
) -> int:
    """Migrate a column from SQL to JSONB overflow due to type drift.

    Steps:
    1. Reads all values for the column from SQL (for this session)
    2. Merges those values into the overflow_data JSONB column
    3. Marks the column for 'jsonb' in the cached schema

    Args:
        session_id: Session identifier
        column_name: Name of the column to migrate
        table_name: SQL table name
        sql_session: SQL session

    Returns:
        Number of records updated

    """
    logger.info("Migrating column '%s' from SQL to JSONB overflow for session %s", column_name, session_id)

    if not IDENTIFIER_RE.fullmatch(column_name):
        msg = f"Invalid column name for JSONB migration: {column_name}"
        raise ValueError(msg)

    if not IDENTIFIER_RE.fullmatch(table_name):
        msg = f"Invalid table name for JSONB migration: {table_name}"
        raise ValueError(msg)

    # 1-2. Merge column values into overflow_data JSONB in one update statement
    started = time.perf_counter()
    update_sql = text(
        'UPDATE "'
        + table_name
        + "\" SET overflow_data = COALESCE(overflow_data, '{}'::jsonb) "
        + '|| jsonb_build_object(:col_key, to_jsonb("'
        + column_name
        + '")) '
        + 'WHERE session_id = :sid AND "'
        + column_name
        + '" IS NOT NULL'
    )
    update_result = await sql_session.execute(update_sql, {"col_key": column_name, "sid": session_id})
    updated_count = int(update_result.rowcount or 0)  # type: ignore[attr-defined]

    max_drift_events = max(1, int(os.getenv("GUARDRAIL_MAX_DRIFT_EVENTS_PER_SESSION", "200")))

    # 3. Update cached schema + drift events + schema version
    snapshot = await load_metadata_snapshot(sql_session, session_id)
    if snapshot:
        updated_schema, updated_drift_events, version_increment = apply_drift_to_metadata(
            snapshot.schema,
            snapshot.drift_events,
            column_name,
        )
        bounded_drift_events = bounded_append_events(
            snapshot.drift_events, [updated_drift_events[-1]], max_drift_events
        )
        logger.info("Updated schema: %s target changed to JSONB overflow", column_name)
        await sql_session.execute(
            text(
                "UPDATE session_metadata "
                "SET schema_json = :schema, "
                "    drift_events = CAST(:drift_events AS jsonb), "
                "    schema_version = :schema_version "
                "WHERE session_id = :sid"
            ),
            {
                "sid": session_id,
                "schema": json.dumps(updated_schema),
                "drift_events": json.dumps(bounded_drift_events),
                "schema_version": snapshot.schema_version + version_increment,
            },
        )
        await sql_session.commit()

    elapsed = time.perf_counter() - started
    rows_per_second = calculate_rows_per_second(updated_count, elapsed)
    logger.info(
        "Column JSONB migration throughput for '%s': %.2f rows/sec (%d rows in %.4fs)",
        column_name,
        rows_per_second,
        updated_count,
        elapsed,
    )

    logger.info("Successfully migrated %d values of column '%s' to JSONB overflow", updated_count, column_name)
    return updated_count


@session
async def migrate_data(
    session_id: str,
    analysis: dict[str, Any],
    sql_session: AsyncSession,
) -> None:
    """Migrate data from staging to permanent storage based on analysis.

    Uses single SQL table 'chiral_data' with overflow_data JSONB column.
    """
    table_name = "chiral_data"
    batch_size = max(1, int(os.getenv("MIGRATION_INSERT_BATCH_SIZE", "100")))
    perf_logging_enabled = os.getenv("ENABLE_PERFORMANCE_LOGGING", "true").lower() == "true"
    structured_metrics_enabled = os.getenv("ENABLE_STRUCTURED_METRICS_LOGGING", "true").lower() == "true"
    max_field_bytes = max(128, int(os.getenv("GUARDRAIL_MAX_FIELD_BYTES", "65536")))
    max_nesting_depth = max(1, int(os.getenv("GUARDRAIL_MAX_NESTING_DEPTH", "8")))
    max_safety_events = max(1, int(os.getenv("GUARDRAIL_MAX_SAFETY_EVENTS_PER_SESSION", "500")))
    started = time.perf_counter()

    # Build key policy from environment
    key_policy = KeyPolicy(
        unique_confidence_threshold=float(os.getenv("ROUTING_STABILITY_THRESHOLD", "1.0")),
    )

    # Get existing columns
    existing_cols_sql = text("SELECT column_name FROM information_schema.columns WHERE table_name = :table_name")
    result = await sql_session.execute(existing_cols_sql, {"table_name": table_name})
    existing_columns = [row[0] for row in result.fetchall()]

    # Setup system columns and get valid SQL columns
    sys_cols_to_add, valid_sql_cols = _ensure_system_columns(existing_columns)
    schema_cols_to_add = _build_schema_columns(analysis, existing_columns, valid_sql_cols, key_policy)

    columns_to_add = sys_cols_to_add + schema_cols_to_add

    # Apply schema changes
    if columns_to_add:
        alter_stmt = f'ALTER TABLE "{table_name}" {", ".join(columns_to_add)};'
        try:
            await sql_session.execute(text(alter_stmt))
            await sql_session.commit()
        except SQLAlchemyError:
            logger.exception("Schema Evolution Failed")
            return

    # Read staging documents from PostgreSQL staging_data table
    staging_result = await sql_session.execute(
        text("SELECT id, data FROM staging_data WHERE session_id = :sid"),
        {"sid": session_id},
    )
    staging_rows = staging_result.fetchall()

    processed_ids = []
    pending_inserts: list[dict[str, Any]] = []
    safety_events: list[dict[str, Any]] = []
    total_key_count = 0
    overflow_key_count = 0
    for row in staging_rows:
        staging_id = row[0]
        raw_data = row[1]

        doc = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

        data_keys = [k for k in doc if k not in ["_id", "session_id", "username", "sys_ingested_at", "t_stamp"]]
        total_key_count += len(data_keys)

        sql_row, overflow = _process_document(
            doc,
            session_id,
            analysis,
            max_field_bytes=max_field_bytes,
            max_nesting_depth=max_nesting_depth,
            safety_events=safety_events,
        )
        overflow_key_count += len(overflow)
        insert_payload = _build_insert_payload(sql_row, overflow, valid_sql_cols)
        if insert_payload:
            pending_inserts.append(insert_payload)

        processed_ids.append(staging_id)

    inserted_count = 0
    if pending_inserts:
        inserted_count = await _bulk_insert_sql_rows(pending_inserts, table_name, sql_session, batch_size)

    # Cleanup staging
    if processed_ids:
        placeholders = ", ".join([f":id_{i}" for i in range(len(processed_ids))])
        delete_sql = text(f"DELETE FROM staging_data WHERE id IN ({placeholders})")
        params = {f"id_{i}": pid for i, pid in enumerate(processed_ids)}
        await sql_session.execute(delete_sql, params)

    schema_json = json.dumps(analysis)
    await sql_session.execute(
        text(
            "UPDATE session_metadata "
            "SET status = 'migrated', "
            "    schema_json = :schema, "
            "    schema_version = COALESCE(schema_version, 1), "
            "    safety_events = COALESCE(safety_events, '[]'::jsonb) || CAST(:safety_events AS jsonb), "
            "    migration_metrics = COALESCE(migration_metrics, '[]'::jsonb) || CAST(:metrics AS jsonb) "
            "WHERE session_id = :sid"
        ),
        {
            "sid": session_id,
            "schema": schema_json,
            "safety_events": json.dumps(bounded_append_events([], safety_events, max_safety_events)),
            "metrics": json.dumps([]),
        },
    )

    elapsed = time.perf_counter() - started
    rows_per_second = calculate_rows_per_second(len(staging_rows), elapsed)
    metrics = build_migration_metrics(
        phase="full",
        rows_processed=len(staging_rows),
        rows_inserted=inserted_count,
        rows_per_second=rows_per_second,
        overflow_key_count=overflow_key_count,
        total_key_count=total_key_count,
        drift_event_count=0,
        guardrail_event_count=len(safety_events),
    ).as_dict()

    await sql_session.execute(
        text(
            "UPDATE session_metadata "
            "SET migration_metrics = COALESCE(migration_metrics, '[]'::jsonb) || CAST(:metrics AS jsonb) "
            "WHERE session_id = :sid"
        ),
        {"sid": session_id, "metrics": json.dumps([metrics])},
    )

    await sql_session.commit()

    if perf_logging_enabled:
        logger.info(
            "Full migration throughput for session %s: %.2f rows/sec (%d rows in %.4fs)",
            session_id,
            rows_per_second,
            len(staging_rows),
            elapsed,
        )
    if structured_metrics_enabled:
        logger.info("migration_metrics=%s", json.dumps(metrics, sort_keys=True))


@session
async def migrate_incremental(
    session_id: str,
    sql_session: AsyncSession,
) -> int:
    """Migrate new data from staging for a session that has already been analyzed.

    Uses the cached schema from session_metadata.

    Args:
        session_id: Session identifier
        sql_session: SQL session (injected)

    Returns:
        Number of records migrated

    """
    # Fetch cached schema deterministically from metadata snapshot
    snapshot = await load_metadata_snapshot(sql_session, session_id)
    if not snapshot or not snapshot.schema:
        logger.warning("No schema found for session %s, cannot perform incremental migration", session_id)
        return 0

    analysis = snapshot.schema

    # Get staging documents from PostgreSQL
    staging_result = await sql_session.execute(
        text("SELECT id, data FROM staging_data WHERE session_id = :sid"),
        {"sid": session_id},
    )
    staging_rows = staging_result.fetchall()

    if not staging_rows:
        return 0

    table_name = "chiral_data"
    migrated_count = 0
    batch_size = max(1, int(os.getenv("MIGRATION_INSERT_BATCH_SIZE", "100")))
    perf_logging_enabled = os.getenv("ENABLE_PERFORMANCE_LOGGING", "true").lower() == "true"
    structured_metrics_enabled = os.getenv("ENABLE_STRUCTURED_METRICS_LOGGING", "true").lower() == "true"
    max_field_bytes = max(128, int(os.getenv("GUARDRAIL_MAX_FIELD_BYTES", "65536")))
    max_nesting_depth = max(1, int(os.getenv("GUARDRAIL_MAX_NESTING_DEPTH", "8")))
    max_safety_events = max(1, int(os.getenv("GUARDRAIL_MAX_SAFETY_EVENTS_PER_SESSION", "500")))
    started = time.perf_counter()

    # Build valid SQL columns list
    valid_sql_cols = ["session_id", "username", "sys_ingested_at", "t_stamp"]
    for col, meta in analysis.items():
        if is_sql_target(meta["target"]) and col not in valid_sql_cols:
            valid_sql_cols.append(col)

    ctx = _IncrementalMigrationContext(session_id, table_name, sql_session)

    processed_ids = []
    pending_inserts: list[dict[str, Any]] = []
    safety_events: list[dict[str, Any]] = []
    total_key_count = 0
    overflow_key_count = 0
    drift_event_count = 0
    for row in staging_rows:
        staging_id = row[0]
        raw_data = row[1]

        doc = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

        data_keys = [k for k in doc if k not in ["_id", "session_id", "username", "sys_ingested_at", "t_stamp"]]
        total_key_count += len(data_keys)

        before_drift_count = len(snapshot.drift_events) if snapshot else 0
        sql_row, overflow, analysis = await _process_document_incremental(
            doc,
            analysis,
            ctx,
            max_field_bytes=max_field_bytes,
            max_nesting_depth=max_nesting_depth,
            safety_events=safety_events,
        )
        latest_snapshot = await load_metadata_snapshot(sql_session, session_id)
        after_drift_count = len(latest_snapshot.drift_events) if latest_snapshot else before_drift_count
        if after_drift_count > before_drift_count:
            drift_event_count += after_drift_count - before_drift_count
            snapshot = latest_snapshot

        overflow_key_count += len(overflow)

        # Rebuild valid_sql_cols if schema changed
        valid_sql_cols = ["session_id", "username", "sys_ingested_at", "t_stamp"]
        for col, meta in analysis.items():
            if is_sql_target(meta["target"]) and col not in valid_sql_cols:
                valid_sql_cols.append(col)

        insert_payload = _build_insert_payload(sql_row, overflow, valid_sql_cols)
        if insert_payload:
            pending_inserts.append(insert_payload)

        migrated_count += 1
        processed_ids.append(staging_id)

    inserted_count = 0
    if pending_inserts:
        inserted_count = await _bulk_insert_sql_rows(pending_inserts, table_name, sql_session, batch_size)

    # Cleanup staging
    if processed_ids:
        placeholders = ", ".join([f":id_{i}" for i in range(len(processed_ids))])
        delete_sql = text(f"DELETE FROM staging_data WHERE id IN ({placeholders})")
        params = {f"id_{i}": pid for i, pid in enumerate(processed_ids)}
        await sql_session.execute(delete_sql, params)

    elapsed = time.perf_counter() - started
    rows_per_second = calculate_rows_per_second(migrated_count, elapsed)
    metrics = build_migration_metrics(
        phase="incremental",
        rows_processed=migrated_count,
        rows_inserted=inserted_count,
        rows_per_second=rows_per_second,
        overflow_key_count=overflow_key_count,
        total_key_count=total_key_count,
        drift_event_count=drift_event_count,
        guardrail_event_count=len(safety_events),
    ).as_dict()

    existing_safety = snapshot.safety_events if snapshot else []
    bounded_safety = bounded_append_events(existing_safety, safety_events, max_safety_events)

    await sql_session.execute(
        text(
            "UPDATE session_metadata "
            "SET status = 'migrated', "
            "    safety_events = CAST(:safety_events AS jsonb), "
            "    migration_metrics = COALESCE(migration_metrics, '[]'::jsonb) || CAST(:metrics AS jsonb) "
            "WHERE session_id = :sid"
        ),
        {
            "sid": session_id,
            "safety_events": json.dumps(bounded_safety),
            "metrics": json.dumps([metrics]),
        },
    )
    await sql_session.commit()

    logger.info("Incrementally migrated %d records for session %s", migrated_count, session_id)
    if perf_logging_enabled:
        logger.info(
            "Incremental migration throughput for session %s: %.2f rows/sec (%d rows in %.4fs)",
            session_id,
            rows_per_second,
            migrated_count,
            elapsed,
        )
    if structured_metrics_enabled:
        logger.info("migration_metrics=%s", json.dumps(metrics, sort_keys=True))
    return migrated_count
