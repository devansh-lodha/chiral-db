# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Data Ingestion Layer."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.schema import init_metadata_table
from chiral.db.sessions import session
from chiral.utils.clock import MonotonicClock

# Constants for stage thresholds
INITIAL_ANALYSIS_THRESHOLD = 100
INCREMENTAL_MIGRATION_BATCH = 10


@session
async def ingest_data(
    data: dict[str, Any],
    session_id: str,
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Ingest a single data record into the staging area.

    Args:
        data: The dictionary of data to ingest.
        session_id: Unique identifier for the current session data stream.
        sql_session: Injected SQL session.

    Returns:
        Status dictionary.

    """
    # Ensure metadata table exists (Idempotent)
    await init_metadata_table(sql_session)

    # 1. Atomically initialize session if not exists using INSERT ON CONFLICT
    init_sql = text("""
        INSERT INTO session_metadata (session_id, record_count, schema_version, drift_events, safety_events, migration_metrics)
        VALUES (:sid, 0, 1, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb)
        ON CONFLICT (session_id) DO NOTHING
    """)
    await sql_session.execute(init_sql, {"sid": session_id})
    await sql_session.commit()

    # Extract/Validate mandatory traceability field
    username = data.get("username", "unknown")
    if username == "unknown":
        logger = logging.getLogger(__name__)
        logger.warning("No username provided in data for session %s, using 'unknown'", session_id)

    # Generate Timestamps (Bi-Temporal)
    t_stamp = data.get("t_stamp", datetime.now(tz=UTC).timestamp())

    clock = MonotonicClock.get_instance()
    sys_ingested_at = clock.get_sys_ingested_at()

    # 2. Insert into staging_data table (JSONB) — replaces MongoDB staging collection
    document = data.copy()
    document["session_id"] = session_id
    document["username"] = username
    document["t_stamp"] = t_stamp
    document["sys_ingested_at"] = sys_ingested_at

    staging_insert = text("INSERT INTO staging_data (session_id, data) VALUES (:sid, :data)")
    await sql_session.execute(staging_insert, {"sid": session_id, "data": json.dumps(document)})

    # 3. Atomically Increment Count in SQL
    lock_sql = text("SELECT record_count FROM session_metadata WHERE session_id = :sid FOR UPDATE")
    lock_result = await sql_session.execute(lock_sql, {"sid": session_id})
    row = lock_result.fetchone()
    if row is None:
        msg = f"Session {session_id} not found after initialization"
        raise ValueError(msg)
    current_count = row[0]

    new_count = current_count + 1
    update_sql = text("UPDATE session_metadata SET record_count = :cnt WHERE session_id = :sid")
    await sql_session.execute(update_sql, {"cnt": new_count, "sid": session_id})

    worker_triggered = False
    incremental = False

    if new_count >= INITIAL_ANALYSIS_THRESHOLD:
        status_check = text("SELECT status FROM session_metadata WHERE session_id = :sid")
        status_res = await sql_session.execute(status_check, {"sid": session_id})
        status_row = status_res.fetchone()

        if status_row and status_row[0] == "collecting":
            await sql_session.execute(
                text("UPDATE session_metadata SET status = 'analyzing' WHERE session_id = :sid"),
                {"sid": session_id},
            )
            await sql_session.commit()
            worker_triggered = True
            incremental = False

    # Check for incremental migration trigger
    if not worker_triggered:
        staging_count_result = await sql_session.execute(
            text("SELECT COUNT(*) FROM staging_data WHERE session_id = :sid"),
            {"sid": session_id},
        )
        staging_count = staging_count_result.scalar()

        if staging_count >= INCREMENTAL_MIGRATION_BATCH:
            update_result = await sql_session.execute(
                text(
                    "UPDATE session_metadata SET status = 'migrating_incremental' "
                    "WHERE session_id = :sid AND status = 'migrated' RETURNING session_id"
                ),
                {"sid": session_id},
            )
            if update_result.fetchone() is not None:
                await sql_session.commit()
                worker_triggered = True
                incremental = True

    return {
        "status": "success",
        "session_id": session_id,
        "count": new_count,
        "worker_triggered": worker_triggered,
        "incremental": incremental,
    }
