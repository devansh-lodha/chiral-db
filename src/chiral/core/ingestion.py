# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Data Ingestion Layer."""

import logging
from datetime import UTC, datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase
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
    mongo_db: AsyncIOMotorDatabase,
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Ingest a single data record into the staging area.

    Args:
        data: The dictionary of data to ingest.
        session_id: Unique identifier for the current session data stream.
        mongo_db: Injected MongoDB database.
        sql_session: Injected SQL session.

    Returns:
        Status dictionary.

    """
    # Ensure metadata table exists (Idempotent)
    await init_metadata_table(sql_session)

    # 1. Atomically initialize session if not exists using INSERT ON CONFLICT
    # This prevents race conditions when multiple concurrent requests try to create the same session
    init_sql = text("""
        INSERT INTO session_metadata (session_id, record_count)
        VALUES (:sid, 0)
        ON CONFLICT (session_id) DO NOTHING
    """)
    await sql_session.execute(init_sql, {"sid": session_id})
    await sql_session.commit()  # Commit initialization immediately

    # Extract/Validate mandatory traceability field
    username = data.get("username", "unknown")
    if username == "unknown":
        logger = logging.getLogger(__name__)
        logger.warning("No username provided in data for session %s, using 'unknown'", session_id)

    # Generate Timestamps (Bi-Temporal)
    # Metric 1: Valid Time (t_stamp) - Client timestamp
    t_stamp = data.get("t_stamp", datetime.now(tz=UTC).timestamp())

    # Metric 2: Transaction Time (sys_ingested_at) - Server timestamp
    clock = MonotonicClock.get_instance()
    sys_ingested_at = clock.get_sys_ingested_at()

    # 3. Insert into Shared Staging Collection in Mongo
    # Ensure mandatory fields are present: username, t_stamp, sys_ingested_at
    document = data.copy()
    document["session_id"] = session_id
    document["username"] = username  # Traceability: mandatory field
    document["t_stamp"] = t_stamp  # Bi-Temporal: Client timestamp (valid time)
    document["sys_ingested_at"] = sys_ingested_at  # Bi-Temporal: Server timestamp (transaction time, join key)

    # Using single collection 'staging'
    collection = mongo_db["staging"]
    await collection.insert_one(document)

    # 3. Atomically Increment Count in SQL (prevents race conditions)
    # Using SELECT FOR UPDATE to lock the row, then update
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

    # "after 100 entries of data stored in the mongodb we will run worker"
    if new_count >= INITIAL_ANALYSIS_THRESHOLD:
        # Check if we currently have 'collecting' status to avoid double trigger
        status_check = text("SELECT status FROM session_metadata WHERE session_id = :sid")
        status_res = await sql_session.execute(status_check, {"sid": session_id})
        status_row = status_res.fetchone()

        if status_row and status_row[0] == "collecting":
            # First time reaching threshold - trigger analysis and initial migration
            await sql_session.execute(
                text("UPDATE session_metadata SET status = 'analyzing' WHERE session_id = :sid"),
                {"sid": session_id},
            )
            # Commit before triggering worker
            await sql_session.commit()

            # Worker will be triggered by variable checking in controller
            worker_triggered = True
            incremental = False

    # Check if status is 'migrated' - trigger incremental migration periodically
    # Use atomic update to prevent concurrent migrations
    if not worker_triggered:
        # Count how many new records are in staging for this session
        staging_count = await mongo_db["staging"].count_documents({"session_id": session_id})

        # Trigger incremental migration when there are new records (batch of 10 or more)
        # Use UPDATE with WHERE clause to atomically claim the migration job
        if staging_count >= INCREMENTAL_MIGRATION_BATCH:
            update_result = await sql_session.execute(
                text(
                    "UPDATE session_metadata SET status = 'migrating_incremental' "
                    "WHERE session_id = :sid AND status = 'migrated' RETURNING session_id"
                ),
                {"sid": session_id},
            )
            # Only trigger if we successfully updated (meaning status WAS 'migrated')
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
