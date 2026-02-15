# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Orchestration Logic."""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from chiral.db.connection import get_sql_engine
from chiral.worker.analyzer import analyze_staging
from chiral.worker.migrator import migrate_data, migrate_incremental

logger = logging.getLogger(__name__)


async def trigger_worker(session_id: str, *, incremental: bool = False) -> None:
    """Orchestrate the worker analysis and migration.

    Args:
        session_id: The session identifier to process.
        incremental: If True, perform incremental migration using cached schema.
                    If False, perform full analysis and migration.

    """
    logger.info("Worker triggered for session: %s (incremental=%s)", session_id, incremental)

    try:
        if incremental:
            # Use cached schema to migrate new data
            logger.info("Starting Incremental Migration...")
            count = await migrate_incremental(session_id=session_id)
            logger.info("Incremental Migration Complete. Migrated %d records.", count)
        else:
            # 1. Analyze Data
            logger.info("Starting Analysis Phase...")
            analysis_result = await analyze_staging()
            logger.info("Analysis Complete. Columns: %s", list(analysis_result.keys()))

            # 2. Migrate Data based on analysis
            logger.info("Starting Migration Phase...")
            await migrate_data(session_id=session_id, analysis=analysis_result)
            logger.info("Migration Complete.")

    except Exception:
        logger.exception("Worker failed for session %s", session_id)

        # Reset status on failure to allow retry
        if incremental:
            try:
                sql_engine = get_sql_engine()
                session_local = async_sessionmaker(bind=sql_engine, expire_on_commit=False)
                async with session_local() as session:
                    await session.execute(
                        text("UPDATE session_metadata SET status = 'migrated' WHERE session_id = :sid"),
                        {"sid": session_id},
                    )
                    await session.commit()
                await sql_engine.dispose()
            except Exception:
                logger.exception("Failed to reset status")


async def flush_staging(session_id: str) -> dict[str, int]:
    """Force migrate any remaining data in staging for a session."""
    logger.info("Flushing staging area for session: %s", session_id)
    # We assume analysis is already done if we are flushing at the end.
    # If not, one could argue we should run analysis, but for this assignment,
    # flushing implies we are done with the stream.
    count = await migrate_incremental(session_id=session_id)
    logger.info("Flush complete. Processed %d remaining records.", count)
    return {"flushed_count": count}
