# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Orchestration Logic."""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from chiral.worker.analyzer import analyze_staging
from chiral.worker.migrator import migrate_data, migrate_incremental

logger = logging.getLogger(__name__)


async def trigger_worker(session_id: str, *, incremental: bool = False, engine: AsyncEngine) -> None:
    """Orchestrate the worker analysis and migration."""
    logger.info("Worker triggered for session: %s (incremental=%s)", session_id, incremental)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    try:
        if incremental:
            logger.info("Starting Incremental Migration...")
            async with session_factory() as session:
                count = await migrate_incremental(session_id=session_id, sql_session=session)
                await session.commit()
            logger.info("Incremental Migration Complete. Migrated %d records.", count)
        else:
            logger.info("Starting Analysis Phase...")
            async with session_factory() as session:
                analysis_result = await analyze_staging(sql_session=session)
                await session.commit()
            logger.info("Analysis Complete. Columns: %s", list(analysis_result.keys()))

            logger.info("Starting Migration Phase...")
            async with session_factory() as session:
                await migrate_data(session_id=session_id, analysis=analysis_result, sql_session=session)
                await session.commit()
            logger.info("Migration Complete.")

    except Exception:
        logger.exception("Worker failed for session %s", session_id)
        if incremental:
            try:
                async with session_factory() as session:
                    await session.execute(
                        text("UPDATE session_metadata SET status = 'migrated' WHERE session_id = :sid"),
                        {"sid": session_id},
                    )
                    await session.commit()
            except Exception:
                logger.exception("Failed to reset status")


async def flush_staging(session_id: str, engine: AsyncEngine) -> dict[str, int]:
    """Force migrate any remaining data in staging for a session."""
    logger.info("Flushing staging area for session: %s", session_id)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    count = 0

    async with session_factory() as session:
        # Check if a schema already exists for this session
        result = await session.execute(
            text("SELECT schema_json FROM session_metadata WHERE session_id = :sid"), {"sid": session_id}
        )
        row = result.fetchone()

        if not row or not row[0] or row[0] == "{}" or row[0] == "null":
            logger.info("No schema found during flush. Forcing early full analysis...")
            analysis_result = await analyze_staging(sql_session=session)
            await session.commit()

            logger.info("Forcing early full migration...")
            await migrate_data(session_id=session_id, analysis=analysis_result, sql_session=session)
            await session.commit()
            count = 1  # Marking that a full migration occurred
        else:
            count = await migrate_incremental(session_id=session_id, sql_session=session)
            await session.commit()

    logger.info("Flush complete. Processed remaining records.")
    return {"flushed_count": count}
