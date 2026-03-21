# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Verification script for Chiral DB Assignment."""

import asyncio
import json
import logging

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from src.chiral.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

logger.info("--- CHIRAL DB: ASSIGNMENT VERIFICATION REPORT ---")


async def verify() -> None:
    """Verify data distribution in PostgreSQL (structured columns + JSONB overflow)."""
    settings = get_settings()

    engine = create_async_engine(settings.database_url)

    # 1. Check SQL structured data
    async with engine.connect() as conn:
        try:
            res = await conn.execute(text("SELECT count(*) FROM chiral_data"))
            sql_count = res.scalar()
        except DBAPIError:
            sql_count = 0
            logger.info("[SQL] Table 'chiral_data' does not exist yet.")

        # Check Schema (Columns)
        if sql_count > 0:
            res = await conn.execute(
                text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'chiral_data'")
            )
            columns = res.fetchall()
            logger.info("\n[POSTGRESQL] Structured Data Storage")
            logger.info("Record Count: %d", sql_count)
            logger.info("Learned Schema (Dynamic Columns):")
            for col in columns:
                if col[0] not in ["id", "session_id", "username", "sys_ingested_at", "t_stamp", "overflow_data"]:
                    logger.info(" - %s: %s", col[0], col[1])

    # 2. Check JSONB overflow data (replaces MongoDB)
    async with engine.connect() as conn:
        try:
            res = await conn.execute(
                text(
                    "SELECT COUNT(*) FROM chiral_data WHERE overflow_data IS NOT NULL AND overflow_data != '{}'::jsonb"
                )
            )
            overflow_count = res.scalar()
            logger.info("\n[JSONB OVERFLOW] Semi-Structured/Overflow Storage")
            logger.info("Records with overflow data: %d", overflow_count)

            if overflow_count > 0:
                sample_res = await conn.execute(
                    text(
                        "SELECT overflow_data FROM chiral_data "
                        "WHERE overflow_data IS NOT NULL AND overflow_data != '{}'::jsonb LIMIT 1"
                    )
                )
                sample_row = sample_res.fetchone()
                if sample_row:
                    sample = sample_row[0]
                    if isinstance(sample, str):
                        sample = json.loads(sample)
                    logger.info("Sample Overflow Document Keys:")
                    keys = [k for k in sample if k not in ["session_id", "username"]]
                    logger.info("  %s", keys)
        except DBAPIError:
            logger.info("[JSONB OVERFLOW] No overflow data found.")

    # 3. Check Metadata
    async with engine.connect() as conn:
        try:
            res = await conn.execute(text("SELECT session_id, status, record_count FROM session_metadata"))
            meta = res.fetchone()
            if meta:
                logger.info("\n[METADATA] Session Tracking")
                logger.info("Session ID: %s", meta[0])
                logger.info("Status: %s", meta[1])
                logger.info("Total Ingested: %d", meta[2])
        except DBAPIError:
            logger.info("[METADATA] Session metadata table unavailable.")

    # 4. Check staging
    async with engine.connect() as conn:
        try:
            res = await conn.execute(text("SELECT COUNT(*) FROM staging_data"))
            staging_count = res.scalar()
            logger.info("\n[STAGING] Pending Records: %d", staging_count)
        except DBAPIError:
            logger.info("[STAGING] Staging table unavailable.")

    await engine.dispose()

    logger.info("\n------------------------------------------------")
    logger.info("CONCLUSION:")
    if sql_count > 0:
        logger.info("SUCCESS: Hybrid storage active. All data in PostgreSQL (structured + JSONB).")
    else:
        logger.info("FAILURE: No data found in SQL.")


if __name__ == "__main__":
    asyncio.run(verify())
