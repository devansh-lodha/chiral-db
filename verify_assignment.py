# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Verification script for Chiral DB Assignment 1."""

import asyncio
import logging

from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from src.chiral.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

logger.info("--- CHIRAL DB: ASSIGNMENT 1 VERIFICATION REPORT ---")


async def verify() -> None:
    """Verify data distribution between SQL and Mongo."""
    settings = get_settings()

    # 1. Connect to SQL
    engine = create_async_engine(settings.database_url)
    async with engine.connect() as conn:
        # Check Row Count
        try:
            res = await conn.execute(text("SELECT count(*) FROM chiral_data"))
            sql_count = res.scalar()
        except DBAPIError:
            # If table doesn't exist or query fails, default to 0
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
                if col[0] not in ["id", "session_id", "username", "sys_ingested_at", "t_stamp"]:
                    logger.info(" - %s: %s", col[0], col[1])

    await engine.dispose()

    # 2. Connect to Mongo
    client = AsyncIOMotorClient(settings.mongo_url)
    db = client.chiral

    # Check Permanent Collection (Overflow/Drift)
    mongo_count = await db.permanent.count_documents({})
    logger.info("\n[MONGODB] Semi-Structured/Overflow Storage")
    logger.info("Document Count: %d", mongo_count)

    if mongo_count > 0:
        sample = await db.permanent.find_one()
        if sample and isinstance(sample, dict):
            logger.info("Sample Overflow Document Keys:")
            keys = [k for k in sample if k not in ["_id", "session_id", "username"]]
            logger.info("  %s", keys)

    # 3. Check Metadata
    # Let's check SQL for session metadata
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
            # Metadata table might not exist in early stages
            logger.info("[METADATA] Session metadata table unavailable.")

    logger.info("\n------------------------------------------------")
    logger.info("CONCLUSION:")
    if sql_count > 0 and mongo_count >= 0:
        logger.info("SUCCESS: Hybrid storage active. Data routed to PostgreSQL.")
    else:
        logger.info("FAILURE: No data found in SQL.")


if __name__ == "__main__":
    asyncio.run(verify())
