# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Database initialization and schema definitions."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def init_metadata_table(session: AsyncSession) -> None:
    """Ensure the session metadata table exists."""
    sql = """
    CREATE TABLE IF NOT EXISTS session_metadata (
        session_id TEXT PRIMARY KEY,
        record_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'collecting',
        schema_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    await session.execute(text(sql))

    # Ensure the main data table exists (initially empty)
    # System columns:
    # - username: Traceability field (mandatory, present in both SQL and MongoDB)
    # - sys_ingested_at: Server timestamp (bi-temporal, join key between SQL and MongoDB)
    # - t_stamp: Client timestamp (bi-temporal, records when event occurred)
    sql_data = """
    CREATE TABLE IF NOT EXISTS chiral_data (
        id SERIAL PRIMARY KEY,
        session_id TEXT,
        username TEXT,
        sys_ingested_at FLOAT,
        t_stamp FLOAT
    );
    """
    await session.execute(text(sql_data))
    await session.commit()
