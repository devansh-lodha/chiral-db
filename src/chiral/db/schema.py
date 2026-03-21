# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Database initialization and schema definitions."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.ddl_helpers import add_foreign_key_safe


async def init_metadata_table(session: AsyncSession) -> None:
    """Ensure the session metadata table exists with PK/FK constraints."""
    sql = """
    CREATE TABLE IF NOT EXISTS session_metadata (
        session_id TEXT PRIMARY KEY,
        record_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'collecting',
        schema_json TEXT,
        schema_version INTEGER DEFAULT 1,
        drift_events JSONB DEFAULT '[]'::jsonb,
        safety_events JSONB DEFAULT '[]'::jsonb,
        migration_metrics JSONB DEFAULT '[]'::jsonb,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    await session.execute(text(sql))
    await session.execute(
        text("ALTER TABLE session_metadata ADD COLUMN IF NOT EXISTS schema_version INTEGER DEFAULT 1")
    )
    await session.execute(
        text("ALTER TABLE session_metadata ADD COLUMN IF NOT EXISTS drift_events JSONB DEFAULT '[]'::jsonb")
    )
    await session.execute(
        text("ALTER TABLE session_metadata ADD COLUMN IF NOT EXISTS safety_events JSONB DEFAULT '[]'::jsonb")
    )
    await session.execute(
        text("ALTER TABLE session_metadata ADD COLUMN IF NOT EXISTS migration_metrics JSONB DEFAULT '[]'::jsonb")
    )

    # Main data table with overflow_data JSONB column (replaces MongoDB permanent collection)
    # System columns:
    # - username: Traceability field (mandatory)
    # - sys_ingested_at: Server timestamp (bi-temporal, join key)
    # - t_stamp: Client timestamp (bi-temporal)
    # - overflow_data: JSONB column for nested/unstructured data (replaces MongoDB)
    sql_data = """
    CREATE TABLE IF NOT EXISTS chiral_data (
        id SERIAL PRIMARY KEY,
        session_id TEXT,
        username TEXT,
        sys_ingested_at FLOAT,
        t_stamp FLOAT,
        overflow_data JSONB DEFAULT '{}'::jsonb
    );
    """
    await session.execute(text(sql_data))

    # Staging table with JSONB column (replaces MongoDB staging collection)
    sql_staging = """
    CREATE TABLE IF NOT EXISTS staging_data (
        id SERIAL PRIMARY KEY,
        session_id TEXT,
        data JSONB NOT NULL
    );
    """
    await session.execute(text(sql_staging))

    # Performance indexes (idempotent)
    await session.execute(text('CREATE INDEX IF NOT EXISTS idx_chiral_data_session_id ON "chiral_data" (session_id)'))
    await session.execute(
        text('CREATE INDEX IF NOT EXISTS idx_chiral_data_sys_ingested_at ON "chiral_data" (sys_ingested_at)')
    )
    await session.execute(text('CREATE INDEX IF NOT EXISTS idx_chiral_data_username ON "chiral_data" (username)'))
    await session.execute(
        text('CREATE INDEX IF NOT EXISTS idx_chiral_data_overflow_gin ON "chiral_data" USING GIN (overflow_data)')
    )
    await session.execute(text('CREATE INDEX IF NOT EXISTS idx_staging_data_session_id ON "staging_data" (session_id)'))

    await session.commit()

    # Add foreign key constraints (idempotent)
    await add_foreign_key_safe(
        session=session,
        table_name="chiral_data",
        constraint_name="fk_chiral_data_session",
        local_column="session_id",
        referenced_table="session_metadata",
        referenced_column="session_id",
        on_delete="CASCADE",
    )

    await add_foreign_key_safe(
        session=session,
        table_name="staging_data",
        constraint_name="fk_staging_data_session",
        local_column="session_id",
        referenced_table="session_metadata",
        referenced_column="session_id",
        on_delete="CASCADE",
    )
