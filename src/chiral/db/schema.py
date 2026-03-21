# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Database initialization and schema definitions."""

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.db.ddl_helpers import (
    add_foreign_key_safe,
    add_index_safe,
    build_fk_constraint_name,
    build_index_name,
)
from chiral.domain.key_policy import build_dynamic_child_key_spec, normalize_identifier

ANALYSIS_METADATA_KEY = "__analysis_metadata__"


def get_decomposition_plan(analysis: dict[str, Any]) -> dict[str, Any]:
    """Extract decomposition plan from analysis metadata with default shape."""
    metadata = analysis.get(ANALYSIS_METADATA_KEY, {}) if isinstance(analysis, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}

    plan = metadata.get("decomposition_plan", {})
    if not isinstance(plan, dict):
        plan = {}

    entities = plan.get("entities", [])
    if not isinstance(entities, list):
        entities = []

    return {
        "version": int(plan.get("version", 1) or 1),
        "parent_table": str(plan.get("parent_table", "chiral_data")),
        "entities": entities,
    }


def _normalize_child_columns(entity: dict[str, Any]) -> list[str]:
    raw_columns = entity.get("child_columns", [])
    if not isinstance(raw_columns, list):
        return []

    normalized: list[str] = []
    for column in raw_columns:
        if not isinstance(column, str):
            continue
        normalized_column = normalize_identifier(column)
        if normalized_column not in normalized:
            normalized.append(normalized_column)
    return normalized


async def materialize_decomposition_tables(
    session: AsyncSession,
    analysis: dict[str, Any],
    *,
    parent_table: str = "chiral_data",
) -> None:
    """Create/extend dynamic child tables for detected repeating entities."""
    plan = get_decomposition_plan(analysis)
    entities = plan.get("entities", [])
    if not entities:
        return

    for entity in entities:
        if not isinstance(entity, dict):
            continue

        source_field = entity.get("source_field")
        if not isinstance(source_field, str) or not source_field:
            continue

        key_spec = build_dynamic_child_key_spec(
            parent_table=parent_table,
            source_field=source_field,
            parent_pk_column="id",
            parent_pk_type="SERIAL",
            include_session_fk=True,
        )

        child_table = key_spec.table_name
        parent_fk_column = key_spec.foreign_keys[0]["local_column"]
        child_columns = _normalize_child_columns(entity)

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS "{child_table}" ('
            f"id SERIAL PRIMARY KEY, "
            f'"{parent_fk_column}" INTEGER NOT NULL, '
            f'"session_id" TEXT, '
            f"overflow_data JSONB DEFAULT '{{}}'::jsonb"
            ");"
        )
        await session.execute(text(create_sql))

        for column in child_columns:
            await session.execute(text(f'ALTER TABLE "{child_table}" ADD COLUMN IF NOT EXISTS "{column}" TEXT'))

        for foreign_key in key_spec.foreign_keys:
            constraint_name = build_fk_constraint_name(
                child_table,
                foreign_key["local_column"],
                foreign_key["referenced_table"],
            )
            await add_foreign_key_safe(
                session=session,
                table_name=child_table,
                constraint_name=constraint_name,
                local_column=foreign_key["local_column"],
                referenced_table=foreign_key["referenced_table"],
                referenced_column=foreign_key["referenced_column"],
                on_delete=foreign_key.get("on_delete", "CASCADE"),
            )

        await add_index_safe(
            session=session,
            table_name=child_table,
            index_name=build_index_name(child_table, parent_fk_column),
            column_name=parent_fk_column,
        )
        await add_index_safe(
            session=session,
            table_name=child_table,
            index_name=build_index_name(child_table, "session_id"),
            column_name="session_id",
        )


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
