# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Idempotent DDL helpers for safe schema evolution."""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def constraint_exists(
    session: AsyncSession,
    table_name: str,
    constraint_name: str,
) -> bool:
    """Check if a constraint exists on a table."""
    query = text("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = :table_name AND constraint_name = :constraint_name
        )
    """)
    result = await session.execute(query, {"table_name": table_name, "constraint_name": constraint_name})
    return result.scalar() or False


async def add_foreign_key_safe(
    session: AsyncSession,
    table_name: str,
    constraint_name: str,
    local_column: str,
    referenced_table: str,
    referenced_column: str,
    on_delete: str = "RESTRICT",
) -> None:
    """Add a foreign key constraint if it does not already exist (idempotent)."""
    if await constraint_exists(session, table_name, constraint_name):
        logger.debug("FK constraint %s already exists on %s", constraint_name, table_name)
        return

    logger.info(
        "Adding FK constraint %s on %s.%s -> %s.%s",
        constraint_name,
        table_name,
        local_column,
        referenced_table,
        referenced_column,
    )
    query = text(
        f'ALTER TABLE "{table_name}" '
        f'ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{local_column}") '
        f'REFERENCES "{referenced_table}" ("{referenced_column}") ON DELETE {on_delete}'
    )
    try:
        await session.execute(query)
        await session.commit()
    except Exception:
        await session.rollback()
        logger.warning("Failed to add FK constraint %s", constraint_name)


async def add_unique_constraint_safe(
    session: AsyncSession,
    table_name: str,
    constraint_name: str,
    column_name: str,
) -> None:
    """Add a unique constraint if it does not already exist (idempotent)."""
    if await constraint_exists(session, table_name, constraint_name):
        logger.debug("UNIQUE constraint %s already exists on %s", constraint_name, table_name)
        return

    logger.info("Adding UNIQUE constraint %s on %s.%s", constraint_name, table_name, column_name)
    query = text(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{constraint_name}" UNIQUE ("{column_name}")')
    try:
        await session.execute(query)
        await session.commit()
    except Exception:
        await session.rollback()
        logger.warning("Failed to add UNIQUE constraint %s", constraint_name)
