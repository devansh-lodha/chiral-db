# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Idempotent DDL helpers for safe schema evolution."""

import logging
import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _normalize_identifier(raw: str) -> str:
    normalized_chars = []
    for char in raw.lower():
        if char.isalnum() or char == "_":
            normalized_chars.append(char)
        else:
            normalized_chars.append("_")

    normalized = "".join(normalized_chars).strip("_")
    if not normalized:
        normalized = "entity"
    if normalized[0].isdigit():
        normalized = f"e_{normalized}"
    return normalized


def build_fk_constraint_name(table_name: str, local_column: str, referenced_table: str) -> str:
    """Build deterministic FK constraint name."""
    name = (
        f"fk_{_normalize_identifier(table_name)}_{_normalize_identifier(local_column)}"
        f"_{_normalize_identifier(referenced_table)}"
    )
    return name[:63]


def build_index_name(table_name: str, column_name: str) -> str:
    """Build deterministic index name."""
    name = f"idx_{_normalize_identifier(table_name)}_{_normalize_identifier(column_name)}"
    return name[:63]


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
        # Isolate DDL failures so a single FK conflict does not rollback
        # earlier CREATE TABLE/ALTER statements in the same session.
        async with session.begin_nested():
            await session.execute(query)
    except Exception as exc:
        logger.warning("Failed to add FK constraint %s: %s", constraint_name, exc)


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
        async with session.begin_nested():
            await session.execute(query)
    except Exception as exc:
        logger.warning("Failed to add UNIQUE constraint %s: %s", constraint_name, exc)


async def add_index_safe(
    session: AsyncSession,
    table_name: str,
    index_name: str,
    column_name: str,
) -> None:
    """Add index idempotently with IF NOT EXISTS semantics."""
    if not IDENTIFIER_RE.fullmatch(table_name) or not IDENTIFIER_RE.fullmatch(index_name):
        logger.warning("Skipping index creation due to invalid identifier: %s.%s", table_name, index_name)
        return
    if not IDENTIFIER_RE.fullmatch(column_name):
        logger.warning("Skipping index creation due to invalid column identifier: %s", column_name)
        return

    query = text(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{column_name}")')
    try:
        async with session.begin_nested():
            await session.execute(query)
    except Exception as exc:
        logger.warning("Failed to add index %s: %s", index_name, exc)
