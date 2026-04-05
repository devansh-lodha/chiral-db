# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Shared database fixtures for integration-style tests."""

import sys
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from chiral.config import get_settings
from chiral.db.schema import init_metadata_table

PROJECT_SRC = Path(__file__).resolve().parent.parent / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))


@pytest_asyncio.fixture(scope="function")
async def acid_engine() -> AsyncGenerator[AsyncEngine]:
    """Create a reusable PostgreSQL engine for ACID tests."""
    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)

    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        await init_metadata_table(session)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def reset_acid_tables(acid_engine: AsyncEngine) -> AsyncGenerator[None]:
    """Reset the test tables before and after every ACID test."""

    async def _truncate() -> None:
        async with acid_engine.begin() as conn:
            await conn.execute(
                text("TRUNCATE TABLE chiral_data, staging_data, session_metadata RESTART IDENTITY CASCADE")
            )

    await _truncate()
    try:
        yield
    finally:
        await _truncate()
