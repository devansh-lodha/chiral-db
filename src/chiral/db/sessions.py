# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Database session management."""

from collections.abc import Callable
from functools import wraps
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from .connection import get_sql_engine


def session(func: Callable[..., Any]) -> Callable[..., Any]:
    """Provide an independent SQL database session to the decorated function.

    Args:
        func: The async function to decorate.

    Lifecycle:
    - Creates new SQL Engine per call.
    - Disposes after execution.
    - Commits SQL transaction on success, Rollbacks on exception.

    """

    @wraps(func)
    async def wrapper(*args: object, **kwargs: object) -> object:
        sql_engine = get_sql_engine()
        session_local = async_sessionmaker(bind=sql_engine, expire_on_commit=False)

        async with session_local() as sql_session:
            try:
                kwargs["sql_session"] = sql_session
                result = await func(*args, **kwargs)
                await sql_session.commit()
            except Exception:
                await sql_session.rollback()
                raise
            else:
                return result
            finally:
                await sql_engine.dispose()

    return wrapper
