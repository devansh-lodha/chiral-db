# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Main Application Entry Point."""

import sys
from pathlib import Path
from typing import Any

# Add the parent directory (src) to sys.path to allow imports from 'chiral' package
sys.path.append(str(Path(__file__).parent.parent.parent))

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chiral.core.ingestion import ingest_data
from chiral.core.orchestrator import flush_staging, trigger_worker
from chiral.core.query_service import (
    CreateExecutionValidationError,
    execute_json_request,
    translate_json_request_with_metadata,
)
from chiral.db.sessions import session

app = FastAPI(title="Chiral DB Assignment")

app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IngestRequest(BaseModel):
    """Request model for data ingestion endpoint."""

    data: dict[str, Any]


class QueryTranslateRequest(BaseModel):
    """Request model for query translation endpoint."""

    operation: str
    table: str = "chiral_data"
    session_id: str | None = None
    select: list[str] | None = None
    filters: list[dict[str, Any]] | None = None
    payload: dict[str, Any] | None = None
    updates: dict[str, Any] | None = None
    limit: int | None = None
    offset: int | None = None
    decomposition_plan: dict[str, Any] | None = None
    analysis_metadata: dict[str, Any] | None = None


@app.post("/flush/{session_id}")
async def flush_endpoint(session_id: str) -> dict[str, int]:
    """Endpoint to force flush staging data."""
    return await flush_staging(session_id)


@app.post("/ingest")
async def ingest_endpoint(request: IngestRequest, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Endpoint to ingest data."""
    result = await ingest_data(data=request.data, session_id=request.data["session_id"])

    if result.get("worker_triggered"):
        incremental = result.get("incremental", False)
        background_tasks.add_task(trigger_worker, request.data["session_id"], incremental=incremental)

    return result


@app.get("/")
def root() -> dict[str, str]:
    """Root endpoint returning API status."""
    return {"message": "Chiral DB Assignment API is running."}


@app.post("/query/translate")
async def translate_query_endpoint(request: QueryTranslateRequest) -> dict[str, Any]:
    """Translate JSON CRUD request into SQL/JSONB query and bind params."""
    built_query = await translate_json_request_with_metadata(request.model_dump(exclude_none=True))
    return {
        "sql": built_query.sql,
        "params": built_query.params,
    }


@app.post("/query/execute")
async def execute_query_endpoint(request: QueryTranslateRequest) -> dict[str, Any]:
    """Translate and execute JSON CRUD request.

    - read: returns rows + row_count
    - update/delete: returns affected_rows
    - create: returns mode-aware contract (migrated_sync or queued_async)
    """
    try:
        return await execute_json_request(request.model_dump(exclude_none=True))
    except CreateExecutionValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "mode": "failed_validation",
                "error": str(exc),
            },
        ) from exc


@app.get("/schema/logical/{session_id}")
async def logical_schema_endpoint(session_id: str) -> list[str]:
    """Returns a flat list of all queryable logical fields for the dashboard."""
    @session
    async def _fetch(sql_session: AsyncSession) -> list[str]:
        result = await sql_session.execute(
            text("SELECT schema_json FROM session_metadata WHERE session_id = :sid"),
            {"sid": session_id}
        )
        row = result.fetchone()
        if not row or not row[0]:
            return ["username", "sys_ingested_at", "t_stamp", "session_id"]

        import json
        schema_json = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        fields = ["username", "sys_ingested_at", "t_stamp", "session_id"]

        for key, meta in schema_json.items():
            if key == "__analysis_metadata__":
                plan = meta.get("decomposition_plan", {})
                for entity in plan.get("entities", []):
                    source = entity.get("source_field")
                    if source and source not in fields:
                        fields.append(source)
                    for child_col in entity.get("child_columns", []):
                        fields.append(f"{source}.{child_col}")
                continue

            if key not in fields:
                fields.append(key)

        return sorted(fields)
    return await _fetch()


@app.get("/sessions/active")
async def active_sessions_endpoint() -> list[str]:
    @session
    async def _fetch(sql_session: AsyncSession) -> list[str]:
        result = await sql_session.execute(text("SELECT session_id FROM session_metadata ORDER BY created_at DESC"))
        return [row[0] for row in result.fetchall()]

    return await _fetch()
