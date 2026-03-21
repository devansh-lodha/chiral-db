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

from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel

from chiral.core.ingestion import ingest_data
from chiral.core.orchestrator import flush_staging, trigger_worker
from chiral.core.query_service import translate_json_request

app = FastAPI(title="Chiral DB Assignment")


class IngestRequest(BaseModel):
    """Request model for data ingestion endpoint."""

    data: dict[str, Any]


class QueryTranslateRequest(BaseModel):
    """Request model for query translation endpoint."""

    operation: str
    table: str = "chiral_data"
    select: list[str] | None = None
    filters: list[dict[str, Any]] | None = None
    payload: dict[str, Any] | None = None
    updates: dict[str, Any] | None = None
    limit: int | None = None
    offset: int | None = None


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
def translate_query_endpoint(request: QueryTranslateRequest) -> dict[str, Any]:
    """Translate JSON CRUD request into SQL/JSONB query and bind params."""
    built_query = translate_json_request(request.model_dump(exclude_none=True))
    return {
        "sql": built_query.sql,
        "params": built_query.params,
    }
