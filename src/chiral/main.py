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

app = FastAPI(title="Chiral DB Assignment")


class IngestRequest(BaseModel):
    """Request model for data ingestion endpoint."""

    data: dict[str, Any]


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
