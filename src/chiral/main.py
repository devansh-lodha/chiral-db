# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Main Application Entry Point."""

import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add the parent directory (src) to sys.path to allow imports from 'chiral' package
sys.path.append(str(Path(__file__).parent.parent.parent))

from chiral.client import ChiralClient
from chiral.config import get_settings
from chiral.core.query_service import CreateExecutionValidationError

logger = logging.getLogger(__name__)

chiral: ChiralClient | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    global chiral  # noqa: PLW0603
    settings = get_settings()
    chiral = ChiralClient(settings.database_url)
    await chiral.connect()
    yield
    await chiral.disconnect()


app = FastAPI(title="Chiral DB Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IngestRequest(BaseModel):
    """Represents a data ingestion request from the UI."""

    data: dict[str, Any]


class QueryTranslateRequest(BaseModel):
    """Represents a query translation or execution request from the UI. The 'operation' field indicates whether this is a translation-only request or an execution request."""

    operation: str
    table: str = "chiral_data"
    session_id: str | None = None
    select: list[str] | None = None
    filters: list[dict[str, Any]] | None = None
    payload: dict[str, Any] | None = None
    updates: dict[str, Any] | None = None
    limit: int | None = None


@app.get("/api/health")
def root() -> dict[str, str]:
    return {"message": "Chiral DB Server is running."}


@app.post("/ingest")
async def ingest_endpoint(request: IngestRequest) -> dict[str, Any]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    return await chiral.ingest(session_id=request.data["session_id"], data=request.data)


@app.post("/flush/{session_id}")
async def flush_endpoint(session_id: str) -> dict[str, int]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    return await chiral.flush(session_id)


@app.post("/query/translate")
async def translate_query_endpoint(request: QueryTranslateRequest) -> dict[str, Any]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    try:
        return await chiral.translate_only(request.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {exc!s}") from exc


@app.post("/query/execute")
async def execute_query_endpoint(request: QueryTranslateRequest) -> dict[str, Any]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    try:
        return await chiral.query(request.model_dump(exclude_none=True))
    except CreateExecutionValidationError as exc:
        raise HTTPException(status_code=400, detail={"mode": "failed_validation", "error": str(exc)}) from exc
    except ValueError as exc:
        # Gracefully catch invalid identifiers (like comments.text) and return to the UI
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {exc!s}") from exc


@app.get("/schema/logical/{session_id}")
async def logical_schema_endpoint(session_id: str) -> list[str]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    return await chiral.get_logical_schema(session_id)


@app.get("/sessions/active")
async def active_sessions_endpoint() -> list[str]:
    if chiral is None:
        raise HTTPException(status_code=500, detail="Database client not initialized")
    return await chiral.get_active_sessions()


# --- Dashboard UI Serving ---
root_dir = Path(__file__).resolve().parent.parent.parent
dashboard_dist = root_dir / "webapp" / "dashboard" / "dist"

if dashboard_dist.exists():
    app.mount("/assets", StaticFiles(directory=dashboard_dist / "assets"), name="assets")

    @app.get("/{catchall:path}", include_in_schema=False)
    async def serve_dashboard(catchall: str) -> FileResponse:  # noqa: ARG001
        """Serve the React SPA index.html."""
        index_file = dashboard_dist / "index.html"
        if not index_file.exists():
            raise HTTPException(status_code=404, detail="Dashboard build missing.")
        return FileResponse(index_file)
else:
    logger.warning("React Dashboard not found at %s", dashboard_dist)
