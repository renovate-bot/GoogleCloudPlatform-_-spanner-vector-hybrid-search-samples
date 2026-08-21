# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from pathlib import Path
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Dict, Any, List

from ..services.gemini_service import GeminiService
from ..services.duckdb_service import DuckDBService
from ..services.store import ResourceStore

router = APIRouter(prefix="/agent", tags=["AI DBRE Agents"])

CONFIG_STORE = ResourceStore(Path("backend/data/config.json"))

def get_gemini() -> GeminiService:
    api_key = CONFIG_STORE.get("google_api_key")
    return GeminiService(api_key=api_key, staging_dir="staging")

def get_duckdb() -> DuckDBService:
    return DuckDBService("my_duckdb.db")

@router.get("/schema/preview")
async def get_schema_preview(gemini_svc: GeminiService = Depends(get_gemini)):
    """Fetches staging schema.sql content snippet for the preview panel."""
    content = gemini_svc.get_schema_content()
    return {
        "exists": bool(content),
        "total_chars": len(content),
        "preview": content[:2000] if content else "",
        "is_configured": gemini_svc.is_configured()
    }

@router.get("/schema/stream")
async def stream_schema_analysis(gemini_svc: GeminiService = Depends(get_gemini)):
    """
    Streams Gemini 2.0 Flash DBRE schema evaluation via Server-Sent Events (SSE).
    """
    async def event_generator():
        try:
            async for chunk in gemini_svc.stream_schema_analysis():
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@router.get("/query-profile/queries")
async def get_high_row_scan_queries(duckdb_svc: DuckDBService = Depends(get_duckdb)):
    """
    Fetches the list of high row-scan queries (>100k rows, >10 execs) for visual cards.
    """
    try:
        queries = duckdb_svc.get_high_row_scan_queries()
        return {
            "count": len(queries),
            "queries": queries
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch high row scan queries: {e}")

@router.get("/query-profile/stream")
async def stream_query_profile_analysis(
    gemini_svc: GeminiService = Depends(get_gemini),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Streams Gemini 2.0 Flash DBRE root-cause analysis for high row scans via SSE.
    """
    async def event_generator():
        try:
            stats = duckdb_svc.get_high_row_scan_queries()
            async for chunk in gemini_svc.stream_query_profile_analysis(stats):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
