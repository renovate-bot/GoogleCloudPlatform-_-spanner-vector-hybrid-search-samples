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
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..models.connection import (
    DatabaseConnection,
    CreateConnectionRequest,
    UpdateConnectionRequest,
    StagingFolderItem
)
from ..services.connection_service import ConnectionService

router = APIRouter(prefix="/connections", tags=["Database Connections"])
service = ConnectionService()

@router.get("", response_model=List[DatabaseConnection])
async def list_connections():
    """Lists all registered database connections with live DuckDB stats."""
    return service.list_connections()

@router.post("", response_model=DatabaseConnection)
async def create_connection(req: CreateConnectionRequest):
    """Creates and registers a new database connection."""
    conn = service.create_connection(req)
    if req.type == "local_staging" and req.auto_ingest:
        try:
            service.ingest_staging_into_duckdb(conn.id)
            conn = service.get_connection(conn.id)
        except Exception as e:
            # Staging directory might not have files yet
            pass
    return conn

@router.get("/staging-folders", response_model=List[StagingFolderItem])
async def list_staging_folders():
    """Scans and lists subdirectories under staging/."""
    return service.scan_staging_folders()

@router.get("/{connection_id}", response_model=DatabaseConnection)
async def get_connection(connection_id: str):
    """Retrieves a database connection by ID."""
    conn = service.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return conn

@router.put("/{connection_id}", response_model=DatabaseConnection)
async def update_connection(connection_id: str, req: UpdateConnectionRequest):
    """Updates connection properties."""
    conn = service.update_connection(connection_id, req)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return conn

@router.delete("/{connection_id}")
async def delete_connection(
    connection_id: str,
    delete_duckdb: bool = Query(True, description="Delete local DuckDB file"),
    delete_staging: bool = Query(False, description="Delete staging directory files")
):
    """Deletes a database connection and optionally its DuckDB database and staging files."""
    success = service.delete_connection(
        connection_id,
        delete_duckdb=delete_duckdb,
        delete_staging=delete_staging
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return {"success": True, "message": f"Connection '{connection_id}' removed"}

@router.post("/{connection_id}/sync")
async def sync_connection(connection_id: str):
    """Triggers ingestion for a connection from staging or GCP."""
    conn = service.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    try:
        res = service.ingest_staging_into_duckdb(connection_id)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{connection_id}/sync/stream")
async def sync_connection_stream(connection_id: str):
    """Streams live ingestion logs for a specific connection via SSE."""
    conn = service.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    async def event_generator():
        yield f"data: {json.dumps({'log': f'🚀 Starting synchronization for {conn.name} ({conn.id})...'})}\n\n"
        await asyncio.sleep(0.05)

        queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def log_cb(msg: str):
            loop.call_soon_threadsafe(queue.put_nowait, {"type": "log", "msg": msg})

        async def run_sync_task():
            try:
                res = await loop.run_in_executor(
                    None,
                    lambda: service.ingest_staging_into_duckdb(connection_id, log_callback=log_cb)
                )
                await queue.put({"type": "done", "res": res})
            except Exception as ex:
                await queue.put({"type": "error", "error": str(ex)})

        task = asyncio.create_task(run_sync_task())

        while True:
            item = await queue.get()
            item_type = item.get("type")

            if item_type == "log":
                yield f"data: {json.dumps({'log': item['msg']})}\n\n"
            elif item_type == "error":
                err_msg = item.get("error", "Unknown error")
                yield f"data: {json.dumps({'log': f'❌ Ingestion failed: {err_msg}', 'error': True, 'done': True})}\n\n"
                break
            elif item_type == "done":
                yield f"data: {json.dumps({'log': f'🎉 Successfully synchronized database for {conn.name}!', 'done': True})}\n\n"
                break

        await task

    return StreamingResponse(event_generator(), media_type="text/event-stream")
