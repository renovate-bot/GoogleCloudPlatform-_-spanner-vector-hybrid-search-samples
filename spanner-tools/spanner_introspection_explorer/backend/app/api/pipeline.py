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

import os
import glob
import json
import asyncio
from typing import Optional, List, Dict, Any
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ..models.config import AppConfig, UpdateConfigRequest
from ..services.store import ResourceStore
from ..services.duckdb_service import DuckDBService

router = APIRouter(prefix="/pipeline", tags=["Data Pipeline"])

CONFIG_STORE = ResourceStore(Path("backend/data/config.json"))

@router.get("/staging")
async def list_staging_files(db: Optional[str] = None):
    """Lists files in staging directory or specific database subdirectory."""
    staging_base = Path("staging")
    if not staging_base.exists():
        return {"files": [], "databases": []}

    target_dir = staging_base / db if db and (staging_base / db).is_dir() else staging_base
    files = []
    for p in target_dir.glob("*.csv"):
        stat = p.stat()
        files.append({
            "name": p.name,
            "database_folder": p.parent.name if p.parent != staging_base else "default",
            "size_bytes": stat.st_size,
            "size_human": f"{stat.st_size / (1024 * 1024):.2f} MB" if stat.st_size > 1024*1024 else f"{stat.st_size / 1024:.1f} KB",
            "modified": stat.st_mtime
        })

    # Also list discovered staging subdirectories
    subdirs = [d.name for d in staging_base.iterdir() if d.is_dir()]

    return {
        "target_dir": str(target_dir),
        "databases": subdirs,
        "files": sorted(files, key=lambda x: x['name'])
    }

@router.get("/reload/stream")
@router.post("/reload/stream")
async def stream_reload_duckdb(db: Optional[str] = None):
    """
    Executes load_to_duckdb.py ingestion process for all databases or a specific database and streams stdout via SSE.
    """
    async def run_ingestion():
        import duckdb
        from ..services.duckdb_service import DATABASE_BASE_DIR, resolve_database_path

        from ..services.connection_service import ConnectionService
        conn_svc = ConnectionService()
        conn = conn_svc.get_connection(db) if db else None

        if conn:
            queue: asyncio.Queue = asyncio.Queue()
            loop = asyncio.get_running_loop()

            def log_cb(msg: str):
                loop.call_soon_threadsafe(queue.put_nowait, {"type": "log", "msg": msg})

            async def run_task():
                try:
                    res = await loop.run_in_executor(
                        None,
                        lambda: conn_svc.ingest_staging_into_duckdb(conn.id, log_callback=log_cb)
                    )
                    await queue.put({"type": "done", "res": res})
                except Exception as ex:
                    await queue.put({"type": "error", "error": str(ex)})

            task = asyncio.create_task(run_task())
            while True:
                item = await queue.get()
                if item.get("type") == "log":
                    yield f"data: {json.dumps({'log': item['msg']})}\n\n"
                elif item.get("type") == "error":
                    err_msg = item.get("error", "Unknown error")
                    yield f"data: {json.dumps({'log': f'❌ Ingestion failed: {err_msg}', 'done': True, 'success': False})}\n\n"
                    break
                elif item.get("type") == "done":
                    yield f"data: {json.dumps({'log': f'🎉 Ingestion complete for {conn.name}!', 'done': True, 'success': True})}\n\n"
                    break
            await task
            return

        os.makedirs("backend/data/dbs", exist_ok=True)
        prefix_to_remove = "export_all_"

        # Target single database or all databases
        if db:
            staging_target = Path("staging") / db if (Path("staging") / db).is_dir() else Path("staging")
            db_target = os.path.join("backend/data/dbs", f"{db}.duckdb") if db != "legacy" else "my_duckdb.db"
            targets = [(str(staging_target), db_target)]
        else:
            targets = []
            # Root staging
            if glob.glob("staging/*.csv"):
                targets.append(("staging", "my_duckdb.db"))
                targets.append(("staging", "backend/data/dbs/default.duckdb"))
            # Subdirectories
            for s in Path("staging").iterdir():
                if s.is_dir():
                    targets.append((str(s), os.path.join("backend/data/dbs", f"{s.name}.duckdb")))

        if not targets:
            yield f"data: {json.dumps({'log': '⚠️ Warning: No CSV files found in staging/'})}\n\n"
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
            return

        total_loaded = 0
        for staging_dir, db_file in targets:
            csv_files = glob.glob(os.path.join(staging_dir, "*.csv"))
            if not csv_files:
                continue

            yield f"data: {json.dumps({'log': f'Connecting to DuckDB: {db_file} (from {staging_dir}/)'})}\n\n"
            await asyncio.sleep(0.05)

            try:
                con = duckdb.connect(database=db_file)
                for file_path in csv_files:
                    base_name = os.path.basename(file_path)
                    if base_name.startswith(prefix_to_remove):
                        table_name = os.path.splitext(base_name[len(prefix_to_remove):])[0]
                    else:
                        table_name = os.path.splitext(base_name)[0]

                    yield f"data: {json.dumps({'log': f'  -> Ingesting {base_name} into {table_name}...' })}\n\n"
                    await asyncio.sleep(0.02)

                    sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}');"
                    con.execute(sql)
                    total_loaded += 1

                con.close()
                yield f"data: {json.dumps({'log': f'  ✓ Loaded {len(csv_files)} tables into {db_file}'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'log': f'❌ Ingestion error on {db_file}: {str(e)}'})}\n\n"

        yield f"data: {json.dumps({'log': f'✅ Completed ingestion! {total_loaded} total table operations processed.', 'done': True, 'success': True})}\n\n"

    return StreamingResponse(
        run_ingestion(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@router.get("/config")
async def get_config():
    """Fetches user configuration settings."""
    cfg = CONFIG_STORE.get_all()
    # Mask API key if present
    key = cfg.get("google_api_key") or os.environ.get("GOOGLE_API_KEY")
    masked_key = f"{key[:4]}...{key[-4:]}" if key and len(key) > 8 else ("Configured" if key else "")
    return {
        "google_api_key_configured": bool(key),
        "google_api_key_preview": masked_key,
        "database_file": cfg.get("database_file", "my_duckdb.db"),
        "staging_dir": cfg.get("staging_dir", "staging"),
        "default_utc_offset": cfg.get("default_utc_offset", "UTC +00:00"),
        "max_display_rows": cfg.get("max_display_rows", 1000000)
    }

@router.post("/config")
async def update_config(req: UpdateConfigRequest):
    """Updates settings in thread-safe config store."""
    if req.google_api_key is not None:
        CONFIG_STORE.update("google_api_key", req.google_api_key)
    if req.default_utc_offset is not None:
        CONFIG_STORE.update("default_utc_offset", req.default_utc_offset)
    if req.max_display_rows is not None:
        CONFIG_STORE.update("max_display_rows", req.max_display_rows)
    return {"status": "success", "message": "Settings updated successfully."}
