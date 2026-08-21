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

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from ..models.table import DatabaseSummary, TableMetadata
from ..services.duckdb_service import DuckDBService, UTC_OFFSETS, resolve_database_path, list_available_databases

router = APIRouter(prefix="/tables", tags=["Discovery"])

def get_duckdb(db: Optional[str] = Query(None, description="Database ID or alias")) -> DuckDBService:
    resolved_path = resolve_database_path(db)
    return DuckDBService(resolved_path)

@router.get("", response_model=DatabaseSummary)
async def list_tables(
    db: Optional[str] = Query(None, description="Database ID"),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """Returns database summary, table categories, row counts, and column counts."""
    return duckdb_svc.get_database_summary()

@router.get("/meta/databases", tags=["Metadata"])
async def get_available_databases():
    """Returns all discovered DuckDB database stores with metadata and row counts."""
    dbs = list_available_databases()
    return {
        "databases": dbs,
        "count": len(dbs)
    }

@router.get("/meta/summaries", tags=["Metadata"])
async def get_all_database_summaries() -> Dict[str, Any]:
    """Returns schemas and table summaries for all registered databases in a single fast call."""
    dbs = list_available_databases()
    summaries: Dict[str, Any] = {}
    for d in dbs:
        db_id = d.get("id")
        if not db_id:
            continue
        try:
            svc = DuckDBService(resolve_database_path(db_id))
            summaries[db_id] = svc.get_database_summary().model_dump()
        except Exception:
            summaries[db_id] = {
                "database_file": d.get("file_path", ""),
                "total_tables": 0,
                "total_rows": 0,
                "categories": {},
                "tables": []
            }
    return summaries

@router.get("/{table_name}/schema", response_model=TableMetadata)
async def get_table_schema(
    table_name: str,
    db: Optional[str] = Query(None, description="Database ID"),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """Returns column information, types, and filter classification for a table."""
    try:
        return duckdb_svc.get_table_metadata(table_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch table schema: {e}")

@router.get("/meta/offsets", tags=["Metadata"])
async def list_utc_offsets():
    """Returns available UTC offsets with numerical shifts."""
    sorted_offsets = sorted(UTC_OFFSETS.keys(), key=lambda x: UTC_OFFSETS[x])
    return {
        "offsets": sorted_offsets,
        "default": "UTC +00:00",
        "mapping": UTC_OFFSETS
    }
