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

from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from ..models.query import TableQueryRequest, QueryResultPage
from ..models.table import TableProfilesResponse
from ..services.duckdb_service import DuckDBService, resolve_database_path

router = APIRouter(prefix="/tables", tags=["Data Engine"])

def get_duckdb(db: Optional[str] = Query(None, description="Database ID or alias")) -> DuckDBService:
    resolved_path = resolve_database_path(db)
    return DuckDBService(resolved_path)

@router.get("/{table_name}/profiles", response_model=TableProfilesResponse)
async def get_table_column_profiles(
    table_name: str,
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Computes fast columnar summary profiles and histograms for all columns in the table.
    """
    try:
        profiles_data = duckdb_svc.get_table_column_profiles(table_name)
        return profiles_data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute column profiles: {e}")

@router.post("/{table_name}/query", response_model=QueryResultPage)
async def query_table(
    table_name: str,
    req: TableQueryRequest,
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Executes a high-performance pushdown query on DuckDB.
    Returns paginated rows, total matching count, duration, and the exact executed SQL.
    """
    try:
        result = duckdb_svc.query_table(
            table_name=table_name,
            page=req.page,
            page_size=req.page_size,
            sort=req.sort,
            filters=req.filters,
            utc_offset=req.utc_offset,
            global_search=req.global_search
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")

@router.get("/{table_name}/intervals")
async def get_table_intervals(
    table_name: str,
    utc_offset: float = Query(0.0, description="UTC offset in hours"),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Fetches distinct interval_end timestamps for time-slice filtering.
    """
    try:
        intervals = duckdb_svc.get_distinct_intervals(table_name, utc_offset)
        return {
            "table": table_name,
            "utc_offset": utc_offset,
            "count": len(intervals),
            "intervals": intervals
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch intervals: {e}")

@router.get("/{table_name}/timeline")
async def get_table_timeline(
    table_name: str,
    utc_offset: float = Query(0.0, description="UTC offset in hours"),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Fetches time-series histogram buckets with record counts per interval_end.
    """
    try:
        buckets = duckdb_svc.get_interval_histogram(table_name, utc_offset)
        total_records = sum(b['count'] for b in buckets)
        return {
            "table": table_name,
            "utc_offset": utc_offset,
            "total_intervals": len(buckets),
            "total_records": total_records,
            "buckets": buckets
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch timeline: {e}")

@router.post("/{table_name}/export")
async def export_table_csv(
    table_name: str,
    req: TableQueryRequest,
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Streams the filtered/sorted dataset as a CSV download without loading the entire result into memory.
    """
    try:
        generator = duckdb_svc.export_csv_stream(
            table_name=table_name,
            sort=req.sort,
            filters=req.filters,
            utc_offset=req.utc_offset,
            global_search=req.global_search,
            chunk_size=5000
        )
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_export_{timestamp_str}.csv"

        return StreamingResponse(
            generator,
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Filename": filename
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")

@router.get("/{table_name}/scatter/defaults")
async def get_scatter_defaults(
    table_name: str,
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Returns smart column defaults and all numeric columns for scatter outlier analysis.
    """
    try:
        return duckdb_svc.get_scatter_defaults(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch scatter defaults: {e}")

@router.post("/{table_name}/scatter")
async def query_scatter_data(
    table_name: str,
    req: TableQueryRequest,
    x_col: Optional[str] = Query(None),
    y_col: Optional[str] = Query(None),
    size_col: Optional[str] = Query(None),
    label_col: Optional[str] = Query(None),
    limit: int = Query(300),
    duckdb_svc: DuckDBService = Depends(get_duckdb)
):
    """
    Queries 2D/3D scatter plot outlier points with pushdown filtering.
    """
    try:
        # Fall back to defaults if not provided in query params
        if not x_col or not y_col:
            defaults = duckdb_svc.get_scatter_defaults(table_name)
            x_col = x_col or defaults.get("x_col")
            y_col = y_col or defaults.get("y_col")
            size_col = size_col or defaults.get("size_col")
            label_col = label_col or defaults.get("label_col")

        if not x_col or not y_col:
            return {"points": [], "count": 0, "x_col": None, "y_col": None}

        points = duckdb_svc.get_scatter_data(
            table_name=table_name,
            x_col=x_col,
            y_col=y_col,
            size_col=size_col,
            label_col=label_col,
            filters=req.filters,
            utc_offset=req.utc_offset,
            limit=limit
        )

        return {
            "table": table_name,
            "x_col": x_col,
            "y_col": y_col,
            "size_col": size_col,
            "label_col": label_col,
            "count": len(points),
            "points": points
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query scatter data: {e}")

