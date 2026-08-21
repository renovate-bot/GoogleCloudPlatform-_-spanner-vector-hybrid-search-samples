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
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Tuple
import duckdb
import pandas as pd

from ..models.query import ColumnFilter, SortConfig
from ..models.table import ColumnMetadata, DatabaseSummary, TableCategory, TableMetadata, TableSummary

UTC_OFFSETS: Dict[str, float] = {
    'UTC +00:00': 0.0,
    'UTC +01:00': 1.0,
    'UTC +02:00': 2.0,
    'UTC +03:00': 3.0,
    'UTC +03:30': 3.5,
    'UTC +04:00': 4.0,
    'UTC +04:30': 4.5,
    'UTC +05:00': 5.0,
    'UTC +05:30': 5.5,
    'UTC +05:45': 5.75,
    'UTC +06:00': 6.0,
    'UTC +06:30': 6.5,
    'UTC +07:00': 7.0,
    'UTC +08:00': 8.0,
    'UTC +09:00': 9.0,
    'UTC +09:30': 9.5,
    'UTC +10:00': 10.0,
    'UTC +10:30': 10.5,
    'UTC +11:00': 11.0,
    'UTC +12:00': 12.0,
    'UTC +12:45': 12.75,
    'UTC +13:00': 13.0,
    'UTC +14:00': 14.0,
    'UTC -01:00': -1.0,
    'UTC -02:00': -2.0,
    'UTC -03:00': -3.0,
    'UTC -03:30': -3.5,
    'UTC -04:00': -4.0,
    'UTC -05:00': -5.0,
    'UTC -06:00': -6.0,
    'UTC -07:00': -7.0,
    'UTC -08:00': -8.0,
    'UTC -09:00': -9.0,
    'UTC -09:30': -9.5,
    'UTC -10:00': -10.0,
    'UTC -11:00': -11.0,
    'UTC -12:00': -12.0,
}

def categorize_table(table_name: str) -> TableCategory:
    if 'LOCK_STATISTICS' in table_name:
        return 'Locking'
    elif 'QUERY_STATS' in table_name:
        return 'Query'
    elif 'TXN_STATS' in table_name:
        return 'Transactions'
    return 'Misc'

def infer_filter_type(dtype_str: str) -> str:
    d = str(dtype_str).lower()
    if any(k in d for k in ['int', 'float', 'double', 'decimal', 'numeric', 'hugeint', 'bigint', 'real']):
        return 'numeric'
    elif any(k in d for k in ['date', 'timestamp', 'time']):
        return 'date'
    return 'text'

DATABASE_BASE_DIR = "backend/data/dbs"

def resolve_database_path(db_id: Optional[str] = None) -> str:
    """
    Resolves a database identifier from registered connections.
    """
    from .connection_service import ConnectionService
    conn_svc = ConnectionService()
    if db_id:
        conn = conn_svc.get_connection(db_id)
        if conn:
            return conn.duckdb_path
        if os.path.exists(db_id):
            return db_id
        cand = os.path.join("backend/data/dbs", f"{db_id}.duckdb")
        if os.path.exists(cand):
            return cand

    all_conns = conn_svc.list_connections()
    if all_conns:
        return all_conns[0].duckdb_path
    return "backend/data/dbs/default.duckdb"

def list_available_databases() -> List[Dict[str, Any]]:
    """
    Returns available databases strictly from registered connections.
    """
    from .connection_service import ConnectionService
    conns = ConnectionService().list_connections()
    dbs = []
    for c in conns:
        dbs.append({
            "id": c.id,
            "name": c.name,
            "file_path": c.duckdb_path,
            "total_rows": c.total_rows,
            "total_tables": c.total_tables,
            "size_mb": c.size_mb,
            "last_modified": c.last_synced_at or c.created_at,
            "is_default": False,
            "status": c.status
        })
    return dbs

class DuckDBService:
    def __init__(self, db_path: str = "my_duckdb.db"):
        self.db_path = db_path

    def _get_connection(self, read_only: bool = True):
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"DuckDB database file '{self.db_path}' does not exist.")
        return duckdb.connect(database=self.db_path, read_only=read_only)

    def database_exists(self) -> bool:
        return os.path.exists(self.db_path)

    def get_table_names(self) -> List[str]:
        if not self.database_exists():
            return []
        try:
            with self._get_connection() as conn:
                df = conn.execute("SHOW TABLES;").fetchdf()
                return df['name'].tolist() if not df.empty and 'name' in df.columns else []
        except Exception:
            return []

    def get_database_summary(self) -> DatabaseSummary:
        if not self.database_exists():
            return DatabaseSummary(
                database_file=self.db_path,
                total_tables=0,
                total_rows=0,
                categories={"Locking": [], "Query": [], "Transactions": [], "Misc": []},
                tables=[]
            )

        table_names = self.get_table_names()
        categories: Dict[str, List[str]] = {"Locking": [], "Query": [], "Transactions": [], "Misc": []}
        tables_summary: List[TableSummary] = []
        total_rows_all = 0

        with self._get_connection() as conn:
            for name in sorted(table_names):
                cat = categorize_table(name)
                categories[cat].append(name)
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0]
                    cols_count = conn.execute(f"SELECT COUNT(*) FROM (DESCRIBE {name});").fetchone()[0]
                except Exception:
                    count = 0
                    cols_count = 0
                
                total_rows_all += count
                tables_summary.append(TableSummary(
                    name=name,
                    category=cat,
                    row_count=count,
                    column_count=cols_count,
                    is_large=count > 1000000
                ))

        return DatabaseSummary(
            database_file=self.db_path,
            total_tables=len(table_names),
            total_rows=total_rows_all,
            categories=categories,
            tables=tables_summary
        )

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        with self._get_connection() as conn:
            # Validate table exists to prevent SQL injection
            tables = conn.execute("SHOW TABLES;").fetchdf()['name'].tolist()
            if table_name not in tables:
                raise ValueError(f"Table '{table_name}' does not exist.")

            info_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            columns: List[ColumnMetadata] = []
            for _, row in info_df.iterrows():
                col_name = str(row['column_name'])
                col_type = str(row['column_type'])
                columns.append(ColumnMetadata(
                    name=col_name,
                    type=col_type,
                    filter_type=infer_filter_type(col_type)
                ))

            return TableMetadata(
                name=table_name,
                category=categorize_table(table_name),
                row_count=count,
                column_count=len(columns),
                columns=columns
            )

    def get_distinct_intervals(self, table_name: str, utc_offset: float = 0.0) -> List[Dict[str, str]]:
        """Returns distinct interval_end timestamps formatted for display and UTC."""
        with self._get_connection() as conn:
            columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            interval_col = None
            for col in columns_df['column_name']:
                if col.lower() == 'interval_end':
                    interval_col = col
                    break

            if not interval_col:
                return []

            query = f"SELECT DISTINCT strftime({interval_col}, '%Y-%m-%d %H:%M:%S') AS raw_ts FROM {table_name} WHERE {interval_col} IS NOT NULL ORDER BY raw_ts DESC"
            result_df = conn.execute(query).fetchdf()

            timestamps: List[Dict[str, str]] = []
            if not result_df.empty and 'raw_ts' in result_df.columns:
                for raw_ts in result_df['raw_ts']:
                    utc_str = str(raw_ts)
                    if utc_offset != 0:
                        utc_dt = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S')
                        display_dt = utc_dt + timedelta(hours=utc_offset)
                        display_str = display_dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        display_str = utc_str

                    timestamps.append({
                        "display": display_str,
                        "utc": utc_str
                    })

            return timestamps

    def get_interval_histogram(self, table_name: str, utc_offset: float = 0.0) -> List[Dict[str, Any]]:
        """Returns time-series histogram buckets with record counts per interval_end."""
        with self._get_connection() as conn:
            columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            interval_col = None
            for col in columns_df['column_name']:
                if col.lower() == 'interval_end':
                    interval_col = col
                    break

            if not interval_col:
                return []

            query = f"""
            SELECT 
              strftime({interval_col}, '%Y-%m-%d %H:%M:%S') AS raw_ts,
              COUNT(*) AS record_count
            FROM {table_name}
            WHERE {interval_col} IS NOT NULL
            GROUP BY {interval_col}
            ORDER BY {interval_col} ASC
            """

            result_df = conn.execute(query).fetchdf()
            if result_df.empty:
                return []

            buckets = []
            for _, row in result_df.iterrows():
                utc_str = str(row['raw_ts'])
                if utc_offset != 0:
                    utc_dt = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S')
                    display_dt = utc_dt + timedelta(hours=utc_offset)
                    display_str = display_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    display_str = utc_str

                buckets.append({
                    "utc": utc_str,
                    "display": display_str,
                    "count": int(row['record_count'])
                })

            return buckets

    def get_table_column_profiles(self, table_name: str) -> Dict[str, Any]:
        """
        Computes fast columnar summary profiles and histograms for all columns in the table using DuckDB.
        """
        with self._get_connection() as conn:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
                [table_name]
            ).fetchall()
            if not tables:
                raise ValueError(f"Table '{table_name}' does not exist.")

            total_rows = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            if total_rows == 0:
                return {"table": table_name, "total_rows": 0, "profiles": {}}

            columns_df = conn.execute(f'DESCRIBE "{table_name}"').fetchdf()
            profiles: Dict[str, Dict[str, Any]] = {}

            for _, row in columns_df.iterrows():
                col_name = row['column_name']
                col_type = row['column_type']
                filter_type = infer_filter_type(col_type)

                profile: Dict[str, Any] = {
                    "name": col_name,
                    "column_type": col_type,
                    "filter_type": filter_type,
                    "total_count": total_rows,
                    "null_count": 0,
                    "distinct_count": 0,
                }

                escaped_col = f'"{col_name}"'

                try:
                    if filter_type == 'numeric':
                        stats_sql = f"""
                            SELECT 
                                COUNT(*) - COUNT({escaped_col}) AS null_cnt,
                                APPROX_COUNT_DISTINCT({escaped_col}) AS dist_cnt,
                                MIN({escaped_col}) AS min_v,
                                MAX({escaped_col}) AS max_v,
                                AVG({escaped_col}) AS avg_v
                            FROM "{table_name}"
                        """
                        stats_row = conn.execute(stats_sql).fetchone()
                        null_cnt = stats_row[0] or 0
                        dist_cnt = stats_row[1] or 0
                        min_v = stats_row[2]
                        max_v = stats_row[3]
                        avg_v = stats_row[4]

                        profile["null_count"] = int(null_cnt)
                        profile["distinct_count"] = int(dist_cnt)
                        profile["min_value"] = float(min_v) if min_v is not None else None
                        profile["max_value"] = float(max_v) if max_v is not None else None
                        profile["avg_value"] = round(float(avg_v), 4) if avg_v is not None else None

                        histogram_buckets = []
                        if min_v is not None and max_v is not None and max_v > min_v:
                            num_bins = 10
                            bin_width = (float(max_v) - float(min_v)) / num_bins
                            hist_sql = f"""
                                SELECT 
                                    CASE 
                                        WHEN {escaped_col} = {float(max_v)} THEN {num_bins - 1}
                                        ELSE LEAST({num_bins - 1}, GREATEST(0, CAST(FLOOR(({escaped_col} - {float(min_v)}) / {bin_width}) AS INTEGER)))
                                    END AS bin_idx,
                                    COUNT(*) AS cnt
                                FROM "{table_name}"
                                WHERE {escaped_col} IS NOT NULL
                                GROUP BY bin_idx
                                ORDER BY bin_idx
                            """
                            hist_df = conn.execute(hist_sql).fetchdf()
                            counts_map = dict(zip(hist_df['bin_idx'], hist_df['cnt']))

                            for b_idx in range(num_bins):
                                b_min = float(min_v) + b_idx * bin_width
                                b_max = float(min_v) + (b_idx + 1) * bin_width
                                histogram_buckets.append({
                                    "bin_index": b_idx,
                                    "bin_min": round(b_min, 4),
                                    "bin_max": round(b_max, 4),
                                    "count": int(counts_map.get(b_idx, 0))
                                })
                        elif min_v is not None and max_v is not None:
                            histogram_buckets.append({
                                "bin_index": 0,
                                "bin_min": float(min_v),
                                "bin_max": float(max_v),
                                "count": total_rows - null_cnt
                            })

                        profile["histogram"] = histogram_buckets

                    elif filter_type == 'text':
                        stats_sql = f"""
                            SELECT 
                                COUNT(*) - COUNT({escaped_col}) AS null_cnt,
                                APPROX_COUNT_DISTINCT({escaped_col}) AS dist_cnt
                            FROM "{table_name}"
                        """
                        stats_row = conn.execute(stats_sql).fetchone()
                        profile["null_count"] = int(stats_row[0] or 0)
                        profile["distinct_count"] = int(stats_row[1] or 0)

                        top_sql = f"""
                            SELECT 
                                CAST({escaped_col} AS VARCHAR) AS val,
                                COUNT(*) AS cnt
                            FROM "{table_name}"
                            WHERE {escaped_col} IS NOT NULL
                            GROUP BY val
                            ORDER BY cnt DESC
                            LIMIT 4
                        """
                        top_df = conn.execute(top_sql).fetchdf()
                        top_cats = []
                        for _, top_row in top_df.iterrows():
                            cnt = int(top_row['cnt'])
                            pct = round((cnt / total_rows) * 100, 1) if total_rows > 0 else 0
                            raw_val = str(top_row['val'])
                            disp_val = (raw_val[:32] + '...') if len(raw_val) > 35 else raw_val
                            top_cats.append({
                                "value": raw_val,
                                "display_value": disp_val,
                                "count": cnt,
                                "percent": pct
                            })
                        profile["top_categories"] = top_cats

                    elif filter_type == 'date':
                        stats_sql = f"""
                            SELECT 
                                COUNT(*) - COUNT({escaped_col}) AS null_cnt,
                                APPROX_COUNT_DISTINCT({escaped_col}) AS dist_cnt,
                                MIN(strftime({escaped_col}, '%Y-%m-%d %H:%M:%S')) AS min_d,
                                MAX(strftime({escaped_col}, '%Y-%m-%d %H:%M:%S')) AS max_d
                            FROM "{table_name}"
                        """
                        stats_row = conn.execute(stats_sql).fetchone()
                        profile["null_count"] = int(stats_row[0] or 0)
                        profile["distinct_count"] = int(stats_row[1] or 0)
                        profile["min_date"] = str(stats_row[2]) if stats_row[2] else None
                        profile["max_date"] = str(stats_row[3]) if stats_row[3] else None

                except Exception:
                    pass

                profiles[col_name] = profile

            return {
                "table": table_name,
                "total_rows": total_rows,
                "profiles": profiles
            }

    def _build_where_clause(
        self,
        table_name: str,
        filters: Dict[str, ColumnFilter],
        timestamp_cols: List[str],
        utc_offset: float,
        global_search: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> Tuple[str, List[Any]]:
        where_clauses = []
        params = []

        for col, f in filters.items():
            if f.type == 'text' and f.value:
                if f.operator == 'exact':
                    where_clauses.append(f'CAST("{col}" AS VARCHAR) = ?')
                    params.append(f.value)
                elif f.operator == 'not_exact':
                    where_clauses.append(f'(CAST("{col}" AS VARCHAR) != ? OR "{col}" IS NULL)')
                    params.append(f.value)
                elif f.operator == 'not_contains':
                    where_clauses.append(f'(CAST("{col}" AS VARCHAR) NOT ILIKE ? OR "{col}" IS NULL)')
                    params.append(f"%{f.value}%")
                else:
                    # Default 'contains' (case-insensitive substring)
                    where_clauses.append(f'CAST("{col}" AS VARCHAR) ILIKE ?')
                    params.append(f"%{f.value}%")
            elif f.type == 'numeric':
                if f.min is not None and str(f.min).strip() != '':
                    where_clauses.append(f'"{col}" >= ?')
                    params.append(float(f.min))
                if f.max is not None and str(f.max).strip() != '':
                    where_clauses.append(f'"{col}" <= ?')
                    params.append(float(f.max))
            elif f.type == 'date':
                if f.selected_timestamps and len(f.selected_timestamps) > 0:
                    ts_conditions = []
                    for ts in f.selected_timestamps:
                        utc_val = ts.split('|')[1] if '|' in ts else ts
                        ts_conditions.append(f'strftime("{col}", \'%Y-%m-%d %H:%M:%S\') = ?')
                        params.append(utc_val)
                    where_clauses.append(f"({' OR '.join(ts_conditions)})")
                else:
                    if f.min:
                        where_clauses.append(f'DATE("{col}") >= CAST(? AS DATE)')
                        params.append(str(f.min))
                    if f.max:
                        where_clauses.append(f'DATE("{col}") <= CAST(? AS DATE)')
                        params.append(str(f.max))

        if global_search and global_search.strip() and columns:
            search_terms = []
            for col in columns:
                search_terms.append(f"CAST({col} AS VARCHAR) ILIKE ?")
                params.append(f"%{global_search.strip()}%")
            where_clauses.append(f"({' OR '.join(search_terms)})")

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql, params

    def query_table(
        self,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
        sort: Optional[SortConfig] = None,
        filters: Optional[Dict[str, ColumnFilter]] = None,
        utc_offset: float = 0.0,
        global_search: Optional[str] = None
    ) -> Dict[str, Any]:
        start_time = time.perf_counter()

        with self._get_connection() as conn:
            # Validate table
            tables = conn.execute("SHOW TABLES;").fetchdf()['name'].tolist()
            if table_name not in tables:
                raise ValueError(f"Table '{table_name}' does not exist.")

            table_info = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            all_cols = table_info['column_name'].tolist()
            timestamp_cols = table_info[table_info['column_type'].str.contains('TIMESTAMP', case=False, na=False)]['column_name'].tolist()

            # Timezone projections
            select_parts = []
            for col in all_cols:
                if col in timestamp_cols:
                    if utc_offset != 0:
                        hours = int(utc_offset)
                        minutes = int((utc_offset - hours) * 60)
                        select_parts.append(f"strftime({col} + INTERVAL '{hours} hours {minutes} minutes', '%Y-%m-%d %H:%M:%S') AS {col}")
                    else:
                        select_parts.append(f"strftime({col}, '%Y-%m-%d %H:%M:%S') AS {col}")
                else:
                    select_parts.append(f'"{col}"')

            where_sql, params = self._build_where_clause(
                table_name, filters or {}, timestamp_cols, utc_offset, global_search, all_cols
            )

            # Count total matching rows (vectorized and instant)
            count_query = f"SELECT COUNT(*) FROM {table_name}{where_sql}"
            total_rows = conn.execute(count_query, params).fetchone()[0]

            # Sorting
            order_sql = ""
            if sort and sort.column and sort.column in all_cols:
                order_dir = "DESC" if sort.order.upper() == "DESC" else "ASC"
                order_sql = f' ORDER BY "{sort.column}" {order_dir}'

            # Pagination
            offset = (page - 1) * page_size
            limit_sql = f" LIMIT {page_size} OFFSET {offset}"

            query_sql = f"SELECT {', '.join(select_parts)} FROM {table_name}{where_sql}{order_sql}{limit_sql}"
            df = conn.execute(query_sql, params).fetchdf()

            # Format readable SQL for CLI exporter with substituted parameters
            readable_where = where_sql
            for p in params:
                if isinstance(p, (int, float)):
                    val_str = str(p)
                else:
                    escaped_val = str(p).replace("'", "''")
                    val_str = f"'{escaped_val}'"
                readable_where = readable_where.replace("?", val_str, 1)

            readable_query_sql = f"SELECT {', '.join(select_parts)} FROM {table_name}{readable_where}{order_sql}{limit_sql}"

            # Clean datetime objects for clean JSON serialization
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                elif pd.api.types.is_numeric_dtype(df[col]):
                    # Replace NaN / Inf with None for JSON compliance
                    df[col] = df[col].where(pd.notnull(df[col]), None)

            # Replace any other NaN
            records = df.to_dict(orient="records")
            for r in records:
                for k, v in r.items():
                    if pd.isna(v):
                        r[k] = None

            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            total_pages = (total_rows + page_size - 1) // page_size if total_rows > 0 else 1

            return {
                "items": records,
                "total": total_rows,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "executed_sql": readable_query_sql,
                "duration_ms": duration_ms
            }

    def export_csv_stream(
        self,
        table_name: str,
        sort: Optional[SortConfig] = None,
        filters: Optional[Dict[str, ColumnFilter]] = None,
        utc_offset: float = 0.0,
        global_search: Optional[str] = None,
        chunk_size: int = 5000
    ) -> Generator[str, None, None]:
        """Streams CSV rows efficiently without loading entire table into memory."""
        with self._get_connection() as conn:
            table_info = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            all_cols = table_info['column_name'].tolist()
            timestamp_cols = table_info[table_info['column_type'].str.contains('TIMESTAMP', case=False, na=False)]['column_name'].tolist()

            select_parts = []
            for col in all_cols:
                if col in timestamp_cols:
                    if utc_offset != 0:
                        hours = int(utc_offset)
                        minutes = int((utc_offset - hours) * 60)
                        select_parts.append(f"strftime({col} + INTERVAL '{hours} hours {minutes} minutes', '%Y-%m-%d %H:%M:%S') AS {col}")
                    else:
                        select_parts.append(f"strftime({col}, '%Y-%m-%d %H:%M:%S') AS {col}")
                else:
                    select_parts.append(f'"{col}"')

            where_sql, params = self._build_where_clause(
                table_name, filters or {}, timestamp_cols, utc_offset, global_search, all_cols
            )

            order_sql = ""
            if sort and sort.column and sort.column in all_cols:
                order_dir = "DESC" if sort.order.upper() == "DESC" else "ASC"
                order_sql = f' ORDER BY "{sort.column}" {order_dir}'

            query_sql = f"SELECT {', '.join(select_parts)} FROM {table_name}{where_sql}{order_sql}"

            # Yield CSV header
            cursor = conn.cursor()
            cursor.execute(query_sql, params)
            
            # Fetch in chunks
            is_first_chunk = True
            while True:
                df_chunk = cursor.fetch_df_chunk(chunk_size)
                if df_chunk is None or df_chunk.empty:
                    break
                
                # Format dates
                for col in df_chunk.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_chunk[col]):
                        df_chunk[col] = df_chunk[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

                csv_text = df_chunk.to_csv(index=False, header=is_first_chunk)
                is_first_chunk = False
                yield csv_text

    def get_high_row_scan_queries(self) -> List[Dict[str, Any]]:
        """Queries DuckDB for high row scan queries (>100k avg rows scanned, >10 execs)."""
        if not self.database_exists():
            return []

        try:
            with self._get_connection() as conn:
                tables = conn.execute("SHOW TABLES").fetchdf()['name'].tolist()

                q1 = "SELECT text AS text, TEXT_FINGERPRINT AS text_fingerprint, AVG_ROWS_SCANNED AS avg_rows_scanned, EXECUTION_COUNT AS execution_count, INTERVAL_END AS interval_end FROM QUERY_STATS_TOP_1HOUR"
                if "QUERY_STATS_TOP_1HOUR" not in tables:
                    q1 = "SELECT NULL as text, NULL as text_fingerprint, NULL as avg_rows_scanned, NULL as execution_count, NULL as interval_end WHERE 1=0"

                q2 = "SELECT text AS text, TEXT_FINGERPRINT AS text_fingerprint, AVG_ROWS_SCANNED AS avg_rows_scanned, EXECUTION_COUNT AS execution_count, INTERVAL_END AS interval_end FROM QUERY_STATS_TOP_10MINUTE"
                if "QUERY_STATS_TOP_10MINUTE" not in tables:
                    q2 = "SELECT NULL as text, NULL as text_fingerprint, NULL as avg_rows_scanned, NULL as execution_count, NULL as interval_end WHERE 1=0"

                safe_query = f"""
                SELECT
                    text,
                    text_fingerprint,
                    ROUND(AVG(avg_rows_scanned), 1) as avg_rows_scanned,
                    ROUND(MAX(avg_rows_scanned), 1) as max_rows_scanned,
                    SUM(execution_count) as total_exec,
                    LIST(CAST(interval_end AS VARCHAR)) as intervals
                FROM (
                    {q1}
                    UNION ALL
                    {q2}
                ) combined
                WHERE text IS NOT NULL
                  AND execution_count > 10
                  AND avg_rows_scanned > 100000
                  AND text NOT LIKE '%SPANNER_SYS%'
                GROUP BY text, text_fingerprint
                ORDER BY avg_rows_scanned DESC
                LIMIT 20
                """
                df = conn.execute(safe_query).fetchdf()
                return df.to_dict('records')
        except Exception:
            return []

    def get_scatter_defaults(self, table_name: str) -> Dict[str, Any]:
        """Detects smart default columns for outlier scatter plots based on table schema."""
        with self._get_connection() as conn:
            columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            cols = {row['column_name'].upper(): row['column_name'] for _, row in columns_df.iterrows()}
            numeric_types = ('INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'HUGEINT', 'SMALLINT', 'TINYINT', 'REAL')
            numeric_cols = [
                row['column_name'] for _, row in columns_df.iterrows()
                if any(t in str(row['column_type']).upper() for t in numeric_types)
            ]

            table_upper = table_name.upper()

            # Query Stats
            if 'QUERY_STATS' in table_upper:
                x_col = cols.get('EXECUTION_COUNT') or (numeric_cols[0] if numeric_cols else None)
                other_numeric = [c for c in numeric_cols if c != x_col]
                y_col = (
                    cols.get('AVG_LATENCY_SECONDS')
                    or cols.get('LATENCY_P50')
                    or cols.get('LATENCY_P90')
                    or cols.get('LATENCY_P95')
                    or cols.get('LATENCY_P99')
                    or cols.get('AVG_CPU_SECONDS')
                    or (other_numeric[0] if other_numeric else (numeric_cols[0] if numeric_cols else None))
                )
                size_col = (
                    cols.get('AVG_ROWS_SCANNED')
                    or cols.get('TOTAL_ROWS_SCANNED')
                    or cols.get('AVG_CPU_SECONDS')
                    or cols.get('CPU_TIME_SECONDS')
                    or (other_numeric[1] if len(other_numeric) > 1 else None)
                )
                label_col = cols.get('TEXT_FINGERPRINT') or cols.get('FINGERPRINT') or cols.get('TEXT')
                return {
                    "numeric_cols": numeric_cols,
                    "x_col": x_col,
                    "y_col": y_col,
                    "size_col": size_col,
                    "label_col": label_col,
                    "title": "Query Outliers: Execution Count vs Latency & Rows Scanned"
                }

            # Transaction Stats
            if 'TXN_STATS' in table_upper:
                x_col = cols.get('COMMIT_ATTEMPT_COUNT') or (numeric_cols[0] if numeric_cols else None)
                other_numeric = [c for c in numeric_cols if c != x_col]
                y_col = (
                    cols.get('AVG_COMMIT_LATENCY_SECONDS')
                    or cols.get('COMMIT_LATENCY_SECONDS')
                    or cols.get('LATENCY_P50')
                    or cols.get('LATENCY_P95')
                    or (other_numeric[0] if other_numeric else (numeric_cols[0] if numeric_cols else None))
                )
                size_col = cols.get('ABORT_COUNT') or cols.get('AVG_PARTICIPANTS') or (other_numeric[1] if len(other_numeric) > 1 else None)
                label_col = cols.get('FINGERPRINT') or cols.get('TRANSACTION_TAG')
                return {
                    "numeric_cols": numeric_cols,
                    "x_col": x_col,
                    "y_col": y_col,
                    "size_col": size_col,
                    "label_col": label_col,
                    "title": "Transaction Outliers: Commit Attempts vs Latency & Abort Volume"
                }

            # Lock Statistics
            if 'LOCK' in table_upper:
                x_col = cols.get('WAIT_COUNT') or cols.get('LOCK_WAIT_COUNT') or (numeric_cols[0] if numeric_cols else None)
                other_numeric = [c for c in numeric_cols if c != x_col]
                y_col = (
                    cols.get('TOTAL_WAIT_TIME_SECONDS')
                    or cols.get('AVG_WAIT_TIME_SECONDS')
                    or (other_numeric[0] if other_numeric else (numeric_cols[0] if numeric_cols else None))
                )
                size_col = other_numeric[1] if len(other_numeric) > 1 else (other_numeric[0] if other_numeric else None)
                label_col = cols.get('ROW_RANGE_START_KEY') or cols.get('LOCK_TARGET') or cols.get('ROW_KEY')
                return {
                    "numeric_cols": numeric_cols,
                    "x_col": x_col,
                    "y_col": y_col,
                    "size_col": size_col,
                    "label_col": label_col,
                    "title": "Lock Outliers: Wait Count vs Total Wait Time"
                }

            # Generic fallback
            x_col = numeric_cols[0] if len(numeric_cols) > 0 else None
            y_col = numeric_cols[1] if len(numeric_cols) > 1 else (numeric_cols[0] if numeric_cols else None)
            size_col = numeric_cols[2] if len(numeric_cols) > 2 else None
            text_cols = [row['column_name'] for _, row in columns_df.iterrows() if 'VARCHAR' in str(row['column_type']).upper()]
            label_col = text_cols[0] if text_cols else None

            return {
                "numeric_cols": numeric_cols,
                "x_col": x_col,
                "y_col": y_col,
                "size_col": size_col,
                "label_col": label_col,
                "title": f"Outlier Distribution: {table_name}"
            }

    def get_scatter_data(
        self,
        table_name: str,
        x_col: str,
        y_col: str,
        size_col: Optional[str] = None,
        label_col: Optional[str] = None,
        filters: Optional[Dict[str, ColumnFilter]] = None,
        utc_offset: float = 0.0,
        limit: int = 300
    ) -> List[Dict[str, Any]]:
        """Queries table for 2D/3D scatter plot outlier data points with pushdown filtering."""
        with self._get_connection() as conn:
            columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
            col_names = columns_df['column_name'].tolist()
            col_map = {c.lower(): c for c in col_names}

            real_x = col_map.get(x_col.lower()) if x_col else None
            real_y = col_map.get(y_col.lower()) if y_col else None
            real_size = col_map.get(size_col.lower()) if size_col else None
            real_label = col_map.get(label_col.lower()) if label_col else None
            real_text = col_map.get('text') or col_map.get('query_text')
            real_interval = col_map.get('interval_end')

            if not real_x or not real_y:
                return []

            select_parts = [
                f"CAST({real_x} AS DOUBLE) AS x_val",
                f"CAST({real_y} AS DOUBLE) AS y_val",
            ]

            if real_size:
                select_parts.append(f"CAST({real_size} AS DOUBLE) AS size_val")
            else:
                select_parts.append("1.0 AS size_val")

            if real_label:
                select_parts.append(f"CAST({real_label} AS VARCHAR) AS label_val")
            else:
                select_parts.append("'' AS label_val")

            if real_text:
                select_parts.append(f"CAST({real_text} AS VARCHAR) AS text_val")
            else:
                select_parts.append("'' AS text_val")

            if real_interval:
                select_parts.append(f"CAST({real_interval} AS VARCHAR) AS interval_val")
            else:
                select_parts.append("'' AS interval_val")

            where_conditions = [
                f"{real_x} IS NOT NULL",
                f"{real_y} IS NOT NULL",
            ]
            params: List[Any] = []

            if filters:
                timestamp_cols = [
                    row['column_name'] for _, row in columns_df.iterrows()
                    if 'TIMESTAMP' in str(row['column_type']).upper() or 'DATE' in str(row['column_type']).upper()
                ]
                filter_where, filter_params = self._build_where_clause(
                    table_name, filters, timestamp_cols, utc_offset
                )
                if filter_where:
                    clean_where = filter_where.strip()
                    if clean_where.startswith("WHERE "):
                        clean_where = clean_where[6:].strip()
                    if clean_where:
                        where_conditions.append(clean_where)
                        params.extend(filter_params)

            where_clause = "WHERE " + " AND ".join(where_conditions)
            query = f"""
            SELECT {", ".join(select_parts)}
            FROM {table_name}
            {where_clause}
            ORDER BY ({real_y} * COALESCE({real_x}, 1.0)) DESC
            LIMIT {limit}
            """

            df = conn.execute(query, params).fetchdf()
            if df.empty:
                return []

            points = []
            for idx, row in df.iterrows():
                x_num = float(row['x_val']) if pd.notnull(row['x_val']) else 0.0
                y_num = float(row['y_val']) if pd.notnull(row['y_val']) else 0.0
                s_num = float(row['size_val']) if pd.notnull(row['size_val']) else 1.0
                lbl = str(row['label_val']) if pd.notnull(row['label_val']) else ''
                txt = str(row['text_val']) if pd.notnull(row['text_val']) else ''
                interval_str = str(row['interval_val']) if pd.notnull(row['interval_val']) else ''

                points.append({
                    "id": idx,
                    "x": x_num,
                    "y": y_num,
                    "size": s_num,
                    "label": lbl,
                    "text": txt[:300],
                    "interval": interval_str
                })

            return points

