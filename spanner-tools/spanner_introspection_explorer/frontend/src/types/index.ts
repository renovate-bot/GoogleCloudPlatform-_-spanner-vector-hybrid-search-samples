/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export type TableCategory = 'Locking' | 'Query' | 'Transactions' | 'Misc';

export interface ColumnMetadata {
  name: string;
  type: string;
  filter_type: 'text' | 'numeric' | 'date';
}

export interface TableSummary {
  name: string;
  category: TableCategory;
  row_count: number;
  column_count: number;
  is_large: boolean;
}

export interface TableMetadata {
  name: string;
  category: TableCategory;
  row_count: number;
  column_count: number;
  columns: ColumnMetadata[];
}

export interface DatabaseSummary {
  database_file: string;
  total_tables: number;
  total_rows: number;
  categories: Record<string, string[]>;
  tables: TableSummary[];
}

export interface DatabaseItem {
  id: string;
  name: string;
  file_path: string;
  total_rows: number;
  total_tables: number;
  size_mb: number;
  last_modified: string;
  is_default: boolean;
  dialect?: string;
}

export interface DatabasesResponse {
  databases: DatabaseItem[];
  count: number;
}

export interface ColumnFilter {
  type: 'text' | 'numeric' | 'date';
  operator?: 'contains' | 'exact' | 'not_contains' | 'not_exact';
  value?: string;
  min?: number | string;
  max?: number | string;
  selected_timestamps?: string[];
}

export interface SortConfig {
  column: string | null;
  order: 'ASC' | 'DESC';
}

export interface TableQueryRequest {
  page: number;
  page_size: number;
  sort?: SortConfig | null;
  filters: Record<string, ColumnFilter>;
  utc_offset: number;
  global_search?: string;
}

export interface QueryResultPage {
  items: Record<string, any>[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  executed_sql: string;
  duration_ms: number;
}

export interface IntervalOption {
  display: string;
  utc: string;
}

export interface TimelineBucket {
  utc: string;
  display: string;
  count: number;
}

export interface TableTimelineResponse {
  table: string;
  utc_offset: number;
  total_intervals: number;
  total_records: number;
  buckets: TimelineBucket[];
}

export interface HighRowScanQuery {
  text: string;
  text_fingerprint: string;
  avg_rows_scanned: number;
  max_rows_scanned: number;
  total_exec: number;
  intervals: string[];
}

export interface PreflightItem {
  id: string;
  title: string;
  status: 'PENDING' | 'CHECKING' | 'PASS' | 'WARNING' | 'FAIL';
  message?: string;
  fix_command?: string;
}

export interface AppConfig {
  google_api_key_configured: boolean;
  google_api_key_preview: string;
  database_file: string;
  staging_dir: string;
  default_utc_offset: string;
  max_display_rows: number;
}

export interface ScatterDataPoint {
  id: number;
  x: number;
  y: number;
  size: number;
  label: string;
  text: string;
  interval: string;
}

export interface ScatterDefaultsResponse {
  numeric_cols: string[];
  x_col: string | null;
  y_col: string | null;
  size_col: string | null;
  label_col: string | null;
  title: string;
}

export interface ScatterPlotResponse {
  table: string;
  x_col: string;
  y_col: string;
  size_col?: string;
  label_col?: string;
  count: number;
  points: ScatterDataPoint[];
}

export interface StagingFile {
  name: string;
  size_bytes: number;
  size_human: string;
  modified: number;
}

export type ConnectionType = 'gcp' | 'local_staging';
export type ConnectionStatus = 'READY' | 'NEEDS_INGESTION' | 'SYNCING' | 'ERROR';
export type SqlDialect = 'GOOGLE_STANDARD_SQL' | 'POSTGRESQL';

export interface DatabaseConnection {
  id: string;
  name: string;
  type: ConnectionType;
  dialect: SqlDialect;
  project_id?: string;
  instance_id?: string;
  database_id?: string;
  staging_path?: string;
  duckdb_path: string;
  status: ConnectionStatus;
  total_tables: number;
  total_rows: number;
  size_mb: number;
  last_synced_at?: string;
  created_at: string;
  error_message?: string;
}

export interface CreateConnectionRequest {
  name: string;
  type: ConnectionType;
  dialect?: SqlDialect;
  project_id?: string;
  instance_id?: string;
  database_id?: string;
  staging_path?: string;
  auto_ingest?: boolean;
}

export interface GcpProjectItem {
  project_id: string;
  name?: string;
  project_number?: string;
}

export interface GcpInstanceItem {
  instance_id: string;
  display_name?: string;
  node_count?: number;
  state?: string;
}

export interface GcpDatabaseItem {
  database_id: string;
  state?: string;
  dialect: SqlDialect;
}

export interface StagingFolderItem {
  name: string;
  path: string;
  csv_count: number;
  has_schema: boolean;
  total_size_mb: number;
}

export interface HistogramBucket {
  bin_index: number;
  bin_min: number;
  bin_max: number;
  count: number;
}

export interface TopCategory {
  value: string;
  display_value?: string;
  count: number;
  percent: number;
}

export interface ColumnProfile {
  name: string;
  column_type: string;
  filter_type: 'text' | 'numeric' | 'date';
  null_count: number;
  distinct_count: number;
  total_count: number;
  min_value?: number;
  max_value?: number;
  avg_value?: number;
  histogram: HistogramBucket[];
  top_categories: TopCategory[];
  min_date?: string;
  max_date?: string;
}

export interface TableProfilesResponse {
  table: string;
  total_rows: number;
  profiles: Record<string, ColumnProfile>;
}



