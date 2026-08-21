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

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime

ConnectionType = Literal['gcp', 'local_staging']
ConnectionStatus = Literal['READY', 'NEEDS_INGESTION', 'SYNCING', 'ERROR']
SqlDialect = Literal['GOOGLE_STANDARD_SQL', 'POSTGRESQL']

class DatabaseConnection(BaseModel):
    id: str = Field(..., description="Unique slug for the connection (e.g., analytics-db, prod-orders)")
    name: str = Field(..., description="Human-readable connection name")
    type: ConnectionType = Field(..., description="Connection type: gcp or local_staging")
    dialect: SqlDialect = Field(default='GOOGLE_STANDARD_SQL', description="Spanner SQL dialect")
    
    # GCP Spanner specific
    project_id: Optional[str] = None
    instance_id: Optional[str] = None
    database_id: Optional[str] = None
    
    # Local Staging specific
    staging_path: Optional[str] = None
    
    # Storage & DuckDB State
    duckdb_path: str = Field(..., description="Path to local DuckDB file")
    status: ConnectionStatus = Field(default='NEEDS_INGESTION', description="Connection & DuckDB state")
    total_tables: int = 0
    total_rows: int = 0
    size_mb: float = 0.0
    last_synced_at: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    error_message: Optional[str] = None

class CreateConnectionRequest(BaseModel):
    name: str
    type: ConnectionType
    dialect: SqlDialect = 'GOOGLE_STANDARD_SQL'
    
    # GCP fields
    project_id: Optional[str] = None
    instance_id: Optional[str] = None
    database_id: Optional[str] = None
    
    # Local staging fields
    staging_path: Optional[str] = None
    auto_ingest: bool = True

class UpdateConnectionRequest(BaseModel):
    name: Optional[str] = None
    dialect: Optional[SqlDialect] = None
    project_id: Optional[str] = None
    instance_id: Optional[str] = None
    database_id: Optional[str] = None
    staging_path: Optional[str] = None

class TestConnectionRequest(BaseModel):
    project_id: str
    instance_id: str
    database_id: str
    dialect: SqlDialect = 'GOOGLE_STANDARD_SQL'

class GcpProjectItem(BaseModel):
    project_id: str
    name: Optional[str] = None
    project_number: Optional[str] = None

class GcpInstanceItem(BaseModel):
    instance_id: str
    display_name: Optional[str] = None
    node_count: Optional[int] = None
    state: Optional[str] = None

class GcpDatabaseItem(BaseModel):
    database_id: str
    state: Optional[str] = None
    dialect: SqlDialect = 'GOOGLE_STANDARD_SQL'

class StagingFolderItem(BaseModel):
    name: str
    path: str
    csv_count: int
    has_schema: bool
    total_size_mb: float
