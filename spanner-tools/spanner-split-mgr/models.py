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

"""Pydantic models for the Spanner Split Points Manager."""
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class OperationType(str, Enum):
    """Type of operation for a local split point."""
    ADD = "ADD"
    DELETE = "DELETE"


class SplitStatus(str, Enum):
    """Status of a split point."""
    SYNCED = "SYNCED"
    PENDING_ADD = "PENDING_ADD"
    PENDING_DELETE = "PENDING_DELETE"


class LocalSplitCreate(BaseModel):
    """Model for creating a new local split point.

    For table splits:
        - table_name: Required
        - split_value: Required (the table key value)

    For index splits:
        - table_name: Required (parent table)
        - index_name: Required
        - index_key: Required (the index key value)
        - split_value: Optional (table key value, if needed)
    """
    table_name: str = Field(..., min_length=1)
    split_value: str = Field(default="")  # Table key value
    operation_type: OperationType = OperationType.ADD
    index_name: Optional[str] = None
    index_key: Optional[str] = None  # Index key value


class LocalSplitResponse(BaseModel):
    """Model for returning a local split point."""
    model_config = {"from_attributes": True}

    id: int
    table_name: str
    split_value: str  # Table key value
    operation_type: OperationType
    created_at: datetime
    index_name: Optional[str] = None
    index_key: Optional[str] = None


class SpannerSplit(BaseModel):
    """Model representing a split point from Spanner."""
    table: str
    index: Optional[str] = None
    initiator: str
    split_key: str
    expire_time: Optional[datetime] = None


class SplitPointDisplay(BaseModel):
    """Combined model for displaying split points with status."""
    table_name: str
    split_value: str  # For table splits: the table key. For index splits: the index key.
    status: SplitStatus
    expire_time: Optional[datetime] = None
    local_id: Optional[int] = None  # ID from local_splits table if applicable
    initiator: Optional[str] = None
    index: Optional[str] = None  # Index name if this is an index split
    index_key: Optional[str] = None  # Index key value for index splits
    table_key: Optional[str] = None  # Table key for index splits (if specified)


class SettingsUpdate(BaseModel):
    """Model for updating settings."""
    project_id: Optional[str] = None
    instance_id: Optional[str] = None
    database_id: Optional[str] = None


class SettingsResponse(BaseModel):
    """Model for settings response."""
    project_id: Optional[str] = None
    instance_id: Optional[str] = None
    database_id: Optional[str] = None


class SyncResult(BaseModel):
    """Model for sync operation results."""
    success: bool
    message: str
    added_count: int = 0
    deleted_count: int = 0
    errors: list[str] = Field(default_factory=list)


class EntityType(str, Enum):
    """Type of entity (table or index)."""
    TABLE = "TABLE"
    INDEX = "INDEX"


class EntitySummary(BaseModel):
    """Summary of an entity (table or index) with split counts."""
    entity_name: str
    entity_type: EntityType
    parent_table: Optional[str] = None  # For indexes, the table they belong to
    total_splits: int = 0
    synced_count: int = 0
    pending_add_count: int = 0
    pending_delete_count: int = 0


class KeyColumnInfo(BaseModel):
    """Information about a key column."""
    column_name: str
    spanner_type: str
    ordinal_position: int


class EntityKeySchema(BaseModel):
    """Key schema information for a table or index."""
    entity_name: str
    entity_type: EntityType
    key_columns: list[KeyColumnInfo]
    is_composite: bool  # True if key has more than one column
    parent_table: Optional[str] = None  # For indexes, the table they belong to
    parent_key_columns: Optional[list[KeyColumnInfo]] = None  # Primary key columns of parent table (for indexes)


class SupportedRangeType(str, Enum):
    """Column types that support range-based split generation."""
    INT64 = "INT64"
    STRING_UUID = "STRING_UUID"
    BYTES_UUID = "BYTES_UUID"


class RangeSplitRequest(BaseModel):
    """Request model for creating range-based splits."""
    table_name: str = Field(..., min_length=1)
    start_value: str = Field(..., min_length=1)
    end_value: str = Field(..., min_length=1)
    num_splits: int = Field(..., ge=2, le=100)
    include_boundaries: bool = Field(default=True)
    index_name: Optional[str] = None


class RangeSplitResponse(BaseModel):
    """Response model for range split generation."""
    success: bool
    message: str
    generated_values: list[str] = Field(default_factory=list)
    splits_created: int = 0
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class RangeValidationResult(BaseModel):
    """Result of validating a range split request."""
    is_valid: bool
    range_type: Optional[SupportedRangeType] = None
    error_message: Optional[str] = None
