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

"""Sample data factories for testing the Spanner Split Manager.

These factory functions create consistent test data that can be used
across unit and integration tests.
"""
from datetime import datetime, timedelta
from typing import Optional

from models import (
    OperationType,
    SplitStatus,
    LocalSplitCreate,
    LocalSplitResponse,
    SpannerSplit,
    SplitPointDisplay,
    SettingsResponse,
    EntityType,
    EntitySummary,
    KeyColumnInfo,
    EntityKeySchema,
)


# =============================================================================
# Local Split Factories
# =============================================================================

def create_local_split_request(
    table_name: str = "UserInfo",
    split_value: str = "12345",
    operation_type: OperationType = OperationType.ADD,
    index_name: Optional[str] = None,
    index_key: Optional[str] = None,
) -> LocalSplitCreate:
    """Create a LocalSplitCreate instance for testing."""
    return LocalSplitCreate(
        table_name=table_name,
        split_value=split_value,
        operation_type=operation_type,
        index_name=index_name,
        index_key=index_key,
    )


def create_local_split_response(
    id: int = 1,
    table_name: str = "UserInfo",
    split_value: str = "12345",
    operation_type: OperationType = OperationType.ADD,
    created_at: Optional[datetime] = None,
    index_name: Optional[str] = None,
    index_key: Optional[str] = None,
) -> LocalSplitResponse:
    """Create a LocalSplitResponse instance for testing."""
    return LocalSplitResponse(
        id=id,
        table_name=table_name,
        split_value=split_value,
        operation_type=operation_type,
        created_at=created_at or datetime.now(),
        index_name=index_name,
        index_key=index_key,
    )


# =============================================================================
# Spanner Split Factories
# =============================================================================

def create_spanner_split(
    table: str = "UserInfo",
    index: Optional[str] = None,
    initiator: str = "USER",
    split_key: str = "UserInfo(12345)",
    expire_time: Optional[datetime] = None,
) -> SpannerSplit:
    """Create a SpannerSplit instance for testing."""
    if expire_time is None:
        expire_time = datetime.now() + timedelta(days=10)

    return SpannerSplit(
        table=table,
        index=index,
        initiator=initiator,
        split_key=split_key,
        expire_time=expire_time,
    )


def create_index_spanner_split(
    table: str = "UserLocationInfo",
    index: str = "UsersByLocation",
    initiator: str = "USER",
    index_key: str = "JP",
    table_key: str = "12,JP",
    expire_time: Optional[datetime] = None,
) -> SpannerSplit:
    """Create a SpannerSplit for an index split point."""
    if expire_time is None:
        expire_time = datetime.now() + timedelta(days=10)

    split_key = f"Index: {index} on {table}, Index Key: ({index_key}), Primary Table Key: ({table_key})"

    return SpannerSplit(
        table=table,
        index=index,
        initiator=initiator,
        split_key=split_key,
        expire_time=expire_time,
    )


# =============================================================================
# Split Point Display Factories
# =============================================================================

def create_split_display(
    table_name: str = "UserInfo",
    split_value: str = "12345",
    status: SplitStatus = SplitStatus.SYNCED,
    expire_time: Optional[datetime] = None,
    local_id: Optional[int] = None,
    initiator: Optional[str] = "USER",
    index: Optional[str] = None,
    index_key: Optional[str] = None,
    table_key: Optional[str] = None,
) -> SplitPointDisplay:
    """Create a SplitPointDisplay instance for testing."""
    return SplitPointDisplay(
        table_name=table_name,
        split_value=split_value,
        status=status,
        expire_time=expire_time or datetime.now() + timedelta(days=10),
        local_id=local_id,
        initiator=initiator,
        index=index,
        index_key=index_key,
        table_key=table_key,
    )


# =============================================================================
# Settings Factories
# =============================================================================

def create_settings(
    project_id: Optional[str] = "test-project",
    instance_id: Optional[str] = "test-instance",
    database_id: Optional[str] = "test-database",
) -> SettingsResponse:
    """Create a SettingsResponse instance for testing."""
    return SettingsResponse(
        project_id=project_id,
        instance_id=instance_id,
        database_id=database_id,
    )


# =============================================================================
# Entity Schema Factories
# =============================================================================

def create_key_column(
    column_name: str = "id",
    spanner_type: str = "INT64",
    ordinal_position: int = 1,
) -> KeyColumnInfo:
    """Create a KeyColumnInfo instance for testing."""
    return KeyColumnInfo(
        column_name=column_name,
        spanner_type=spanner_type,
        ordinal_position=ordinal_position,
    )


def create_table_key_schema(
    entity_name: str = "UserInfo",
    key_columns: Optional[list[KeyColumnInfo]] = None,
) -> EntityKeySchema:
    """Create an EntityKeySchema for a table."""
    if key_columns is None:
        key_columns = [create_key_column()]

    return EntityKeySchema(
        entity_name=entity_name,
        entity_type=EntityType.TABLE,
        key_columns=key_columns,
        is_composite=len(key_columns) > 1,
    )


def create_index_key_schema(
    entity_name: str = "UsersByLocation",
    key_columns: Optional[list[KeyColumnInfo]] = None,
    parent_table: str = "UserLocationInfo",
    parent_key_columns: Optional[list[KeyColumnInfo]] = None,
) -> EntityKeySchema:
    """Create an EntityKeySchema for an index."""
    if key_columns is None:
        key_columns = [create_key_column(column_name="location", spanner_type="STRING(MAX)")]
    if parent_key_columns is None:
        parent_key_columns = [create_key_column(column_name="user_id", spanner_type="INT64")]

    return EntityKeySchema(
        entity_name=entity_name,
        entity_type=EntityType.INDEX,
        key_columns=key_columns,
        is_composite=len(key_columns) > 1,
        parent_table=parent_table,
        parent_key_columns=parent_key_columns,
    )


def create_entity_summary(
    entity_name: str = "UserInfo",
    entity_type: EntityType = EntityType.TABLE,
    parent_table: Optional[str] = None,
    total_splits: int = 5,
    synced_count: int = 3,
    pending_add_count: int = 1,
    pending_delete_count: int = 1,
) -> EntitySummary:
    """Create an EntitySummary instance for testing."""
    return EntitySummary(
        entity_name=entity_name,
        entity_type=entity_type,
        parent_table=parent_table,
        total_splits=total_splits,
        synced_count=synced_count,
        pending_add_count=pending_add_count,
        pending_delete_count=pending_delete_count,
    )


# =============================================================================
# Batch Test Data
# =============================================================================

def create_batch_splits(count: int = 150, table_name: str = "UserInfo") -> list[dict]:
    """Create a batch of split data for testing batch limits.

    This is useful for testing the 100-split batch limit enforcement.
    """
    return [
        {
            "table_name": table_name,
            "split_value": str(i),
            "operation_type": OperationType.ADD,
        }
        for i in range(count)
    ]


def create_batch_local_splits(
    count: int = 150,
    table_name: str = "UserInfo",
    operation_type: OperationType = OperationType.ADD,
) -> list[LocalSplitCreate]:
    """Create a batch of LocalSplitCreate instances for testing."""
    return [
        create_local_split_request(
            table_name=table_name,
            split_value=str(i),
            operation_type=operation_type,
        )
        for i in range(count)
    ]
