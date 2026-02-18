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

"""Unit tests for Pydantic models.

Tests model validation, serialization, and enum behavior.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import (
    OperationType,
    SplitStatus,
    LocalSplitCreate,
    LocalSplitResponse,
    SpannerSplit,
    SplitPointDisplay,
    SettingsUpdate,
    SettingsResponse,
    SyncResult,
    EntityType,
    EntitySummary,
    KeyColumnInfo,
    EntityKeySchema,
)


# =============================================================================
# Enum Tests
# =============================================================================

@pytest.mark.unit
class TestOperationType:
    """Tests for OperationType enum."""

    def test_add_value(self):
        """Test ADD enum value."""
        assert OperationType.ADD.value == "ADD"
        assert OperationType.ADD == OperationType("ADD")

    def test_delete_value(self):
        """Test DELETE enum value."""
        assert OperationType.DELETE.value == "DELETE"
        assert OperationType.DELETE == OperationType("DELETE")

    def test_invalid_value_raises_error(self):
        """Test that invalid value raises ValueError."""
        with pytest.raises(ValueError):
            OperationType("INVALID")


@pytest.mark.unit
class TestSplitStatus:
    """Tests for SplitStatus enum."""

    def test_synced_value(self):
        """Test SYNCED status."""
        assert SplitStatus.SYNCED.value == "SYNCED"

    def test_pending_add_value(self):
        """Test PENDING_ADD status."""
        assert SplitStatus.PENDING_ADD.value == "PENDING_ADD"

    def test_pending_delete_value(self):
        """Test PENDING_DELETE status."""
        assert SplitStatus.PENDING_DELETE.value == "PENDING_DELETE"


@pytest.mark.unit
class TestEntityType:
    """Tests for EntityType enum."""

    def test_table_value(self):
        """Test TABLE type."""
        assert EntityType.TABLE.value == "TABLE"

    def test_index_value(self):
        """Test INDEX type."""
        assert EntityType.INDEX.value == "INDEX"


# =============================================================================
# LocalSplitCreate Tests
# =============================================================================

@pytest.mark.unit
class TestLocalSplitCreate:
    """Tests for LocalSplitCreate model."""

    def test_valid_table_split(self):
        """Test creating a valid table split."""
        split = LocalSplitCreate(
            table_name="UserInfo",
            split_value="12345",
            operation_type=OperationType.ADD
        )
        assert split.table_name == "UserInfo"
        assert split.split_value == "12345"
        assert split.operation_type == OperationType.ADD
        assert split.index_name is None
        assert split.index_key is None

    def test_valid_index_split(self):
        """Test creating a valid index split."""
        split = LocalSplitCreate(
            table_name="UserLocationInfo",
            split_value="12,JP",
            operation_type=OperationType.ADD,
            index_name="UsersByLocation",
            index_key="JP"
        )
        assert split.table_name == "UserLocationInfo"
        assert split.index_name == "UsersByLocation"
        assert split.index_key == "JP"

    def test_empty_table_name_raises_error(self):
        """Test that empty table_name raises validation error."""
        with pytest.raises(ValidationError):
            LocalSplitCreate(
                table_name="",
                split_value="12345"
            )

    def test_default_operation_type(self):
        """Test default operation type is ADD."""
        split = LocalSplitCreate(table_name="Test", split_value="1")
        assert split.operation_type == OperationType.ADD

    def test_default_split_value_is_empty_string(self):
        """Test default split_value is empty string."""
        split = LocalSplitCreate(table_name="Test")
        assert split.split_value == ""


# =============================================================================
# LocalSplitResponse Tests
# =============================================================================

@pytest.mark.unit
class TestLocalSplitResponse:
    """Tests for LocalSplitResponse model."""

    def test_valid_response(self):
        """Test creating a valid response."""
        now = datetime.now()
        response = LocalSplitResponse(
            id=1,
            table_name="UserInfo",
            split_value="12345",
            operation_type=OperationType.ADD,
            created_at=now
        )
        assert response.id == 1
        assert response.table_name == "UserInfo"
        assert response.split_value == "12345"
        assert response.operation_type == OperationType.ADD
        assert response.created_at == now

    def test_with_index_fields(self):
        """Test response with index fields."""
        response = LocalSplitResponse(
            id=1,
            table_name="UserLocationInfo",
            split_value="12,JP",
            operation_type=OperationType.ADD,
            created_at=datetime.now(),
            index_name="UsersByLocation",
            index_key="JP"
        )
        assert response.index_name == "UsersByLocation"
        assert response.index_key == "JP"


# =============================================================================
# SpannerSplit Tests
# =============================================================================

@pytest.mark.unit
class TestSpannerSplit:
    """Tests for SpannerSplit model."""

    def test_table_split(self):
        """Test creating a table split."""
        split = SpannerSplit(
            table="UserInfo",
            initiator="USER",
            split_key="UserInfo(12345)"
        )
        assert split.table == "UserInfo"
        assert split.index is None
        assert split.split_key == "UserInfo(12345)"

    def test_index_split(self):
        """Test creating an index split."""
        split = SpannerSplit(
            table="UserLocationInfo",
            index="UsersByLocation",
            initiator="USER",
            split_key="Index: UsersByLocation on UserLocationInfo, Index Key: (JP), Primary Table Key: (12,JP)"
        )
        assert split.table == "UserLocationInfo"
        assert split.index == "UsersByLocation"

    def test_with_expire_time(self):
        """Test split with expiration time."""
        expire = datetime.now()
        split = SpannerSplit(
            table="UserInfo",
            initiator="USER",
            split_key="test",
            expire_time=expire
        )
        assert split.expire_time == expire


# =============================================================================
# SplitPointDisplay Tests
# =============================================================================

@pytest.mark.unit
class TestSplitPointDisplay:
    """Tests for SplitPointDisplay model."""

    def test_synced_split(self):
        """Test display of synced split."""
        display = SplitPointDisplay(
            table_name="UserInfo",
            split_value="12345",
            status=SplitStatus.SYNCED,
            initiator="USER"
        )
        assert display.status == SplitStatus.SYNCED
        assert display.local_id is None

    def test_pending_add_split(self):
        """Test display of pending add split."""
        display = SplitPointDisplay(
            table_name="UserInfo",
            split_value="12345",
            status=SplitStatus.PENDING_ADD,
            local_id=1
        )
        assert display.status == SplitStatus.PENDING_ADD
        assert display.local_id == 1

    def test_pending_delete_split(self):
        """Test display of pending delete split."""
        display = SplitPointDisplay(
            table_name="UserInfo",
            split_value="12345",
            status=SplitStatus.PENDING_DELETE,
            local_id=2
        )
        assert display.status == SplitStatus.PENDING_DELETE
        assert display.local_id == 2


# =============================================================================
# Settings Tests
# =============================================================================

@pytest.mark.unit
class TestSettingsUpdate:
    """Tests for SettingsUpdate model."""

    def test_all_fields(self):
        """Test with all fields set."""
        settings = SettingsUpdate(
            project_id="my-project",
            instance_id="my-instance",
            database_id="my-database"
        )
        assert settings.project_id == "my-project"
        assert settings.instance_id == "my-instance"
        assert settings.database_id == "my-database"

    def test_partial_fields(self):
        """Test with only some fields set."""
        settings = SettingsUpdate(project_id="my-project")
        assert settings.project_id == "my-project"
        assert settings.instance_id is None
        assert settings.database_id is None

    def test_empty_settings(self):
        """Test with no fields set."""
        settings = SettingsUpdate()
        assert settings.project_id is None
        assert settings.instance_id is None
        assert settings.database_id is None


@pytest.mark.unit
class TestSettingsResponse:
    """Tests for SettingsResponse model."""

    def test_with_values(self):
        """Test response with all values."""
        response = SettingsResponse(
            project_id="project",
            instance_id="instance",
            database_id="database"
        )
        assert response.project_id == "project"
        assert response.instance_id == "instance"
        assert response.database_id == "database"


# =============================================================================
# SyncResult Tests
# =============================================================================

@pytest.mark.unit
class TestSyncResult:
    """Tests for SyncResult model."""

    def test_successful_sync(self):
        """Test successful sync result."""
        result = SyncResult(
            success=True,
            message="Synced 5 split points",
            added_count=3,
            deleted_count=2
        )
        assert result.success is True
        assert result.added_count == 3
        assert result.deleted_count == 2
        assert result.errors == []

    def test_failed_sync_with_errors(self):
        """Test failed sync with errors."""
        result = SyncResult(
            success=False,
            message="Sync failed",
            errors=["Error 1", "Error 2"]
        )
        assert result.success is False
        assert len(result.errors) == 2

    def test_default_counts(self):
        """Test default count values."""
        result = SyncResult(success=True, message="Done")
        assert result.added_count == 0
        assert result.deleted_count == 0
        assert result.errors == []


# =============================================================================
# Entity Schema Tests
# =============================================================================

@pytest.mark.unit
class TestKeyColumnInfo:
    """Tests for KeyColumnInfo model."""

    def test_valid_key_column(self):
        """Test creating a valid key column."""
        col = KeyColumnInfo(
            column_name="user_id",
            spanner_type="INT64",
            ordinal_position=1
        )
        assert col.column_name == "user_id"
        assert col.spanner_type == "INT64"
        assert col.ordinal_position == 1


@pytest.mark.unit
class TestEntityKeySchema:
    """Tests for EntityKeySchema model."""

    def test_table_schema(self):
        """Test table key schema."""
        schema = EntityKeySchema(
            entity_name="UserInfo",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(column_name="id", spanner_type="INT64", ordinal_position=1)
            ],
            is_composite=False
        )
        assert schema.entity_name == "UserInfo"
        assert schema.entity_type == EntityType.TABLE
        assert len(schema.key_columns) == 1
        assert schema.is_composite is False
        assert schema.parent_table is None

    def test_index_schema(self):
        """Test index key schema with parent table."""
        schema = EntityKeySchema(
            entity_name="UsersByLocation",
            entity_type=EntityType.INDEX,
            key_columns=[
                KeyColumnInfo(column_name="location", spanner_type="STRING(MAX)", ordinal_position=1)
            ],
            is_composite=False,
            parent_table="UserLocationInfo",
            parent_key_columns=[
                KeyColumnInfo(column_name="user_id", spanner_type="INT64", ordinal_position=1)
            ]
        )
        assert schema.entity_name == "UsersByLocation"
        assert schema.entity_type == EntityType.INDEX
        assert schema.parent_table == "UserLocationInfo"
        assert len(schema.parent_key_columns) == 1

    def test_composite_key_schema(self):
        """Test composite key schema."""
        schema = EntityKeySchema(
            entity_name="CompositeTable",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(column_name="part1", spanner_type="INT64", ordinal_position=1),
                KeyColumnInfo(column_name="part2", spanner_type="STRING(50)", ordinal_position=2)
            ],
            is_composite=True
        )
        assert schema.is_composite is True
        assert len(schema.key_columns) == 2


# =============================================================================
# EntitySummary Tests
# =============================================================================

@pytest.mark.unit
class TestEntitySummary:
    """Tests for EntitySummary model."""

    def test_table_summary(self):
        """Test table entity summary."""
        summary = EntitySummary(
            entity_name="UserInfo",
            entity_type=EntityType.TABLE,
            total_splits=10,
            synced_count=7,
            pending_add_count=2,
            pending_delete_count=1
        )
        assert summary.entity_name == "UserInfo"
        assert summary.entity_type == EntityType.TABLE
        assert summary.parent_table is None
        assert summary.total_splits == 10

    def test_index_summary(self):
        """Test index entity summary."""
        summary = EntitySummary(
            entity_name="UsersByLocation",
            entity_type=EntityType.INDEX,
            parent_table="UserLocationInfo",
            total_splits=5
        )
        assert summary.entity_type == EntityType.INDEX
        assert summary.parent_table == "UserLocationInfo"

    def test_default_counts(self):
        """Test default count values."""
        summary = EntitySummary(
            entity_name="Empty",
            entity_type=EntityType.TABLE
        )
        assert summary.total_splits == 0
        assert summary.synced_count == 0
        assert summary.pending_add_count == 0
        assert summary.pending_delete_count == 0
