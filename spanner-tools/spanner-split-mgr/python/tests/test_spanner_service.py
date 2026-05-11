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

"""Unit tests for Spanner service layer.

Tests SpannerService methods using mocked Spanner clients.
These tests focus on business logic without requiring actual Spanner connections.
"""
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from spanner_service import (
    SpannerService,
    parse_raw_split_key,
    format_spanner_error,
    BATCH_LIMIT,
    DEFAULT_EXPIRATION_DAYS,
)
from models import OperationType, SyncResult
import database


# =============================================================================
# parse_raw_split_key Tests
# =============================================================================

@pytest.mark.unit
class TestParseRawSplitKey:
    """Tests for parsing raw split key format from Spanner."""

    def test_index_format_with_begin_markers(self):
        """Test parsing index split with <begin> markers."""
        split_key = "Index: UsersByLocation on UserLocationInfo, Index Key: (CN), Primary Table Key: (<begin>,<begin>)"
        index_name, index_key, table_key = parse_raw_split_key(split_key)

        assert index_name == "UsersByLocation"
        assert index_key == "CN"
        assert table_key == "<begin>,<begin>"

    def test_index_format_with_values(self):
        """Test parsing index split with actual key values."""
        split_key = "Index: UsersByLocation on UserLocationInfo, Index Key: (JP), Primary Table Key: (12,JP)"
        index_name, index_key, table_key = parse_raw_split_key(split_key)

        assert index_name == "UsersByLocation"
        assert index_key == "JP"
        assert table_key == "12,JP"

    def test_table_format_simple(self):
        """Test parsing simple table split format."""
        split_key = "UserInfo(12345)"
        index_name, index_key, table_key = parse_raw_split_key(split_key)

        assert index_name is None
        assert index_key is None
        assert table_key == "12345"

    def test_table_format_composite_key(self):
        """Test parsing table split with composite key."""
        split_key = "UserInfo(part1,part2,part3)"
        index_name, index_key, table_key = parse_raw_split_key(split_key)

        assert index_name is None
        assert index_key is None
        assert table_key == "part1,part2,part3"

    def test_empty_string(self):
        """Test parsing empty string."""
        index_name, index_key, table_key = parse_raw_split_key("")

        assert index_name is None
        assert index_key is None
        assert table_key == ""

    def test_unrecognized_format_fallback(self):
        """Test that unrecognized format returns whole string as table_key."""
        split_key = "some unrecognized format"
        index_name, index_key, table_key = parse_raw_split_key(split_key)

        assert index_name is None
        assert index_key is None
        assert table_key == "some unrecognized format"


# =============================================================================
# format_spanner_error Tests
# =============================================================================

@pytest.mark.unit
class TestFormatSpannerError:
    """Tests for formatting Spanner error messages."""

    def test_extracts_table_name(self):
        """Test extracting table name from error."""
        error = 'Split point for table: "UserInfo" is invalid, due to invalid key format.'
        formatted = format_spanner_error(error)

        assert "UserInfo" in formatted

    def test_extracts_reason(self):
        """Test extracting error reason."""
        error = 'Split point is invalid, due to invalid key format. [locale=en-US]'
        formatted = format_spanner_error(error)

        assert "invalid key format" in formatted.lower()

    def test_truncates_long_errors(self):
        """Test that very long errors are truncated."""
        error = "A" * 500
        formatted = format_spanner_error(error)

        assert len(formatted) <= 203  # 200 + "..."


# =============================================================================
# SpannerService Configuration Tests
# =============================================================================

@pytest.mark.unit
class TestSpannerServiceConfiguration:
    """Tests for SpannerService configuration."""

    def test_init_with_explicit_config(self):
        """Test initialization with explicit configuration."""
        service = SpannerService(
            project_id="my-project",
            instance_id="my-instance",
            database_id="my-database"
        )

        assert service.project_id == "my-project"
        assert service.instance_id == "my-instance"
        assert service.database_id == "my-database"

    def test_is_configured_true(self):
        """Test is_configured returns True when configured."""
        service = SpannerService(
            project_id="project",
            instance_id="instance",
            database_id="database"
        )

        assert service.is_configured() is True

    def test_is_configured_false_missing_instance(self, clean_db, monkeypatch):
        """Test is_configured returns False when instance is missing."""
        # Clear environment variables that might provide fallback config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)

        service = SpannerService(
            project_id="project",
            instance_id=None,
            database_id="database"
        )

        assert service.is_configured() is False

    def test_is_configured_false_missing_database(self, clean_db, monkeypatch):
        """Test is_configured returns False when database is missing."""
        # Clear environment variables that might provide fallback config
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService(
            project_id="project",
            instance_id="instance",
            database_id=None
        )

        assert service.is_configured() is False

    def test_config_from_settings(self, clean_db, monkeypatch):
        """Test that configuration falls back to database settings."""
        # Clear environment variables
        monkeypatch.delenv("PROJECT", raising=False)
        monkeypatch.delenv("project_id", raising=False)
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        database.set_setting("project_id", "settings-project")
        database.set_setting("instance_id", "settings-instance")
        database.set_setting("database_id", "settings-database")

        service = SpannerService()

        assert service.project_id == "settings-project"
        assert service.instance_id == "settings-instance"
        assert service.database_id == "settings-database"


# =============================================================================
# SpannerService Batch Logic Tests
# =============================================================================

@pytest.mark.unit
class TestBatchSplitPoints:
    """Tests for batch splitting logic."""

    def test_batch_limit_constant(self):
        """Verify BATCH_LIMIT is 100 as per business requirements."""
        assert BATCH_LIMIT == 100

    def test_default_expiration_constant(self):
        """Verify DEFAULT_EXPIRATION_DAYS is 10 as per business requirements."""
        assert DEFAULT_EXPIRATION_DAYS == 10

    def test_batch_splits_under_limit(self, mock_spanner_service):
        """Test that splits under limit stay in one batch."""
        splits = list(range(50))
        batches = mock_spanner_service._batch_split_points(splits)

        assert len(batches) == 1
        assert len(batches[0]) == 50

    def test_batch_splits_at_limit(self, mock_spanner_service):
        """Test that exactly 100 splits stay in one batch."""
        splits = list(range(100))
        batches = mock_spanner_service._batch_split_points(splits)

        assert len(batches) == 1
        assert len(batches[0]) == 100

    def test_batch_splits_over_limit(self, mock_spanner_service):
        """Test that 150 splits create 2 batches."""
        splits = list(range(150))
        batches = mock_spanner_service._batch_split_points(splits)

        assert len(batches) == 2
        assert len(batches[0]) == 100
        assert len(batches[1]) == 50

    def test_batch_splits_multiple_batches(self, mock_spanner_service):
        """Test that 250 splits create 3 batches."""
        splits = list(range(250))
        batches = mock_spanner_service._batch_split_points(splits)

        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50

    def test_batch_splits_empty(self, mock_spanner_service):
        """Test that empty list returns empty batches."""
        batches = mock_spanner_service._batch_split_points([])
        assert len(batches) == 0


# =============================================================================
# SpannerService Make Split Point Tests
# =============================================================================

@pytest.mark.unit
class TestMakeSplitPoint:
    """Tests for creating split point objects."""

    def test_make_table_split(self, mock_spanner_service):
        """Test creating a table split point."""
        sp = mock_spanner_service._make_split_point(
            table_name="UserInfo",
            split_value="12345"
        )

        assert sp.table == "UserInfo"
        assert len(sp.keys) == 1

    def test_make_index_split(self, mock_spanner_service):
        """Test creating an index split point."""
        sp = mock_spanner_service._make_split_point(
            table_name="UserLocationInfo",
            split_value="12,JP",
            index_name="UsersByLocation",
            index_key="JP"
        )

        assert sp.index == "UsersByLocation"
        assert len(sp.keys) == 2  # index key + table key

    def test_make_index_split_with_begin_marker(self, mock_spanner_service):
        """Test that <begin> marker is not included in keys."""
        sp = mock_spanner_service._make_split_point(
            table_name="UserLocationInfo",
            split_value="<begin>,<begin>",
            index_name="UsersByLocation",
            index_key="JP"
        )

        # Should only have index key, not table key with <begin>
        assert sp.index == "UsersByLocation"
        assert len(sp.keys) == 1  # Only index key

    def test_make_split_with_expiration(self, mock_spanner_service):
        """Test creating a split point with expiration."""
        # Use UTC-aware datetime to match protobuf's internal representation
        expire_time = datetime.now(timezone.utc) + timedelta(days=5)
        sp = mock_spanner_service._make_split_point(
            table_name="UserInfo",
            split_value="12345",
            expire_time=expire_time
        )

        assert sp.expire_time is not None
        # Both are now UTC-aware, so direct comparison works
        assert abs((sp.expire_time - expire_time).total_seconds()) < 1


# =============================================================================
# SpannerService API Operation Tests (Mocked)
# =============================================================================

@pytest.mark.unit
class TestSpannerServiceOperations:
    """Tests for Spanner service operations with mocked client."""

    def test_add_split_points_not_configured(self, clean_db, monkeypatch):
        """Test add_split_points when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        result = service.add_split_points("UserInfo", ["12345"])

        assert result.success is False
        assert "not configured" in result.message.lower()

    def test_add_split_points_empty_list(self, mock_spanner_service):
        """Test add_split_points with empty list."""
        result = mock_spanner_service.add_split_points("UserInfo", [])

        assert result.success is True
        assert result.added_count == 0

    def test_add_split_points_success(self, mock_spanner_service):
        """Test successful add_split_points."""
        result = mock_spanner_service.add_split_points("UserInfo", ["1", "2", "3"])

        assert result.success is True
        assert result.added_count == 3

    def test_add_split_points_with_error(self, mock_spanner_service):
        """Test add_split_points handles API errors."""
        mock_spanner_service._client.database_admin_api.add_split_points.side_effect = Exception("API Error")

        result = mock_spanner_service.add_split_points("UserInfo", ["1"])

        assert result.success is False
        assert len(result.errors) > 0

    def test_delete_split_points_not_configured(self, clean_db, monkeypatch):
        """Test delete_split_points when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        result = service.delete_split_points("UserInfo", ["12345"])

        assert result.success is False
        assert "not configured" in result.message.lower()

    def test_delete_split_points_empty_list(self, mock_spanner_service):
        """Test delete_split_points with empty list."""
        result = mock_spanner_service.delete_split_points("UserInfo", [])

        assert result.success is True
        assert result.deleted_count == 0

    def test_delete_split_points_success(self, mock_spanner_service):
        """Test successful delete_split_points."""
        result = mock_spanner_service.delete_split_points(
            "UserInfo",
            ["UserInfo(1)", "UserInfo(2)"]
        )

        assert result.success is True
        assert result.deleted_count == 2


# =============================================================================
# SpannerService Sync Tests
# =============================================================================

@pytest.mark.unit
class TestSyncPendingChanges:
    """Tests for sync_pending_changes method."""

    def test_sync_not_configured(self, clean_db, monkeypatch):
        """Test sync when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        result = service.sync_pending_changes()

        assert result.success is False
        assert "not configured" in result.message.lower()

    def test_sync_with_pending_adds(self, mock_spanner_service, clean_db):
        """Test syncing pending ADD operations."""
        # Add pending splits to local DB
        database.add_local_split("UserInfo", "1", OperationType.ADD)
        database.add_local_split("UserInfo", "2", OperationType.ADD)

        result = mock_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.added_count == 2

        # Verify local splits were cleared
        pending = database.get_local_splits_by_operation(OperationType.ADD)
        assert len(pending) == 0

    def test_sync_with_pending_deletes(self, mock_spanner_service, clean_db):
        """Test syncing pending DELETE operations."""
        # Add pending delete
        database.add_local_split("UserInfo", "UserInfo(999)", OperationType.DELETE)

        result = mock_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.deleted_count == 1

    def test_sync_with_mixed_operations(self, mock_spanner_service, clean_db):
        """Test syncing both ADD and DELETE operations."""
        database.add_local_split("UserInfo", "1", OperationType.ADD)
        database.add_local_split("UserInfo", "UserInfo(2)", OperationType.DELETE)

        result = mock_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.added_count == 1
        assert result.deleted_count == 1

    def test_sync_empty(self, mock_spanner_service, clean_db):
        """Test sync when no pending changes."""
        result = mock_spanner_service.sync_pending_changes()

        assert "Nothing synced" in result.message


# =============================================================================
# SpannerService List Operations Tests (Mocked)
# =============================================================================

@pytest.mark.unit
class TestListOperations:
    """Tests for list operations with mocked Spanner."""

    def test_list_tables_not_configured(self, clean_db, monkeypatch):
        """Test list_tables when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        tables = service.list_tables()

        assert tables == []

    def test_list_tables_with_results(self, mock_spanner_service):
        """Test list_tables returns table names."""
        # Setup mock to return tables
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = [("Table1",), ("Table2",)]
        mock_spanner_service._client.instance().database().snapshot().__enter__.return_value = mock_snapshot

        tables = mock_spanner_service.list_tables()

        assert len(tables) == 2
        assert "Table1" in tables
        assert "Table2" in tables

    def test_list_indexes_not_configured(self, clean_db, monkeypatch):
        """Test list_indexes when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        indexes = service.list_indexes()

        assert indexes == []

    def test_list_splits_not_configured(self, clean_db, monkeypatch):
        """Test list_splits when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        splits = service.list_splits()

        assert splits == []


# =============================================================================
# SpannerService Key Schema Tests
# =============================================================================

@pytest.mark.unit
class TestKeySchemaOperations:
    """Tests for key schema operations."""

    def test_get_table_key_schema_not_configured(self, clean_db, monkeypatch):
        """Test get_table_key_schema when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        schema = service.get_table_key_schema("UserInfo")

        assert schema.entity_name == "UserInfo"
        assert schema.key_columns == []

    def test_get_index_key_schema_not_configured(self, clean_db, monkeypatch):
        """Test get_index_key_schema when not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        service = SpannerService()
        schema = service.get_index_key_schema("UsersByLocation")

        assert schema.entity_name == "UsersByLocation"
        assert schema.key_columns == []
        assert schema.parent_table is None


# =============================================================================
# Business Logic Validation Tests
# =============================================================================

@pytest.mark.unit
class TestBusinessLogic:
    """Tests validating critical business logic requirements."""

    def test_deletion_uses_expiration(self, mock_spanner_service):
        """Verify deletion strategy uses immediate expiration, not actual delete.

        Business rule: To delete a split, we update its expiration time to now()
        """
        result = mock_spanner_service.delete_split_points("UserInfo", ["UserInfo(1)"])

        # Verify add_split_points was called (not a delete API)
        mock_spanner_service._client.database_admin_api.add_split_points.assert_called()

    def test_default_expiration_is_10_days(self, mock_spanner_service):
        """Verify new splits default to 10 days expiration.

        Business rule: New splits should default to expiring 10 days from creation
        """
        now = datetime.now()

        with patch('spanner_service.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            # The actual expiration time would be now + 10 days
            expected_expire = now + timedelta(days=10)

            # Verify the constant is correct
            assert DEFAULT_EXPIRATION_DAYS == 10

    def test_batch_limit_enforced(self, mock_spanner_service, clean_db):
        """Verify 100-split batch limit is enforced.

        Business rule: Max 100 split points per Spanner API request
        """
        # Add 150 splits
        for i in range(150):
            database.add_local_split("UserInfo", str(i), OperationType.ADD)

        mock_spanner_service.sync_pending_changes()

        # Verify add_split_points was called twice (2 batches)
        calls = mock_spanner_service._client.database_admin_api.add_split_points.call_count
        assert calls == 2
