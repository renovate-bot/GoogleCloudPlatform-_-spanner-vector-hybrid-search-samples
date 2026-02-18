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

"""Unit tests for SQLite database layer.

Tests CRUD operations for settings and local splits using an in-memory database.
"""
import pytest
import sqlite3
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

import database
from models import OperationType, LocalSplitResponse


# =============================================================================
# Database Initialization Tests
# =============================================================================

@pytest.mark.unit
class TestDatabaseInit:
    """Tests for database initialization."""

    def test_init_creates_tables(self, clean_db):
        """Test that init_db creates required tables."""
        with database.get_db() as conn:
            cursor = conn.cursor()

            # Check settings table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='settings'"
            )
            assert cursor.fetchone() is not None

            # Check local_splits table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='local_splits'"
            )
            assert cursor.fetchone() is not None

    def test_init_idempotent(self, clean_db):
        """Test that init_db can be called multiple times safely."""
        # Should not raise any errors
        database.init_db()
        database.init_db()

    def test_local_splits_has_index_columns(self, clean_db):
        """Test that local_splits table has index_name and index_key columns."""
        with database.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(local_splits)")
            columns = {row[1] for row in cursor.fetchall()}

            assert "index_name" in columns
            assert "index_key" in columns


# =============================================================================
# Settings Tests
# =============================================================================

@pytest.mark.unit
class TestSettings:
    """Tests for settings operations."""

    def test_set_and_get_setting(self, clean_db):
        """Test setting and retrieving a setting."""
        database.set_setting("project_id", "test-project")
        value = database.get_setting("project_id")
        assert value == "test-project"

    def test_get_nonexistent_setting(self, clean_db):
        """Test getting a setting that doesn't exist."""
        value = database.get_setting("nonexistent")
        assert value is None

    def test_update_existing_setting(self, clean_db):
        """Test updating an existing setting."""
        database.set_setting("project_id", "old-project")
        database.set_setting("project_id", "new-project")
        value = database.get_setting("project_id")
        assert value == "new-project"

    def test_get_all_settings_empty(self, clean_db):
        """Test getting all settings when none are set."""
        settings = database.get_all_settings()
        assert settings.project_id is None
        assert settings.instance_id is None
        assert settings.database_id is None

    def test_get_all_settings_populated(self, clean_db):
        """Test getting all settings when populated."""
        database.set_setting("project_id", "p1")
        database.set_setting("instance_id", "i1")
        database.set_setting("database_id", "d1")

        settings = database.get_all_settings()
        assert settings.project_id == "p1"
        assert settings.instance_id == "i1"
        assert settings.database_id == "d1"

    def test_update_settings_partial(self, clean_db):
        """Test updating only some settings."""
        database.update_settings(
            project_id="project1",
            instance_id=None,
            database_id=None
        )

        settings = database.get_all_settings()
        assert settings.project_id == "project1"
        assert settings.instance_id is None

    def test_update_settings_all(self, clean_db):
        """Test updating all settings at once."""
        database.update_settings(
            project_id="p2",
            instance_id="i2",
            database_id="d2"
        )

        settings = database.get_all_settings()
        assert settings.project_id == "p2"
        assert settings.instance_id == "i2"
        assert settings.database_id == "d2"


# =============================================================================
# Local Splits - Add Tests
# =============================================================================

@pytest.mark.unit
class TestAddLocalSplit:
    """Tests for adding local splits."""

    def test_add_table_split(self, clean_db):
        """Test adding a table split."""
        result = database.add_local_split(
            table_name="UserInfo",
            split_value="12345",
            operation_type=OperationType.ADD
        )

        assert isinstance(result, LocalSplitResponse)
        assert result.table_name == "UserInfo"
        assert result.split_value == "12345"
        assert result.operation_type == OperationType.ADD
        assert result.id is not None

    def test_add_index_split(self, clean_db):
        """Test adding an index split."""
        result = database.add_local_split(
            table_name="UserLocationInfo",
            split_value="12,JP",
            operation_type=OperationType.ADD,
            index_name="UsersByLocation",
            index_key="JP"
        )

        assert result.table_name == "UserLocationInfo"
        assert result.index_name == "UsersByLocation"
        assert result.index_key == "JP"

    def test_add_delete_operation(self, clean_db):
        """Test adding a delete operation."""
        result = database.add_local_split(
            table_name="UserInfo",
            split_value="99999",
            operation_type=OperationType.DELETE
        )

        assert result.operation_type == OperationType.DELETE

    def test_upsert_updates_existing(self, clean_db):
        """Test that adding same split updates operation type."""
        # Add first
        database.add_local_split(
            table_name="UserInfo",
            split_value="12345",
            operation_type=OperationType.ADD
        )

        # Upsert with different operation
        result = database.add_local_split(
            table_name="UserInfo",
            split_value="12345",
            operation_type=OperationType.DELETE
        )

        assert result.operation_type == OperationType.DELETE

        # Should still be only one record
        all_splits = database.get_all_local_splits()
        assert len(all_splits) == 1

    def test_add_split_with_empty_split_value(self, clean_db):
        """Test adding a split with empty split value."""
        result = database.add_local_split(
            table_name="TableWithoutSplitValue",
            split_value="",
            operation_type=OperationType.ADD
        )

        assert result.split_value == ""


# =============================================================================
# Local Splits - Query Tests
# =============================================================================

@pytest.mark.unit
class TestQueryLocalSplits:
    """Tests for querying local splits."""

    def test_get_all_local_splits_empty(self, clean_db):
        """Test getting splits when none exist."""
        splits = database.get_all_local_splits()
        assert splits == []

    def test_get_all_local_splits_multiple(self, clean_db):
        """Test getting multiple splits."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.ADD)
        database.add_local_split("Table3", "3", OperationType.DELETE)

        splits = database.get_all_local_splits()
        assert len(splits) == 3

    def test_get_splits_by_operation_add(self, clean_db):
        """Test filtering splits by ADD operation."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.DELETE)
        database.add_local_split("Table3", "3", OperationType.ADD)

        adds = database.get_local_splits_by_operation(OperationType.ADD)
        assert len(adds) == 2
        assert all(s.operation_type == OperationType.ADD for s in adds)

    def test_get_splits_by_operation_delete(self, clean_db):
        """Test filtering splits by DELETE operation."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.DELETE)

        deletes = database.get_local_splits_by_operation(OperationType.DELETE)
        assert len(deletes) == 1
        assert deletes[0].operation_type == OperationType.DELETE

    def test_get_local_split_by_table_and_value(self, clean_db):
        """Test getting a specific split by table and value."""
        database.add_local_split("UserInfo", "12345", OperationType.ADD)

        result = database.get_local_split_by_table_and_value("UserInfo", "12345")
        assert result is not None
        assert result.table_name == "UserInfo"
        assert result.split_value == "12345"

    def test_get_local_split_by_table_and_value_with_index(self, clean_db):
        """Test getting a specific index split."""
        database.add_local_split(
            "UserLocationInfo",
            "12,JP",
            OperationType.ADD,
            index_name="UsersByLocation",
            index_key="JP"
        )

        result = database.get_local_split_by_table_and_value(
            "UserLocationInfo",
            "12,JP",
            index_name="UsersByLocation",
            index_key="JP"
        )
        assert result is not None
        assert result.index_name == "UsersByLocation"

    def test_get_nonexistent_split(self, clean_db):
        """Test getting a split that doesn't exist."""
        result = database.get_local_split_by_table_and_value("Nonexistent", "999")
        assert result is None


# =============================================================================
# Local Splits - Delete Tests
# =============================================================================

@pytest.mark.unit
class TestDeleteLocalSplits:
    """Tests for deleting local splits."""

    def test_delete_by_id(self, clean_db):
        """Test deleting a split by ID."""
        result = database.add_local_split("UserInfo", "12345", OperationType.ADD)
        split_id = result.id

        success = database.delete_local_split(split_id)
        assert success is True

        # Verify deleted
        splits = database.get_all_local_splits()
        assert len(splits) == 0

    def test_delete_nonexistent_id(self, clean_db):
        """Test deleting a non-existent ID."""
        success = database.delete_local_split(99999)
        assert success is False

    def test_delete_by_value(self, clean_db):
        """Test deleting a split by table and value."""
        database.add_local_split("UserInfo", "12345", OperationType.ADD)

        success = database.delete_local_split_by_value("UserInfo", "12345")
        assert success is True

        # Verify deleted
        result = database.get_local_split_by_table_and_value("UserInfo", "12345")
        assert result is None

    def test_delete_by_value_with_index(self, clean_db):
        """Test deleting an index split by value."""
        database.add_local_split(
            "UserLocationInfo",
            "12,JP",
            OperationType.ADD,
            index_name="UsersByLocation",
            index_key="JP"
        )

        success = database.delete_local_split_by_value(
            "UserLocationInfo",
            "12,JP",
            index_name="UsersByLocation",
            index_key="JP"
        )
        assert success is True

    def test_delete_nonexistent_value(self, clean_db):
        """Test deleting a non-existent split by value."""
        success = database.delete_local_split_by_value("Nonexistent", "999")
        assert success is False


# =============================================================================
# Local Splits - Clear Tests
# =============================================================================

@pytest.mark.unit
class TestClearPendingSplits:
    """Tests for clearing pending splits."""

    def test_clear_all_splits(self, clean_db):
        """Test clearing all pending splits."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.DELETE)
        database.add_local_split("Table3", "3", OperationType.ADD)

        count = database.clear_pending_splits()
        assert count == 3

        splits = database.get_all_local_splits()
        assert len(splits) == 0

    def test_clear_only_adds(self, clean_db):
        """Test clearing only ADD operations."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.DELETE)
        database.add_local_split("Table3", "3", OperationType.ADD)

        count = database.clear_pending_splits(OperationType.ADD)
        assert count == 2

        # Only DELETE should remain
        splits = database.get_all_local_splits()
        assert len(splits) == 1
        assert splits[0].operation_type == OperationType.DELETE

    def test_clear_only_deletes(self, clean_db):
        """Test clearing only DELETE operations."""
        database.add_local_split("Table1", "1", OperationType.ADD)
        database.add_local_split("Table2", "2", OperationType.DELETE)

        count = database.clear_pending_splits(OperationType.DELETE)
        assert count == 1

        # Only ADD should remain
        splits = database.get_all_local_splits()
        assert len(splits) == 1
        assert splits[0].operation_type == OperationType.ADD

    def test_clear_empty_database(self, clean_db):
        """Test clearing when no splits exist."""
        count = database.clear_pending_splits()
        assert count == 0


# =============================================================================
# Edge Cases and Data Integrity Tests
# =============================================================================

@pytest.mark.unit
class TestDataIntegrity:
    """Tests for data integrity and edge cases."""

    def test_unique_constraint_table_split(self, clean_db):
        """Test unique constraint on table, split_value, index_name, index_key."""
        database.add_local_split("UserInfo", "12345", OperationType.ADD)

        # Same table/value should update, not create duplicate
        database.add_local_split("UserInfo", "12345", OperationType.DELETE)

        splits = database.get_all_local_splits()
        assert len(splits) == 1

    def test_unique_constraint_allows_different_tables(self, clean_db):
        """Test that same split_value can exist for different tables."""
        database.add_local_split("Table1", "12345", OperationType.ADD)
        database.add_local_split("Table2", "12345", OperationType.ADD)

        splits = database.get_all_local_splits()
        assert len(splits) == 2

    def test_unique_constraint_allows_different_indexes(self, clean_db):
        """Test that same table/value can have different index splits."""
        # Table split
        database.add_local_split("UserInfo", "12345", OperationType.ADD)

        # Index split on same table
        database.add_local_split(
            "UserInfo",
            "12345",
            OperationType.ADD,
            index_name="Index1",
            index_key="key1"
        )

        splits = database.get_all_local_splits()
        assert len(splits) == 2

    def test_created_at_timestamp(self, clean_db):
        """Test that created_at is set automatically."""
        result = database.add_local_split("UserInfo", "12345", OperationType.ADD)

        assert result.created_at is not None
        assert isinstance(result.created_at, datetime)

    def test_special_characters_in_values(self, clean_db):
        """Test handling of special characters in split values."""
        special_value = "test'value\"with;special<chars>"

        result = database.add_local_split("UserInfo", special_value, OperationType.ADD)
        assert result.split_value == special_value

        # Verify we can retrieve it
        retrieved = database.get_local_split_by_table_and_value("UserInfo", special_value)
        assert retrieved is not None
        assert retrieved.split_value == special_value

    def test_unicode_in_values(self, clean_db):
        """Test handling of unicode characters."""
        unicode_value = "split_value_\u4e2d\u6587_\u65e5\u672c\u8a9e"

        result = database.add_local_split("UserInfo", unicode_value, OperationType.ADD)
        assert result.split_value == unicode_value
