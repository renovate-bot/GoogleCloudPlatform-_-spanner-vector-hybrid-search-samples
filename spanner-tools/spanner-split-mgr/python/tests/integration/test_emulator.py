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

"""Integration tests using Spanner emulator.

These tests run against the Cloud Spanner emulator container,
testing the full workflow from local SQLite staging to Spanner sync.

Run with: pytest -m integration tests/integration/test_emulator.py
"""
import pytest
from pathlib import Path
import sys
import os

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# =============================================================================
# Emulator Connection Tests
# =============================================================================

@pytest.mark.integration
class TestEmulatorConnection:
    """Tests verifying emulator connectivity."""

    def test_emulator_starts(self, spanner_emulator):
        """Test that the emulator container starts successfully."""
        assert spanner_emulator["endpoint"] is not None
        assert spanner_emulator["grpc_port"] is not None

    def test_client_connects(self, emulator_client):
        """Test that the Spanner client can connect to emulator."""
        assert emulator_client is not None
        assert emulator_client.project == "test-project"

    def test_database_created(self, emulator_database):
        """Test that the test database is created with schema."""
        assert emulator_database is not None

        # Verify we can query the database
        with emulator_database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            tables = [row[0] for row in results]

            assert "UserInfo" in tables
            assert "UserLocationInfo" in tables


# =============================================================================
# SpannerService Integration Tests
# =============================================================================

@pytest.mark.integration
class TestSpannerServiceIntegration:
    """Integration tests for SpannerService with emulator."""

    def test_service_is_configured(self, emulator_spanner_service):
        """Test that service is properly configured for emulator."""
        assert emulator_spanner_service.is_configured() is True
        assert emulator_spanner_service.project_id == "test-project"
        assert emulator_spanner_service.instance_id == "test-instance"
        assert emulator_spanner_service.database_id == "test-database"

    def test_list_tables(self, emulator_spanner_service):
        """Test listing tables from emulator."""
        tables = emulator_spanner_service.list_tables()

        assert isinstance(tables, list)
        assert "UserInfo" in tables
        assert "UserLocationInfo" in tables

    def test_list_indexes(self, emulator_spanner_service):
        """Test listing indexes from emulator."""
        indexes = emulator_spanner_service.list_indexes()

        assert isinstance(indexes, list)
        # Find UsersByLocation index
        index_names = [idx[0] for idx in indexes]
        assert "UsersByLocation" in index_names

    def test_get_table_key_schema(self, emulator_spanner_service):
        """Test getting table key schema."""
        schema = emulator_spanner_service.get_table_key_schema("UserInfo")

        assert schema.entity_name == "UserInfo"
        assert len(schema.key_columns) > 0
        assert schema.key_columns[0].column_name == "user_id"

    def test_get_index_key_schema(self, emulator_spanner_service):
        """Test getting index key schema."""
        schema = emulator_spanner_service.get_index_key_schema("UsersByLocation")

        assert schema.entity_name == "UsersByLocation"
        assert schema.parent_table == "UserLocationInfo"


# =============================================================================
# Split Point Sync Integration Tests
# =============================================================================

@pytest.mark.integration
class TestSplitPointSync:
    """Integration tests for split point sync workflow."""

    def test_add_split_point_sync(self, emulator_spanner_service, clean_emulator_splits):
        """Test adding a split point and syncing to emulator.

        Workflow:
        1. Add split to local SQLite
        2. Sync to Spanner
        3. Verify split exists in Spanner
        """
        import database
        from models import OperationType

        # Add local split
        database.add_local_split(
            table_name="UserInfo",
            split_value="1000",
            operation_type=OperationType.ADD
        )

        # Verify it's pending locally
        pending = database.get_local_splits_by_operation(OperationType.ADD)
        assert len(pending) == 1

        # Sync to Spanner
        result = emulator_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.added_count == 1

        # Verify local pending was cleared
        pending_after = database.get_local_splits_by_operation(OperationType.ADD)
        assert len(pending_after) == 0

    def test_delete_split_point_sync(self, emulator_spanner_service, clean_emulator_splits):
        """Test deleting a split point via sync.

        Note: In Spanner, deletion is done via immediate expiration.
        """
        import database
        from models import OperationType

        # First add a split
        database.add_local_split(
            table_name="UserInfo",
            split_value="2000",
            operation_type=OperationType.ADD
        )
        emulator_spanner_service.sync_pending_changes()

        # Now mark for deletion
        database.add_local_split(
            table_name="UserInfo",
            split_value="UserInfo(2000)",
            operation_type=OperationType.DELETE
        )

        # Sync the delete
        result = emulator_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.deleted_count == 1

    def test_batch_sync_under_limit(self, emulator_spanner_service, clean_emulator_splits):
        """Test syncing multiple splits under batch limit."""
        import database
        from models import OperationType

        # Add 50 splits (under 100 limit)
        for i in range(50):
            database.add_local_split(
                table_name="UserInfo",
                split_value=str(3000 + i),
                operation_type=OperationType.ADD
            )

        result = emulator_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.added_count == 50


# NOTE: Batch tests over 100 may be slow with emulator
# Uncomment for thorough testing
#
# @pytest.mark.integration
# @pytest.mark.slow
# class TestBatchLimitIntegration:
#     """Integration tests for batch limit enforcement."""
#
#     def test_batch_sync_over_limit(self, emulator_spanner_service, clean_emulator_splits):
#         """Test syncing splits over 100 batch limit.
#
#         This verifies the 100-split batch limit is properly enforced
#         when syncing to Spanner.
#         """
#         import database
#         from models import OperationType
#
#         # Add 150 splits (over 100 limit)
#         for i in range(150):
#             database.add_local_split(
#                 table_name="UserInfo",
#                 split_value=str(10000 + i),
#                 operation_type=OperationType.ADD
#             )
#
#         result = emulator_spanner_service.sync_pending_changes()
#
#         assert result.success is True
#         assert result.added_count == 150


# =============================================================================
# Index Split Integration Tests
# =============================================================================

@pytest.mark.integration
class TestIndexSplitSync:
    """Integration tests for index split point sync."""

    def test_add_index_split(self, emulator_spanner_service, clean_emulator_splits):
        """Test adding an index split point."""
        import database
        from models import OperationType

        # Add index split
        database.add_local_split(
            table_name="UserLocationInfo",
            split_value="",  # Table key not always needed for index splits
            operation_type=OperationType.ADD,
            index_name="UsersByLocation",
            index_key="US"
        )

        result = emulator_spanner_service.sync_pending_changes()

        assert result.success is True
        assert result.added_count == 1


# =============================================================================
# End-to-End Workflow Tests
# =============================================================================

@pytest.mark.integration
class TestEndToEndWorkflow:
    """End-to-end workflow tests."""

    def test_full_workflow(self, emulator_spanner_service, clean_emulator_splits):
        """Test complete workflow: add -> sync -> verify -> delete -> sync.

        This tests the complete lifecycle of split point management.
        """
        import database
        from models import OperationType

        # 1. Add splits locally
        database.add_local_split("UserInfo", "5000", OperationType.ADD)
        database.add_local_split("UserInfo", "5001", OperationType.ADD)

        # 2. Verify pending
        pending = database.get_all_local_splits()
        assert len(pending) == 2

        # 3. Sync adds
        add_result = emulator_spanner_service.sync_pending_changes()
        assert add_result.success is True
        assert add_result.added_count == 2

        # 4. Verify local is cleared
        pending_after_add = database.get_all_local_splits()
        assert len(pending_after_add) == 0

        # 5. List splits from Spanner
        spanner_splits = emulator_spanner_service.list_splits()
        # Splits should exist (may have others from previous tests)
        assert isinstance(spanner_splits, list)

        # 6. Mark one for deletion
        database.add_local_split("UserInfo", "UserInfo(5000)", OperationType.DELETE)

        # 7. Sync delete
        delete_result = emulator_spanner_service.sync_pending_changes()
        assert delete_result.success is True
        assert delete_result.deleted_count == 1

    def test_settings_persistence_workflow(self, emulator_spanner_service):
        """Test that settings persist correctly."""
        import database

        # Get current settings
        settings = database.get_all_settings()
        assert settings.project_id == "test-project"
        assert settings.instance_id == "test-instance"
        assert settings.database_id == "test-database"

        # Update and verify
        database.update_settings(
            project_id="updated-project",
            instance_id=None,  # Don't change
            database_id=None   # Don't change
        )

        updated_settings = database.get_all_settings()
        assert updated_settings.project_id == "updated-project"
        assert updated_settings.instance_id == "test-instance"  # Unchanged

        # Restore original for other tests
        database.update_settings(
            project_id="test-project",
            instance_id=None,
            database_id=None
        )
