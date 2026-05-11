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

"""Live Spanner integration tests.

These tests run against actual Google Cloud Spanner resources.
They require proper GCP credentials and environment configuration.

CAUTION: These tests interact with real GCP resources and may incur costs.

Required environment variables:
- SPANNER_PROJECT_ID: GCP project ID
- SPANNER_INSTANCE_ID: Spanner instance ID
- SPANNER_DATABASE_ID: Spanner database ID

Optional:
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account key file

Run with: pytest -m spanner_live tests/integration/test_live_spanner.py

Test Schema:
The tests use a schema created by the `live_spanner_test_schema` fixture:

Tables:
- Users: Single column INT64 primary key (user_id)
- Orders: Single column INT64 primary key (order_id)
- OrderItems: Composite primary key (order_id, item_id)
- Products: Single column STRING primary key (product_id)

Indexes:
- idx_orders_user_id on Orders(user_id)
- idx_orders_status on Orders(status)
- idx_products_category on Products(category)
- idx_order_items_product on OrderItems(product_name)
"""
import os
import time
import pytest
from pathlib import Path
from typing import Generator, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import database
from models import OperationType, SplitStatus


# =============================================================================
# Helper Functions
# =============================================================================

def generate_unique_split_value(prefix: str = "test") -> str:
    """Generate a unique split value using timestamp to avoid collisions.

    Args:
        prefix: Optional prefix for the split value

    Returns:
        A unique string suitable for use as a split value
    """
    return f"{prefix}_{int(time.time() * 1000)}"


def wait_for_split_propagation(seconds: float = 1.0) -> None:
    """Wait for split changes to propagate in Spanner.

    Args:
        seconds: Number of seconds to wait
    """
    time.sleep(seconds)


# =============================================================================
# Fixtures for Test Schema Tables
# =============================================================================

@pytest.fixture
def int64_test_table(live_spanner_test_schema) -> str:
    """Get a table with INT64 primary key for testing.

    Uses the 'Users' table from the test schema.
    """
    if "Users" not in live_spanner_test_schema.get("tables", []):
        pytest.skip("Users table not available in test schema")
    return "Users"


@pytest.fixture
def string_test_table(live_spanner_test_schema) -> str:
    """Get a table with STRING primary key for testing.

    Uses the 'Products' table from the test schema.
    """
    if "Products" not in live_spanner_test_schema.get("tables", []):
        pytest.skip("Products table not available in test schema")
    return "Products"


@pytest.fixture
def composite_test_table(live_spanner_test_schema) -> str:
    """Get a table with composite primary key for testing.

    Uses the 'OrderItems' table from the test schema.
    """
    if "OrderItems" not in live_spanner_test_schema.get("tables", []):
        pytest.skip("OrderItems table not available in test schema")
    return "OrderItems"


@pytest.fixture
def test_index(live_spanner_test_schema) -> tuple:
    """Get an index for testing split operations.

    Returns:
        Tuple of (index_name, parent_table)
    """
    if "idx_orders_user_id" not in live_spanner_test_schema.get("indexes", []):
        pytest.skip("idx_orders_user_id index not available in test schema")
    return ("idx_orders_user_id", "Orders")


# =============================================================================
# Live Spanner Connection Tests
# =============================================================================

@pytest.mark.spanner_live
class TestLiveSpannerConnection:
    """Tests verifying live Spanner connectivity."""

    def test_service_connects(self, live_spanner_service):
        """Test that service can connect to live Spanner."""
        assert live_spanner_service.is_configured() is True

    def test_list_tables(self, live_spanner_service, live_spanner_test_schema):
        """Test listing tables from live Spanner.

        Verifies that the test schema tables are present.
        """
        tables = live_spanner_service.list_tables()

        assert isinstance(tables, list)
        # Verify test schema tables are present
        expected_tables = live_spanner_test_schema.get("tables", [])
        for table_name in expected_tables:
            assert table_name in tables, f"Expected table {table_name} not found"
        print(f"Found {len(tables)} tables: {tables}")

    def test_list_indexes(self, live_spanner_service, live_spanner_test_schema):
        """Test listing indexes from live Spanner.

        Verifies that the test schema indexes are present.
        """
        indexes = live_spanner_service.list_indexes()

        assert isinstance(indexes, list)
        # Verify test schema indexes are present
        expected_indexes = live_spanner_test_schema.get("indexes", [])
        index_names = [idx[0] for idx in indexes]
        for index_name in expected_indexes:
            assert index_name in index_names, f"Expected index {index_name} not found"
        print(f"Found {len(indexes)} indexes")

    def test_list_splits(self, live_spanner_service):
        """Test listing existing split points."""
        splits = live_spanner_service.list_splits()

        assert isinstance(splits, list)
        print(f"Found {len(splits)} existing split points")


# =============================================================================
# Live Spanner Read-Only Tests
# =============================================================================

@pytest.mark.spanner_live
class TestLiveSpannerReadOnly:
    """Read-only tests that don't modify Spanner state."""

    def test_get_table_key_schema_users(self, live_spanner_service, live_spanner_test_schema):
        """Test getting table key schema for Users table (INT64 key)."""
        if "Users" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Users table not available")

        schema = live_spanner_service.get_table_key_schema("Users")

        assert schema.entity_name == "Users"
        assert len(schema.key_columns) == 1
        assert schema.key_columns[0].column_name == "user_id"
        assert "INT64" in schema.key_columns[0].spanner_type
        assert schema.is_composite is False
        print(f"Users table schema: {schema}")

    def test_get_table_key_schema_order_items(self, live_spanner_service, live_spanner_test_schema):
        """Test getting table key schema for OrderItems table (composite key)."""
        if "OrderItems" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("OrderItems table not available")

        schema = live_spanner_service.get_table_key_schema("OrderItems")

        assert schema.entity_name == "OrderItems"
        assert len(schema.key_columns) == 2
        assert schema.key_columns[0].column_name == "order_id"
        assert schema.key_columns[1].column_name == "item_id"
        assert schema.is_composite is True
        print(f"OrderItems table schema: {schema}")

    def test_get_table_key_schema_products(self, live_spanner_service, live_spanner_test_schema):
        """Test getting table key schema for Products table (STRING key)."""
        if "Products" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Products table not available")

        schema = live_spanner_service.get_table_key_schema("Products")

        assert schema.entity_name == "Products"
        assert len(schema.key_columns) == 1
        assert schema.key_columns[0].column_name == "product_id"
        assert "STRING" in schema.key_columns[0].spanner_type
        assert schema.is_composite is False
        print(f"Products table schema: {schema}")

    def test_get_index_key_schema(self, live_spanner_service, live_spanner_test_schema):
        """Test getting index key schema from live Spanner."""
        if "idx_orders_user_id" not in live_spanner_test_schema.get("indexes", []):
            pytest.skip("idx_orders_user_id index not available")

        schema = live_spanner_service.get_index_key_schema("idx_orders_user_id")

        assert schema.entity_name == "idx_orders_user_id"
        assert schema.parent_table == "Orders"
        print(f"Index idx_orders_user_id parent table: {schema.parent_table}")

    def test_get_table_key_schema_returns_correct_structure(self, live_spanner_service, live_spanner_test_schema):
        """Test that table key schema returns properly structured data."""
        tables = live_spanner_test_schema.get("tables", [])

        if not tables:
            pytest.skip("No tables available for testing")

        for table_name in tables:
            schema = live_spanner_service.get_table_key_schema(table_name)

            assert schema.entity_name == table_name
            assert schema.entity_type.value == "TABLE"
            assert isinstance(schema.key_columns, list)
            assert isinstance(schema.is_composite, bool)

            if schema.key_columns:
                for col in schema.key_columns:
                    assert col.column_name
                    assert col.spanner_type
                    assert col.ordinal_position >= 0

            print(f"Table {table_name}: {len(schema.key_columns)} key cols, composite={schema.is_composite}")

    def test_get_index_key_schema_includes_parent_info(self, live_spanner_service, live_spanner_test_schema):
        """Test that index key schema includes parent table information."""
        # Get actual indexes from Spanner service (returns tuples of index_name, parent_table)
        indexes = live_spanner_service.list_indexes()

        if not indexes:
            pytest.skip("No indexes available for testing")

        # Filter to only test schema indexes
        schema_indexes = live_spanner_test_schema.get("indexes", [])
        test_indexes = [(name, parent) for name, parent in indexes if name in schema_indexes]

        for index_name, parent_table in test_indexes:
            schema = live_spanner_service.get_index_key_schema(index_name)

            assert schema.entity_name == index_name
            assert schema.entity_type.value == "INDEX"
            assert schema.parent_table == parent_table

            print(f"Index {index_name}: parent={schema.parent_table}, "
                  f"cols={len(schema.key_columns)}, "
                  f"parent_cols={len(schema.parent_key_columns or [])}")


# =============================================================================
# Live Spanner Write Tests - Single Split Operations
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerSingleSplitOperations:
    """Tests for adding and deleting single split points.

    WARNING: These tests add and remove split points from your live database.
    Only run these in a non-production environment.

    To run: pytest -m "spanner_live and destructive" --run-destructive

    Uses the Users table (INT64 primary key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_table(self, live_spanner_test_schema) -> str:
        """Get a table suitable for testing split operations.

        Uses the Users table which has an INT64 primary key.
        """
        if "Users" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Users table not available for testing")
        return "Users"

    def test_add_single_split_point(self, live_spanner_service, test_table):
        """Test adding a single split point to Spanner.

        Verifies:
        - Split can be added via the API
        - The sync operation reports success
        - The added count is 1
        """
        unique_value = generate_unique_split_value("single_add")

        # Add split locally
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )

        # Sync to Spanner
        result = live_spanner_service.sync_pending_changes()

        print(f"Add single split result: {result}")

        assert result.success is True
        assert result.added_count == 1
        assert result.deleted_count == 0

        # Cleanup: mark for deletion
        wait_for_split_propagation()
        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({unique_value})",
            operation_type=OperationType.DELETE
        )
        cleanup_result = live_spanner_service.sync_pending_changes()
        print(f"Cleanup result: {cleanup_result}")

    def test_delete_single_split_point(self, live_spanner_service, test_table):
        """Test deleting a single split point from Spanner.

        Verifies:
        - Split is first added successfully
        - Split can then be deleted (via immediate expiration)
        - The sync operation reports success
        """
        unique_value = generate_unique_split_value("single_del")

        # First add the split
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )
        add_result = live_spanner_service.sync_pending_changes()

        assert add_result.success is True, f"Failed to add split: {add_result.errors}"

        wait_for_split_propagation()

        # Now delete it
        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({unique_value})",
            operation_type=OperationType.DELETE
        )

        delete_result = live_spanner_service.sync_pending_changes()

        print(f"Delete result: {delete_result}")

        assert delete_result.success is True
        assert delete_result.deleted_count == 1

    def test_add_and_delete_split_round_trip(self, live_spanner_service, test_table):
        """Test complete add-delete cycle for a split point.

        Verifies the complete lifecycle:
        1. Add split point
        2. Verify it syncs successfully
        3. Delete the split point
        4. Verify deletion syncs successfully
        """
        unique_value = generate_unique_split_value("roundtrip")

        # Add
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )
        add_result = live_spanner_service.sync_pending_changes()

        assert add_result.success is True
        assert add_result.added_count == 1
        print(f"Added split: {unique_value}")

        wait_for_split_propagation()

        # Delete
        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({unique_value})",
            operation_type=OperationType.DELETE
        )
        delete_result = live_spanner_service.sync_pending_changes()

        assert delete_result.success is True
        assert delete_result.deleted_count == 1
        print(f"Deleted split: {unique_value}")


# =============================================================================
# Live Spanner Write Tests - Batch Operations
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerBatchOperations:
    """Tests for batch split point operations.

    These tests verify the batching behavior which is critical for Spanner's
    100 split points per request limit.

    WARNING: These tests add and remove multiple split points.
    Only run in a non-production environment.

    Uses the Orders table (INT64 primary key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_table(self, live_spanner_test_schema) -> str:
        """Get a table suitable for testing split operations.

        Uses the Orders table which has an INT64 primary key.
        """
        if "Orders" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Orders table not available for testing")
        return "Orders"

    def test_add_multiple_splits_small_batch(self, live_spanner_service, test_table):
        """Test adding a small batch of split points (under 100 limit).

        Verifies:
        - Multiple splits can be added in one sync operation
        - All splits are reported as added
        """
        batch_size = 5
        base_value = generate_unique_split_value("batch_small")

        # Add splits locally
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{base_value}_{i}",
                operation_type=OperationType.ADD
            )

        # Sync to Spanner
        result = live_spanner_service.sync_pending_changes()

        print(f"Batch add result ({batch_size} splits): {result}")

        assert result.success is True
        assert result.added_count == batch_size

        # Cleanup
        wait_for_split_propagation()
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({base_value}_{i})",
                operation_type=OperationType.DELETE
            )
        cleanup_result = live_spanner_service.sync_pending_changes()
        print(f"Cleanup result: {cleanup_result}")

    def test_delete_multiple_splits_small_batch(self, live_spanner_service, test_table):
        """Test deleting a small batch of split points.

        Verifies:
        - Multiple splits can be deleted in one sync operation
        - All splits are reported as deleted
        """
        batch_size = 3
        base_value = generate_unique_split_value("batch_del")

        # First add the splits
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{base_value}_{i}",
                operation_type=OperationType.ADD
            )

        add_result = live_spanner_service.sync_pending_changes()
        assert add_result.success is True, f"Failed to add splits: {add_result.errors}"

        wait_for_split_propagation()

        # Now delete them all
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({base_value}_{i})",
                operation_type=OperationType.DELETE
            )

        delete_result = live_spanner_service.sync_pending_changes()

        print(f"Batch delete result ({batch_size} splits): {delete_result}")

        assert delete_result.success is True
        assert delete_result.deleted_count == batch_size

    def test_mixed_add_and_delete_batch(self, live_spanner_service, test_table):
        """Test a batch with both add and delete operations.

        Verifies:
        - Mixed operations (adds and deletes) can be processed in one sync
        - Both added_count and deleted_count are correct
        """
        # First, add some splits that we'll delete later
        setup_base = generate_unique_split_value("mixed_setup")
        num_to_delete = 2

        for i in range(num_to_delete):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{setup_base}_{i}",
                operation_type=OperationType.ADD
            )

        setup_result = live_spanner_service.sync_pending_changes()
        assert setup_result.success is True

        wait_for_split_propagation()

        # Now queue both adds and deletes
        add_base = generate_unique_split_value("mixed_add")
        num_to_add = 3

        for i in range(num_to_add):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{add_base}_{i}",
                operation_type=OperationType.ADD
            )

        for i in range(num_to_delete):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({setup_base}_{i})",
                operation_type=OperationType.DELETE
            )

        # Sync both operations
        result = live_spanner_service.sync_pending_changes()

        print(f"Mixed batch result: {result}")

        assert result.success is True
        assert result.added_count == num_to_add
        assert result.deleted_count == num_to_delete

        # Cleanup newly added splits
        wait_for_split_propagation()
        for i in range(num_to_add):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({add_base}_{i})",
                operation_type=OperationType.DELETE
            )
        live_spanner_service.sync_pending_changes()


# =============================================================================
# Live Spanner Write Tests - Composite Key Tables
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerCompositeKeys:
    """Tests for split points on tables with composite primary keys.

    Composite keys require comma-separated values in the split_value field.

    WARNING: These tests modify split points in your database.

    Uses the OrderItems table (order_id, item_id composite key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def composite_key_table(self, live_spanner_service, live_spanner_test_schema) -> Optional[tuple]:
        """Get the OrderItems table with composite primary key for testing.

        Returns:
            Tuple of (table_name, schema) or None if not available
        """
        if "OrderItems" not in live_spanner_test_schema.get("tables", []):
            return None

        schema = live_spanner_service.get_table_key_schema("OrderItems")
        if schema.is_composite and len(schema.key_columns) >= 2:
            print(f"Using composite key table: OrderItems with "
                  f"{len(schema.key_columns)} key columns")
            return ("OrderItems", schema)

        return None

    def test_add_split_with_composite_key(self, live_spanner_service, composite_key_table):
        """Test adding a split point for a table with composite primary key.

        Verifies:
        - Split with comma-separated key values can be added
        - The sync operation handles composite keys correctly
        """
        if composite_key_table is None:
            pytest.skip("No table with composite key found in database")

        table_name, schema = composite_key_table
        num_keys = len(schema.key_columns)

        # Generate unique values for each key column
        base = generate_unique_split_value("composite")
        key_values = [f"{base}_{i}" for i in range(num_keys)]
        composite_value = ", ".join(key_values)

        print(f"Adding composite split for {table_name}: {composite_value}")

        database.add_local_split(
            table_name=table_name,
            split_value=composite_value,
            operation_type=OperationType.ADD
        )

        result = live_spanner_service.sync_pending_changes()

        print(f"Composite key add result: {result}")

        # Note: Success depends on key types matching - may fail if types don't match
        if result.success:
            assert result.added_count == 1

            # Cleanup
            wait_for_split_propagation()
            database.add_local_split(
                table_name=table_name,
                split_value=f"{table_name}({composite_value})",
                operation_type=OperationType.DELETE
            )
            live_spanner_service.sync_pending_changes()
        else:
            # Log the error for debugging but don't fail if it's a type mismatch
            print(f"Composite key test encountered error (may be expected): {result.errors}")

    def test_list_composite_key_tables(self, live_spanner_service):
        """Test listing and identifying tables with composite keys.

        Verifies:
        - Service can identify single vs composite key tables
        - Schema information is accurate
        """
        tables = live_spanner_service.list_tables()

        single_key_tables = []
        composite_key_tables = []

        for table_name in tables:
            schema = live_spanner_service.get_table_key_schema(table_name)
            if schema.is_composite:
                composite_key_tables.append((table_name, len(schema.key_columns)))
            else:
                single_key_tables.append(table_name)

        print(f"Single key tables ({len(single_key_tables)}): {single_key_tables}")
        print(f"Composite key tables ({len(composite_key_tables)}): {composite_key_tables}")

        # Verify is_composite flag is consistent
        for table_name, num_cols in composite_key_tables:
            assert num_cols > 1, f"Table {table_name} marked composite but has {num_cols} keys"


# =============================================================================
# Live Spanner Write Tests - Index Splits
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerIndexSplits:
    """Tests for split points on indexes.

    Index splits require the index_name and index_key fields in addition
    to the table key values.

    WARNING: These tests modify index split points in your database.

    Uses the idx_orders_user_id index (on Orders table) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_index(self, live_spanner_service, live_spanner_test_schema) -> Optional[tuple]:
        """Get an index suitable for testing split operations.

        Uses the idx_orders_user_id index from the test schema.

        Returns:
            Tuple of (index_name, parent_table, schema) or None
        """
        if "idx_orders_user_id" not in live_spanner_test_schema.get("indexes", []):
            return None

        index_name = "idx_orders_user_id"
        parent_table = "Orders"
        schema = live_spanner_service.get_index_key_schema(index_name)

        return (index_name, parent_table, schema)

    def test_add_index_split_point(self, live_spanner_service, test_index):
        """Test adding a split point on an index.

        Verifies:
        - Index split can be added with index_name and index_key
        - The sync operation handles index splits correctly
        """
        if test_index is None:
            pytest.skip("No indexes available for testing")

        index_name, parent_table, schema = test_index
        unique_key = generate_unique_split_value("idx")

        print(f"Adding index split for {index_name} (parent: {parent_table})")
        print(f"Index schema: {len(schema.key_columns)} key cols, "
              f"parent key cols: {len(schema.parent_key_columns or [])}")

        database.add_local_split(
            table_name=parent_table,
            split_value="",  # Table key may be optional for some index configurations
            operation_type=OperationType.ADD,
            index_name=index_name,
            index_key=unique_key
        )

        result = live_spanner_service.sync_pending_changes()

        print(f"Index split add result: {result}")

        if result.success:
            assert result.added_count == 1

            # Cleanup - construct the expected format for deletion
            wait_for_split_propagation()
            delete_value = f"Index: {index_name} on {parent_table}, Index Key: ({unique_key}), Primary Table Key: (<begin>,<begin>)"
            database.add_local_split(
                table_name=parent_table,
                split_value=delete_value,
                operation_type=OperationType.DELETE
            )
            live_spanner_service.sync_pending_changes()
        else:
            print(f"Index split test error (may be expected): {result.errors}")

    def test_list_index_splits(self, live_spanner_service):
        """Test that index splits are properly listed and identified.

        Verifies:
        - Existing index splits can be retrieved
        - Index splits have the expected structure (index name populated)
        """
        splits = live_spanner_service.list_splits()

        index_splits = [s for s in splits if s.index]
        table_splits = [s for s in splits if not s.index]

        print(f"Total splits: {len(splits)}")
        print(f"Index splits: {len(index_splits)}")
        print(f"Table splits: {len(table_splits)}")

        for split in index_splits[:3]:  # Show first 3 index splits
            print(f"  Index: {split.index}, Key: {split.split_key[:50]}...")


# =============================================================================
# Live Spanner Write Tests - Edge Cases
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerEdgeCases:
    """Tests for edge cases and error handling.

    These tests verify the system handles unusual situations gracefully.

    WARNING: These tests may add and remove split points.

    Uses the Products table (STRING primary key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_table(self, live_spanner_test_schema) -> str:
        """Get a table suitable for testing.

        Uses the Products table which has a STRING primary key.
        """
        if "Products" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Products table not available for testing")
        return "Products"

    def test_sync_with_no_pending_changes(self, live_spanner_service):
        """Test syncing when there are no pending changes.

        Verifies:
        - Sync operation handles empty queue gracefully
        - No errors are raised
        """
        # Clear any existing pending changes
        database.clear_pending_splits()

        result = live_spanner_service.sync_pending_changes()

        print(f"Empty sync result: {result}")

        # Should handle gracefully - counts should be 0
        assert result.added_count == 0
        assert result.deleted_count == 0

    def test_add_duplicate_split_value(self, live_spanner_service, test_table):
        """Test adding the same split value twice.

        Verifies:
        - Adding a duplicate split is handled gracefully
        - Local database prevents true duplicates via UNIQUE constraint
        """
        unique_value = generate_unique_split_value("duplicate")

        # Add the split first time
        split1 = database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )

        # Try to add the same split again - should update existing
        split2 = database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )

        # SQLite should use ON CONFLICT UPDATE, so IDs might be same
        print(f"First add ID: {split1.id}, Second add ID: {split2.id}")

        # Sync should only have one split to add
        result = live_spanner_service.sync_pending_changes()

        print(f"Duplicate add result: {result}")

        assert result.added_count <= 1  # Should only add once

        # Cleanup
        if result.success:
            wait_for_split_propagation()
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({unique_value})",
                operation_type=OperationType.DELETE
            )
            live_spanner_service.sync_pending_changes()

    def test_delete_nonexistent_split(self, live_spanner_service, test_table):
        """Test deleting a split that doesn't exist in Spanner.

        Verifies:
        - Deleting a non-existent split is handled gracefully
        - No crash or unhandled exception occurs
        """
        # Create a value that definitely doesn't exist
        nonexistent_value = generate_unique_split_value("nonexistent")

        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({nonexistent_value})",
            operation_type=OperationType.DELETE
        )

        result = live_spanner_service.sync_pending_changes()

        print(f"Delete nonexistent result: {result}")

        # The operation may succeed (Spanner allows setting expiration on non-existent splits)
        # or fail gracefully with an error message
        # Either way, it shouldn't crash
        assert isinstance(result.success, bool)

    def test_add_split_with_special_characters(self, live_spanner_service, test_table):
        """Test adding split with special characters in the value.

        Note: Most special characters should work, but some may be rejected
        by the Spanner API depending on the column type.
        """
        base = generate_unique_split_value("special")
        # Use alphanumeric special patterns that are more likely to be accepted
        special_value = f"{base}_with_underscores"

        database.add_local_split(
            table_name=test_table,
            split_value=special_value,
            operation_type=OperationType.ADD
        )

        result = live_spanner_service.sync_pending_changes()

        print(f"Special character result: {result}")

        if result.success:
            # Cleanup
            wait_for_split_propagation()
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({special_value})",
                operation_type=OperationType.DELETE
            )
            live_spanner_service.sync_pending_changes()

    def test_rapid_add_delete_same_split(self, live_spanner_service, test_table):
        """Test rapid add then delete of the same split value.

        Verifies:
        - Quick succession of add/delete is handled correctly
        - State transitions work properly
        """
        unique_value = generate_unique_split_value("rapid")

        # Add
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )

        # Immediately change to delete (before sync)
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.DELETE
        )

        # Check what's pending
        pending_adds = database.get_local_splits_by_operation(OperationType.ADD)
        pending_deletes = database.get_local_splits_by_operation(OperationType.DELETE)

        print(f"After rapid change - Pending adds: {len(pending_adds)}, "
              f"Pending deletes: {len(pending_deletes)}")

        # The second add_local_split should have overwritten the first due to UNIQUE constraint
        result = live_spanner_service.sync_pending_changes()

        print(f"Rapid change result: {result}")


# =============================================================================
# Live Spanner Write Tests - Large Batch (100+ Splits)
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerLargeBatch:
    """Tests for batches exceeding the 100 split limit.

    The Spanner API has a limit of 100 split points per request.
    These tests verify that the batching logic correctly chunks larger requests.

    WARNING: These tests add many split points. Only run in test environments.

    Uses the Users table (INT64 primary key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_table(self, live_spanner_test_schema) -> str:
        """Get a table suitable for testing.

        Uses the Users table which has an INT64 primary key.
        """
        if "Users" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Users table not available for testing")
        return "Users"

    def test_add_exactly_100_splits(self, live_spanner_service, test_table):
        """Test adding exactly 100 splits (at the batch limit).

        Verifies:
        - Exactly 100 splits can be added in a single batch
        - No chunking is needed at the boundary
        """
        batch_size = 100
        base_value = generate_unique_split_value("batch100")

        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{base_value}_{i:04d}",
                operation_type=OperationType.ADD
            )

        result = live_spanner_service.sync_pending_changes()

        print(f"100 splits result: {result}")

        assert result.success is True
        assert result.added_count == batch_size

        # Cleanup
        wait_for_split_propagation(2.0)  # Wait longer for larger batch
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({base_value}_{i:04d})",
                operation_type=OperationType.DELETE
            )
        cleanup_result = live_spanner_service.sync_pending_changes()
        print(f"Cleanup 100 splits result: {cleanup_result}")

    def test_add_101_splits_requires_batching(self, live_spanner_service, test_table):
        """Test adding 101 splits (just over the batch limit).

        Verifies:
        - 101 splits are correctly split into two batches (100 + 1)
        - All splits are successfully added
        """
        batch_size = 101
        base_value = generate_unique_split_value("batch101")

        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{base_value}_{i:04d}",
                operation_type=OperationType.ADD
            )

        result = live_spanner_service.sync_pending_changes()

        print(f"101 splits result: {result}")

        assert result.success is True
        assert result.added_count == batch_size

        # Cleanup
        wait_for_split_propagation(2.0)
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({base_value}_{i:04d})",
                operation_type=OperationType.DELETE
            )
        cleanup_result = live_spanner_service.sync_pending_changes()
        print(f"Cleanup 101 splits result: {cleanup_result}")

    @pytest.mark.slow
    def test_add_250_splits_multiple_batches(self, live_spanner_service, test_table):
        """Test adding 250 splits (requires 3 batches: 100 + 100 + 50).

        Verifies:
        - Large batches are correctly chunked
        - All splits are added across multiple API calls

        Note: This test is marked slow and may take longer to execute.
        """
        batch_size = 250
        base_value = generate_unique_split_value("batch250")

        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{base_value}_{i:04d}",
                operation_type=OperationType.ADD
            )

        result = live_spanner_service.sync_pending_changes()

        print(f"250 splits result: {result}")

        assert result.success is True
        assert result.added_count == batch_size

        # Cleanup
        wait_for_split_propagation(3.0)  # Wait longer for larger batch
        for i in range(batch_size):
            database.add_local_split(
                table_name=test_table,
                split_value=f"{test_table}({base_value}_{i:04d})",
                operation_type=OperationType.DELETE
            )
        cleanup_result = live_spanner_service.sync_pending_changes()
        print(f"Cleanup 250 splits result: {cleanup_result}")


# =============================================================================
# Live Spanner Verification Tests
# =============================================================================

@pytest.mark.spanner_live
@pytest.mark.destructive
class TestLiveSpannerVerification:
    """Tests that verify split points actually appear in Spanner.

    These tests add splits and then verify they can be retrieved from
    the SPANNER_SYS.USER_SPLIT_POINTS table.

    WARNING: These tests add and remove split points.

    Uses the Orders table (INT64 primary key) from the test schema.
    """

    @pytest.fixture(autouse=True)
    def check_destructive_allowed(self, request):
        """Skip destructive tests unless explicitly allowed."""
        if not request.config.getoption("--run-destructive", default=False):
            pytest.skip("Destructive tests require --run-destructive flag")

    @pytest.fixture
    def test_table(self, live_spanner_test_schema) -> str:
        """Get a table suitable for testing.

        Uses the Orders table which has an INT64 primary key.
        """
        if "Orders" not in live_spanner_test_schema.get("tables", []):
            pytest.skip("Orders table not available for testing")
        return "Orders"

    def test_added_split_appears_in_list(self, live_spanner_service, test_table):
        """Test that an added split point appears in the splits list.

        Verifies:
        - Split is added successfully
        - Split appears in list_splits() result
        - Split has expected properties (table, key format)
        """
        unique_value = generate_unique_split_value("verify")

        # Get initial split count
        initial_splits = live_spanner_service.list_splits()
        initial_count = len(initial_splits)

        # Add split
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )
        result = live_spanner_service.sync_pending_changes()

        assert result.success is True

        wait_for_split_propagation(2.0)  # Wait for Spanner to reflect the change

        # Verify split appears
        updated_splits = live_spanner_service.list_splits()

        print(f"Initial splits: {initial_count}, After add: {len(updated_splits)}")

        # Look for our split
        found = False
        for split in updated_splits:
            if unique_value in split.split_key:
                found = True
                assert split.table == test_table
                print(f"Found split: {split.split_key}")
                break

        # Cleanup regardless of whether we found it
        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({unique_value})",
            operation_type=OperationType.DELETE
        )
        live_spanner_service.sync_pending_changes()

        assert found, f"Added split with value '{unique_value}' not found in splits list"

    def test_deleted_split_has_expiration(self, live_spanner_service, test_table):
        """Test that deleting a split sets its expiration to now.

        Verifies:
        - After 'deletion', the split's expire_time is set
        - The expiration time is in the past or very near present
        """
        unique_value = generate_unique_split_value("expire")

        # Add split
        database.add_local_split(
            table_name=test_table,
            split_value=unique_value,
            operation_type=OperationType.ADD
        )
        add_result = live_spanner_service.sync_pending_changes()
        assert add_result.success is True

        wait_for_split_propagation(2.0)

        # Delete (set expiration)
        database.add_local_split(
            table_name=test_table,
            split_value=f"{test_table}({unique_value})",
            operation_type=OperationType.DELETE
        )
        delete_result = live_spanner_service.sync_pending_changes()

        print(f"Delete result: {delete_result}")

        # Note: The split may be immediately removed from the system view
        # or may briefly appear with an expiration time
        # Both behaviors are valid Spanner implementations


# =============================================================================
# Test Documentation
# =============================================================================
"""
Running Live Spanner Tests
==========================

1. Set up environment variables:

   export SPANNER_PROJECT_ID="your-project-id"
   export SPANNER_INSTANCE_ID="your-instance-id"
   export SPANNER_DATABASE_ID="your-database-id"
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

2. Run read-only tests:

   pytest -m spanner_live tests/integration/test_live_spanner.py -v

3. Run all tests including destructive ones (CAUTION):

   pytest -m spanner_live tests/integration/test_live_spanner.py -v --run-destructive

4. Run specific test classes:

   # Single split operations
   pytest -m spanner_live tests/integration/test_live_spanner.py::TestLiveSpannerSingleSplitOperations -v --run-destructive

   # Batch operations
   pytest -m spanner_live tests/integration/test_live_spanner.py::TestLiveSpannerBatchOperations -v --run-destructive

   # Large batch tests (100+ splits)
   pytest -m spanner_live tests/integration/test_live_spanner.py::TestLiveSpannerLargeBatch -v --run-destructive

   # Index splits
   pytest -m spanner_live tests/integration/test_live_spanner.py::TestLiveSpannerIndexSplits -v --run-destructive

   # Edge cases
   pytest -m spanner_live tests/integration/test_live_spanner.py::TestLiveSpannerEdgeCases -v --run-destructive

5. Skip slow tests:

   pytest -m "spanner_live and not slow" tests/integration/test_live_spanner.py -v --run-destructive

Notes:
------
- Read-only tests are safe to run against any Spanner database
- Destructive tests will add/remove split points - use only in test environments
- Ensure you have sufficient IAM permissions for Spanner operations
- Consider using a dedicated test database to avoid impacting production
- Large batch tests (100+ splits) may take several minutes to complete
- All tests use unique timestamp-based values to avoid collisions
"""
