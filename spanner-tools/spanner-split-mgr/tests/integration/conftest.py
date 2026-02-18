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

"""Integration test fixtures using Spanner emulator.

These fixtures set up a Spanner emulator container using testcontainers
for end-to-end testing without requiring actual GCP resources.
"""
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import pytest


def pytest_addoption(parser):
    """Add custom command line options for integration tests."""
    try:
        parser.addoption(
            "--run-destructive",
            action="store_true",
            default=False,
            help="Run tests that modify live Spanner state"
        )
    except ValueError:
        # Option already added
        pass

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Configure logging for fixtures
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Try to import testcontainers - if not available, skip integration tests
try:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import LogMessageWaitStrategy
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False
    DockerContainer = None
    LogMessageWaitStrategy = None

try:
    from google.cloud import spanner
    SPANNER_CLIENT_AVAILABLE = True
except ImportError:
    SPANNER_CLIENT_AVAILABLE = False
    spanner = None


# =============================================================================
# Spanner Emulator Container Fixture
# =============================================================================

@pytest.fixture(scope="session")
def spanner_emulator():
    """Start Spanner emulator container for integration tests.

    This fixture starts the Cloud Spanner emulator in a Docker container
    and yields connection information.

    The emulator runs on ports:
    - 9010: gRPC port (for Spanner client)
    - 9020: REST port (for admin operations)
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed - install with: pip install testcontainers")

    container = DockerContainer("gcr.io/cloud-spanner-emulator/emulator:latest")
    container.with_exposed_ports(9010, 9020)

    # Use structured wait strategy for emulator readiness
    wait_strategy = LogMessageWaitStrategy("gRPC server listening").with_timeout(60)
    container.waiting_for(wait_strategy)

    try:
        container.start()

        # Give it a moment to fully initialize
        time.sleep(2)

        host = container.get_container_host_ip()
        grpc_port = container.get_exposed_port(9010)
        rest_port = container.get_exposed_port(9020)

        emulator_endpoint = f"{host}:{grpc_port}"

        yield {
            "host": host,
            "grpc_port": grpc_port,
            "rest_port": rest_port,
            "endpoint": emulator_endpoint,
            "container": container
        }

    finally:
        container.stop()


@pytest.fixture(scope="session")
def emulator_client(spanner_emulator):
    """Create a Spanner client configured for the emulator.

    This client can be used to perform operations against the emulator.
    """
    if not SPANNER_CLIENT_AVAILABLE:
        pytest.skip("google-cloud-spanner not installed")

    # Set environment variable for emulator
    os.environ["SPANNER_EMULATOR_HOST"] = spanner_emulator["endpoint"]

    client = spanner.Client(project="test-project")

    yield client

    # Cleanup
    if "SPANNER_EMULATOR_HOST" in os.environ:
        del os.environ["SPANNER_EMULATOR_HOST"]


@pytest.fixture(scope="session")
def emulator_instance(emulator_client):
    """Create a test instance in the emulator.

    The Spanner emulator requires explicit instance creation - instances
    are NOT auto-created. This fixture creates the instance before yielding.
    """
    instance_id = "test-instance"
    config_name = "emulator-config"

    # Create instance configuration reference
    instance = emulator_client.instance(
        instance_id,
        configuration_name=config_name,
        display_name="Test Instance",
        node_count=1
    )

    # Create the instance in the emulator
    try:
        operation = instance.create()
        operation.result(timeout=60)
    except Exception as e:
        # Instance might already exist from a previous test run
        if "already exists" not in str(e).lower():
            raise

    yield instance


@pytest.fixture(scope="session")
def emulator_database(emulator_instance):
    """Create a test database with schema in the emulator."""
    database_id = "test-database"

    # Define the schema for testing
    ddl_statements = [
        """
        CREATE TABLE UserInfo (
            user_id INT64 NOT NULL,
            username STRING(100),
            email STRING(255)
        ) PRIMARY KEY (user_id)
        """,
        """
        CREATE TABLE UserLocationInfo (
            user_id INT64 NOT NULL,
            location STRING(100) NOT NULL,
            country STRING(50)
        ) PRIMARY KEY (user_id, location)
        """,
        """
        CREATE INDEX UsersByLocation ON UserLocationInfo(location)
        """
    ]

    database = emulator_instance.database(database_id, ddl_statements=ddl_statements)

    # Create the database
    try:
        operation = database.create()
        operation.result(timeout=60)
    except Exception as e:
        # Database might already exist
        if "already exists" not in str(e).lower():
            raise

    yield database


# =============================================================================
# Integration Test SpannerService Fixture
# =============================================================================

@pytest.fixture
def emulator_spanner_service(spanner_emulator, emulator_database, tmp_path):
    """Create a SpannerService connected to the emulator.

    This provides a fully functional SpannerService for integration testing.
    """
    import database
    from spanner_service import SpannerService

    # Set up environment for emulator
    os.environ["SPANNER_EMULATOR_HOST"] = spanner_emulator["endpoint"]

    # Use a temp database for local SQLite
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = tmp_path / "test_sqlite.db"
    database.init_db()

    # Configure settings
    database.update_settings(
        project_id="test-project",
        instance_id="test-instance",
        database_id="test-database"
    )

    service = SpannerService(
        project_id="test-project",
        instance_id="test-instance",
        database_id="test-database"
    )

    yield service

    # Cleanup
    database.DATABASE_PATH = original_path
    if "SPANNER_EMULATOR_HOST" in os.environ:
        del os.environ["SPANNER_EMULATOR_HOST"]


# =============================================================================
# Live Spanner Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def live_spanner_config():
    """Get configuration for live Spanner tests from environment.

    Required environment variables:
    - SPANNER_PROJECT_ID: GCP project ID
    - SPANNER_INSTANCE_ID: Spanner instance ID
    - SPANNER_DATABASE_ID: Spanner database ID

    Optional:
    - GOOGLE_APPLICATION_CREDENTIALS: Path to service account key
    """
    project_id = os.environ.get("SPANNER_PROJECT_ID")
    instance_id = os.environ.get("SPANNER_INSTANCE_ID")
    database_id = os.environ.get("SPANNER_DATABASE_ID")

    if not all([project_id, instance_id, database_id]):
        pytest.skip(
            "Live Spanner tests require SPANNER_PROJECT_ID, "
            "SPANNER_INSTANCE_ID, and SPANNER_DATABASE_ID environment variables"
        )

    return {
        "project_id": project_id,
        "instance_id": instance_id,
        "database_id": database_id
    }


# =============================================================================
# Live Spanner Test Schema Setup
# =============================================================================

# DDL statements for creating test tables and indexes
LIVE_SPANNER_TEST_TABLES_DDL: List[str] = [
    # Users table - single column primary key (INT64)
    """
    CREATE TABLE Users (
        user_id INT64 NOT NULL,
        email STRING(255),
        created_at TIMESTAMP
    ) PRIMARY KEY (user_id)
    """,
    # Orders table - single column primary key (INT64)
    """
    CREATE TABLE Orders (
        order_id INT64 NOT NULL,
        user_id INT64,
        total_amount FLOAT64,
        status STRING(50),
        created_at TIMESTAMP
    ) PRIMARY KEY (order_id)
    """,
    # OrderItems table - composite primary key (order_id, item_id)
    """
    CREATE TABLE OrderItems (
        order_id INT64 NOT NULL,
        item_id INT64 NOT NULL,
        product_name STRING(255),
        quantity INT64,
        price FLOAT64
    ) PRIMARY KEY (order_id, item_id)
    """,
    # Products table - single column primary key (STRING)
    """
    CREATE TABLE Products (
        product_id STRING(100) NOT NULL,
        name STRING(255),
        category STRING(100),
        price FLOAT64
    ) PRIMARY KEY (product_id)
    """,
]

LIVE_SPANNER_TEST_INDEXES_DDL: List[str] = [
    # Index on Orders(user_id)
    "CREATE INDEX idx_orders_user_id ON Orders(user_id)",
    # Index on Orders(status)
    "CREATE INDEX idx_orders_status ON Orders(status)",
    # Index on Products(category)
    "CREATE INDEX idx_products_category ON Products(category)",
    # Index on OrderItems(product_name)
    "CREATE INDEX idx_order_items_product ON OrderItems(product_name)",
]


def _table_exists(database, table_name: str) -> bool:
    """Check if a table exists in the Spanner database.

    Args:
        database: Spanner database instance
        table_name: Name of the table to check

    Returns:
        True if the table exists, False otherwise
    """
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = @table_name
          AND TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = ''
    """
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                sql,
                params={"table_name": table_name},
                param_types={"table_name": spanner.param_types.STRING}
            )
            for row in results:
                return row[0] > 0
    except Exception as e:
        logger.warning(f"Error checking if table {table_name} exists: {e}")
    return False


def _index_exists(database, index_name: str) -> bool:
    """Check if an index exists in the Spanner database.

    Args:
        database: Spanner database instance
        index_name: Name of the index to check

    Returns:
        True if the index exists, False otherwise
    """
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.INDEXES
        WHERE INDEX_NAME = @index_name
    """
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                sql,
                params={"index_name": index_name},
                param_types={"index_name": spanner.param_types.STRING}
            )
            for row in results:
                return row[0] > 0
    except Exception as e:
        logger.warning(f"Error checking if index {index_name} exists: {e}")
    return False


def _execute_ddl_with_retry(database, ddl_statement: str, description: str, max_retries: int = 3) -> bool:
    """Execute a DDL statement with retry logic.

    Args:
        database: Spanner database instance
        ddl_statement: DDL statement to execute
        description: Human-readable description for logging
        max_retries: Maximum number of retry attempts

    Returns:
        True if successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            operation = database.update_ddl([ddl_statement])
            operation.result(timeout=300)  # 5 minute timeout for DDL operations
            logger.info(f"Successfully created {description}")
            return True
        except Exception as e:
            error_str = str(e).lower()
            # Check for "already exists" errors - this is acceptable
            if "already exists" in error_str or "duplicate" in error_str:
                logger.info(f"{description} already exists, skipping")
                return True
            # Check for concurrent schema change error
            if "concurrent schema change" in error_str or "pending schema modifications" in error_str:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10  # Exponential backoff: 10s, 20s, 30s
                    logger.warning(
                        f"Concurrent schema change detected for {description}, "
                        f"waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
            logger.error(f"Error creating {description}: {e}")
            return False
    return False


@pytest.fixture(scope="session")
def live_spanner_test_schema(live_spanner_config):
    """Set up test tables and indexes in the live Spanner database.

    This fixture creates a realistic schema for testing split point operations:

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

    The fixture:
    - Checks if tables/indexes already exist before creating
    - Handles errors gracefully (tables may exist from previous runs)
    - Does NOT drop tables after tests (leaves them for future runs)

    Returns:
        dict containing schema information:
        - tables: List of table names created/verified
        - indexes: List of index names created/verified
        - single_key_tables: List of tables with single-column primary keys
        - composite_key_tables: List of tables with composite primary keys
    """
    if not SPANNER_CLIENT_AVAILABLE:
        pytest.skip("google-cloud-spanner not installed")

    project_id = live_spanner_config["project_id"]
    instance_id = live_spanner_config["instance_id"]
    database_id = live_spanner_config["database_id"]

    logger.info(f"Setting up test schema in {project_id}/{instance_id}/{database_id}")

    # Create Spanner client and get database
    client = spanner.Client(project=project_id)
    instance = client.instance(instance_id)
    database = instance.database(database_id)

    # Track created/verified tables and indexes
    tables_created: List[str] = []
    indexes_created: List[str] = []

    # Create tables
    table_names = ["Users", "Orders", "OrderItems", "Products"]
    for i, ddl in enumerate(LIVE_SPANNER_TEST_TABLES_DDL):
        table_name = table_names[i]
        if _table_exists(database, table_name):
            logger.info(f"Table {table_name} already exists")
            tables_created.append(table_name)
        else:
            if _execute_ddl_with_retry(database, ddl, f"table {table_name}"):
                tables_created.append(table_name)
            else:
                logger.warning(f"Failed to create table {table_name}")

    # Create indexes (need to wait for tables to be fully created)
    time.sleep(2)  # Brief pause to ensure tables are ready

    index_names = [
        "idx_orders_user_id",
        "idx_orders_status",
        "idx_products_category",
        "idx_order_items_product",
    ]
    for i, ddl in enumerate(LIVE_SPANNER_TEST_INDEXES_DDL):
        index_name = index_names[i]
        if _index_exists(database, index_name):
            logger.info(f"Index {index_name} already exists")
            indexes_created.append(index_name)
        else:
            if _execute_ddl_with_retry(database, ddl, f"index {index_name}"):
                indexes_created.append(index_name)
            else:
                logger.warning(f"Failed to create index {index_name}")

    logger.info(
        f"Test schema setup complete: {len(tables_created)} tables, "
        f"{len(indexes_created)} indexes"
    )

    schema_info = {
        "tables": tables_created,
        "indexes": indexes_created,
        "single_key_tables": ["Users", "Orders", "Products"],
        "composite_key_tables": ["OrderItems"],
        "int64_key_tables": ["Users", "Orders"],
        "string_key_tables": ["Products"],
    }

    yield schema_info

    # Note: We intentionally do NOT drop tables after tests
    # to allow reuse in future test runs
    logger.info("Test schema fixture complete (tables left in place for future runs)")


@pytest.fixture(scope="session")
def live_spanner_client(live_spanner_config):
    """Create a Spanner client for live tests.

    This is a session-scoped fixture that provides a Spanner client
    configured for the live database.
    """
    if not SPANNER_CLIENT_AVAILABLE:
        pytest.skip("google-cloud-spanner not installed")

    client = spanner.Client(project=live_spanner_config["project_id"])
    yield client


@pytest.fixture(scope="session")
def live_spanner_database(live_spanner_config, live_spanner_client):
    """Get the live Spanner database instance.

    This fixture provides direct access to the Spanner database for
    tests that need to run SQL queries directly.
    """
    instance = live_spanner_client.instance(live_spanner_config["instance_id"])
    database = instance.database(live_spanner_config["database_id"])
    yield database


@pytest.fixture
def live_spanner_service(live_spanner_config, live_spanner_test_schema, tmp_path):
    """Create a SpannerService connected to live Spanner.

    WARNING: This fixture connects to real GCP resources. Use with caution.
    """
    import database
    from spanner_service import SpannerService

    # Use a temp database for local SQLite
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = tmp_path / "test_sqlite.db"
    database.init_db()

    # Configure settings
    database.update_settings(
        project_id=live_spanner_config["project_id"],
        instance_id=live_spanner_config["instance_id"],
        database_id=live_spanner_config["database_id"]
    )

    service = SpannerService(
        project_id=live_spanner_config["project_id"],
        instance_id=live_spanner_config["instance_id"],
        database_id=live_spanner_config["database_id"]
    )

    yield service

    # Cleanup
    database.DATABASE_PATH = original_path


# =============================================================================
# Utility Fixtures
# =============================================================================

@pytest.fixture
def clean_emulator_splits(emulator_spanner_service):
    """Ensure no splits exist before test.

    This fixture attempts to clean up any existing splits in the emulator.
    Note: In the emulator, this may not be perfectly reliable.
    """
    yield

    # After test, try to clean up
    try:
        import database
        database.clear_pending_splits()
    except Exception:
        pass
