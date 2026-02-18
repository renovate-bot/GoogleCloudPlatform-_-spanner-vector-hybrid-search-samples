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

"""Shared pytest fixtures for Spanner Split Manager tests."""
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import database
from models import OperationType, LocalSplitCreate


# =============================================================================
# Database Fixtures
# =============================================================================

@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test_sqlite.db"


@pytest.fixture
def db_connection(temp_db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Create an in-memory SQLite database connection for testing."""
    # Patch DATABASE_PATH to use temp path
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = temp_db_path

    # Initialize the database
    database.init_db()

    # Get a connection
    conn = database.get_connection()

    yield conn

    conn.close()
    # Restore original path
    database.DATABASE_PATH = original_path


@pytest.fixture
def clean_db(temp_db_path: Path) -> Generator[None, None, None]:
    """Fixture that provides a clean database for each test.

    This patches the DATABASE_PATH and initializes a fresh database.
    """
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = temp_db_path

    # Initialize the database
    database.init_db()

    yield

    # Cleanup
    database.DATABASE_PATH = original_path
    if temp_db_path.exists():
        temp_db_path.unlink()


# =============================================================================
# Spanner Service Fixtures
# =============================================================================

@pytest.fixture
def mock_spanner_client() -> MagicMock:
    """Create a mocked Spanner client."""
    mock_client = MagicMock()

    # Mock database admin API
    mock_admin_api = MagicMock()
    mock_client.database_admin_api = mock_admin_api
    mock_admin_api.database_path.return_value = "projects/test/instances/test/databases/test"

    # Mock instance and database
    mock_instance = MagicMock()
    mock_database = MagicMock()
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database

    # Mock snapshot for queries
    mock_snapshot = MagicMock()
    mock_database.snapshot.return_value.__enter__ = MagicMock(return_value=mock_snapshot)
    mock_database.snapshot.return_value.__exit__ = MagicMock(return_value=None)

    return mock_client


@pytest.fixture
def mock_spanner_service(mock_spanner_client: MagicMock, clean_db):
    """Create a SpannerService with mocked client."""
    from spanner_service import SpannerService

    service = SpannerService(
        project_id="test-project",
        instance_id="test-instance",
        database_id="test-database"
    )
    service._client = mock_spanner_client

    return service


# =============================================================================
# FastAPI Test Client Fixtures
# =============================================================================

@pytest.fixture
def test_client(temp_db_path: Path) -> Generator[TestClient, None, None]:
    """Create a FastAPI TestClient with a clean database."""
    # Patch DATABASE_PATH before importing main
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = temp_db_path

    # Initialize the database
    database.init_db()

    # Import app after patching
    from main import app

    with TestClient(app) as client:
        yield client

    # Restore original path
    database.DATABASE_PATH = original_path


@pytest.fixture
def test_client_with_mock_spanner(
    temp_db_path: Path,
    mock_spanner_client: MagicMock
) -> Generator[TestClient, None, None]:
    """Create a FastAPI TestClient with mocked Spanner service."""
    original_path = database.DATABASE_PATH
    database.DATABASE_PATH = temp_db_path

    # Initialize the database
    database.init_db()

    from main import app
    import spanner_service

    # Create a mock service
    mock_service = spanner_service.SpannerService(
        project_id="test-project",
        instance_id="test-instance",
        database_id="test-database"
    )
    mock_service._client = mock_spanner_client

    # Patch the global service
    original_service = spanner_service._spanner_service
    spanner_service._spanner_service = mock_service

    with TestClient(app) as client:
        yield client

    # Restore
    spanner_service._spanner_service = original_service
    database.DATABASE_PATH = original_path


# =============================================================================
# Sample Data Fixtures
# =============================================================================

@pytest.fixture
def sample_split_data() -> dict:
    """Sample split point data for testing."""
    return {
        "table_name": "UserInfo",
        "split_value": "12345",
        "operation_type": OperationType.ADD,
        "index_name": None,
        "index_key": None
    }


@pytest.fixture
def sample_index_split_data() -> dict:
    """Sample index split point data for testing."""
    return {
        "table_name": "UserLocationInfo",
        "split_value": "12,JP",
        "operation_type": OperationType.ADD,
        "index_name": "UsersByLocation",
        "index_key": "JP"
    }


@pytest.fixture
def sample_settings() -> dict:
    """Sample settings data for testing."""
    return {
        "project_id": "test-project",
        "instance_id": "test-instance",
        "database_id": "test-database"
    }


# =============================================================================
# Markers
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (requires Docker)")
    config.addinivalue_line("markers", "spanner_live: Live Spanner tests (requires GCP credentials)")
