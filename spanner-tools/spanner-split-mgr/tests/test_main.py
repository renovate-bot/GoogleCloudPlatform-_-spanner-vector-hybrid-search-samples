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

"""Unit tests for FastAPI routes.

Tests API endpoints and web routes using FastAPI TestClient.
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models import OperationType


# =============================================================================
# Settings API Tests
# =============================================================================

@pytest.mark.unit
class TestSettingsAPI:
    """Tests for settings API endpoints."""

    def test_get_settings_empty(self, test_client):
        """Test getting settings when none are configured."""
        response = test_client.get("/api/settings")

        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] is None
        assert data["instance_id"] is None
        assert data["database_id"] is None

    def test_post_settings_form(self, test_client):
        """Test saving settings via form submission."""
        response = test_client.post(
            "/settings",
            data={
                "project_id": "my-project",
                "instance_id": "my-instance",
                "database_id": "my-database"
            },
            follow_redirects=False
        )

        # Should return 200 with settings page
        assert response.status_code == 200
        # The form values should be present in the response (settings were saved)
        # Note: connection test may fail since we don't have real Spanner credentials
        assert b"my-project" in response.content
        assert b"my-instance" in response.content
        assert b"my-database" in response.content

    def test_settings_persistence(self, test_client):
        """Test that settings persist after being saved."""
        # Save settings
        test_client.post(
            "/settings",
            data={
                "project_id": "persistent-project",
                "instance_id": "persistent-instance",
                "database_id": "persistent-database"
            },
            follow_redirects=False
        )

        # Retrieve settings
        response = test_client.get("/api/settings")
        data = response.json()

        assert data["project_id"] == "persistent-project"
        assert data["instance_id"] == "persistent-instance"
        assert data["database_id"] == "persistent-database"


# =============================================================================
# Splits API Tests
# =============================================================================

@pytest.mark.unit
class TestSplitsAPI:
    """Tests for splits API endpoints."""

    def test_get_splits_empty(self, test_client):
        """Test getting splits when none exist."""
        response = test_client.get("/api/splits")

        assert response.status_code == 200
        data = response.json()
        assert data == []

    def test_add_split(self, test_client):
        """Test adding a new split."""
        response = test_client.post(
            "/api/splits",
            json={
                "table_name": "UserInfo",
                "split_value": "12345",
                "operation_type": "ADD"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "UserInfo"
        assert data["split_value"] == "12345"
        assert data["operation_type"] == "ADD"
        assert "id" in data

    def test_add_index_split(self, test_client):
        """Test adding an index split."""
        response = test_client.post(
            "/api/splits",
            json={
                "table_name": "UserLocationInfo",
                "split_value": "12,JP",
                "operation_type": "ADD",
                "index_name": "UsersByLocation",
                "index_key": "JP"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["index_name"] == "UsersByLocation"
        assert data["index_key"] == "JP"

    def test_add_split_validates_table_name(self, test_client):
        """Test that empty table_name is rejected."""
        response = test_client.post(
            "/api/splits",
            json={
                "table_name": "",
                "split_value": "12345"
            }
        )

        assert response.status_code == 422  # Validation error

    def test_delete_split(self, test_client):
        """Test deleting a split."""
        # First add a split
        add_response = test_client.post(
            "/api/splits",
            json={
                "table_name": "UserInfo",
                "split_value": "12345",
                "operation_type": "ADD"
            }
        )
        split_id = add_response.json()["id"]

        # Delete it
        delete_response = test_client.delete(f"/api/splits/{split_id}")

        assert delete_response.status_code == 200
        assert delete_response.json()["success"] is True

    def test_delete_nonexistent_split(self, test_client):
        """Test deleting a split that doesn't exist."""
        response = test_client.delete("/api/splits/99999")

        assert response.status_code == 404

    def test_clear_pending_splits(self, test_client):
        """Test clearing all pending splits."""
        # Add some splits
        test_client.post(
            "/api/splits",
            json={"table_name": "Table1", "split_value": "1", "operation_type": "ADD"}
        )
        test_client.post(
            "/api/splits",
            json={"table_name": "Table2", "split_value": "2", "operation_type": "DELETE"}
        )

        # Clear all
        response = test_client.post("/api/splits/clear")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["cleared"] == 2

        # Verify cleared
        splits_response = test_client.get("/api/splits")
        assert splits_response.json() == []


# =============================================================================
# Entity API Tests
# =============================================================================

@pytest.mark.unit
class TestEntityAPI:
    """Tests for entity API endpoints."""

    def test_list_entities_not_configured(self, test_client):
        """Test listing entities when Spanner is not configured."""
        response = test_client.get("/api/entities")

        assert response.status_code == 200
        # Should return empty list when not configured
        data = response.json()
        assert isinstance(data, list)


# =============================================================================
# Sync API Tests
# =============================================================================

@pytest.mark.unit
class TestSyncAPI:
    """Tests for sync API endpoint."""

    def test_sync_not_configured(self, test_client, monkeypatch):
        """Test sync when Spanner is not configured."""
        # Clear all environment variables that could provide config
        monkeypatch.delenv("SPANNER_INSTANCE", raising=False)
        monkeypatch.delenv("INSTANCE", raising=False)
        monkeypatch.delenv("SPANNER_DATABASE", raising=False)
        monkeypatch.delenv("DATABASE", raising=False)

        response = test_client.post("/api/sync")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "not configured" in data["message"].lower()

    def test_sync_with_mock_spanner(self, test_client_with_mock_spanner):
        """Test sync with mocked Spanner service."""
        # Add a pending split
        test_client_with_mock_spanner.post(
            "/api/splits",
            json={
                "table_name": "UserInfo",
                "split_value": "12345",
                "operation_type": "ADD"
            }
        )

        # Trigger sync
        response = test_client_with_mock_spanner.post("/api/sync")

        assert response.status_code == 200
        data = response.json()
        # With mock, sync should succeed
        assert data["success"] is True


# =============================================================================
# Web UI Routes Tests
# =============================================================================

@pytest.mark.unit
class TestWebUIRoutes:
    """Tests for web UI routes (HTML responses)."""

    def test_index_page(self, test_client):
        """Test index page loads."""
        response = test_client.get("/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_settings_page(self, test_client):
        """Test settings page loads."""
        response = test_client.get("/settings")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]


# =============================================================================
# Entity Schema API Tests
# =============================================================================

@pytest.mark.unit
class TestEntitySchemaAPI:
    """Tests for entity schema API endpoint."""

    def test_get_table_schema(self, test_client):
        """Test getting table key schema."""
        response = test_client.get(
            "/api/entity-schema",
            params={"entity_name": "UserInfo", "entity_type": "TABLE"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_name"] == "UserInfo"
        assert data["entity_type"] == "TABLE"

    def test_get_index_schema(self, test_client):
        """Test getting index key schema."""
        response = test_client.get(
            "/api/entity-schema",
            params={"entity_name": "UsersByLocation", "entity_type": "INDEX"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_name"] == "UsersByLocation"
        assert data["entity_type"] == "INDEX"

    def test_missing_entity_name(self, test_client):
        """Test that missing entity_name returns validation error."""
        response = test_client.get(
            "/api/entity-schema",
            params={"entity_type": "TABLE"}
        )

        assert response.status_code == 422

    def test_invalid_entity_type(self, test_client):
        """Test that invalid entity_type returns validation error."""
        response = test_client.get(
            "/api/entity-schema",
            params={"entity_name": "UserInfo", "entity_type": "INVALID"}
        )

        assert response.status_code == 422


# =============================================================================
# Splits Filtering Tests
# =============================================================================

@pytest.mark.unit
class TestSplitsFiltering:
    """Tests for splits filtering by entity."""

    def test_filter_splits_by_entity_name(self, test_client):
        """Test filtering splits by entity name."""
        # Add splits for different tables
        test_client.post(
            "/api/splits",
            json={"table_name": "Table1", "split_value": "1", "operation_type": "ADD"}
        )
        test_client.post(
            "/api/splits",
            json={"table_name": "Table2", "split_value": "2", "operation_type": "ADD"}
        )

        # Filter by Table1
        response = test_client.get("/api/splits", params={"entity_name": "Table1"})

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["table_name"] == "Table1"

    def test_filter_splits_by_entity_type(self, test_client):
        """Test filtering splits by entity type."""
        response = test_client.get("/api/splits", params={"entity_type": "TABLE"})

        assert response.status_code == 200


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.unit
class TestErrorHandling:
    """Tests for API error handling."""

    def test_invalid_json_body(self, test_client):
        """Test handling of invalid JSON in request body."""
        response = test_client.post(
            "/api/splits",
            content="not valid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422

    def test_missing_required_fields(self, test_client):
        """Test handling of missing required fields."""
        response = test_client.post(
            "/api/splits",
            json={}  # Missing table_name
        )

        assert response.status_code == 422


# =============================================================================
# Content Type Tests
# =============================================================================

@pytest.mark.unit
class TestContentTypes:
    """Tests for correct content types."""

    def test_api_returns_json(self, test_client):
        """Test that API endpoints return JSON."""
        response = test_client.get("/api/settings")

        assert "application/json" in response.headers["content-type"]

    def test_web_returns_html(self, test_client):
        """Test that web routes return HTML."""
        response = test_client.get("/")

        assert "text/html" in response.headers["content-type"]
