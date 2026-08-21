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

import os
import shutil
import pytest
from pathlib import Path
from starlette.testclient import TestClient
from backend.app.main import app

@pytest.fixture
def client():
    return TestClient(app)

def test_list_connections(client):
    response = client.get("/api/v1/connections")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

def test_staging_folders_scan(client):
    test_folder = Path("staging") / "test-mock-folder"
    test_folder.mkdir(parents=True, exist_ok=True)
    mock_csv = test_folder / "export_all_SAMPLE_TABLE.csv"
    mock_csv.write_text("id,name\n1,alice\n")
    try:
        response = client.get("/api/v1/connections/staging-folders")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert any(f["name"] == "test-mock-folder" for f in data)
    finally:
        if test_folder.exists():
            shutil.rmtree(test_folder)

def test_create_and_delete_local_staging_connection(client):
    payload = {
        "name": "Test Staging Connection",
        "type": "local_staging",
        "staging_path": "staging/test-temp-staging",
        "auto_ingest": False
    }
    create_res = client.post("/api/v1/connections", json=payload)
    assert create_res.status_code == 200
    conn = create_res.json()
    assert "test-staging-connection" in conn["id"]
    assert conn["type"] == "local_staging"

    # Fetch it
    get_res = client.get(f"/api/v1/connections/{conn['id']}")
    assert get_res.status_code == 200
    assert get_res.json()["name"] == "Test Staging Connection"

    # Delete it
    del_res = client.delete(f"/api/v1/connections/{conn['id']}")
    assert del_res.status_code == 200
    assert del_res.json()["success"] is True

    # Ensure it is not in the list anymore
    list_after = client.get("/api/v1/connections").json()
    assert not any(c["id"] == conn["id"] for c in list_after)

def test_gcp_discovery_caching(client, monkeypatch):
    from backend.app.services.gcp_service import GcpDiscoveryService
    monkeypatch.setattr(GcpDiscoveryService, "list_projects", lambda self, refresh=False: [{"project_id": "test-project", "name": "Test Project"}])
    # Test project listing endpoint returns a list (cached or empty)
    res = client.get("/api/v1/gcp/projects?refresh=false")
    assert res.status_code == 200
    assert isinstance(res.json(), list)

def test_sync_staging_connection(client):
    test_staging = Path("staging") / "test-sync-staging"
    test_staging.mkdir(parents=True, exist_ok=True)
    mock_csv = test_staging / "export_all_QUERY_STATS_TOP_10MINUTE.csv"
    mock_csv.write_text("text,text_fingerprint,avg_rows_scanned,execution_count,interval_end\nSELECT 1,fp_1,100.0,5,2026-08-20 10:00:00\n")

    create_payload = {
        "name": "Test Sync Connection",
        "type": "local_staging",
        "staging_path": str(test_staging),
        "auto_ingest": False
    }
    create_res = client.post("/api/v1/connections", json=create_payload)
    assert create_res.status_code == 200
    conn = create_res.json()
    conn_id = conn["id"]

    try:
        res = client.post(f"/api/v1/connections/{conn_id}/sync")
        assert res.status_code == 200
        data = res.json()
        assert data["success"] is True
        assert data["tables_ingested"] >= 1

        # Verify connection status is now READY with row count > 0
        conn_res = client.get(f"/api/v1/connections/{conn_id}")
        assert conn_res.status_code == 200
        conn_data = conn_res.json()
        assert conn_data["status"] == "READY"
        assert conn_data["total_rows"] > 0
        assert conn_data["total_tables"] >= 1
    finally:
        client.delete(f"/api/v1/connections/{conn_id}")
        if test_staging.exists():
            shutil.rmtree(test_staging)

def test_gcp_connection_sync_flow(client, monkeypatch):
    from backend.app.services.gcp_service import GcpDiscoveryService

    def mock_export(self, project_id, instance_id, database_id, dialect, staging_dir, log_callback=None):
        staging_dir.mkdir(parents=True, exist_ok=True)
        # Create a mock CSV
        (staging_dir / "export_all_QUERY_STATS_TOP_10MINUTE.csv").write_text(
            "text,text_fingerprint,avg_rows_scanned,execution_count,interval_end\nSELECT 1,fp_x,500.0,10,2026-08-20 10:00:00\n"
        )
        if log_callback:
            log_callback("✓ Exported mock table")
        return [{"table": "QUERY_STATS_TOP_10MINUTE", "rows": 1, "status": "ok"}]

    monkeypatch.setattr(GcpDiscoveryService, "export_database_to_staging", mock_export)

    payload = {
        "name": "Test GCP Spanner DB",
        "type": "gcp",
        "project_id": "test-project",
        "instance_id": "test-instance",
        "database_id": "test-db",
        "dialect": "GOOGLE_STANDARD_SQL",
        "auto_ingest": False
    }
    create_res = client.post("/api/v1/connections", json=payload)
    assert create_res.status_code == 200
    conn = create_res.json()
    conn_id = conn["id"]

    try:
        sync_res = client.post(f"/api/v1/connections/{conn_id}/sync")
        assert sync_res.status_code == 200
        sync_data = sync_res.json()
        assert sync_data["success"] is True

        conn_res = client.get(f"/api/v1/connections/{conn_id}")
        assert conn_res.status_code == 200
        assert conn_res.json()["status"] == "READY"
    finally:
        client.delete(f"/api/v1/connections/{conn_id}?delete_duckdb=true&delete_staging=true")
