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
import duckdb
import pytest
from starlette.testclient import TestClient
from backend.app.main import app
from backend.app.services.duckdb_service import DuckDBService

from backend.app.api.data import get_duckdb as get_data_duckdb
from backend.app.api.discovery import get_duckdb as get_disc_duckdb

TEST_DB = "test_api_duckdb.db"

@pytest.fixture(autouse=True)
def setup_test_db():
    if os.path.exists(TEST_DB):
        try:
            os.remove(TEST_DB)
        except Exception:
            pass

    con = duckdb.connect(TEST_DB)
    con.execute("""
        CREATE TABLE QUERY_STATS_TOP_10MINUTE (
            text VARCHAR,
            text_fingerprint VARCHAR,
            avg_rows_scanned DOUBLE,
            execution_count BIGINT,
            interval_end TIMESTAMP
        );
    """)
    con.execute("""
        INSERT INTO QUERY_STATS_TOP_10MINUTE VALUES
        ('SELECT 1', 'fp_a', 120000.0, 20, '2026-08-19 09:00:00'),
        ('SELECT 2', 'fp_b', 500.0, 2, '2026-08-19 09:10:00');
    """)
    con.close()

    test_svc = DuckDBService(TEST_DB)
    app.dependency_overrides[get_data_duckdb] = lambda: test_svc
    app.dependency_overrides[get_disc_duckdb] = lambda: test_svc

    yield

    app.dependency_overrides.clear()
    if os.path.exists(TEST_DB):
        try:
            os.remove(TEST_DB)
        except Exception:
            pass


def test_api_list_tables():
    client = TestClient(app)
    response = client.get("/api/v1/tables")
    assert response.status_code == 200
    data = response.json()
    assert "QUERY_STATS_TOP_10MINUTE" in [t["name"] for t in data["tables"]]

def test_api_table_schema():
    client = TestClient(app)
    response = client.get("/api/v1/tables/QUERY_STATS_TOP_10MINUTE/schema")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "QUERY_STATS_TOP_10MINUTE"
    assert len(data["columns"]) == 5

def test_api_query_table():
    client = TestClient(app)
    payload = {
        "page": 1,
        "page_size": 10,
        "filters": {
            "text": {"type": "text", "value": "SELECT 1"}
        },
        "utc_offset": 1.0
    }
    response = client.post("/api/v1/tables/QUERY_STATS_TOP_10MINUTE/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["text_fingerprint"] == "fp_a"
    assert "duration_ms" in data

def test_api_export_csv():
    client = TestClient(app)
    payload = {
        "page": 1,
        "page_size": 10,
        "filters": {}
    }
    response = client.post("/api/v1/tables/QUERY_STATS_TOP_10MINUTE/export", json=payload)
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "text_fingerprint" in response.text

def test_api_timeline():
    client = TestClient(app)
    response = client.get("/api/v1/tables/QUERY_STATS_TOP_10MINUTE/timeline?utc_offset=1.0")
    assert response.status_code == 200
    data = response.json()
    assert data["total_intervals"] == 2
    assert data["total_records"] == 2
    assert len(data["buckets"]) == 2
    assert data["buckets"][0]["utc"] == "2026-08-19 09:00:00"
    assert data["buckets"][0]["display"] == "2026-08-19 10:00:00"
    assert data["buckets"][0]["count"] == 1

def test_api_scatter_defaults():
    client = TestClient(app)
    response = client.get("/api/v1/tables/QUERY_STATS_TOP_10MINUTE/scatter/defaults")
    assert response.status_code == 200
    data = response.json()
    assert data["x_col"] == "execution_count"
    assert data["y_col"] == "avg_rows_scanned"
    assert "execution_count" in data["numeric_cols"]

def test_api_scatter_query():
    client = TestClient(app)
    payload = {
        "page": 1,
        "page_size": 10,
        "filters": {
            "execution_count": {
                "type": "numeric",
                "min": 15
            }
        }
    }
    response = client.post(
        "/api/v1/tables/QUERY_STATS_TOP_10MINUTE/scatter?x_col=execution_count&y_col=avg_rows_scanned",
        json=payload
    )
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["points"]) == 1
    assert data["points"][0]["x"] == 20.0
    assert data["points"][0]["y"] == 120000.0

def test_api_meta_databases():
    client = TestClient(app)
    response = client.get("/api/v1/tables/meta/databases")
    assert response.status_code == 200
    data = response.json()
    assert "databases" in data
    assert "count" in data
    assert isinstance(data["databases"], list)

def test_api_meta_summaries():
    client = TestClient(app)
    response = client.get("/api/v1/tables/meta/summaries")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)


