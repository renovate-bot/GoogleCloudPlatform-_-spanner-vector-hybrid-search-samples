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
from backend.app.services.duckdb_service import DuckDBService

TEST_DB_FILE = "test_profiles.db"

@pytest.fixture
def profile_test_service():
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

    con = duckdb.connect(TEST_DB_FILE)
    con.execute("""
        CREATE TABLE TEST_METRICS (
            query_name VARCHAR,
            execution_count BIGINT,
            avg_latency DOUBLE,
            lock_mode VARCHAR,
            created_at TIMESTAMP
        );
    """)
    con.execute("""
        INSERT INTO TEST_METRICS VALUES
        ('Query A', 10, 0.05, 'SHARED', '2026-08-20 10:00:00'),
        ('Query B', 100, 1.25, 'EXCLUSIVE', '2026-08-20 10:05:00'),
        ('Query C', 500, 3.40, 'SHARED', '2026-08-20 10:10:00'),
        ('Query D', 50, 0.12, 'SHARED', '2026-08-20 10:15:00'),
        ('Query E', NULL, NULL, NULL, '2026-08-20 10:20:00');
    """)
    con.close()

    service = DuckDBService(TEST_DB_FILE)
    yield service

    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

def test_column_profiles_numeric(profile_test_service):
    res = profile_test_service.get_table_column_profiles("TEST_METRICS")
    assert res["table"] == "TEST_METRICS"
    assert res["total_rows"] == 5

    profiles = res["profiles"]
    assert "execution_count" in profiles
    exec_prof = profiles["execution_count"]
    assert exec_prof["filter_type"] == "numeric"
    assert exec_prof["null_count"] == 1
    assert exec_prof["min_value"] == 10.0
    assert exec_prof["max_value"] == 500.0
    assert len(exec_prof["histogram"]) == 10
    total_hist_count = sum(b["count"] for b in exec_prof["histogram"])
    assert total_hist_count == 4

def test_column_profiles_categorical(profile_test_service):
    res = profile_test_service.get_table_column_profiles("TEST_METRICS")
    profiles = res["profiles"]

    assert "lock_mode" in profiles
    lock_prof = profiles["lock_mode"]
    assert lock_prof["filter_type"] == "text"
    assert lock_prof["null_count"] == 1
    assert lock_prof["distinct_count"] == 2
    top_cats = lock_prof["top_categories"]
    assert len(top_cats) == 2
    assert top_cats[0]["value"] == "SHARED"
    assert top_cats[0]["count"] == 3
    assert top_cats[0]["percent"] == 60.0

def test_column_profiles_date(profile_test_service):
    res = profile_test_service.get_table_column_profiles("TEST_METRICS")
    profiles = res["profiles"]

    assert "created_at" in profiles
    date_prof = profiles["created_at"]
    assert date_prof["filter_type"] == "date"
    assert date_prof["null_count"] == 0
    assert date_prof["min_date"] == "2026-08-20 10:00:00"
    assert date_prof["max_date"] == "2026-08-20 10:20:00"
