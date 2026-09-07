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

def test_column_profiles_categorical_with_other(tmp_path):
    db_file = str(tmp_path / "test_other_profile.db")
    con = duckdb.connect(db_file)
    con.execute("""
        CREATE TABLE TAG_TEST (
            request_tag VARCHAR
        );
    """)
    # Insert 8 distinct tags + 2 nulls (total 100 rows)
    # tag_1: 30 rows (30%)
    # tag_2: 20 rows (20%)
    # tag_3: 15 rows (15%)
    # tag_4: 10 rows (10%)
    # tag_5: 8 rows (8%)
    # tag_6: 6 rows (6%)
    # tag_7: 4 rows (4%)
    # tag_8: 2 rows (2%)
    # NULL: 5 rows (5%)
    tag_counts = [
        ('tag_1', 30),
        ('tag_2', 20),
        ('tag_3', 15),
        ('tag_4', 10),
        ('tag_5', 8),
        ('tag_6', 6),
        ('tag_7', 4),
        ('tag_8', 2),
        (None, 5),
    ]
    for tag, count in tag_counts:
        for _ in range(count):
            con.execute("INSERT INTO TAG_TEST VALUES (?);", [tag])
    con.close()

    svc = DuckDBService(db_file)
    res = svc.get_table_column_profiles("TAG_TEST")
    prof = res["profiles"]["request_tag"]

    assert prof["total_count"] == 100
    assert prof["null_count"] == 5
    assert prof["distinct_count"] in (8, 9)

    # Top categories should be top 4
    top_cats = prof["top_categories"]
    assert len(top_cats) == 4
    assert [c["value"] for c in top_cats] == ["tag_1", "tag_2", "tag_3", "tag_4"]
    assert sum(c["count"] for c in top_cats) == 75
    assert sum(c["percent"] for c in top_cats) == 75.0

    # Remaining 4 tags should be in "other"
    other = prof["other"]
    assert other is not None
    assert other["count"] == 20  # 8 + 6 + 4 + 2
    assert other["percent"] == 20.0
    assert other["distinct_count"] in (4, 5)  # approx distinct - 4

    # Full 100% accounting: top (75) + other (20) + null (5) = 100
    assert sum(c["count"] for c in top_cats) + other["count"] + prof["null_count"] == 100

