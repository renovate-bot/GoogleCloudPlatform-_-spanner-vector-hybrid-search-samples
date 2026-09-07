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
from backend.app.services.duckdb_service import DuckDBService, categorize_table, infer_filter_type, _parse_numeric_bound
from backend.app.models.query import ColumnFilter, SortConfig

TEST_DB_FILE = "test_duckdb.db"

@pytest.fixture
def test_duckdb():
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

    con = duckdb.connect(TEST_DB_FILE)
    con.execute("""
        CREATE TABLE QUERY_STATS_TOP_1HOUR (
            text VARCHAR,
            text_fingerprint VARCHAR,
            avg_rows_scanned DOUBLE,
            execution_count BIGINT,
            interval_end TIMESTAMP
        );
    """)
    con.execute("""
        INSERT INTO QUERY_STATS_TOP_1HOUR VALUES
        ('SELECT * FROM Accounts WHERE id = 1', 'fp_001', 150000.0, 50, '2026-08-19 08:00:00'),
        ('SELECT * FROM Users WHERE status = active', 'fp_002', 250000.0, 15, '2026-08-19 08:10:00'),
        ('SELECT * FROM Orders WHERE order_id = 9', 'fp_003', 5000.0, 5, '2026-08-19 08:20:00');
    """)
    con.execute("""
        CREATE TABLE LOCK_STATISTICS_TOP_10MINUTE (
            lock_mode VARCHAR,
            wait_time_ms BIGINT
        );
    """)
    con.execute("""
        INSERT INTO LOCK_STATISTICS_TOP_10MINUTE VALUES
        ('EXCLUSIVE', 450),
        ('SHARED', 120);
    """)
    con.close()

    service = DuckDBService(TEST_DB_FILE)
    yield service

    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

def test_categorize_table():
    assert categorize_table("export_all_QUERY_STATS_TOP_1MIN") == "Query"
    assert categorize_table("LOCK_STATISTICS_TOP_1HOUR") == "Locking"
    assert categorize_table("TXN_STATS_TOP_10MINUTE") == "Transactions"
    assert categorize_table("CUSTOM_TABLE") == "Misc"

def test_infer_filter_type():
    assert infer_filter_type("BIGINT") == "numeric"
    assert infer_filter_type("DOUBLE") == "numeric"
    assert infer_filter_type("TIMESTAMP") == "date"
    assert infer_filter_type("VARCHAR") == "text"

def test_get_database_summary(test_duckdb):
    summary = test_duckdb.get_database_summary()
    assert summary.total_tables == 2
    assert summary.total_rows == 5
    assert "QUERY_STATS_TOP_1HOUR" in summary.categories["Query"]
    assert "LOCK_STATISTICS_TOP_10MINUTE" in summary.categories["Locking"]

def test_get_table_metadata(test_duckdb):
    meta = test_duckdb.get_table_metadata("QUERY_STATS_TOP_1HOUR")
    assert meta.name == "QUERY_STATS_TOP_1HOUR"
    assert meta.row_count == 3
    assert len(meta.columns) == 5

def test_query_table_pagination_and_filter(test_duckdb):
    # Test text filter contains (default)
    filters = {"text": ColumnFilter(type="text", operator="contains", value="Accounts")}
    res = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=filters)
    assert res["total"] == 1
    assert len(res["items"]) == 1
    assert res["items"][0]["text_fingerprint"] == "fp_001"

    # Test text filter exact match
    exact_match = {"text": ColumnFilter(type="text", operator="exact", value="SELECT * FROM Accounts WHERE id = 1")}
    res_exact = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=exact_match)
    assert res_exact["total"] == 1

    # Exact match substring should return 0 (unlike contains)
    exact_mismatch = {"text": ColumnFilter(type="text", operator="exact", value="Accounts")}
    res_no_match = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=exact_mismatch)
    assert res_no_match["total"] == 0

    # Test not_exact
    not_exact_filter = {"text": ColumnFilter(type="text", operator="not_exact", value="SELECT * FROM Accounts WHERE id = 1")}
    res_not_exact = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=not_exact_filter)
    assert res_not_exact["total"] == 2

    # Test not_contains
    not_contains_filter = {"text": ColumnFilter(type="text", operator="not_contains", value="Accounts")}
    res_not_contains = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=not_contains_filter)
    assert res_not_contains["total"] == 2

    # Test numeric filter
    num_filters = {"avg_rows_scanned": ColumnFilter(type="numeric", min=100000)}
    res2 = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, filters=num_filters)
    assert res2["total"] == 2

    # Test sorting
    sort = SortConfig(column="avg_rows_scanned", order="DESC")
    res3 = test_duckdb.query_table("QUERY_STATS_TOP_1HOUR", page=1, page_size=10, sort=sort)
    assert res3["items"][0]["avg_rows_scanned"] == 250000.0

def test_distinct_intervals(test_duckdb):
    intervals = test_duckdb.get_distinct_intervals("QUERY_STATS_TOP_1HOUR", utc_offset=2.0)
    assert len(intervals) == 3
    assert intervals[0]["utc"] == "2026-08-19 08:20:00"
    assert intervals[0]["display"] == "2026-08-19 10:20:00"

def test_high_row_scan_queries(test_duckdb):
    high_scans = test_duckdb.get_high_row_scan_queries()
    # Matches > 10 execs and > 100k avg rows scanned
    assert len(high_scans) == 2
    assert high_scans[0]["text_fingerprint"] in ["fp_001", "fp_002"]

def test_export_csv_stream(test_duckdb):
    chunks = list(test_duckdb.export_csv_stream("QUERY_STATS_TOP_1HOUR"))
    full_csv = "".join(chunks)
    assert "text,text_fingerprint" in full_csv
    assert "Accounts" in full_csv

def test_interval_histogram(test_duckdb):
    timeline = test_duckdb.get_interval_histogram("QUERY_STATS_TOP_1HOUR", utc_offset=1.0)
    assert len(timeline) == 3
    assert timeline[0]["utc"] == "2026-08-19 08:00:00"
    assert timeline[0]["display"] == "2026-08-19 09:00:00"
    assert timeline[0]["count"] == 1

def test_parse_numeric_bound():
    assert _parse_numeric_bound(100) == 100
    assert _parse_numeric_bound(12.34) == 12.34
    assert _parse_numeric_bound("-1,737,658,763,448,670,700") == -1737658763448670700
    assert _parse_numeric_bound("1,000,000") == 1000000
    assert _parse_numeric_bound("1,234.56") == 1234.56
    assert isinstance(_parse_numeric_bound("-1,737,658,763,448,670,700"), int)

def test_bigint_fingerprint_filtering(tmp_path):
    db_file = str(tmp_path / "test_fingerprint.db")
    con = duckdb.connect(db_file)
    con.execute("""
        CREATE TABLE QUERY_STATS (
            text VARCHAR,
            text_fingerprint BIGINT,
            execution_count BIGINT
        );
    """)
    target_fp = -1737658763448670700
    other_fp = -5555555555555555555
    con.execute("INSERT INTO QUERY_STATS VALUES ('SELECT 1', ?, 10);", [target_fp])
    con.execute("INSERT INTO QUERY_STATS VALUES ('SELECT 2', ?, 20);", [other_fp])
    con.close()

    svc = DuckDBService(db_file)

    # 1. Test numeric min filter with commas (the user's scenario)
    num_filter_min = {"text_fingerprint": ColumnFilter(type="numeric", min="-1,737,658,763,448,670,700")}
    res = svc.query_table("QUERY_STATS", filters=num_filter_min)
    assert res["total"] >= 1
    assert any(item["text_fingerprint"] == str(target_fp) for item in res["items"])

    # 2. Test numeric min/max exact range with commas
    num_filter_range = {
        "text_fingerprint": ColumnFilter(
            type="numeric",
            min="-1,737,658,763,448,670,700",
            max="-1,737,658,763,448,670,700"
        )
    }
    res_range = svc.query_table("QUERY_STATS", filters=num_filter_range)
    assert res_range["total"] == 1
    assert res_range["items"][0]["text_fingerprint"] == str(target_fp)

    # 3. Test global search with commas
    res_search = svc.query_table("QUERY_STATS", global_search="-1,737,658,763,448,670,700")
    assert res_search["total"] == 1
    assert res_search["items"][0]["text_fingerprint"] == str(target_fp)

    # 4. Test text exact match with commas
    text_filter = {"text_fingerprint": ColumnFilter(type="text", operator="exact", value="-1,737,658,763,448,670,700")}
    res_text = svc.query_table("QUERY_STATS", filters=text_filter)
    assert res_text["total"] == 1
    assert res_text["items"][0]["text_fingerprint"] == str(target_fp)

    # 5. Verify 64-bit integer is serialized as exact string (preventing JS JSON.parse rounding)
    all_res = svc.query_table("QUERY_STATS")
    for item in all_res["items"]:
        assert isinstance(item["text_fingerprint"], str)


def test_null_and_other_filtering(tmp_path):
    db_file = str(tmp_path / "test_filter_null_other.db")
    con = duckdb.connect(db_file)
    con.execute("""
        CREATE TABLE SAMPLE_DATA (
            id INT,
            tag VARCHAR
        );
    """)
    con.execute("""
        INSERT INTO SAMPLE_DATA VALUES
        (1, 'cat_A'),
        (2, 'cat_B'),
        (3, 'cat_C'),
        (4, 'cat_Other1'),
        (5, 'cat_Other2'),
        (6, NULL),
        (7, NULL);
    """)
    con.close()

    svc = DuckDBService(db_file)

    # 1. Test is_null operator
    res_null = svc.query_table("SAMPLE_DATA", filters={"tag": ColumnFilter(type="text", operator="is_null")})
    assert res_null["total"] == 2
    assert all(item["tag"] is None for item in res_null["items"])
    assert {item["id"] for item in res_null["items"]} == {6, 7}

    # 2. Test is_not_null operator
    res_not_null = svc.query_table("SAMPLE_DATA", filters={"tag": ColumnFilter(type="text", operator="is_not_null")})
    assert res_not_null["total"] == 5
    assert all(item["tag"] is not None for item in res_not_null["items"])

    # 3. Test not_in operator (filtering for 'Other')
    top_categories = ["cat_A", "cat_B", "cat_C"]
    res_other = svc.query_table("SAMPLE_DATA", filters={"tag": ColumnFilter(type="text", operator="not_in", values=top_categories)})
    assert res_other["total"] == 2
    assert {item["tag"] for item in res_other["items"]} == {"cat_Other1", "cat_Other2"}
    assert {item["id"] for item in res_other["items"]} == {4, 5}

    # 4. Test not_in with empty values (behaves as IS NOT NULL)
    res_empty_not_in = svc.query_table("SAMPLE_DATA", filters={"tag": ColumnFilter(type="text", operator="not_in", values=[])})
    assert res_empty_not_in["total"] == 5



