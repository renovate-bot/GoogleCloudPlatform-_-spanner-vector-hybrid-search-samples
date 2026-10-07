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

import math
from datetime import datetime, date, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import duckdb

from backend.app.services.spanner_direct_service import (
    SpannerDirectService,
    format_spanner_value,
)
from google.auth.exceptions import DefaultCredentialsError


def test_format_spanner_value_types():
    assert format_spanner_value(None) == ""
    assert format_spanner_value(123) == "123"
    assert format_spanner_value("hello world") == "hello world"
    assert format_spanner_value(True) == "true"
    assert format_spanner_value(False) == "false"

    # Datetimes and dates
    dt = datetime(2026, 8, 20, 10, 30, 45, tzinfo=timezone.utc)
    assert format_spanner_value(dt) == "2026-08-20 10:30:45"

    dt_micro = datetime(2026, 8, 20, 10, 30, 45, 123456, tzinfo=timezone.utc)
    assert format_spanner_value(dt_micro) == "2026-08-20 10:30:45.123456"

    d = date(2026, 8, 20)
    assert format_spanner_value(d) == "2026-08-20"

    # Floats and special values
    assert format_spanner_value(12.34) == "12.34"
    assert format_spanner_value(float("nan")) == ""
    assert format_spanner_value(float("inf")) == ""
    assert format_spanner_value(float("-inf")) == ""

    # Arrays and nested structures
    assert format_spanner_value(["Users", "Messages"]) == '["Users", "Messages"]'
    assert format_spanner_value({"key": "value"}) == '{"key": "value"}'

    # Bytes
    assert format_spanner_value(b"plain_bytes") == "plain_bytes"
    assert format_spanner_value(b"\xff\xfe") == "fffe"


def test_spanner_direct_service_missing_adc():
    service = SpannerDirectService()
    with patch("google.cloud.spanner.Client", side_effect=DefaultCredentialsError("No credentials")):
        with pytest.raises(RuntimeError) as exc_info:
            service.get_client("test-project")
        assert "Google Application Default Credentials (ADC) not found" in str(exc_info.value)


def test_spanner_direct_service_test_connection_success():
    service = SpannerDirectService()
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_database = MagicMock()
    mock_snapshot = MagicMock()
    mock_results = iter([[1]])

    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.snapshot.return_value.__enter__.return_value = mock_snapshot
    mock_snapshot.execute_sql.return_value = mock_results

    with patch.object(service, "get_client", return_value=mock_client):
        res = service.test_connection("proj", "inst", "db")
        assert res["success"] is True
        assert "Successfully connected" in res["message"]


def test_spanner_direct_service_test_connection_failure():
    service = SpannerDirectService()
    with patch.object(service, "get_client", side_effect=Exception("Spanner connection refused")):
        res = service.test_connection("proj", "inst", "db")
        assert res["success"] is False
        assert "Spanner connection refused" in res["message"]


def test_spanner_direct_service_fetch_schema_ddl(tmp_path):
    service = SpannerDirectService()
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_database = MagicMock()

    mock_database.ddl_statements = [
        "CREATE TABLE Users (UserId INT64, Name STRING(MAX)) PRIMARY KEY (UserId)",
        "CREATE TABLE Orders (OrderId INT64) PRIMARY KEY (OrderId)"
    ]

    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database

    with patch.object(service, "get_client", return_value=mock_client):
        success = service.fetch_schema_ddl("proj", "inst", "db", tmp_path)
        assert success is True
        schema_file = tmp_path / "schema.sql"
        assert schema_file.exists()
        content = schema_file.read_text()
        assert "CREATE TABLE Users" in content
        assert "CREATE TABLE Orders" in content


def test_spanner_direct_service_export_database_to_staging(tmp_path):
    service = SpannerDirectService()
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_database = MagicMock()
    mock_snapshot = MagicMock()

    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.snapshot.return_value.__enter__.return_value = mock_snapshot

    class MockField:
        def __init__(self, name):
            self.name = name

    class MockStreamedResultSet:
        def __init__(self, fields, rows):
            self.fields = [MockField(f) for f in fields]
            self._rows = rows

        def __iter__(self):
            return iter(self._rows)

    def mock_execute_sql(sql):
        # Return mock stream with rows
        return MockStreamedResultSet(
            fields=["interval_end", "text", "execution_count", "avg_latency_seconds"],
            rows=[
                [datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc), "SELECT * FROM Users WHERE id = 1", 50, 0.005],
                [datetime(2026, 8, 20, 10, 1, 0, tzinfo=timezone.utc), "UPDATE Users SET active = true", 20, 0.012],
            ]
        )

    mock_snapshot.execute_sql.side_effect = mock_execute_sql
    mock_database.ddl_statements = ["CREATE TABLE Users (id INT64) PRIMARY KEY (id)"]

    logs = []
    with patch.object(service, "get_client", return_value=mock_client):
        results = service.export_database_to_staging(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="test-db",
            dialect="GOOGLE_STANDARD_SQL",
            staging_dir=tmp_path,
            log_callback=logs.append
        )

        assert len(results) > 0
        assert all(r["status"] == "ok" for r in results)
        assert any("✓ Exported" in log for log in logs)

        # Verify CSV content
        query_csv = tmp_path / "export_all_QUERY_STATS_TOP_1MIN.csv"
        assert query_csv.exists()
        lines = query_csv.read_text().strip().split("\n")
        assert lines[0] == "interval_end,text,execution_count,avg_latency_seconds"
        assert "2026-08-20 10:00:00" in lines[1]
        assert "SELECT * FROM Users WHERE id = 1" in lines[1]

        # Verify DuckDB loads the exported CSV smoothly
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_csv_auto('{query_csv}', header=True)").df()
        assert len(df) == 2
        assert df["execution_count"].iloc[0] == 50


def test_spanner_direct_service_single_use_snapshot_regression(tmp_path):
    """
    Verifies that a fresh snapshot is created for each table query so single-use snapshots
    do not raise ValueError('Cannot re-use single-use snapshot.').
    """
    service = SpannerDirectService()
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_database = MagicMock()

    class SingleUseSnapshotMock:
        def __init__(self):
            self.executed = False

        def execute_sql(self, sql):
            if self.executed:
                raise ValueError("Cannot re-use single-use snapshot.")
            self.executed = True
            
            class FieldMock:
                name = "col1"

            class StreamMock:
                fields = [FieldMock()]
                def __iter__(self):
                    return iter([["val1"]])
            return StreamMock()

    class SnapshotCM:
        def __enter__(self):
            return SingleUseSnapshotMock()
        def __exit__(self, *args):
            pass

    mock_database.snapshot.side_effect = lambda **kw: SnapshotCM()
    mock_database.ddl_statements = []
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database

    with patch.object(service, "get_client", return_value=mock_client):
        results = service.export_database_to_staging(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="test-db",
            dialect="GOOGLE_STANDARD_SQL",
            staging_dir=tmp_path
        )
        assert len(results) > 0
        assert all(r["status"] == "ok" for r in results)
