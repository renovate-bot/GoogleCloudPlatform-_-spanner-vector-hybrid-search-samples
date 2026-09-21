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
from pathlib import Path
from backend.app.services.spanner_queries import (
    GSQL_INTROSPECTION_QUERIES,
    PG_INTROSPECTION_QUERIES,
)

EXPECTED_SPLIT_KEYS = [
    "export_all_SPLIT_STATS_TOP_MINUTE",
    "export_all_SPLIT_STATS_TOP_10MINUTE",
    "export_all_SPLIT_STATS_TOP_HOUR",
]

EXPECTED_COLUMNS = [
    "interval_end",
    "split_start",
    "split_limit",
    "cpu_usage_score",
    "affected_tables",
    "unsplittable_reasons",
]

EXPECTED_VIEWS = {
    "export_all_SPLIT_STATS_TOP_MINUTE": "SPANNER_SYS.SPLIT_STATS_TOP_MINUTE",
    "export_all_SPLIT_STATS_TOP_10MINUTE": "SPANNER_SYS.SPLIT_STATS_TOP_10MINUTE",
    "export_all_SPLIT_STATS_TOP_HOUR": "SPANNER_SYS.SPLIT_STATS_TOP_HOUR",
}


@pytest.mark.parametrize("query_dict,dialect_name", [
    (GSQL_INTROSPECTION_QUERIES, "GoogleSQL"),
    (PG_INTROSPECTION_QUERIES, "PostgreSQL"),
])
def test_split_stats_query_definitions(query_dict, dialect_name):
    for key in EXPECTED_SPLIT_KEYS:
        assert key in query_dict, f"Missing key {key} in {dialect_name} query dictionary"
        sql = query_dict[key]

        # Check that all 6 required attributes are selected
        for col in EXPECTED_COLUMNS:
            assert col in sql.lower(), f"Missing column {col} in {key} for {dialect_name}"

        # Check that view is referenced
        expected_view = EXPECTED_VIEWS[key]
        assert expected_view.lower() in sql.lower(), f"Missing view {expected_view} in {key} for {dialect_name}"

        # Check ORDER BY clause is present
        assert "order by" in sql.lower(), f"Missing ORDER BY clause in {key} for {dialect_name}"


def test_internal_sql_script_files_exist():
    repo_root = Path(__file__).resolve().parent.parent.parent
    gsql_dir = repo_root / "internal" / "gsql_scripts"
    pg_dir = repo_root / "internal" / "pg_scripts"

    for key in EXPECTED_SPLIT_KEYS:
        filename = f"{key}.sql"
        gsql_file = gsql_dir / filename
        pg_file = pg_dir / filename

        assert gsql_file.exists(), f"Missing GoogleSQL script: {gsql_file}"
        assert pg_file.exists(), f"Missing PostgreSQL script: {pg_file}"

        for script_path in [gsql_file, pg_file]:
            content = script_path.read_text()
            assert "set cloud_mode true;" in content
            assert "set row_format csv;" in content
            for col in EXPECTED_COLUMNS:
                assert col in content.lower(), f"Missing column {col} in {script_path}"
            assert EXPECTED_VIEWS[key].lower() in content.lower()
            assert "order by" in content.lower()


def test_duckdb_ingest_all_split_stats(tmp_path):
    csv_content = (
        "interval_end,split_start,split_limit,cpu_usage_score,affected_tables,unsplittable_reasons\n"
        '2026-09-21 10:00:00,Users(101),Users(102),95,"[""Messages"", ""Users""]","[""HOT_ROW""]"\n'
        '2026-09-21 10:00:00,Users(13),Users(76),82,"[""Users""]","[""LARGE_SCAN_HOT_SPOT""]"\n'
    )

    db_file = tmp_path / "test_split.duckdb"
    con = duckdb.connect(str(db_file))

    for key in EXPECTED_SPLIT_KEYS:
        table_name = key.replace("export_all_", "")
        csv_file = tmp_path / f"{key}.csv"
        csv_file.write_text(csv_content)

        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}', header=True);")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        assert count == 2

        # Check column types and values
        cols = [r[0].lower() for r in con.execute(f"DESCRIBE {table_name};").fetchall()]
        for col in EXPECTED_COLUMNS:
            assert col.lower() in cols, f"DuckDB table {table_name} missing column {col}"

        # Query top CPU usage split
        top_row = con.execute(
            f"SELECT split_start, cpu_usage_score, unsplittable_reasons FROM {table_name} ORDER BY cpu_usage_score DESC LIMIT 1;"
        ).fetchone()
        assert top_row[0] == "Users(101)"
        assert top_row[1] == 95
        assert "HOT_ROW" in top_row[2]

    con.close()
