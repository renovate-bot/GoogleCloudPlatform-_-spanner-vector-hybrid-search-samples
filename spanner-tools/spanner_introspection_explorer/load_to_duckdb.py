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

import duckdb
import glob
import os
import sys
from pathlib import Path

STAGING_BASE_DIR = "staging"
DATABASE_BASE_DIR = "backend/data/dbs"
DEFAULT_DB_FILE = "my_duckdb.db"
PREFIX_TO_REMOVE = "export_all_"

def get_table_name(filename: str, prefix_to_remove: str) -> str:
    if filename.startswith(prefix_to_remove):
        without_prefix = filename[len(prefix_to_remove):]
    else:
        without_prefix = filename
    return os.path.splitext(without_prefix)[0]

def load_csvs_to_duckdb(staging_dir: str, db_file: str, prefix_to_remove: str = PREFIX_TO_REMOVE):
    os.makedirs(os.path.dirname(db_file) if os.path.dirname(db_file) else ".", exist_ok=True)
    print(f"\nConnecting to DuckDB database: {db_file}")
    print(f"Reading CSVs from staging directory: {staging_dir}")
    
    file_pattern = os.path.join(staging_dir, "*.csv")
    csv_files = glob.glob(file_pattern)
    
    if not csv_files:
        print(f"Warning: No CSV files found in {staging_dir} matching {file_pattern}.")
        return None, []

    print(f"Found {len(csv_files)} CSV file(s) to load.")
    con = duckdb.connect(database=db_file)
    loaded_tables = []

    for file_path in csv_files:
        base_name = os.path.basename(file_path)
        table_name = get_table_name(base_name, prefix_to_remove)
        
        sql_command = f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{file_path}');
        """
        try:
            con.execute(sql_command)
            loaded_tables.append(table_name)
            print(f"  ✓ Loaded '{base_name}' into table '{table_name}'")
        except Exception as e:
            print(f"  ✗ Error loading '{base_name}': {e}")
            
    con.close()
    return db_file, loaded_tables

def print_table_counts(db_file: str, table_list: list):
    if not table_list:
        return

    print(f"\n--- Table Counts Verification ({db_file}) ---")
    try:
        con = duckdb.connect(database=db_file, read_only=True)
        results = {}
        for table_name in table_list:
            try:
                count_sql = f"SELECT COUNT(*) FROM {table_name};"
                count = con.execute(count_sql).fetchone()[0]
                results[table_name] = count
            except Exception as e:
                results[table_name] = f"ERROR: {e}"
        con.close()

        print(f"| {'Table Name':<35} | {'Row Count':<12} |")
        print(f"|{'-'*37}|{'-'*14}|")
        for table, count in results.items():
            print(f"| {table:<35} | {str(count):<12} |")
    except Exception as e:
        print(f"Could not verify table counts: {e}")

def process_all_databases():
    """
    Scans staging/ and loads:
    1. Root staging/*.csv into my_duckdb.db (legacy/default)
    2. Subdirectories staging/<db_id>/*.csv into backend/data/dbs/<db_id>.duckdb
    """
    os.makedirs(DATABASE_BASE_DIR, exist_ok=True)
    
    # 1. Check root staging/
    root_csvs = glob.glob(os.path.join(STAGING_BASE_DIR, "*.csv"))
    if root_csvs:
        db_file, loaded = load_csvs_to_duckdb(STAGING_BASE_DIR, DEFAULT_DB_FILE)
        if loaded:
            print_table_counts(db_file, loaded)
            # Also mirror into backend/data/dbs/default.duckdb
            load_csvs_to_duckdb(STAGING_BASE_DIR, os.path.join(DATABASE_BASE_DIR, "default.duckdb"))

    # 2. Check staging subdirectories
    if os.path.exists(STAGING_BASE_DIR):
        subdirs = [d for d in Path(STAGING_BASE_DIR).iterdir() if d.is_dir()]
        for subdir in subdirs:
            db_id = subdir.name
            target_db = os.path.join(DATABASE_BASE_DIR, f"{db_id}.duckdb")
            db_file, loaded = load_csvs_to_duckdb(str(subdir), target_db)
            if loaded:
                print_table_counts(db_file, loaded)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        custom_path = sys.argv[1]
        if os.path.isdir(custom_path):
            dir_name = os.path.basename(os.path.normpath(custom_path))
            target_db = os.path.join(DATABASE_BASE_DIR, f"{dir_name}.duckdb")
            db_file, loaded = load_csvs_to_duckdb(custom_path, target_db)
            if loaded:
                print_table_counts(db_file, loaded)
        else:
            print(f"Error: Directory '{custom_path}' not found.")
    else:
        process_all_databases()