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

import argparse
from google.cloud import spanner

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--source-instance", required=True)
    parser.add_argument("--source-database", required=True)
    parser.add_argument("--dest-instance", required=True)
    parser.add_argument("--dest-database", required=True)
    args = parser.parse_args()

    client = spanner.Client(project=args.project)
    source_db = client.instance(args.source_instance).database(args.source_database)
    dest_db = client.instance(args.dest_instance).database(args.dest_database)
    
    tables = ["Users", "Products", "Orders", "OrderItems"]
    print(f"Validating data between source ({args.source_database}) and dest ({args.dest_database})...")
    
    all_match = True
    for table in tables:
        with source_db.snapshot() as snapshot:
            source_count = list(snapshot.execute_sql(f"SELECT COUNT(*) FROM {table}"))[0][0]
        with dest_db.snapshot() as snapshot:
            dest_count = list(snapshot.execute_sql(f"SELECT COUNT(*) FROM {table}"))[0][0]
            
        match = source_count == dest_count
        status = "MATCH" if match else "MISMATCH"
        print(f"Table {table:<12} | Source: {source_count:<6} | Dest: {dest_count:<6} | {status}")
        if not match:
            all_match = False
            
    if all_match:
        print("\nSuccess! All tables match perfectly.")
        exit(0)
    else:
        print("\nFailure! Some tables do not match (replication might still be in-flight).")
        exit(1)

if __name__ == "__main__":
    main()
