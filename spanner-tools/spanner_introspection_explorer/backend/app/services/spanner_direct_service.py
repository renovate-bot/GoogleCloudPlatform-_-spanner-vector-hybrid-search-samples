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
import csv
import json
import math
import logging
import subprocess
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable

from google.cloud import spanner
from google.auth.exceptions import DefaultCredentialsError

from .spanner_queries import GSQL_INTROSPECTION_QUERIES, PG_INTROSPECTION_QUERIES

logger = logging.getLogger(__name__)


def format_spanner_value(val: Any) -> str:
    """
    Formats a Spanner column value for CSV output compatible with DuckDB ingestion.
    Handles timestamps, arrays, structs, nulls, and numeric boundaries.
    """
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        if isinstance(val, datetime):
            if val.microsecond:
                return val.strftime("%Y-%m-%d %H:%M:%S.%f")
            return val.strftime("%Y-%m-%d %H:%M:%S")
        return val.isoformat()
    if isinstance(val, (list, dict)):
        return json.dumps(val, default=str)
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return ""
        return str(val)
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8")
        except Exception:
            return val.hex()
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)


class SpannerDirectService:
    """
    Direct Cloud Spanner client service using the official google-cloud-spanner SDK.
    Executes queries using ExecuteStreamingSql to stream full result sets without
    the 10.00 MiB limit imposed by unary execute-sql calls.
    """

    def __init__(self, credentials_path: Optional[str] = None):
        self.credentials_path = credentials_path

    def get_client(self, project_id: str) -> spanner.Client:
        clean_project_id = project_id.strip().split("/")[-1]
        try:
            if self.credentials_path and os.path.exists(self.credentials_path):
                return spanner.Client.from_service_account_json(self.credentials_path, project=clean_project_id)
            return spanner.Client(project=clean_project_id)
        except DefaultCredentialsError as e:
            raise RuntimeError(
                f"Google Application Default Credentials (ADC) not found: {e}. "
                "Please run 'gcloud auth application-default login' in your terminal or configure GOOGLE_APPLICATION_CREDENTIALS."
            ) from e
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Cloud Spanner client: {e}") from e

    def test_connection(self, project_id: str, instance_id: str, database_id: str) -> Dict[str, Any]:
        """
        Executes a lightweight query against the database to verify connectivity.
        """
        clean_instance_id = instance_id.strip().split("/")[-1]
        clean_database_id = database_id.strip().split("/")[-1]

        try:
            client = self.get_client(project_id)
            instance = client.instance(clean_instance_id)
            database = instance.database(clean_database_id)

            with database.snapshot() as snapshot:
                results = snapshot.execute_sql("SELECT 1 AS status")
                list(results)

            return {
                "success": True,
                "message": f"Successfully connected to Cloud Spanner database '{clean_database_id}' via direct client!"
            }
        except Exception as e:
            logger.warning(f"Connection test failed for {database_id}: {e}")
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}"
            }

    def fetch_schema_ddl(
        self,
        project_id: str,
        instance_id: str,
        database_id: str,
        staging_dir: Path,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> bool:
        """
        Fetches the database schema DDL statements and saves them to schema.sql.
        Attempts direct client reload() first; falls back to gcloud ddl describe if needed.
        """
        clean_project_id = project_id.strip().split("/")[-1]
        clean_instance_id = instance_id.strip().split("/")[-1]
        clean_database_id = database_id.strip().split("/")[-1]

        if log_callback:
            log_callback("⏳ Fetching database schema DDL...")

        # 1. Try direct Spanner client API
        try:
            client = self.get_client(clean_project_id)
            instance = client.instance(clean_instance_id)
            database = instance.database(clean_database_id)

            database.reload()
            ddl_statements = database.ddl_statements

            if ddl_statements:
                schema_text = ";\n\n".join(ddl_statements) + ";\n"
                (staging_dir / "schema.sql").write_text(schema_text, encoding="utf-8")
                if log_callback:
                    log_callback("✓ Successfully exported schema.sql DDL via direct client")
                return True
        except Exception as e:
            logger.warning(f"Direct client DDL fetch failed, attempting gcloud fallback: {e}")

        # 2. Fallback to gcloud if available
        try:
            ddl_cmd = [
                "gcloud", "spanner", "databases", "ddl", "describe", clean_database_id,
                f"--instance={clean_instance_id}",
                f"--project={clean_project_id}"
            ]
            ddl_res = subprocess.run(ddl_cmd, capture_output=True, text=True, timeout=30)
            if ddl_res.returncode == 0 and ddl_res.stdout.strip():
                (staging_dir / "schema.sql").write_text(ddl_res.stdout, encoding="utf-8")
                if log_callback:
                    log_callback("✓ Successfully exported schema.sql DDL via gcloud")
                return True
        except Exception:
            pass

        return False

    def export_database_to_staging(
        self,
        project_id: str,
        instance_id: str,
        database_id: str,
        dialect: str,
        staging_dir: Path,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes all introspection queries via streaming ExecuteStreamingSql API
        and exports them as CSVs into staging_dir.
        """
        clean_project_id = project_id.strip().split("/")[-1]
        clean_instance_id = instance_id.strip().split("/")[-1]
        clean_database_id = database_id.strip().split("/")[-1]

        staging_dir.mkdir(parents=True, exist_ok=True)
        query_map = PG_INTROSPECTION_QUERIES if dialect == "POSTGRESQL" else GSQL_INTROSPECTION_QUERIES

        client = self.get_client(clean_project_id)
        instance = client.instance(clean_instance_id)
        database = instance.database(clean_database_id)

        results = []

        for table_stem, clean_sql in query_map.items():
            csv_file = staging_dir / f"{table_stem}.csv"
            clean_sql = clean_sql.strip()
            if not clean_sql:
                continue

            try:
                if log_callback:
                    log_callback(f"⏳ Exporting {table_stem} from Cloud Spanner...")

                # ExecuteStreamingSql with a dedicated snapshot per table query
                with database.snapshot() as snapshot:
                    result_stream = snapshot.execute_sql(clean_sql)

                    row_count = 0
                    with open(csv_file, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        header_written = False

                        for row in result_stream:
                            if not header_written:
                                fields = [f.name for f in result_stream.fields if f.name]
                                if fields:
                                    writer.writerow(fields)
                                header_written = True

                            formatted_row = [format_spanner_value(cell) for cell in row]
                            writer.writerow(formatted_row)
                            row_count += 1

                        if not header_written:
                            try:
                                fields = [f.name for f in result_stream.fields if f.name]
                                if fields:
                                    writer.writerow(fields)
                            except Exception:
                                pass

                results.append({"table": table_stem, "rows": row_count, "status": "ok"})
                if log_callback:
                    log_callback(f"✓ Exported {table_stem}: {row_count:,} rows")

            except Exception as ex:
                logger.error(f"Error exporting {table_stem}: {ex}", exc_info=True)
                results.append({"table": table_stem, "rows": 0, "status": "error", "error": str(ex)})
                if log_callback:
                    log_callback(f"⚠️ Could not export {table_stem}: {str(ex)[:150]}")

        # Export Schema DDL
        self.fetch_schema_ddl(clean_project_id, clean_instance_id, clean_database_id, staging_dir, log_callback)

        return results
