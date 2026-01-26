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

"""Spanner service for managing split points."""
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

from google.cloud import spanner
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin
from google.protobuf import struct_pb2

from models import SpannerSplit, SyncResult, OperationType, KeyColumnInfo, EntityKeySchema, EntityType


def parse_raw_split_key(split_key: str) -> Tuple[Optional[str], Optional[str], str]:
    """Parse the raw split key format from Spanner's USER_SPLIT_POINTS table.

    Handles two formats:
    1. Index format: "Index: UsersByLocation on UserLocationInfo, Index Key: (CN), Primary Table Key: (<begin>,<begin>)"
    2. Table format: "UserInfo(922337203685477580)"

    Args:
        split_key: Raw split key string from Spanner

    Returns:
        Tuple of (index_name, index_key, table_key)
        - For index splits: (index_name, index_key_values, table_key_values)
        - For table splits: (None, None, key_values)
    """
    if not split_key:
        return (None, None, "")

    s = split_key.strip()

    # Index-style format
    index_match = re.match(
        r"^Index:\s*(?P<index>.+?)\s+on\s+(?P<index_table>[^,]+),\s*Index Key:\s*\((?P<index_key>.*?)\),\s*Primary Table Key:\s*\((?P<table_key>.*?)\)\s*$",
        s,
    )
    if index_match:
        index_name = index_match.group("index").strip()
        index_key = index_match.group("index_key").strip()
        table_key = index_match.group("table_key").strip()
        return (index_name, index_key, table_key)

    # Table(key) simple format: TableName(keycomponents)
    table_match = re.match(r"^(?P<table>[^\(]+)\((?P<table_key>.*)\)\s*$", s)
    if table_match:
        table_key = table_match.group("table_key").strip()
        return (None, None, table_key)

    # Fallback: return the whole string as the key
    return (None, None, s)


def _unescape_string(s: str) -> str:
    """Remove escape backslashes from a string."""
    # Replace escaped quotes and backslashes
    s = s.replace('\\"', '"')
    s = s.replace("\\'", "'")
    s = s.replace('\\n', ' ')
    s = s.replace('\\t', ' ')
    s = s.replace('\\\\', '\\')
    return s


def format_spanner_error(error_str: str) -> str:
    """Parse a Spanner API error message and return a human-friendly version.

    Extracts key information from verbose protobuf-style error messages.

    Args:
        error_str: Raw error string from Spanner API exception

    Returns:
        Human-friendly error message
    """
    # Extract table name from 'table: "TableName"'
    table_match = re.search(r'table:\s*[\\]?"([^"]+)[\\]?"', error_str)
    table_name = table_match.group(1).strip() if table_match else None

    # Extract the actual error reason from 'due to <reason>.'
    reason_match = re.search(r'due to\s+(.+?)(?:\.\s*\[|$)', error_str)
    if reason_match:
        reason = reason_match.group(1).strip().rstrip('.')
    else:
        # Try to find error after "is invalid,"
        invalid_match = re.search(r'is invalid,?\s*(.+?)(?:\.\s*\[|$)', error_str)
        if invalid_match:
            reason = invalid_match.group(1).strip().rstrip('.')
        else:
            reason = None

    # Build human-friendly message
    if table_name and reason:
        return _unescape_string(f"Table '{table_name}': {reason}")
    elif reason:
        return _unescape_string(reason)
    elif table_name:
        # Extract any message after the status code
        status_match = re.search(r'^\d+\s+(.+?)(?:\s*\[locale|$)', error_str)
        if status_match:
            return _unescape_string(f"Table '{table_name}': {status_match.group(1).strip()[:200]}")
        return f"Table '{table_name}': Operation failed"

    # Fallback: return a truncated version of the original
    # Remove the duplicate locale message at the end
    cleaned = re.sub(r'\s*\[locale.*$', '', error_str)
    # Remove protobuf formatting
    cleaned = re.sub(r'go/debugproto\s*\\n', '', cleaned)
    cleaned = re.sub(r'\\n', ' ', cleaned)
    # Truncate if still too long
    if len(cleaned) > 200:
        cleaned = cleaned[:200] + '...'
    return _unescape_string(cleaned.strip())


from database import (
    get_all_settings,
    get_local_splits_by_operation,
    delete_local_split_by_value,
    get_setting,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Maximum split points per Spanner API request
BATCH_LIMIT = 100

# Default expiration time for new splits (10 days)
DEFAULT_EXPIRATION_DAYS = 10


class SpannerService:
    """Service for interacting with Google Cloud Spanner."""

    def __init__(self, project_id: Optional[str] = None, instance_id: Optional[str] = None, database_id: Optional[str] = None):
        """Initialize the Spanner service."""
        self._project_id = project_id
        self._instance_id = instance_id
        self._database_id = database_id
        self._client: Optional[spanner.Client] = None

    @property
    def project_id(self) -> Optional[str]:
        """Get project ID from settings or environment."""
        if self._project_id:
            return self._project_id
        return get_setting("project_id") or os.getenv("PROJECT") or os.getenv("project_id")

    @property
    def instance_id(self) -> Optional[str]:
        """Get instance ID from settings or environment."""
        if self._instance_id:
            return self._instance_id
        return get_setting("instance_id") or os.getenv("SPANNER_INSTANCE") or os.getenv("INSTANCE")

    @property
    def database_id(self) -> Optional[str]:
        """Get database ID from settings or environment."""
        if self._database_id:
            return self._database_id
        return get_setting("database_id") or os.getenv("SPANNER_DATABASE") or os.getenv("DATABASE")

    @property
    def client(self) -> spanner.Client:
        """Get or create Spanner client."""
        if self._client is None:
            self._client = spanner.Client(project=self.project_id)
        return self._client

    def is_configured(self) -> bool:
        """Check if the service is properly configured."""
        return bool(self.instance_id and self.database_id)

    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test the connection to Spanner.

        Attempts to execute a simple query to verify the connection works.

        Returns:
            Tuple of (success: bool, error_message: Optional[str])
            - (True, None) if connection is successful
            - (False, error_message) if connection fails
        """
        if not self.is_configured():
            return (False, "Instance and database must be configured")

        try:
            db = self.get_database()
            # Run a simple query to test connectivity
            with db.snapshot() as snapshot:
                # Query a system table that always exists
                results = snapshot.execute_sql("SELECT 1")
                # Consume the results to ensure the query executes
                list(results)
            return (True, None)
        except Exception as e:
            error_str = str(e)
            # Extract a user-friendly error message
            if "NOT_FOUND" in error_str or "not found" in error_str.lower():
                if "Instance" in error_str:
                    return (False, f"Instance '{self.instance_id}' not found. Please check the instance ID.")
                elif "Database" in error_str or "database" in error_str.lower():
                    return (False, f"Database '{self.database_id}' not found in instance '{self.instance_id}'. Please check the database ID.")
                else:
                    return (False, f"Resource not found: {error_str[:200]}")
            elif "PERMISSION_DENIED" in error_str or "permission" in error_str.lower():
                return (False, "Permission denied. Please check your credentials and IAM permissions.")
            elif "UNAUTHENTICATED" in error_str or "authentication" in error_str.lower():
                return (False, "Authentication failed. Please run 'gcloud auth application-default login' and try again.")
            elif "INVALID_ARGUMENT" in error_str:
                return (False, f"Invalid configuration: {error_str[:200]}")
            else:
                # Generic error - truncate if too long
                msg = error_str if len(error_str) <= 300 else error_str[:300] + "..."
                return (False, f"Connection failed: {msg}")

    def get_database(self):
        """Get Spanner database instance."""
        if not self.is_configured():
            raise ValueError("Spanner instance and database must be configured")
        return self.client.instance(self.instance_id).database(self.database_id)

    def list_tables(self) -> list[str]:
        """List all base tables from Spanner INFORMATION_SCHEMA."""
        if not self.is_configured():
            return []

        db = self.get_database()
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ''"

        tables: list[str] = []

        try:
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(sql)
                for row in results:
                    if row[0]:
                        tables.append(str(row[0]))
        except Exception as e:
            logging.error("Error listing tables: %s", e)

        return tables

    def list_indexes(self) -> list[tuple[str, str]]:
        """List all indexes from Spanner INFORMATION_SCHEMA.

        Returns:
            List of tuples: (index_name, parent_table_name)
        """
        if not self.is_configured():
            return []

        db = self.get_database()
        # Get non-primary-key indexes
        sql = """
            SELECT INDEX_NAME, TABLE_NAME
            FROM INFORMATION_SCHEMA.INDEXES
            WHERE INDEX_TYPE != 'PRIMARY_KEY'
              AND SPANNER_IS_MANAGED = FALSE
        """

        indexes: list[tuple[str, str]] = []

        try:
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(sql)
                for row in results:
                    index_name = str(row[0]) if row[0] else ""
                    table_name = str(row[1]) if row[1] else ""
                    if index_name:
                        indexes.append((index_name, table_name))
        except Exception as e:
            logging.error("Error listing indexes: %s", e)

        return indexes

    def get_table_key_schema(self, table_name: str) -> EntityKeySchema:
        """Get the primary key schema for a table.

        Args:
            table_name: Name of the table

        Returns:
            EntityKeySchema with key column information
        """
        key_columns: list[KeyColumnInfo] = []

        if self.is_configured():
            db = self.get_database()
            sql = """
                SELECT ic.COLUMN_NAME, c.SPANNER_TYPE, ic.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic
                JOIN INFORMATION_SCHEMA.COLUMNS c
                  ON ic.TABLE_NAME = c.TABLE_NAME AND ic.COLUMN_NAME = c.COLUMN_NAME
                WHERE ic.TABLE_NAME = @table_name
                  AND ic.INDEX_TYPE = 'PRIMARY_KEY'
                ORDER BY ic.ORDINAL_POSITION
            """

            try:
                with db.snapshot() as snapshot:
                    results = snapshot.execute_sql(
                        sql,
                        params={"table_name": table_name},
                        param_types={"table_name": spanner.param_types.STRING}
                    )
                    for row in results:
                        key_columns.append(KeyColumnInfo(
                            column_name=str(row[0]) if row[0] else "",
                            spanner_type=str(row[1]) if row[1] else "",
                            ordinal_position=int(row[2]) if row[2] else 0
                        ))
            except Exception as e:
                logging.error("Error getting table key schema: %s", e)

        return EntityKeySchema(
            entity_name=table_name,
            entity_type=EntityType.TABLE,
            key_columns=key_columns,
            is_composite=len(key_columns) > 1
        )

    def get_index_key_schema(self, index_name: str) -> EntityKeySchema:
        """Get the key schema for an index.

        Args:
            index_name: Name of the index

        Returns:
            EntityKeySchema with key column information including parent table's primary key
        """
        key_columns: list[KeyColumnInfo] = []
        parent_table: Optional[str] = None
        parent_key_columns: list[KeyColumnInfo] = []

        if self.is_configured():
            db = self.get_database()

            # First, get the parent table name
            parent_sql = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.INDEXES
                WHERE INDEX_NAME = @index_name
                LIMIT 1
            """

            try:
                with db.snapshot() as snapshot:
                    results = snapshot.execute_sql(
                        parent_sql,
                        params={"index_name": index_name},
                        param_types={"index_name": spanner.param_types.STRING}
                    )
                    for row in results:
                        parent_table = str(row[0]) if row[0] else None
            except Exception as e:
                logging.error("Error getting index parent table: %s", e)

            # Get the index columns and their types
            sql = """
                SELECT ic.COLUMN_NAME, c.SPANNER_TYPE, ic.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic
                JOIN INFORMATION_SCHEMA.COLUMNS c
                  ON ic.TABLE_NAME = c.TABLE_NAME AND ic.COLUMN_NAME = c.COLUMN_NAME
                WHERE ic.INDEX_NAME = @index_name
                ORDER BY ic.ORDINAL_POSITION
            """

            try:
                with db.snapshot() as snapshot:
                    results = snapshot.execute_sql(
                        sql,
                        params={"index_name": index_name},
                        param_types={"index_name": spanner.param_types.STRING}
                    )
                    for row in results:
                        key_columns.append(KeyColumnInfo(
                            column_name=str(row[0]) if row[0] else "",
                            spanner_type=str(row[1]) if row[1] else "",
                            ordinal_position=int(row[2]) if row[2] else 0
                        ))
            except Exception as e:
                logging.error("Error getting index key schema: %s", e)

            # Get the parent table's primary key columns
            if parent_table:
                parent_schema = self.get_table_key_schema(parent_table)
                parent_key_columns = parent_schema.key_columns

        return EntityKeySchema(
            entity_name=index_name,
            entity_type=EntityType.INDEX,
            key_columns=key_columns,
            is_composite=len(key_columns) > 1,
            parent_table=parent_table,
            parent_key_columns=parent_key_columns if parent_key_columns else None
        )

    def list_splits(self) -> list[SpannerSplit]:
        """List all split points from Spanner."""
        if not self.is_configured():
            return []

        db = self.get_database()
        sql = "SELECT * FROM SPANNER_SYS.USER_SPLIT_POINTS"

        splits: list[SpannerSplit] = []

        try:
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(sql)
                for row in results:
                    # Expected row shape: [table, index, initiator, split_key, expire_time]
                    table = row[0] if len(row) > 0 else ""
                    index = row[1] if len(row) > 1 else ""
                    initiator = row[2] if len(row) > 2 else ""
                    split_key = row[3] if len(row) > 3 else ""
                    expire_time = row[4] if len(row) > 4 else None

                    splits.append(SpannerSplit(
                        table=str(table) if table else "",
                        index=str(index) if index else None,
                        initiator=str(initiator) if initiator else "",
                        split_key=str(split_key) if split_key else "",
                        expire_time=expire_time
                    ))
        except Exception as e:
            logging.error("Error listing split points: %s", e)

        return splits

    def _make_key(self, key_value: str) -> spanner_database_admin.SplitPoints.Key:
        """Create a Key object from a comma-separated key value string.

        Args:
            key_value: Comma-separated key values (e.g., "12345" or "val1, val2")

        Returns:
            A SplitPoints.Key object
        """
        key_parts = [v.strip() for v in key_value.split(",") if v.strip()]
        return spanner_database_admin.SplitPoints.Key(
            key_parts=struct_pb2.ListValue(
                values=[struct_pb2.Value(string_value=str(v)) for v in key_parts]
            )
        )

    def _make_split_point(
        self,
        table_name: str,
        split_value: str,
        expire_time: Optional[datetime] = None,
        index_name: Optional[str] = None,
        index_key: Optional[str] = None
    ) -> spanner_database_admin.SplitPoints:
        """Create a SplitPoints object for the API.

        Args:
            table_name: Name of the table
            split_value: Table key value (comma-separated for composite keys)
            expire_time: Optional expiration time
            index_name: Optional index name (if this is an index split)
            index_key: Optional index key value (if this is an index split)

        Returns:
            A SplitPoints object for the Spanner API
        """
        keys = []

        if index_name:
            # Index split - may have both index key and table key
            if index_key:
                keys.append(self._make_key(index_key))
            # Only include table key if it doesn't contain <begin> marker
            if split_value and "<begin>" not in split_value:
                keys.append(self._make_key(split_value))

            sp = spanner_database_admin.SplitPoints(
                index=index_name,
                keys=keys
            )
        else:
            # Table split
            keys.append(self._make_key(split_value))
            sp = spanner_database_admin.SplitPoints(
                table=table_name,
                keys=keys
            )

        print(f"Created split point: index={index_name}, table={table_name}, keys={keys}")

        if expire_time:
            sp.expire_time = expire_time

        return sp

    def _batch_split_points(self, split_points: list) -> list[list]:
        """Split a list of split points into batches of BATCH_LIMIT size."""
        return [
            split_points[i:i + BATCH_LIMIT]
            for i in range(0, len(split_points), BATCH_LIMIT)
        ]

    def add_split_points(self, table_name: str, split_values: list[str]) -> SyncResult:
        """Add split points to Spanner.

        Args:
            table_name: Name of the table
            split_values: List of split values to add
        """
        if not self.is_configured():
            return SyncResult(
                success=False,
                message="Spanner not configured",
                errors=["Instance and database must be configured"]
            )

        if not split_values:
            return SyncResult(success=True, message="No splits to add", added_count=0)

        # Set default expiration to 10 days from now
        default_expire = datetime.now() + timedelta(days=DEFAULT_EXPIRATION_DAYS)

        # Create split point objects
        api_splits = [
            self._make_split_point(table_name, sv, default_expire)
            for sv in split_values
        ]

        # Batch and send
        batches = self._batch_split_points(api_splits)
        errors: list[str] = []
        total_added = 0

        database_admin_api = self.client.database_admin_api
        db_path = database_admin_api.database_path(
            self.client.project, self.instance_id, self.database_id
        )

        for batch in batches:
            request = spanner_database_admin.AddSplitPointsRequest(
                database=db_path,
                split_points=batch,
            )
            try:
                database_admin_api.add_split_points(request)
                total_added += len(batch)
            except Exception as e:
                errors.append(str(e))
                logging.error("Error adding split points batch: %s", e)

        return SyncResult(
            success=len(errors) == 0,
            message=f"Added {total_added} split points" if total_added > 0 else "Failed to add splits",
            added_count=total_added,
            errors=errors
        )

    def delete_split_points(self, table_name: str, split_values: list[str]) -> SyncResult:
        """Delete split points from Spanner by setting immediate expiration.

        Args:
            table_name: Name of the table
            split_values: List of split values to delete
        """
        if not self.is_configured():
            return SyncResult(
                success=False,
                message="Spanner not configured",
                errors=["Instance and database must be configured"]
            )

        if not split_values:
            return SyncResult(success=True, message="No splits to delete", deleted_count=0)

        # Set expiration to now (immediate expiration = delete)
        expire_now = datetime.now() - timedelta(seconds=10)

        print("Split values to delete: ", split_values)
        # Create split point objects with immediate expiration
        # Parse the raw split key format to extract the actual key values
        api_splits = []
        for sv in split_values:
            index_name, index_key, table_key = parse_raw_split_key(sv)
            print(f"Parsed split key: index={index_name}, index_key={index_key}, table_key={table_key}")
            # Create split point with index info if applicable
            api_splits.append(self._make_split_point(
                table_name=table_name,
                split_value=table_key,
                expire_time=expire_now,
                index_name=index_name,
                index_key=index_key
            ))

        # Batch and send
        batches = self._batch_split_points(api_splits)
        errors: list[str] = []
        total_deleted = 0

        database_admin_api = self.client.database_admin_api
        db_path = database_admin_api.database_path(
            self.client.project, self.instance_id, self.database_id
        )

        for batch in batches:
            request = spanner_database_admin.AddSplitPointsRequest(
                database=db_path,
                split_points=batch,
            )
            try:
                database_admin_api.add_split_points(request)
                total_deleted += len(batch)
            except Exception as e:
                errors.append(str(e))
                logging.error("Error deleting split points batch: %s", e)

        return SyncResult(
            success=len(errors) == 0,
            message=f"Deleted {total_deleted} split points" if total_deleted > 0 else "Failed to delete splits",
            deleted_count=total_deleted,
            errors=errors
        )

    def sync_pending_changes(self) -> SyncResult:
        """Sync all pending local changes to Spanner.

        This processes both PENDING_ADD and PENDING_DELETE operations,
        handling both table and index splits.
        """
        if not self.is_configured():
            return SyncResult(
                success=False,
                message="Spanner not configured",
                errors=["Instance and database must be configured"]
            )

        # Get pending adds and deletes
        pending_adds = get_local_splits_by_operation(OperationType.ADD)
        pending_deletes = get_local_splits_by_operation(OperationType.DELETE)

        total_added = 0
        total_deleted = 0
        all_errors: list[str] = []

        # Set default expiration to 10 days from now for adds
        default_expire = datetime.now() + timedelta(days=DEFAULT_EXPIRATION_DAYS)

        # Process adds - create split points directly
        if pending_adds:
            api_splits = []
            for split in pending_adds:
                sp = self._make_split_point(
                    table_name=split.table_name,
                    split_value=split.split_value,
                    expire_time=default_expire,
                    index_name=split.index_name,
                    index_key=split.index_key
                )
                api_splits.append(sp)

            # Batch and send
            batches = self._batch_split_points(api_splits)
            database_admin_api = self.client.database_admin_api
            db_path = database_admin_api.database_path(
                self.client.project, self.instance_id, self.database_id
            )

            for i, batch in enumerate(batches):
                request = spanner_database_admin.AddSplitPointsRequest(
                    database=db_path,
                    split_points=batch,
                )
                try:
                    database_admin_api.add_split_points(request)
                    total_added += len(batch)
                    # Clear successfully synced from local DB
                    batch_start = i * BATCH_LIMIT
                    batch_end = batch_start + len(batch)
                    for split in pending_adds[batch_start:batch_end]:
                        delete_local_split_by_value(
                            split.table_name, split.split_value,
                            split.index_name, split.index_key
                        )
                except Exception as e:
                    all_errors.append(format_spanner_error(str(e)))
                    logging.error("Error adding split points batch: %s", e)

        # Process deletes - set immediate expiration
        if pending_deletes:
            expire_now = datetime.now() - timedelta(seconds=10)
            api_splits = []
            for split in pending_deletes:
                # For deletes from Spanner, we need to parse the raw split_value
                # which contains the format like "UserInfo(123)"
                index_name, index_key, table_key = parse_raw_split_key(split.split_value)
                sp = self._make_split_point(
                    table_name=split.table_name,
                    split_value=table_key,
                    expire_time=expire_now,
                    index_name=index_name,
                    index_key=index_key
                )
                api_splits.append(sp)

            # Batch and send
            batches = self._batch_split_points(api_splits)
            database_admin_api = self.client.database_admin_api
            db_path = database_admin_api.database_path(
                self.client.project, self.instance_id, self.database_id
            )

            for i, batch in enumerate(batches):
                request = spanner_database_admin.AddSplitPointsRequest(
                    database=db_path,
                    split_points=batch,
                )
                try:
                    database_admin_api.add_split_points(request)
                    total_deleted += len(batch)
                    # Clear successfully synced from local DB
                    batch_start = i * BATCH_LIMIT
                    batch_end = batch_start + len(batch)
                    for split in pending_deletes[batch_start:batch_end]:
                        delete_local_split_by_value(
                            split.table_name, split.split_value,
                            split.index_name, split.index_key
                        )
                except Exception as e:
                    all_errors.append(format_spanner_error(str(e)))
                    logging.error("Error deleting split points batch: %s", e)

        success = len(all_errors) == 0
        message_parts = []
        total = total_added + total_deleted
        if total_added > 0:
            message_parts.append(f"Added {total_added}")
        if total_deleted > 0:
            message_parts.append(f"Deleted {total_deleted}")
        if not message_parts:
            message = "Nothing synced. Check for *potential* errors below."
        else:
            point_word = "split point" if total == 1 else "split points"
            message = ", ".join(message_parts) + f" {point_word}"

        return SyncResult(
            success=success,
            message=message,
            added_count=total_added,
            deleted_count=total_deleted,
            errors=all_errors
        )


# Global service instance
_spanner_service: Optional[SpannerService] = None


def get_spanner_service() -> SpannerService:
    """Get the global Spanner service instance."""
    global _spanner_service
    if _spanner_service is None:
        _spanner_service = SpannerService()
    return _spanner_service
