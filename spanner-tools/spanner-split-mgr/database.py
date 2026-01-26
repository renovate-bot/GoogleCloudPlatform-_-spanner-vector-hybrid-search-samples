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

"""SQLite database layer for local staging of split points."""
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from models import LocalSplitResponse, OperationType, SettingsResponse


DATABASE_PATH = Path(__file__).parent / "sqlite.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Initialize the database schema."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Settings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # Check if we need to migrate the local_splits table
        # The old schema had UNIQUE(table_name, split_value), new one needs
        # UNIQUE(table_name, split_value, index_name, index_key)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='local_splits'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            # Check if the table has the old schema by looking at the table info
            cursor.execute("PRAGMA table_info(local_splits)")
            columns = {row[1] for row in cursor.fetchall()}

            # If index_name column doesn't exist, we need to migrate
            if 'index_name' not in columns:
                # Migrate: rename old table, create new one, copy data, drop old
                cursor.execute("ALTER TABLE local_splits RENAME TO local_splits_old")
                cursor.execute("""
                    CREATE TABLE local_splits (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_name TEXT NOT NULL,
                        split_value TEXT NOT NULL DEFAULT '',
                        operation_type TEXT NOT NULL,
                        index_name TEXT DEFAULT '',
                        index_key TEXT DEFAULT '',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(table_name, split_value, index_name, index_key)
                    )
                """)
                cursor.execute("""
                    INSERT INTO local_splits (id, table_name, split_value, operation_type, index_name, index_key, created_at)
                    SELECT id, table_name, split_value, operation_type, '', '', created_at
                    FROM local_splits_old
                """)
                cursor.execute("DROP TABLE local_splits_old")
            else:
                # Columns exist but UNIQUE constraint might be wrong
                # Check by looking at the SQL that created the table
                cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='local_splits'")
                create_sql = cursor.fetchone()[0]

                # If the UNIQUE constraint doesn't include index_name, recreate
                if 'UNIQUE(table_name, split_value, index_name, index_key)' not in create_sql:
                    cursor.execute("ALTER TABLE local_splits RENAME TO local_splits_old")
                    cursor.execute("""
                        CREATE TABLE local_splits (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            table_name TEXT NOT NULL,
                            split_value TEXT NOT NULL DEFAULT '',
                            operation_type TEXT NOT NULL,
                            index_name TEXT DEFAULT '',
                            index_key TEXT DEFAULT '',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(table_name, split_value, index_name, index_key)
                        )
                    """)
                    cursor.execute("""
                        INSERT INTO local_splits (id, table_name, split_value, operation_type, index_name, index_key, created_at)
                        SELECT id, table_name, split_value, operation_type, COALESCE(index_name, ''), COALESCE(index_key, ''), created_at
                        FROM local_splits_old
                    """)
                    cursor.execute("DROP TABLE local_splits_old")
        else:
            # Create fresh table with correct schema
            cursor.execute("""
                CREATE TABLE local_splits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    split_value TEXT NOT NULL DEFAULT '',
                    operation_type TEXT NOT NULL,
                    index_name TEXT DEFAULT '',
                    index_key TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name, split_value, index_name, index_key)
                )
            """)


def _row_to_response(row: sqlite3.Row) -> LocalSplitResponse:
    """Convert a database row to a LocalSplitResponse."""
    return LocalSplitResponse(
        id=row["id"],
        table_name=row["table_name"],
        split_value=row["split_value"] or "",
        operation_type=OperationType(row["operation_type"]),
        created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"],
        index_name=row["index_name"] if "index_name" in row.keys() else None,
        index_key=row["index_key"] if "index_key" in row.keys() else None
    )


# Settings operations

def get_setting(key: str) -> Optional[str]:
    """Get a setting value by key."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row["value"] if row else None


def set_setting(key: str, value: str) -> None:
    """Set a setting value."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value)
        )


def get_all_settings() -> SettingsResponse:
    """Get all settings."""
    return SettingsResponse(
        project_id=get_setting("project_id"),
        instance_id=get_setting("instance_id"),
        database_id=get_setting("database_id")
    )


def update_settings(project_id: Optional[str], instance_id: Optional[str], database_id: Optional[str]) -> None:
    """Update multiple settings at once."""
    with get_db() as conn:
        cursor = conn.cursor()
        if project_id is not None:
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("project_id", project_id)
            )
        if instance_id is not None:
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("instance_id", instance_id)
            )
        if database_id is not None:
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("database_id", database_id)
            )


def clear_settings() -> None:
    """Clear all settings from the database."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM settings WHERE key IN ('project_id', 'instance_id', 'database_id')")


# Local splits operations

def add_local_split(
    table_name: str,
    split_value: str,
    operation_type: OperationType,
    index_name: Optional[str] = None,
    index_key: Optional[str] = None
) -> LocalSplitResponse:
    """Add a new local split point.

    Args:
        table_name: Name of the table
        split_value: Table key value (comma-separated for composite keys)
        operation_type: ADD or DELETE
        index_name: Optional index name for index splits
        index_key: Optional index key value for index splits
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # For uniqueness, treat None as empty string
        idx_name = index_name or ""
        idx_key = index_key or ""

        cursor.execute(
            """
            INSERT INTO local_splits (table_name, split_value, operation_type, index_name, index_key)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(table_name, split_value, index_name, index_key) DO UPDATE SET
                operation_type = excluded.operation_type,
                created_at = CURRENT_TIMESTAMP
            """,
            (table_name, split_value or "", operation_type.value, idx_name, idx_key)
        )

        # Fetch the inserted/updated row
        cursor.execute(
            """SELECT * FROM local_splits
               WHERE table_name = ? AND split_value = ?
               AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?""",
            (table_name, split_value or "", idx_name, idx_key)
        )
        row = cursor.fetchone()

        return _row_to_response(row)


def get_local_splits_by_operation(operation_type: OperationType) -> list[LocalSplitResponse]:
    """Get all local splits by operation type."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM local_splits WHERE operation_type = ? ORDER BY created_at DESC",
            (operation_type.value,)
        )
        rows = cursor.fetchall()
        return [_row_to_response(row) for row in rows]


def get_all_local_splits() -> list[LocalSplitResponse]:
    """Get all local splits."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM local_splits ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [_row_to_response(row) for row in rows]


def delete_local_split(split_id: int) -> bool:
    """Delete a local split by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM local_splits WHERE id = ?", (split_id,))
        return cursor.rowcount > 0


def delete_local_split_by_value(
    table_name: str,
    split_value: str,
    index_name: Optional[str] = None,
    index_key: Optional[str] = None
) -> bool:
    """Delete a local split by table name, split value, and optionally index info."""
    with get_db() as conn:
        cursor = conn.cursor()
        idx_name = index_name or ""
        idx_key = index_key or ""
        cursor.execute(
            """DELETE FROM local_splits
               WHERE table_name = ? AND split_value = ?
               AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?""",
            (table_name, split_value or "", idx_name, idx_key)
        )
        return cursor.rowcount > 0


def clear_pending_splits(operation_type: Optional[OperationType] = None) -> int:
    """Clear pending splits, optionally filtered by operation type."""
    with get_db() as conn:
        cursor = conn.cursor()
        if operation_type:
            cursor.execute(
                "DELETE FROM local_splits WHERE operation_type = ?",
                (operation_type.value,)
            )
        else:
            cursor.execute("DELETE FROM local_splits")
        return cursor.rowcount


def get_local_split_by_table_and_value(
    table_name: str,
    split_value: str,
    index_name: Optional[str] = None,
    index_key: Optional[str] = None
) -> Optional[LocalSplitResponse]:
    """Get a local split by table name, split value, and optionally index info."""
    with get_db() as conn:
        cursor = conn.cursor()
        idx_name = index_name or ""
        idx_key = index_key or ""
        cursor.execute(
            """SELECT * FROM local_splits
               WHERE table_name = ? AND split_value = ?
               AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?""",
            (table_name, split_value or "", idx_name, idx_key)
        )
        row = cursor.fetchone()

        if not row:
            return None

        return _row_to_response(row)
