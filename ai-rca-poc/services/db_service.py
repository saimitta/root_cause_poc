import os
import sqlite3
import pandas as pd

# Resolve DB path relative to project root (works regardless of where the script is launched from)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "database", "db.sqlite")

# Ensure the database directory always exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def run_query(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a SELECT query and return results as a DataFrame."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


def execute(query: str, params: tuple = ()) -> None:
    """Execute a non-SELECT SQL statement (INSERT, UPDATE, DELETE)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()


def get_table_info(table: str) -> dict:
    """Return basic metadata about a table."""
    count_df = run_query(f"SELECT COUNT(*) as row_count FROM {table}")
    cols_df = run_query(f"PRAGMA table_info({table})")
    return {
        "table": table,
        "row_count": int(count_df["row_count"].iloc[0]),
        "columns": cols_df["name"].tolist(),
    }


def get_all_tables() -> list:
    """List all tables in the database."""
    df = run_query("SELECT name FROM sqlite_master WHERE type='table'")
    return df["name"].tolist()
