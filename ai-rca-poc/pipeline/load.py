import os
import sqlite3
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "database", "db.sqlite")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def load_to_db(df: pd.DataFrame, table: str, if_exists: str = "replace") -> None:
    """Load a DataFrame into a SQLite table."""
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(table, conn, if_exists=if_exists, index=False)
    conn.close()
    print(f"[Load] Table '{table}' loaded with {len(df)} rows.")
