from services.db_service import run_query


def check_nulls(table: str, columns: list) -> dict:
    """Check for NULL values in specified columns."""
    results = {}
    for col in columns:
        df = run_query(f"SELECT COUNT(*) as null_count FROM {table} WHERE {col} IS NULL")
        results[col] = int(df["null_count"].iloc[0])
    return results


def check_duplicates(table: str, key_col: str) -> int:
    """Return number of duplicate rows based on a key column."""
    df = run_query(
        f"""
        SELECT COUNT(*) as dup_count FROM (
            SELECT {key_col}, COUNT(*) as cnt
            FROM {table}
            GROUP BY {key_col}
            HAVING cnt > 1
        )
        """
    )
    return int(df["dup_count"].iloc[0])


def check_schema(table: str, expected_columns: list) -> dict:
    """Verify that a table has all expected columns."""
    df = run_query(f"PRAGMA table_info({table})")
    actual_columns = df["name"].tolist()
    missing = [c for c in expected_columns if c not in actual_columns]
    extra = [c for c in actual_columns if c not in expected_columns]
    return {
        "missing_columns": missing,
        "extra_columns": extra,
        "schema_ok": len(missing) == 0,
    }


def check_negative_revenue(table: str) -> int:
    """Count rows with negative revenue."""
    df = run_query(f"SELECT COUNT(*) as cnt FROM {table} WHERE revenue < 0")
    return int(df["cnt"].iloc[0])


def run_all_checks() -> dict:
    """Run all data quality checks and return a full report."""
    report = {}

    # Null checks
    report["nulls_sales_clean"] = check_nulls(
        "sales_clean", ["order_id", "date", "product", "region", "revenue"]
    )

    # Duplicate checks
    report["duplicates_order_id"] = check_duplicates("sales_clean", "order_id")

    # Schema checks
    report["schema_sales_clean"] = check_schema(
        "sales_clean", ["order_id", "date", "product", "region", "revenue", "quantity"]
    )

    # Revenue sanity
    report["negative_revenue"] = check_negative_revenue("sales_clean")

    return report
