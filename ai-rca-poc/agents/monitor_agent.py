import yaml
from services.db_service import run_query


def load_thresholds(path: str = "configs/thresholds.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def check_sales() -> tuple[bool, float, dict]:
    """
    Check latest daily sales against threshold.
    Returns (alert_triggered, latest_value, context_dict).
    """
    thresholds = load_thresholds()
    min_sales = thresholds.get("daily_sales_min", 1000)

    latest_df = run_query(
        """
        SELECT date, total_sales, total_orders, avg_order_value
        FROM daily_sales_summary
        ORDER BY date DESC
        LIMIT 1
        """
    )

    if latest_df.empty:
        return True, 0, {"error": "No data found in daily_sales_summary"}

    row = latest_df.iloc[0]
    value = float(row["total_sales"])
    alert = value < min_sales

    context = {
        "date": str(row["date"]),
        "total_sales": value,
        "total_orders": int(row["total_orders"]),
        "avg_order_value": float(row["avg_order_value"]),
        "threshold": min_sales,
    }

    if alert:
        print(f"[Monitor] ⚠️  ALERT: Sales {value} below threshold {min_sales} on {row['date']}")
    else:
        print(f"[Monitor] ✅ Sales {value} OK on {row['date']}")

    return alert, value, context


def get_sales_trend(days: int = 7) -> list[dict]:
    """Fetch last N days of sales for trend analysis."""
    df = run_query(
        f"""
        SELECT date, total_sales, total_orders
        FROM daily_sales_summary
        ORDER BY date DESC
        LIMIT {days}
        """
    )
    return df.to_dict(orient="records")
