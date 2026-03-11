import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw dataframe: dedup, fix types, fill nulls."""
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    after = len(df)
    if before != after:
        print(f"[Transform] Removed {before - after} duplicate rows.")

    df["date"] = pd.to_datetime(df["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    print(f"[Transform] Clean data: {len(df)} rows.")
    return df


def create_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate clean data into a daily sales summary."""
    daily = (
        df.groupby("date")
        .agg(
            total_sales=("revenue", "sum"),
            total_orders=("order_id", "count"),
            avg_order_value=("revenue", "mean"),
        )
        .reset_index()
    )
    daily["date"] = daily["date"].astype(str)
    print(f"[Transform] Daily summary: {len(daily)} rows.")
    return daily


def create_product_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate clean data into a per-product sales summary."""
    product = (
        df.groupby("product")
        .agg(
            total_sales=("revenue", "sum"),
            total_orders=("order_id", "count"),
        )
        .reset_index()
    )
    print(f"[Transform] Product summary: {len(product)} rows.")
    return product


def create_region_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate clean data into a per-region sales summary."""
    region = (
        df.groupby("region")
        .agg(
            total_sales=("revenue", "sum"),
            total_orders=("order_id", "count"),
        )
        .reset_index()
    )
    print(f"[Transform] Region summary: {len(region)} rows.")
    return region
