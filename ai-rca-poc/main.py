"""
main.py — Entry point for the AI RCA POC.

Usage:
    python main.py           → Run ETL pipeline + RCA check
    python main.py --etl     → Run ETL pipeline only
    python main.py --rca     → Run RCA check only
    python main.py --no-llm  → Run RCA without LLM
"""

import argparse
import sys

from pipeline.ingest import load_raw_data
from pipeline.transform import (
    clean_data,
    create_daily_summary,
    create_product_summary,
    create_region_summary,
)
from pipeline.load import load_to_db
from services.orchestrator import run_rca


def run_pipeline():
    print("\n── ETL PIPELINE ─────────────────────────────────────────")
    df = load_raw_data("data/sales_raw.csv")
    df_clean = clean_data(df)
    daily = create_daily_summary(df_clean)
    product = create_product_summary(df_clean)
    region = create_region_summary(df_clean)

    load_to_db(df_clean, "sales_clean")
    load_to_db(daily, "daily_sales_summary")
    load_to_db(product, "product_summary")
    load_to_db(region, "region_summary")
    print("── Pipeline Complete ─────────────────────────────────────\n")


def main():
    parser = argparse.ArgumentParser(description="AI RCA POC")
    parser.add_argument("--etl", action="store_true", help="Run ETL pipeline only")
    parser.add_argument("--rca", action="store_true", help="Run RCA only")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM in RCA")
    args = parser.parse_args()

    use_llm = not args.no_llm

    if args.etl:
        run_pipeline()
    elif args.rca:
        run_rca(use_llm=use_llm)
    else:
        # Default: run both
        run_pipeline()
        run_rca(use_llm=use_llm)


if __name__ == "__main__":
    main()
