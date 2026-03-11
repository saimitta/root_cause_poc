import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI

from services.db_service import run_query, get_table_info, get_all_tables
from services.validator import run_all_checks
from agents.monitor_agent import get_sales_trend

load_dotenv()


def _get_azure_client() -> AzureOpenAI:
    return AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_ENDPOINT"),
        api_key=os.getenv("AZURE_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
    )


def collect_diagnostic_data(alert_context: dict) -> dict:
    """Collect all diagnostic data needed for LLM investigation."""
    diagnostics = {}

    # Table metadata
    tables = get_all_tables()
    diagnostics["tables"] = tables
    diagnostics["table_info"] = {t: get_table_info(t) for t in tables}

    # Data quality checks
    diagnostics["quality_checks"] = run_all_checks()

    # Sales trend (last 7 days)
    diagnostics["sales_trend"] = get_sales_trend(days=7)

    # Region breakdown for latest date
    if alert_context.get("date"):
        region_df = run_query(
            f"""
            SELECT region, SUM(revenue) as revenue, COUNT(order_id) as orders
            FROM sales_clean
            WHERE date = '{alert_context["date"]}'
            GROUP BY region
            """
        )
        diagnostics["region_breakdown"] = region_df.to_dict(orient="records")

        # Product breakdown for latest date
        product_df = run_query(
            f"""
            SELECT product, SUM(revenue) as revenue, COUNT(order_id) as orders
            FROM sales_clean
            WHERE date = '{alert_context["date"]}'
            GROUP BY product
            """
        )
        diagnostics["product_breakdown"] = product_df.to_dict(orient="records")

    # Null and duplicate counts
    diagnostics["alert_context"] = alert_context

    return diagnostics


def investigate_with_llm(alert_context: dict) -> str:
    """
    Send diagnostic data to Azure OpenAI and return an RCA report.
    """
    client = _get_azure_client()
    deployment = os.getenv("AZURE_DEPLOYMENT", "gpt-4o-mini")

    diagnostics = collect_diagnostic_data(alert_context)

    system_prompt = """You are a senior data engineer performing root cause analysis (RCA) on a sales data pipeline.
Your job is to:
1. Identify the most likely root cause of the anomaly.
2. Explain clearly what the data shows.
3. Suggest concrete fixes (SQL patches, pipeline changes, or data corrections).
4. Rate severity: LOW / MEDIUM / HIGH.

Be concise and structured. Use sections: SUMMARY, ROOT CAUSE, EVIDENCE, RECOMMENDED FIXES, SEVERITY."""

    user_prompt = f"""A sales threshold alert has been triggered. Investigate and provide a root cause analysis.

ALERT CONTEXT:
{json.dumps(alert_context, indent=2)}

DIAGNOSTIC DATA:
{json.dumps(diagnostics, indent=2, default=str)}

Tables available: sales_clean, daily_sales_summary, product_summary, region_summary

Please provide a detailed RCA report."""

    print("[Investigation] Sending data to LLM for analysis...")

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=1500,
    )

    report = response.choices[0].message.content
    print("[Investigation] LLM analysis complete.")
    return report


def investigate_basic(alert_context: dict) -> dict:
    """
    Fallback rule-based investigation (no LLM required).
    Returns a structured dict of findings.
    """
    checks = {}

    # Row counts
    raw_count = run_query("SELECT COUNT(*) as c FROM sales_clean")
    daily_count = run_query("SELECT COUNT(*) as c FROM daily_sales_summary")
    checks["raw_rows"] = int(raw_count["c"].iloc[0])
    checks["daily_rows"] = int(daily_count["c"].iloc[0])

    # Duplicates
    dup_df = run_query(
        """
        SELECT COUNT(*) as dup_count FROM (
            SELECT order_id, COUNT(*) as cnt
            FROM sales_clean GROUP BY order_id HAVING cnt > 1
        )
        """
    )
    checks["duplicate_order_ids"] = int(dup_df["dup_count"].iloc[0])

    # Nulls
    null_df = run_query("SELECT COUNT(*) as c FROM sales_clean WHERE revenue IS NULL OR revenue = 0")
    checks["zero_or_null_revenue"] = int(null_df["c"].iloc[0])

    # Sales trend
    checks["sales_trend"] = get_sales_trend(days=5)

    # Diagnosis
    if checks["duplicate_order_ids"] > 0:
        checks["diagnosis"] = "Duplicate order_ids detected — revenue may be inflated or data pipeline ran twice."
    elif checks["zero_or_null_revenue"] > 0:
        checks["diagnosis"] = "Zero or null revenue rows detected — missing data in pipeline."
    elif checks["raw_rows"] < 5:
        checks["diagnosis"] = "Very few rows in sales_clean — possible ingestion failure."
    else:
        checks["diagnosis"] = "No obvious structural issues found. Check upstream data source."

    return checks
