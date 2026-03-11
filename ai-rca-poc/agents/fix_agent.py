import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI

from services.db_service import execute, run_query

load_dotenv()


# ─── Rule-based fixes ────────────────────────────────────────────────────────

def fix_duplicates() -> str:
    """Remove duplicate order_ids, keeping the first occurrence."""
    execute(
        """
        DELETE FROM sales_clean
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM sales_clean
            GROUP BY order_id
        )
        """
    )
    return "✅ Fix applied: Duplicate order_ids removed from sales_clean."


def fix_null_revenue() -> str:
    """Set NULL or negative revenue to 0."""
    execute(
        """
        UPDATE sales_clean
        SET revenue = 0
        WHERE revenue IS NULL OR revenue < 0
        """
    )
    return "✅ Fix applied: NULL/negative revenue values set to 0."


def rebuild_daily_summary() -> str:
    """Recompute and overwrite the daily_sales_summary table."""
    execute("DELETE FROM daily_sales_summary")
    execute(
        """
        INSERT INTO daily_sales_summary (date, total_sales, total_orders, avg_order_value)
        SELECT
            date,
            SUM(revenue)        AS total_sales,
            COUNT(order_id)     AS total_orders,
            AVG(revenue)        AS avg_order_value
        FROM sales_clean
        GROUP BY date
        """
    )
    return "✅ Fix applied: daily_sales_summary rebuilt from sales_clean."


# ─── LLM-suggested fix ───────────────────────────────────────────────────────

def suggest_fix_with_llm(rca_report: str) -> str:
    """Ask the LLM to generate a SQL fix based on the RCA report."""
    client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_ENDPOINT"),
        api_key=os.getenv("AZURE_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
    )
    deployment = os.getenv("AZURE_DEPLOYMENT", "gpt-4o-mini")

    system_prompt = """You are a senior data engineer.
Based on the provided RCA report, generate the exact SQL fix statements needed.
Return ONLY a JSON object with keys:
- "fixes": list of {"description": str, "sql": str}
- "explanation": str (brief summary of what will be fixed)"""

    user_prompt = f"""RCA Report:
{rca_report}

Tables available:
- sales_clean (order_id, date, product, region, revenue, quantity)
- daily_sales_summary (date, total_sales, total_orders, avg_order_value)

Generate SQL fix statements."""

    print("[Fix Agent] Requesting LLM-generated SQL fixes...")
    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=800,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content
    return content


def apply_rule_based_fixes(findings: dict) -> list[str]:
    """Apply standard rule-based fixes based on investigation findings."""
    applied = []

    if findings.get("duplicate_order_ids", 0) > 0:
        msg = fix_duplicates()
        applied.append(msg)

    if findings.get("zero_or_null_revenue", 0) > 0:
        msg = fix_null_revenue()
        applied.append(msg)

    # Always rebuild daily summary after any fix
    if applied:
        msg = rebuild_daily_summary()
        applied.append(msg)

    if not applied:
        applied.append("ℹ️  No automatic fixes needed based on rule-based checks.")

    return applied
