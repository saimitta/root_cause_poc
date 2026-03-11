import sys
import os
import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.db_service import run_query
from agents.monitor_agent import load_thresholds
from services.orchestrator import run_rca

# ── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sales RCA Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Sales RCA Dashboard")
st.caption("Threshold monitoring + AI-powered Root Cause Analysis")

# ── DB ready check ───────────────────────────────────────────────────────────
def is_db_ready() -> bool:
    try:
        tables = run_query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )["name"].tolist()
        return "daily_sales_summary" in tables and "sales_clean" in tables
    except Exception:
        return False

# ── Show setup screen if DB not ready ────────────────────────────────────────
if not is_db_ready():
    st.warning("⚠️ Database not initialised yet. Run the ETL pipeline first.")
    st.code("python main.py --etl", language="bash")
    if st.button("▶️ Run ETL Pipeline Now"):
        import subprocess, sys
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        with st.spinner("Running ETL pipeline…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--etl"],
                capture_output=True, text=True, cwd=project_root
            )
        if result.returncode == 0:
            st.success("✅ Pipeline complete! Refreshing…")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Pipeline failed:")
            st.code(result.stderr or result.stdout)
    st.stop()

# ── Load Data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def get_daily_sales():
    return run_query("SELECT * FROM daily_sales_summary ORDER BY date")

@st.cache_data(ttl=30)
def get_sales_clean():
    return run_query("SELECT * FROM sales_clean")

thresholds = load_thresholds()
THRESHOLD = thresholds.get("daily_sales_min", 1000)

daily_df = get_daily_sales()
sales_df = get_sales_clean()

# ── KPI Row ──────────────────────────────────────────────────────────────────
latest = daily_df.iloc[-1] if not daily_df.empty else None

col1, col2, col3, col4 = st.columns(4)

if latest is not None:
    latest_sales = float(latest["total_sales"])
    col1.metric(
        "Latest Day Sales",
        f"${latest_sales:,.0f}",
        delta=f"Threshold: ${THRESHOLD:,}",
        delta_color="normal" if latest_sales >= THRESHOLD else "inverse",
    )
    col2.metric("Total Orders (Latest)", int(latest["total_orders"]))
    col3.metric("Avg Order Value (Latest)", f"${float(latest['avg_order_value']):,.0f}")
    col4.metric("Total Days Tracked", len(daily_df))

# ── Alert Banner ─────────────────────────────────────────────────────────────
if latest is not None:
    if float(latest["total_sales"]) < THRESHOLD:
        st.error(
            f"⚠️ ALERT: Latest sales (${float(latest['total_sales']):,.0f}) is below threshold (${THRESHOLD:,}). "
            "Trigger RCA below."
        )
    else:
        st.success(f"✅ Sales are within normal range (above ${THRESHOLD:,} threshold).")

st.divider()

# ── Daily Sales Chart ────────────────────────────────────────────────────────
st.subheader("📈 Daily Sales Trend")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=daily_df["date"],
    y=daily_df["total_sales"],
    mode="lines+markers",
    name="Total Sales",
    line=dict(color="#4F8EF7", width=2),
    marker=dict(size=6),
))
fig.add_hline(
    y=THRESHOLD,
    line_dash="dash",
    line_color="red",
    annotation_text=f"Threshold: ${THRESHOLD:,}",
    annotation_position="bottom right",
)
fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Revenue ($)",
    hovermode="x unified",
    height=350,
)
st.plotly_chart(fig, use_container_width=True)

# ── Breakdown Charts ─────────────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("🛍️ Revenue by Product")
    product_df = sales_df.groupby("product")["revenue"].sum().reset_index()
    fig2 = px.bar(product_df, x="product", y="revenue", color="product", height=300)
    st.plotly_chart(fig2, use_container_width=True)

with col_b:
    st.subheader("🗺️ Revenue by Region")
    region_df = sales_df.groupby("region")["revenue"].sum().reset_index()
    fig3 = px.pie(region_df, values="revenue", names="region", height=300)
    st.plotly_chart(fig3, use_container_width=True)

# ── Raw Data Tables ──────────────────────────────────────────────────────────
with st.expander("🔍 View Daily Summary Table"):
    st.dataframe(daily_df, use_container_width=True)

with st.expander("🔍 View Sales Clean Table"):
    st.dataframe(sales_df, use_container_width=True)

st.divider()

# ── RCA Trigger ──────────────────────────────────────────────────────────────
st.subheader("🤖 AI Root Cause Analysis")

use_llm = st.toggle("Use Azure OpenAI LLM for deep analysis", value=True)

if st.button("🚀 Run RCA Now", type="primary"):
    with st.spinner("Running RCA pipeline..."):
        result = run_rca(use_llm=use_llm)

    if result["status"] == "NORMAL":
        st.success("✅ System is normal. No alert triggered.")
    else:
        st.warning("⚠️ Alert was triggered. RCA results below:")

        st.subheader("📋 Alert Context")
        st.json(result["alert_context"])

        st.subheader("🔬 Basic Findings")
        findings = result["basic_findings"]
        st.json(findings)

        if result.get("llm_rca_report"):
            st.subheader("🧠 LLM Root Cause Analysis Report")
            st.markdown(result["llm_rca_report"])

        st.subheader("🔧 Applied Fixes")
        for fix in result.get("applied_fixes", []):
            st.markdown(f"- {fix}")

        if result.get("llm_fix_suggestions"):
            import json
            st.subheader("💡 LLM Fix Suggestions")
            try:
                suggestions = json.loads(result["llm_fix_suggestions"])
                st.markdown(f"**{suggestions.get('explanation', '')}**")
                for fix in suggestions.get("fixes", []):
                    st.markdown(f"**{fix['description']}**")
                    st.code(fix["sql"], language="sql")
            except Exception:
                st.text(result["llm_fix_suggestions"])

        # Refresh charts after fix
        st.info("🔄 Refresh the page to see updated charts after fixes.")

# ── Simulate Anomaly ─────────────────────────────────────────────────────────
st.divider()
st.subheader("🧪 Simulate Anomaly (for testing)")

col_sim1, col_sim2 = st.columns(2)

with col_sim1:
    if st.button("➕ Inject Duplicate Rows"):
        from services.db_service import execute
        execute("""
            INSERT INTO sales_clean (order_id, date, product, region, revenue, quantity)
            SELECT order_id, date, product, region, revenue, quantity
            FROM sales_clean LIMIT 5
        """)
        execute("""
            INSERT INTO daily_sales_summary (date, total_sales, total_orders, avg_order_value)
            SELECT date, total_sales * 2, total_orders, avg_order_value
            FROM daily_sales_summary ORDER BY date DESC LIMIT 1
        """)
        st.success("Duplicate rows injected! Run RCA to detect and fix.")
        st.cache_data.clear()

with col_sim2:
    if st.button("📉 Inject Low Sales Day"):
        from services.db_service import execute
        execute("""
            INSERT INTO daily_sales_summary (date, total_sales, total_orders, avg_order_value)
            VALUES ('2024-02-01', 50, 1, 50)
        """)
        st.success("Low sales day injected! Run RCA to investigate.")
        st.cache_data.clear()
