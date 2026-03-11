# AI RCA POC — Sales Anomaly Detection System

An end-to-end proof of concept for AI-powered Root Cause Analysis on sales data, using Python, SQLite, Streamlit, and Azure OpenAI.

---

## Architecture

```
Sales CSV → ETL Pipeline → SQLite DB → Streamlit Dashboard
                                              ↓
                                   Threshold Monitor Agent
                                              ↓
                                   Investigation Agent (LLM)
                                              ↓
                                   Fix Agent (Rule-based + LLM)
                                              ↓
                                   Root Cause Report + SQL Fixes
```

---

## Project Structure

```
ai-rca-poc/
├── data/               → Raw CSV data
├── database/           → SQLite database (auto-created)
├── pipeline/           → ETL: ingest, transform, load
├── agents/             → Monitor, Investigation, Fix agents
├── services/           → DB service, Validator, Orchestrator
├── dashboard/          → Streamlit app
├── configs/            → Threshold config (YAML)
├── .env                → Azure OpenAI credentials
├── requirements.txt
└── main.py             → Entry point
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Azure OpenAI
Edit `.env`:
```
AZURE_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_API_KEY=your-api-key-here
AZURE_API_VERSION=2024-12-01-preview
AZURE_DEPLOYMENT=gpt-4o-mini
```

### 3. Run the ETL pipeline
```bash
python main.py --etl
```

### 4. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

### 5. Run RCA (with LLM)
```bash
python main.py --rca
```

### 6. Run RCA (without LLM / rule-based only)
```bash
python main.py --rca --no-llm
```

---

## Threshold Configuration

Edit `configs/thresholds.yaml`:
```yaml
daily_sales_min: 1000     # Alert if daily sales drop below this
daily_orders_min: 2       # Alert if order count drops below this
revenue_drop_pct: 50      # Alert if revenue drops by this % vs prior day
```

---

## Testing Anomaly Detection

In the Streamlit dashboard, use the **Simulate Anomaly** section:

- **Inject Duplicate Rows** → Simulates a pipeline running twice (inflated revenue)
- **Inject Low Sales Day** → Simulates a revenue drop below threshold

Then click **Run RCA Now** to see the AI investigate and fix the issue.

---

## RCA Flow

1. **Monitor Agent** checks latest day's sales vs threshold
2. **Investigation Agent** collects diagnostics (row counts, nulls, duplicates, trends)
3. **Azure OpenAI LLM** analyzes data and writes an RCA report
4. **Fix Agent** applies rule-based SQL fixes (deduplicate, fix nulls, rebuild summary)
5. **LLM Fix Suggestions** generates additional SQL patches based on the RCA report

---

## Key Files

| File | Purpose |
|------|---------|
| `agents/monitor_agent.py` | Threshold checks |
| `agents/investigation_agent.py` | LLM + rule-based diagnostics |
| `agents/fix_agent.py` | SQL fixes |
| `services/orchestrator.py` | Full RCA pipeline |
| `services/validator.py` | Data quality checks |
| `dashboard/app.py` | Streamlit UI |
