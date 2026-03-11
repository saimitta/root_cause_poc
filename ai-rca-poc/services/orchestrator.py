import json
from datetime import datetime

from agents.monitor_agent import check_sales
from agents.investigation_agent import investigate_with_llm, investigate_basic
from agents.fix_agent import apply_rule_based_fixes, suggest_fix_with_llm


def run_rca(use_llm: bool = True) -> dict:
    """
    Full RCA orchestration pipeline.

    Steps:
    1. Monitor: check if alert is triggered
    2. Investigate: collect diagnostics + optionally use LLM
    3. Fix: apply rule-based fixes + optionally ask LLM for SQL patches
    4. Return full RCA report
    """
    print("\n" + "=" * 60)
    print("   RCA ORCHESTRATOR STARTED")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ── Step 1: Monitor ──────────────────────────────────────────
    alert, value, alert_context = check_sales()

    if not alert:
        print("\n✅ System Normal. No RCA needed.")
        return {
            "status": "NORMAL",
            "value": value,
            "alert_context": alert_context,
        }

    print(f"\n⚠️  Alert triggered! Sales value: {value}")

    # ── Step 2: Investigate ──────────────────────────────────────
    print("\n[Step 2] Running investigation...")
    basic_findings = investigate_basic(alert_context)
    print(f"   Diagnosis: {basic_findings.get('diagnosis')}")

    llm_report = None
    if use_llm:
        try:
            llm_report = investigate_with_llm(alert_context)
            print("\n── LLM RCA REPORT ──────────────────────────────────────")
            print(llm_report)
            print("────────────────────────────────────────────────────────\n")
        except Exception as e:
            print(f"[Investigation] LLM call failed: {e}. Using rule-based only.")

    # ── Step 3: Fix ──────────────────────────────────────────────
    print("\n[Step 3] Applying fixes...")
    applied_fixes = apply_rule_based_fixes(basic_findings)
    for fix in applied_fixes:
        print(f"   {fix}")

    llm_fix_suggestions = None
    if use_llm and llm_report:
        try:
            llm_fix_suggestions = suggest_fix_with_llm(llm_report)
            print("\n── LLM FIX SUGGESTIONS ─────────────────────────────────")
            suggestions = json.loads(llm_fix_suggestions)
            print(f"   {suggestions.get('explanation', '')}")
            for fix in suggestions.get("fixes", []):
                print(f"   [{fix['description']}]")
                print(f"   SQL: {fix['sql']}")
            print("────────────────────────────────────────────────────────\n")
        except Exception as e:
            print(f"[Fix Agent] LLM fix suggestion failed: {e}")

    print("=" * 60)
    print("   RCA ORCHESTRATOR COMPLETE")
    print("=" * 60 + "\n")

    return {
        "status": "ALERT_RESOLVED",
        "alert_context": alert_context,
        "basic_findings": basic_findings,
        "llm_rca_report": llm_report,
        "applied_fixes": applied_fixes,
        "llm_fix_suggestions": llm_fix_suggestions,
    }
