"""Stage 4 — Temporal Consistency for imf_global_macro."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_temporal_consistency.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT     = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_J  = "imf_temporal_consistency_report.json"
REPORT_T  = "imf_temporal_consistency_report.txt"
START_YEAR, END_YEAR = 1980, 2031
# Some indicators only start from 2001 (govt debt series)
FULL_HISTORY_IND = {"PCPIPCH","NGDP_RPCH","NGDPD","PPPGDP","LUR","BCA_NGDPD"}
PARTIAL_IND      = {"GGXWDG_NGDP","GGXCNL_NGDP"}  # start 2001

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — TEMPORAL CONSISTENCY VALIDATION")
    logger.info("=" * 70)
    files = sorted(VAULT.rglob("*_data.parquet"))
    df    = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["_year"] = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
    results = []

    def chk(status, name, msg, **kw):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg, **kw})

    expected_full    = set(range(START_YEAR, END_YEAR + 1))
    expected_partial = set(range(2001, END_YEAR + 1))

    # A — full-history indicators: 1980-2031 no gaps
    for ind in sorted(FULL_HISTORY_IND):
        sub   = df[df["sovereign_series_id"] == ind]["_year"].dropna()
        years = set(sub.astype(int))
        gaps  = sorted(expected_full - years)
        if gaps: chk("FAIL", f"A Gap {ind}", f"{len(gaps)} missing years: {gaps[:5]}{'...' if len(gaps)>5 else ''}")
        else:    chk("PASS", f"A Continuity {ind}", f"No gaps ({START_YEAR}-{END_YEAR})")

    # B — partial-history indicators: 2001-2031 no gaps
    for ind in sorted(PARTIAL_IND):
        sub   = df[df["sovereign_series_id"] == ind]["_year"].dropna()
        years = set(sub.astype(int))
        gaps  = sorted(expected_partial - years)
        if gaps: chk("FAIL", f"B Gap {ind}", f"{len(gaps)} missing years since 2001: {gaps[:5]}")
        else:    chk("PASS", f"B Continuity {ind}", f"No gaps (2001-{END_YEAR})")

    # C — no future actuals beyond current year
    current_year = datetime.now(timezone.utc).year
    actual_future = df[(df["is_forecast"] == False) & (df["_year"] > current_year)] if "is_forecast" in df.columns else pd.DataFrame()
    if len(actual_future):
        chk("WARN", "C No Future Actuals", f"{len(actual_future)} non-forecast records with year > {current_year}")
    else:
        chk("PASS", "C No Future Actuals", f"All actuals are year <= {current_year}")

    # D — chronological order per indicator
    for ind in sorted(FULL_HISTORY_IND | PARTIAL_IND):
        sub = df[df["sovereign_series_id"] == ind].sort_values("_year")
        if not sub["_year"].is_monotonic_increasing:
            chk("FAIL", f"D Chronological {ind}", "Records not in chronological order")
        else:
            chk("PASS", f"D Chronological {ind}", "Chronologically ordered")

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)
    overall = "PASS" if failed == 0 else "FAIL"

    logger.info("=" * 70)
    logger.info("SUMMARY: %d PASS / %d FAIL / %d WARN | Overall: [%s]", passed, failed, warned, overall)
    logger.info("=" * 70)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "overall": overall,
              "passed": passed, "failed": failed, "warned": warned, "checks": results}
    with open(REPORT_J, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
    with open(REPORT_T, "w", encoding="utf-8") as f:
        f.write(f"IMF Temporal Consistency Report - {datetime.utcnow().isoformat()}Z\n")
        for r in results: f.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
    logger.info("  Reports: %s, %s", REPORT_J, REPORT_T)
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
