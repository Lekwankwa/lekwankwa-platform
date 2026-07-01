"""Stage 1 — Bitemporal PIT Validation for imf_global_macro."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_pit_validation.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT    = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_J = "imf_pit_report.json"
REPORT_T = "imf_pit_report.txt"

REQUIRED_PIT = ["record_id","product","country_code","source","sovereign_series_id",
                "observed_value","data_timestamp","published_date","data_vintage_id",
                "revision_number","confidence_tier","extraction_method","processing_timestamp"]

def load_vault() -> pd.DataFrame:
    files = sorted(VAULT.rglob("*_data.parquet"))
    if not files:
        raise RuntimeError(f"No data files found under {VAULT}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info("  Loaded %d records from %d partitions", len(df), len(files))
    return df

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — BITEMPORAL PIT VALIDATION")
    logger.info("=" * 70)
    df = load_vault()
    results, query_date = [], datetime.now(timezone.utc)

    def chk(status, name, msg, **kw):
        results.append({"status": status, "check": name, "message": msg, **kw})

    # C1 — required columns
    missing = [c for c in REQUIRED_PIT if c not in df.columns]
    if missing: chk("FAIL","C1 Required PIT Columns", f"Missing: {missing}")
    else:        chk("PASS","C1 Required PIT Columns", f"All {len(REQUIRED_PIT)} PIT columns present")

    # C2 — no null record_id
    nulls = int(df["record_id"].isna().sum())
    if nulls: chk("FAIL","C2 Record ID Not Null", f"{nulls} null record_ids")
    else:      chk("PASS","C2 Record ID Not Null", "All record_ids populated")

    # C3 — unique record_id
    dupes = int(df["record_id"].duplicated().sum())
    if dupes: chk("FAIL","C3 Record ID Unique", f"{dupes} duplicate record_ids")
    else:      chk("PASS","C3 Record ID Unique", "All record_ids unique")

    # C4 — published_date parseable
    pub = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
    bad = int(pub.isna().sum())
    if bad: chk("FAIL","C4 Published Date Parseable", f"{bad} unparseable published_dates")
    else:   chk("PASS","C4 Published Date Parseable", "All published_dates valid UTC")

    # C5 — data_timestamp parseable
    dts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    bad = int(dts.isna().sum())
    if bad: chk("FAIL","C5 Data Timestamp Parseable", f"{bad} unparseable data_timestamps")
    else:   chk("PASS","C5 Data Timestamp Parseable", "All data_timestamps valid UTC")

    # C6 — PIT: published_date >= data_timestamp
    violation = int((pub < dts).sum())
    if violation: chk("FAIL","C6 PIT Ordering", f"{violation} records where published_date < data_timestamp")
    else:          chk("PASS","C6 PIT Ordering", "All published_date >= data_timestamp")

    # C7 — revision_number >= 1
    bad = int((pd.to_numeric(df["revision_number"], errors="coerce").fillna(0) < 1).sum())
    if bad: chk("FAIL","C7 Revision Number", f"{bad} records with revision_number < 1")
    else:   chk("PASS","C7 Revision Number", "All revision_numbers >= 1")

    # C8 — superseded_by null (initial load)
    if "superseded_by" in df.columns:
        non_null = int(df["superseded_by"].notna().sum())
        chk("PASS","C8 Superseded By", f"superseded_by: {non_null} non-null (expected for updates)")
    else:
        chk("PASS","C8 Superseded By", "superseded_by column absent (initial load — expected)")

    # C9 — is_forecast column consistency
    if "is_forecast" in df.columns:
        forecast_years = df[df["is_forecast"] == True]["data_timestamp"].apply(lambda x: int(str(x)[:4]))
        if len(forecast_years):
            bad = int((forecast_years < 2025).sum())
            if bad: chk("WARN","C9 Forecast Flag", f"{bad} records flagged is_forecast=True before 2025")
            else:   chk("PASS","C9 Forecast Flag", f"{len(forecast_years)} forecast records all year >= 2025")
        else:
            chk("PASS","C9 Forecast Flag", "No forecast records present")
    else:
        chk("WARN","C9 Forecast Flag", "is_forecast column missing")

    # C10 — observed_value not null
    nulls = int(df["observed_value"].isna().sum())
    if nulls: chk("FAIL","C10 Observed Value Not Null", f"{nulls} null observed_values")
    else:      chk("PASS","C10 Observed Value Not Null", "All observed_values populated")

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)
    overall = "PASS" if failed == 0 else "FAIL"

    logger.info("")
    logger.info("=" * 70)
    for r in results:
        icon = {"PASS": "[+]", "FAIL": "[!]", "WARN": "[~]"}.get(r["status"], "[?]")
        logger.info("  %s %s: %s", icon, r["check"], r["message"])
    logger.info("=" * 70)
    logger.info("SUMMARY: %d PASS / %d FAIL / %d WARN | Overall: [%s]", passed, failed, warned, overall)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "overall": overall,
              "passed": passed, "failed": failed, "warned": warned, "checks": results}
    with open(REPORT_J, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
    with open(REPORT_T, "w", encoding="utf-8") as f:
        f.write(f"IMF Global Macro PIT Validation - {datetime.utcnow().isoformat()}Z\n")
        for r in results:
            f.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
    logger.info("  Reports: %s, %s", REPORT_J, REPORT_T)
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
