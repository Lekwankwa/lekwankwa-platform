"""Stage 7 — GX Universal Validation for imf_global_macro."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_gx_validation.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT    = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
CONFIG_J = Path("gx_config_global_macro.json")
REPORT_J = "imf_gx_validation_report.json"

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — GX UNIVERSAL VALIDATION")
    logger.info("=" * 70)

    cfg = json.loads(CONFIG_J.read_text(encoding="utf-8")) if CONFIG_J.exists() else {}
    cc  = cfg.get("critical_checks", {})

    files = sorted(VAULT.rglob("*_data.parquet"))
    df    = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info("  Total records: %d from %d files", len(df), len(files))

    results, passed, failed = [], 0, 0

    def chk(ok, name, msg):
        nonlocal passed, failed
        status = "PASS" if ok else "FAIL"
        if ok: passed += 1
        else:  failed += 1
        logger.info("  [%s] %s: %s", status, name, msg)
        results.append({"status": status, "check": name, "message": msg})

    # Row count
    rc = cc.get("row_count", {})
    chk(rc.get("min",0) <= len(df) <= rc.get("max",999999),
        "CHECK 1: Row count", f"{len(df):,} rows (expected {rc.get('min',0)}-{rc.get('max',999999)})")

    # Required columns
    req = cc.get("required_columns", [])
    miss = [c for c in req if c not in df.columns]
    chk(not miss, "CHECK 2: Required columns",
        f"All {len(req)} required columns present" if not miss else f"Missing: {miss}")

    # Non-null fields
    nulls_found = []
    for col in cc.get("non_null_fields", []):
        if col in df.columns and df[col].isna().any():
            nulls_found.append(col)
    chk(not nulls_found, "CHECK 3: No nulls in critical fields",
        "No nulls in critical fields" if not nulls_found else f"Nulls in: {nulls_found}")

    # Value constraints
    vc_issues = []
    for col, bounds in cc.get("value_constraints", {}).items():
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            oob = int(((vals < bounds["min"]) | (vals > bounds["max"])).sum())
            if oob: vc_issues.append(f"{col}: {oob} out of [{bounds['min']},{bounds['max']}]")
    chk(not vc_issues, "CHECK 4: Value constraints",
        "All observed_values in valid range" if not vc_issues else "; ".join(vc_issues))

    # Vocabulary checks
    for field, valid in cc.get("vocabulary_checks", {}).items():
        if field in df.columns:
            invalid = set(df[field].dropna().unique()) - set(valid)
            chk(not invalid, f"CHECK 5: Vocabulary {field}",
                f"All {field} values valid" if not invalid else f"Invalid: {invalid}")

    # Timestamp range
    tr = cc.get("timestamp_range", {})
    if tr and "data_timestamp" in df.columns:
        years = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year.dropna()
        bad = int(((years < tr["min_year"]) | (years > tr["max_year"])).sum())
        chk(not bad, f"CHECK 6: Timestamp range ({tr['min_year']}-{tr['max_year']})",
            f"All timestamps in range ({int(years.min())}-{int(years.max())})" if not bad
            else f"{bad} out of range")

    # Gold standard reference
    gs = cfg.get("gold_standards", {})
    gs_ok = all(Path(v).exists() for v in gs.values())
    chk(gs_ok, "CHECK 7: Gold standard files exist",
        f"All {len(gs)} gold standard files accessible" if gs_ok else "Some gold standard files missing")

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info("=" * 70)
    logger.info("VALIDATION COMPLETE: [%s] | %d/%d checks passed", overall, passed, passed + failed)
    logger.info("=" * 70)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "overall": overall,
              "passed": passed, "failed": failed, "checks": results}
    with open(REPORT_J, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
    logger.info("  Report: %s", REPORT_J)
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
