"""Stage 2 — Sanity Checks for imf_global_macro vault."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_sanity_check.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT     = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_T  = "imf_sanity_check_report.txt"
REPORT_J  = "imf_sanity_check_failures.json"

EXPECTED_INDICATORS = {"PCPIPCH","NGDP_RPCH","NGDPD","PPPGDP","LUR","BCA_NGDPD","GGXWDG_NGDP","GGXCNL_NGDP"}
REQUIRED_COLS       = ["record_id","product","country_code","source","sovereign_series_id",
                        "observed_value","data_timestamp","published_date","data_vintage_id",
                        "confidence_tier","extraction_method","is_forecast"]
VALUE_BOUNDS = {
    "PCPIPCH":    (-5.0,  50.0),
    "NGDP_RPCH":  (-20.0, 20.0),
    "LUR":        (0.0,   30.0),
    "NGDPD":      (100.0, 40000.0),
    "PPPGDP":     (100.0, 40000.0),
    "BCA_NGDPD":  (-20.0, 20.0),
    "GGXWDG_NGDP":(0.0,   200.0),
    "GGXCNL_NGDP":(-30.0, 10.0),
}

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — VAULT SANITY CHECK")
    logger.info("=" * 70)
    logger.info("Run timestamp: %s", datetime.now(timezone.utc).isoformat())

    files   = sorted(VAULT.rglob("*_data.parquet"))
    failures, warnings = [], []
    total_rows = 0

    for fp in files:
        df = pd.read_parquet(fp)
        total_rows += len(df)
        rel = str(fp.relative_to(VAULT))

        # Required columns
        miss = [c for c in REQUIRED_COLS if c not in df.columns]
        if miss: failures.append({"file": rel, "issue": "missing_columns", "detail": miss})

        # No nulls in critical fields
        for col in ["record_id","observed_value","data_timestamp","sovereign_series_id"]:
            if col in df.columns and df[col].isna().any():
                failures.append({"file": rel, "issue": f"nulls_in_{col}", "detail": int(df[col].isna().sum())})

        # Value bounds per indicator
        for ind, (lo, hi) in VALUE_BOUNDS.items():
            sub = df[df["sovereign_series_id"] == ind]["observed_value"] if "sovereign_series_id" in df.columns else pd.Series(dtype=float)
            sub = pd.to_numeric(sub, errors="coerce").dropna()
            oob = int(((sub < lo) | (sub > hi)).sum())
            if oob:
                warnings.append({"file": rel, "indicator": ind, "issue": "out_of_bounds", "count": oob, "bounds": [lo, hi]})

    # Dataset-level checks
    df_all = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()

    missing_ind = EXPECTED_INDICATORS - set(df_all.get("sovereign_series_id", pd.Series()).unique())
    if missing_ind:
        failures.append({"issue": "missing_indicators", "detail": sorted(missing_ind)})

    year_range = sorted(df_all["data_timestamp"].apply(lambda x: int(str(x)[:4])).unique()) if len(df_all) else []
    forecast_count = int(df_all.get("is_forecast", pd.Series(False)).sum()) if len(df_all) else 0

    logger.info("  Files   : %d", len(files))
    logger.info("  Rows    : %d", total_rows)
    logger.info("  Failures: %d", len(failures))
    logger.info("  Warnings: %d", len(warnings))
    logger.info("  Year range: %s — %s", year_range[0] if year_range else "?", year_range[-1] if year_range else "?")
    logger.info("  Indicators: %d / %d", len(EXPECTED_INDICATORS) - len(missing_ind), len(EXPECTED_INDICATORS))
    logger.info("  Forecast rows: %d", forecast_count)

    overall = "PASS" if not failures else "FAIL"
    logger.info("=" * 70)
    logger.info("SUMMARY — Files: %d | Rows: %d | Failures: %d | Warnings: %d | [%s]",
                len(files), total_rows, len(failures), len(warnings), overall)
    logger.info("=" * 70)

    with open(REPORT_T, "w", encoding="utf-8") as f:
        f.write(f"IMF Global Macro Sanity Check - {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Files: {len(files)} | Rows: {total_rows} | Overall: {overall}\n")
    with open(REPORT_J, "w", encoding="utf-8") as f:
        json.dump({"failures": failures, "warnings": warnings}, f, indent=2)
    logger.info("  Reports: %s, %s", REPORT_T, REPORT_J)
    return not failures

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
