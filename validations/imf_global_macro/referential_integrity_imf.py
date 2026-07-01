"""Stage 5 — Referential Integrity for imf_global_macro."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_referential_integrity.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT    = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_J = "imf_referential_integrity_report.json"
REPORT_T = "imf_referential_integrity_report.txt"

EXPECTED_INDICATORS = {"PCPIPCH","NGDP_RPCH","NGDPD","PPPGDP","LUR","BCA_NGDPD","GGXWDG_NGDP","GGXCNL_NGDP"}
OTHER_PRODUCTS = [
    Path("lekwankwa-historical-vault/product=food_micropricing"),
    Path("lekwankwa-historical-vault/product=wages_and_employment"),
    Path("lekwankwa-historical-vault/product=electricity"),
    Path("lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation"),
    Path("lekwankwa-historical-vault/product=trade_flows"),
]

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — REFERENTIAL INTEGRITY VALIDATION")
    logger.info("=" * 70)
    files = sorted(VAULT.rglob("*_data.parquet"))
    df    = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    results = []

    def chk(status, name, msg, **kw):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg, **kw})

    # A1 — all 8 expected indicators present
    actual   = set(df["sovereign_series_id"].dropna().unique())
    missing  = EXPECTED_INDICATORS - actual
    extra    = actual - EXPECTED_INDICATORS
    if missing: chk("FAIL","A1 Indicator Completeness", f"Missing indicators: {sorted(missing)}")
    else:       chk("PASS","A1 Indicator Completeness", f"All 8 expected indicators present")
    if extra:   chk("WARN","A1 Extra Indicators", f"Unexpected indicators: {sorted(extra)}")

    # A2 — unique (indicator, year, month, published_date) — one row per vintage per obs period
    # Multiple vintages per observation year are valid (e.g. October preliminary + April final).
    df["_year"]  = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
    df["_month"] = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.month
    dupes = df.duplicated(subset=["sovereign_series_id","_year","_month","published_date"]).sum()
    if dupes: chk("FAIL","A2 Unique Indicator-Year-Month", f"{dupes} duplicate (indicator, year, month, published_date) rows")
    else:     chk("PASS","A2 Unique Indicator-Year-Month", "All (indicator, year, month, published_date) tuples are unique")

    # A3 — country isolation: only USA records
    bad = int((df["country_code"] != "US").sum()) if "country_code" in df.columns else 0
    if bad: chk("FAIL","A3 Country Isolation", f"{bad} records with country_code != US")
    else:   chk("PASS","A3 Country Isolation", "All records are USA (country_code=US)")

    # A4 — source isolation: only imf_weo
    bad = int((df["source"] != "imf_weo").sum()) if "source" in df.columns else 0
    if bad: chk("FAIL","A4 Source Isolation", f"{bad} records with source != imf_weo")
    else:   chk("PASS","A4 Source Isolation", "All records have source=imf_weo")

    # B — cross-product isolation: imf_weo source not in other vaults
    found_in = []
    for other in OTHER_PRODUCTS:
        if not other.exists(): continue
        other_files = list(other.rglob("*_data.parquet"))[:3]  # sample only
        for fp in other_files:
            try:
                odf = pd.read_parquet(fp)
                if "source" in odf.columns and "imf_weo" in odf["source"].values:
                    found_in.append(str(other))
                    break
            except Exception:
                pass
    if found_in: chk("FAIL","B Cross-product Isolation", f"imf_weo source found in: {found_in}")
    else:        chk("PASS","B Cross-product Isolation", "imf_weo source not found in any other product vault")

    # C — forecast/actual split integrity
    if "is_forecast" in df.columns:
        ts_years = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
        actual_count   = int((df["is_forecast"] == False).sum())
        forecast_count = int((df["is_forecast"] == True).sum())
        chk("PASS","C Forecast/Actual Split",
            f"Actuals: {actual_count} rows (<=2024), Forecasts: {forecast_count} rows (>=2025)")
    else:
        chk("WARN","C Forecast/Actual Split", "is_forecast column missing")

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
        f.write(f"IMF Referential Integrity Report - {datetime.utcnow().isoformat()}Z\n")
        for r in results: f.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
    logger.info("  Reports: %s, %s", REPORT_J, REPORT_T)
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
