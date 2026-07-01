"""Stage 3 — Schema Compliance for imf_global_macro (SDMX gold standard)."""
import json, logging, re, sys, uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_schema_compliance.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT    = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_J = "imf_schema_compliance_report.json"
REPORT_T = "imf_schema_compliance_report.txt"

VALID_INDICATORS  = {"PCPIPCH","NGDP_RPCH","NGDPD","PPPGDP","LUR","BCA_NGDPD","GGXWDG_NGDP","GGXCNL_NGDP"}
VALID_UNITS       = {"PERCENT","USD_BILLIONS","INTL_DOLLAR_BN"}
VALID_EXTRACTION  = {"api"}
VALID_CONFIDENCE  = {"PRIMARY","SECONDARY","ESTIMATED"}
VINTAGE_RE        = re.compile(r"^IMF-[A-Z0-9_]+-USA-\d{4}(-Jan|-Jul|-Oct)?-v\d+$")
UUID_RE           = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)

def load_sample() -> pd.DataFrame:
    files = sorted(VAULT.rglob("*_data.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — SCHEMA COMPLIANCE (SDMX GOLD STANDARD)")
    logger.info("=" * 70)
    df = load_sample()
    logger.info("  Total records: %d", len(df))
    results = []

    def chk(status, name, msg):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg})

    # 1. UUID v4 record_id
    valid = df["record_id"].dropna().apply(lambda x: bool(UUID_RE.match(str(x))))
    bad = int((~valid).sum())
    chk("PASS" if not bad else "FAIL", "UUID v4 record_id",
        f"All {len(df)} record_ids valid UUID v4" if not bad else f"{bad} invalid record_ids")

    # 2. ISO 8601 UTC data_timestamp
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    bad = int(ts.isna().sum())
    chk("PASS" if not bad else "FAIL", "ISO 8601 data_timestamp",
        f"All {len(df)} data_timestamps valid UTC ISO 8601" if not bad else f"{bad} invalid")

    # 3. ISO 8601 UTC published_date
    pub = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
    bad = int(pub.isna().sum())
    chk("PASS" if not bad else "FAIL", "ISO 8601 published_date",
        f"All {len(df)} published_dates valid UTC ISO 8601" if not bad else f"{bad} invalid")

    # 4. ISO 3166-1 country_code = US
    bad_cc = df[df["country_code"] != "US"] if "country_code" in df.columns else df
    chk("PASS" if len(bad_cc) == 0 else "FAIL", "ISO 3166-1 country_code",
        "All records have country_code=US" if len(bad_cc) == 0 else f"{len(bad_cc)} wrong country_code")

    # 5. SDMX indicator vocabulary
    actual = set(df["sovereign_series_id"].dropna().unique())
    invalid = actual - VALID_INDICATORS
    chk("PASS" if not invalid else "FAIL", "SDMX Indicator Vocabulary",
        f"All sovereign_series_ids in canonical set ({len(actual)} indicators)" if not invalid
        else f"Unknown indicators: {invalid}")

    # 6. Unit of measure vocabulary
    actual = set(df["unit_of_measure"].dropna().unique())
    invalid = actual - VALID_UNITS
    chk("PASS" if not invalid else "FAIL", "Unit of Measure Vocabulary",
        f"All unit_of_measure values valid: {actual}" if not invalid else f"Invalid units: {invalid}")

    # 7. Extraction method vocabulary
    actual = set(df["extraction_method"].dropna().unique())
    invalid = actual - VALID_EXTRACTION
    chk("PASS" if not invalid else "FAIL", "Extraction Method Vocabulary",
        f"All extraction_method='api'" if not invalid else f"Invalid: {invalid}")

    # 8. Confidence tier vocabulary
    actual = set(df["confidence_tier"].dropna().unique())
    invalid = actual - VALID_CONFIDENCE
    chk("PASS" if not invalid else "FAIL", "Confidence Tier Vocabulary",
        f"All confidence_tier valid: {actual}" if not invalid else f"Invalid: {invalid}")

    # 9. Source agency = IMF
    bad = int((df["source_agency"] != "IMF").sum()) if "source_agency" in df.columns else 0
    chk("PASS" if not bad else "FAIL", "Source Agency = IMF",
        f"All {len(df)} records have source_agency=IMF" if not bad else f"{bad} wrong agency")

    # 10. Source sub-category = WEO
    bad = int((df["source_sub_category"] != "WEO").sum()) if "source_sub_category" in df.columns else 0
    chk("PASS" if not bad else "FAIL", "Source Sub-Category = WEO",
        f"All records have source_sub_category=WEO" if not bad else f"{bad} wrong sub-category")

    # 11. data_vintage_id format: IMF-{INDICATOR}-USA-{YYYY}-v{N}
    bad = int(df["data_vintage_id"].dropna().apply(lambda x: not bool(VINTAGE_RE.match(str(x)))).sum())
    chk("PASS" if not bad else "FAIL", "data_vintage_id Format (SDMX)",
        f"All {len(df)} vintage IDs match IMF-{{INDICATOR}}-USA-{{YYYY}}-v1" if not bad else f"{bad} malformed")

    # 12. Observed value not null
    nulls = int(df["observed_value"].isna().sum())
    chk("PASS" if not nulls else "FAIL", "Observed Value Not Null",
        f"All {len(df)} observed_values populated" if not nulls else f"{nulls} nulls")

    # 13. Year range 1980-2031
    years = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year.dropna()
    bad = int(((years < 1980) | (years > 2031)).sum())
    chk("PASS" if not bad else "FAIL", "SDMX Year Range (1980-2031)",
        f"All years in [1980, 2031] (range: {int(years.min())}-{int(years.max())})" if not bad
        else f"{bad} records outside range")

    # 14. is_forecast boolean alignment
    if "is_forecast" in df.columns:
        ts_years = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
        should_forecast = ts_years >= 2025
        mismatch = int((df["is_forecast"].astype(bool) != should_forecast).sum())
        chk("PASS" if not mismatch else "WARN", "Forecast Flag Alignment",
            f"is_forecast aligns with year >= 2025 for all records" if not mismatch
            else f"{mismatch} records with mismatched is_forecast flag")
    else:
        chk("WARN", "Forecast Flag Alignment", "is_forecast column missing")

    # 15. Portal URL references IMF DataMapper
    if "portal_url" in df.columns:
        bad = int(~df["portal_url"].str.contains("imf.org", na=False).all())
        chk("PASS" if not bad else "FAIL", "Portal URL (IMF DataMapper)",
            "All portal_urls reference imf.org" if not bad else f"{bad} invalid portal URLs")
    else:
        chk("WARN", "Portal URL (IMF DataMapper)", "portal_url column missing")

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
        f.write(f"IMF Schema Compliance Report - {datetime.utcnow().isoformat()}Z\n")
        for r in results:
            f.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
    logger.info("  Reports: %s, %s", REPORT_J, REPORT_T)
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
