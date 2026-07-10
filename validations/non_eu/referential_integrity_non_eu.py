"""
Stage 5 — Referential Integrity: GBR / CAN.

Checks:
  1. Vintage ID format matches source-specific pattern
  2. sovereign_series_id contains ISO3 code
  3. source_agency matches expected value per country
  4. All active countries present
  5. iso_alpha3 → country_name consistency

Usage:
  python validations/non_eu/referential_integrity_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, re, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import load, active_countries, COUNTRIES, ALL_PRODUCTS

ISO3_TO_NAME = {
    "GBR": "United Kingdom",
    "CAN": "Canada",
}

ISO3_TO_AGENCY = {
    "GBR": "ONS",
    "CAN": "STATCAN",   # ingested as all-caps STATCAN
}

# Each source prefix produces IDs like: ONS-GBR-METRIC-YYYY-MM-vN
# CAN uses STATCAN (all-caps) as ingested by the StatCan scraper
_VID_PATTERN = re.compile(
    r"^(ONS|STATCAN)-(GBR|CAN)-.+-\d{4}(-\d{2})?-v\d+$"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def check_vintage_id_format(df: pd.DataFrame) -> dict:
    if "data_vintage_id" not in df.columns:
        return {"status": "SKIP", "check": "Vintage ID Format", "message": "Column missing"}
    sample = df["data_vintage_id"].dropna().head(5000)
    bad = [v for v in sample if not _VID_PATTERN.match(str(v))]
    if not bad:
        return {"status": "PASS", "check": "Vintage ID Format",
                "message": f"All sampled vintage IDs match SOURCE-ISO3-METRIC-YYYY-MM-vN format"}
    return {"status": "FAIL", "check": "Vintage ID Format",
            "message": f"{len(bad)} IDs don't match expected format",
            "details": {"examples": bad[:5]}}


def check_source_agency(df: pd.DataFrame, product: str) -> dict:
    if "source_agency" not in df.columns or "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "Source Agency", "message": "Columns missing"}

    mismatches = []
    for iso, expected_agency in ISO3_TO_AGENCY.items():
        subset = df[df["iso_alpha3"] == iso]
        if subset.empty:
            continue
        bad = subset[subset["source_agency"] != expected_agency]
        if not bad.empty:
            found = list(bad["source_agency"].unique())
            mismatches.append({"iso3": iso, "expected": expected_agency, "found": found})

    if not mismatches:
        return {"status": "PASS", "check": "Source Agency",
                "message": f"All source_agency values match expected per country"}
    return {"status": "FAIL", "check": "Source Agency",
            "message": f"{len(mismatches)} countries have wrong source_agency",
            "details": {"mismatches": mismatches}}


def check_iso3_in_series_id(df: pd.DataFrame) -> dict:
    if "sovereign_series_id" not in df.columns or "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "ISO3 in Series ID", "message": "Columns missing"}
    sample = df[["sovereign_series_id", "iso_alpha3"]].dropna().head(5000)
    bad = sample[~sample.apply(lambda r: str(r["iso_alpha3"]) in str(r["sovereign_series_id"]), axis=1)]
    if bad.empty:
        return {"status": "PASS", "check": "ISO3 in Series ID",
                "message": f"iso_alpha3 present in sovereign_series_id for all sampled rows"}
    return {"status": "WARN", "check": "ISO3 in Series ID",
            "message": f"{len(bad)} rows where iso_alpha3 not in sovereign_series_id",
            "details": {"examples": bad.head(5).to_dict("records")}}


def check_country_name_consistency(df: pd.DataFrame) -> dict:
    if "iso_alpha3" not in df.columns or "country_name" not in df.columns:
        return {"status": "SKIP", "check": "Country Name Consistency", "message": "Columns missing"}
    mismatches = []
    for iso, expected_name in ISO3_TO_NAME.items():
        rows = df[df["iso_alpha3"] == iso]["country_name"].dropna().unique()
        for name in rows:
            if expected_name.lower() not in str(name).lower() and str(name).lower() not in expected_name.lower():
                mismatches.append({"iso3": iso, "expected": expected_name, "found": str(name)})
    if not mismatches:
        return {"status": "PASS", "check": "Country Name Consistency",
                "message": f"iso_alpha3 → country_name consistent for all 4 countries"}
    return {"status": "WARN", "check": "Country Name Consistency",
            "message": f"{len(mismatches)} mismatches found",
            "details": {"mismatches": mismatches[:10]}}


def check_all_countries_present(df: pd.DataFrame, product: str) -> dict:
    if "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "All Countries Present", "message": "Column missing"}
    expected = set(active_countries(product).keys())
    found    = set(df["iso_alpha3"].dropna().unique())
    missing  = sorted(expected - found)
    if not missing:
        return {"status": "PASS", "check": "All Countries Present",
                "message": f"All {len(expected)} expected countries present: {sorted(found)}"}
    return {"status": "WARN", "check": "All Countries Present",
            "message": f"{len(missing)} expected countries absent: {missing}",
            "details": {"missing": missing}}


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU REFERENTIAL INTEGRITY — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    df = load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False
    logger.info(f"  Loaded {len(df):,} rows")

    results = [
        check_vintage_id_format(df),
        check_source_agency(df, product),
        check_iso3_in_series_id(df),
        check_country_name_consistency(df),
        check_all_countries_present(df, product),
    ]

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")

    for r in results:
        tag = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]", "SKIP": "[SKIP]"}.get(r["status"], "[????]")
        logger.info(f"  {tag} {r['check']}: {r['message']}")

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  OVERALL: [{overall}] — {passed} PASS, {warned} WARN, {failed} FAIL")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product, "scope": "non_eu GBR/CAN",
        "total_records": len(df),
        "checks_passed": passed, "checks_warned": warned, "checks_failed": failed,
        "overall": overall, "results": results,
    }
    out = Path(f"{product}_non_eu_referential_integrity.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    args = p.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
