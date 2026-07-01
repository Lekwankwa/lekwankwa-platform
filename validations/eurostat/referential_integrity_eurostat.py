"""
Referential integrity validation — EU27 Eurostat products.

Checks:
  1. data_vintage_id format matches expected Eurostat pattern
  2. sovereign_series_id contains iso_alpha3 for EU27 data
  3. source_agency = EUROSTAT for all rows
  4. All 27 EU countries present (completeness)
  5. iso_alpha3 matches country_name (spot-check known mappings)

Usage:
  python validations/eurostat/referential_integrity_eurostat.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, re, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT  = Path("lekwankwa-historical-vault")
SOURCE = "eurostat_sdmx"
EU27   = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
          "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

ISO3_TO_NAME = {
    "AUT": "Austria", "BEL": "Belgium", "BGR": "Bulgaria", "HRV": "Croatia",
    "CYP": "Cyprus", "CZE": "Czechia", "DNK": "Denmark", "EST": "Estonia",
    "FIN": "Finland", "FRA": "France", "DEU": "Germany", "GRC": "Greece",
    "HUN": "Hungary", "IRL": "Ireland", "ITA": "Italy", "LVA": "Latvia",
    "LTU": "Lithuania", "LUX": "Luxembourg", "MLT": "Malta", "NLD": "Netherlands",
    "POL": "Poland", "PRT": "Portugal", "ROU": "Romania", "SVK": "Slovakia",
    "SVN": "Slovenia", "ESP": "Spain", "SWE": "Sweden",
}

_VID_PATTERN = re.compile(r"^EUROSTAT-.+-\d{4}(-\d{2})?-v\d+$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _load(product: str) -> pd.DataFrame:
    base = VAULT / f"product={product}"
    frames = []
    for iso in EU27:
        src = base / f"country={iso}" / f"source={SOURCE}"
        if not src.exists(): continue
        for f in sorted(src.rglob("*.parquet")):
            if "outlier" in f.name or "changelog" in f.name: continue
            try: frames.append(pd.read_parquet(f))
            except Exception: pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def check_vintage_id_format(df: pd.DataFrame) -> dict:
    if "data_vintage_id" not in df.columns:
        return {"status": "SKIP", "check": "Vintage ID Format", "message": "Column missing"}
    sample = df["data_vintage_id"].dropna().head(5000)
    bad = [v for v in sample if not _VID_PATTERN.match(str(v))]
    if not bad:
        return {"status": "PASS", "check": "Vintage ID Format",
                "message": f"All sampled data_vintage_id match EUROSTAT-*-YYYY-MM-vN format"}
    return {"status": "FAIL", "check": "Vintage ID Format",
            "message": f"{len(bad)} vintage IDs don't match expected format",
            "details": {"examples": bad[:5]}}


def check_source_agency(df: pd.DataFrame) -> dict:
    if "source_agency" not in df.columns:
        return {"status": "SKIP", "check": "Source Agency", "message": "Column missing"}
    bad = df[df["source_agency"] != "EUROSTAT"]
    if bad.empty:
        return {"status": "PASS", "check": "Source Agency",
                "message": f"All {len(df):,} rows have source_agency=EUROSTAT"}
    return {"status": "FAIL", "check": "Source Agency",
            "message": f"{len(bad)} rows have source_agency != EUROSTAT",
            "details": {"bad_values": list(bad["source_agency"].unique()[:5])}}


def check_iso_in_series_id(df: pd.DataFrame) -> dict:
    """sovereign_series_id should contain the country's ISO3 code."""
    if "sovereign_series_id" not in df.columns or "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "ISO3 in Series ID", "message": "Columns missing"}
    sample = df[["sovereign_series_id", "iso_alpha3"]].dropna().head(5000)
    bad = sample[~sample.apply(lambda r: str(r["iso_alpha3"]) in str(r["sovereign_series_id"]), axis=1)]
    if bad.empty:
        return {"status": "PASS", "check": "ISO3 in Series ID",
                "message": f"iso_alpha3 present in sovereign_series_id for all sampled rows"}
    return {"status": "WARN", "check": "ISO3 in Series ID",
            "message": f"{len(bad)} sampled rows where iso_alpha3 not found in sovereign_series_id",
            "details": {"examples": bad.head(5).to_dict("records")}}


def check_country_name_consistency(df: pd.DataFrame) -> dict:
    """iso_alpha3 → country_name should match known mapping."""
    if "iso_alpha3" not in df.columns or "country_name" not in df.columns:
        return {"status": "SKIP", "check": "Country Name Consistency", "message": "Columns missing"}
    mismatches = []
    for iso, expected_name in ISO3_TO_NAME.items():
        rows = df[df["iso_alpha3"] == iso]["country_name"].dropna().unique()
        for name in rows:
            if expected_name.lower() not in str(name).lower() and str(name).lower() not in expected_name.lower():
                mismatches.append({"iso_alpha3": iso, "expected": expected_name, "found": str(name)})
    if not mismatches:
        return {"status": "PASS", "check": "Country Name Consistency",
                "message": f"iso_alpha3 → country_name mapping consistent for all 27 EU countries"}
    return {"status": "WARN", "check": "Country Name Consistency",
            "message": f"{len(mismatches)} iso_alpha3/country_name mismatches found",
            "details": {"mismatches": mismatches[:10]}}


def check_all_countries_present(df: pd.DataFrame) -> dict:
    if "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "All EU27 Countries Present", "message": "Column missing"}
    found = set(df["iso_alpha3"].dropna().unique())
    missing = sorted(set(EU27) - found)
    if not missing:
        return {"status": "PASS", "check": "All EU27 Countries Present",
                "message": "All 27 EU countries have data"}
    return {"status": "WARN", "check": "All EU27 Countries Present",
            "message": f"{len(missing)} EU countries absent: {missing}",
            "details": {"missing": missing}}


def run(product: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 REFERENTIAL INTEGRITY — {product.upper()}")
    logger.info("=" * 70)

    df = _load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False

    iso_in_series = (
        {"status": "SKIP", "check": "ISO3 in Series ID",
         "message": "food_micropricing uses product-level series IDs (no country prefix) by design"}
        if product == "food_micropricing"
        else check_iso_in_series_id(df)
    )
    results = [
        check_vintage_id_format(df),
        check_source_agency(df),
        iso_in_series,
        check_country_name_consistency(df),
        check_all_countries_present(df),
    ]

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")

    for r in results:
        tag = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]", "SKIP": "[SKIP]"}.get(r["status"])
        logger.info(f"  {tag} {r['check']}: {r['message']}")

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  OVERALL: [{overall}] — {passed} PASS, {warned} WARN, {failed} FAIL")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product,
        "scope": "EU27 eurostat_sdmx",
        "total_records": len(df),
        "checks_passed": passed, "checks_warned": warned, "checks_failed": failed,
        "overall": overall, "results": results,
    }
    out = Path(f"{product}_eu27_referential_integrity.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="EU27 referential integrity")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    args = parser.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
