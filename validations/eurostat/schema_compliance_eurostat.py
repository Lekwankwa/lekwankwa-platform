"""
Schema compliance validation — EU27 Eurostat products.

Checks all required SCHEMA_STANDARD.yaml v5.0 fields are present and valid
across every partition file for the specified product.

Usage:
  python validations/eurostat/schema_compliance_eurostat.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT  = Path("lekwankwa-historical-vault")
SOURCE = "eurostat_sdmx"
EU27   = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
          "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

# Fields required across ALL EU27 Eurostat files (SCHEMA_STANDARD.yaml v5.0 core)
REQUIRED_FIELDS = [
    "data_vintage_id", "confidence_tier", "sovereign_series_id", "macro_metric_name",
    "reporting_date", "official_release_date", "as_of_date", "observed_value",
    "unit_of_measure", "is_revised_figure", "data_timestamp", "revision_number",
    "iso_alpha3", "country_name", "source", "source_agency", "source_sub_category",
    "sdmx_frequency", "published_date", "data_quality_certified",
]

FOOD_REQUIRED_FIELDS = [
    "data_vintage_id", "confidence_tier", "global_coicop_code", "standard_name",
    "local_name", "category", "observation_period", "official_release_date",
    "as_of_date", "is_revised_figure", "observed_price_local", "sovereign_series_id",
    "data_timestamp", "revision_number", "iso_alpha3", "country_name", "source",
    "source_agency", "source_sub_category", "sdmx_frequency", "unit_of_measure",
    "published_date", "data_quality_certified",
]
FOOD_NON_NULL_FIELDS = [
    "data_vintage_id", "sovereign_series_id", "standard_name",
    "observation_period", "observed_price_local", "iso_alpha3",
]

VALID_CONFIDENCE_TIERS = {"PRIMARY", "SECONDARY", "DERIVED"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _check_file(fpath: Path, product: str = "") -> list[str]:
    """Return list of violation strings for one file; empty = clean."""
    issues = []
    is_food = product == "food_micropricing"
    req_fields = FOOD_REQUIRED_FIELDS if is_food else REQUIRED_FIELDS
    non_null   = FOOD_NON_NULL_FIELDS  if is_food else [
        "data_vintage_id", "sovereign_series_id", "macro_metric_name",
        "reporting_date", "observed_value", "iso_alpha3",
    ]
    value_col = "observed_price_local" if is_food else "observed_value"

    try:
        df = pd.read_parquet(fpath)
    except Exception as e:
        return [f"READ_ERROR: {e}"]

    missing = [c for c in req_fields if c not in df.columns]
    if missing:
        issues.append(f"MISSING_COLS: {missing}")

    if df.empty:
        issues.append("EMPTY_FILE")
        return issues

    for col in non_null:
        if col in df.columns and df[col].isna().any():
            n = int(df[col].isna().sum())
            issues.append(f"NULL_{col.upper()}: {n} nulls")

    if "confidence_tier" in df.columns:
        bad = set(df["confidence_tier"].dropna().unique()) - VALID_CONFIDENCE_TIERS
        if bad:
            issues.append(f"BAD_CONFIDENCE_TIER: {bad}")

    if "confidence_tier" in df.columns and "is_interpolated" in df.columns:
        derived = df[df["confidence_tier"] == "DERIVED"]
        if not derived.empty:
            # is_interpolated is always explicitly True or False (never None) since the
            # June 2026 vault backfill — safe to use == False directly.
            not_interp = derived[derived["is_interpolated"] == False]
            if not not_interp.empty:
                issues.append(f"DERIVED_NOT_INTERPOLATED: {len(not_interp)} rows")

    if "iso_alpha3" in df.columns:
        bad_iso = set(df["iso_alpha3"].dropna().unique()) - set(EU27)
        if bad_iso:
            issues.append(f"UNEXPECTED_ISO3: {bad_iso}")

    if value_col in df.columns:
        non_num = df[value_col].apply(lambda x: not isinstance(x, (int, float))).sum()
        if non_num > 0:
            issues.append(f"NON_NUMERIC_{value_col.upper()}: {non_num}")

    return issues


def run(product: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 SCHEMA COMPLIANCE — {product.upper()}")
    logger.info("=" * 70)

    base = VAULT / f"product={product}"
    total = clean = violations = 0
    violation_log: list[dict] = []

    for iso in EU27:
        src_dir = base / f"country={iso}" / f"source={SOURCE}"
        if not src_dir.exists():
            continue
        for f in sorted(src_dir.rglob("*.parquet")):
            if "outlier" in f.name or "changelog" in f.name:
                continue
            total += 1
            issues = _check_file(f, product)
            if issues:
                violations += 1
                rel = str(f.relative_to(base))
                violation_log.append({"file": rel, "issues": issues})
                if violations <= 10:
                    logger.warning(f"  {rel}: {issues}")
            else:
                clean += 1

    overall = "PASS" if violations == 0 else "FAIL"
    logger.info(f"\n  Files scanned   : {total:,}")
    logger.info(f"  Clean           : {clean:,}")
    logger.info(f"  Violations      : {violations:,}")
    logger.info(f"  OVERALL         : [{overall}]")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product,
        "scope": "EU27 eurostat_sdmx",
        "total_files": total,
        "clean_files": clean,
        "violation_files": violations,
        "overall": overall,
        "violations": violation_log[:50],  # cap at 50 in report
    }
    out = Path(f"{product}_eu27_schema_compliance.json")
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return violations == 0


def main():
    parser = argparse.ArgumentParser(description="EU27 Eurostat schema compliance")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    args = parser.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
