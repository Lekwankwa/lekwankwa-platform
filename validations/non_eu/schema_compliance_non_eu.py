"""
Stage 2 — Schema Compliance: GBR / CAN.

Usage:
  python validations/non_eu/schema_compliance_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import VAULT, active_countries, PRODUCT_FILENAMES, ALL_PRODUCTS

REQUIRED_FIELDS = [
    "data_vintage_id", "confidence_tier", "sovereign_series_id", "macro_metric_name",
    "reporting_date", "official_release_date", "as_of_date", "observed_value",
    "unit_of_measure", "is_revised_figure", "data_timestamp", "revision_number",
    "iso_alpha3", "country_name", "source", "source_agency", "source_sub_category",
    "sdmx_frequency", "published_date", "data_quality_certified",
]

NON_NULL_FIELDS = [
    "data_vintage_id", "sovereign_series_id", "macro_metric_name",
    "reporting_date", "observed_value", "iso_alpha3",
]

VALID_CONFIDENCE_TIERS = {"PRIMARY", "SECONDARY", "DERIVED"}
VALID_ISO3 = {"GBR", "CAN"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _check_file(fpath: Path, product: str, valid_iso3: set) -> list[str]:
    issues = []
    try:
        df = pd.read_parquet(fpath)
    except Exception as e:
        return [f"READ_ERROR: {e}"]

    missing = [c for c in REQUIRED_FIELDS if c not in df.columns]
    if missing:
        issues.append(f"MISSING_COLS: {missing}")

    if df.empty:
        issues.append("EMPTY_FILE")
        return issues

    for col in NON_NULL_FIELDS:
        if col in df.columns and df[col].isna().any():
            issues.append(f"NULL_{col.upper()}: {int(df[col].isna().sum())} nulls")

    if "confidence_tier" in df.columns:
        bad = set(df["confidence_tier"].dropna().unique()) - VALID_CONFIDENCE_TIERS
        if bad:
            issues.append(f"BAD_CONFIDENCE_TIER: {bad}")

    if "iso_alpha3" in df.columns:
        unexpected = set(df["iso_alpha3"].dropna().unique()) - valid_iso3
        if unexpected:
            issues.append(f"UNEXPECTED_ISO3: {unexpected}")

    if "observed_value" in df.columns:
        non_num = df["observed_value"].apply(lambda x: not isinstance(x, (int, float))).sum()
        if non_num > 0:
            issues.append(f"NON_NUMERIC_OBSERVED_VALUE: {non_num}")

    return issues


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU SCHEMA COMPLIANCE — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    filename = PRODUCT_FILENAMES[product]
    base = VAULT / f"product={product}"
    valid_iso3 = set(countries.keys())
    total = clean = violations = 0
    violation_log: list[dict] = []

    for iso, (_, source, _) in countries.items():
        src_dir = base / f"country={iso}" / f"source={source}"
        if not src_dir.exists():
            continue
        for f in sorted(src_dir.rglob(filename)):
            if "outlier" in f.name or "changelog" in f.name:
                continue
            total += 1
            issues = _check_file(f, product, valid_iso3)
            if issues:
                violations += 1
                violation_log.append({"file": str(f.relative_to(base)), "issues": issues})
                if violations <= 10:
                    logger.warning(f"  {f.relative_to(base)}: {issues}")
            else:
                clean += 1

    overall = "PASS" if violations == 0 else "FAIL"
    logger.info(f"\n  Files scanned : {total:,}")
    logger.info(f"  Clean         : {clean:,}")
    logger.info(f"  Violations    : {violations:,}")
    logger.info(f"  OVERALL       : [{overall}]")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product, "scope": "non_eu GBR/CAN",
        "total_files": total, "clean_files": clean, "violation_files": violations,
        "overall": overall, "violations": violation_log[:50],
    }
    out = Path(f"{product}_non_eu_schema_compliance.json")
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return violations == 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    args = p.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
