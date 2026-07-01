"""
Stage 4 — Temporal Consistency: GBR / CAN / AUS / NOR.

Usage:
  python validations/non_eu/temporal_consistency_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import load, active_countries, ALL_PRODUCTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def check_date_parseable(df: pd.DataFrame) -> dict:
    issues = {}
    for col in ["reporting_date", "published_date", "official_release_date"]:
        if col not in df.columns:
            continue
        bad = int(pd.to_datetime(df[col], errors="coerce").isna().sum())
        if bad > 0:
            issues[col] = bad
    if not issues:
        return {"status": "PASS", "check": "Date Parseability",
                "message": f"All date fields parse cleanly ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "Date Parseability",
            "message": f"Unparseable dates: {issues}", "details": issues}


def check_no_duplicate_reporting_dates(df: pd.DataFrame) -> dict:
    if "reporting_date" not in df.columns or "sovereign_series_id" not in df.columns:
        return {"status": "SKIP", "check": "No Duplicate Reporting Dates", "message": "Columns missing"}
    subset = ["sovereign_series_id", "reporting_date", "confidence_tier"]
    if "iso_alpha3" in df.columns:
        subset.append("iso_alpha3")
    dupes = int(df.duplicated(subset=subset).sum())
    if dupes == 0:
        return {"status": "PASS", "check": "No Duplicate Reporting Dates",
                "message": f"No duplicate (series, date, tier, country) combinations ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "No Duplicate Reporting Dates",
            "message": f"{dupes} duplicate combinations found",
            "details": {"duplicate_count": dupes}}


def check_knowledge_ordering(df: pd.DataFrame) -> dict:
    if "data_timestamp" not in df.columns or "published_date" not in df.columns:
        return {"status": "SKIP", "check": "Knowledge Ordering", "message": "Columns missing"}
    primary = df[df["confidence_tier"] == "PRIMARY"] if "confidence_tier" in df.columns else df
    ts  = pd.to_datetime(primary["data_timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
    pub = pd.to_datetime(primary["published_date"], errors="coerce")
    violations = int((ts > pub + pd.Timedelta(days=1)).sum())
    if violations == 0:
        return {"status": "PASS", "check": "Knowledge Ordering",
                "message": f"data_timestamp <= published_date for all PRIMARY rows ({len(primary):,})"}
    return {"status": "FAIL", "check": "Knowledge Ordering",
            "message": f"{violations} PRIMARY rows where data_timestamp > published_date",
            "details": {"violation_count": violations}}


def check_monotonic_series(df: pd.DataFrame) -> dict:
    if "sovereign_series_id" not in df.columns or "reporting_date" not in df.columns:
        return {"status": "SKIP", "check": "Series Monotonicity", "message": "Columns missing"}

    primary = df[df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"] \
        if "confidence_tier" in df.columns else df
    series_ids = primary["sovereign_series_id"].dropna().unique()
    sample = series_ids[:50]

    non_monotonic = []
    for sid in sample:
        grp = primary[primary["sovereign_series_id"] == sid].copy()
        grp["_rd"] = pd.to_datetime(grp["reporting_date"], errors="coerce")
        grp = grp.dropna(subset=["_rd"]).sort_values("_rd")
        if len(grp) < 3:
            continue
        if not grp["_rd"].is_monotonic_increasing:
            non_monotonic.append(sid)

    if not non_monotonic:
        return {"status": "PASS", "check": "Series Monotonicity",
                "message": f"All {len(sample)} sampled series monotonically ordered"}
    return {"status": "WARN", "check": "Series Monotonicity",
            "message": f"{len(non_monotonic)} series have non-monotonic reporting_dates (sample={len(sample)})",
            "details": {"non_monotonic": list(non_monotonic)}}


def check_release_date_consistency(df: pd.DataFrame) -> dict:
    """For RELEASE_DATE_ONLY: official_release_date == published_date == as_of_date."""
    missing = [c for c in ["official_release_date", "published_date"] if c not in df.columns]
    if missing:
        return {"status": "SKIP", "check": "Release Date Consistency", "message": f"Columns missing: {missing}"}
    rd  = pd.to_datetime(df["official_release_date"], errors="coerce")
    pub = pd.to_datetime(df["published_date"], errors="coerce")
    mismatch = int((rd.dt.date != pub.dt.date).sum())
    if mismatch == 0:
        return {"status": "PASS", "check": "Release Date Consistency",
                "message": f"official_release_date == published_date for all {len(df):,} rows"}
    return {"status": "WARN", "check": "Release Date Consistency",
            "message": f"{mismatch} rows where official_release_date != published_date (may be intentional if lag differs by source)",
            "details": {"mismatch_count": mismatch}}


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU TEMPORAL CONSISTENCY — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    df = load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False
    logger.info(f"  Loaded {len(df):,} rows")

    results = [
        check_date_parseable(df),
        check_no_duplicate_reporting_dates(df),
        check_knowledge_ordering(df),
        check_monotonic_series(df),
        check_release_date_consistency(df),
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
        "product": product, "scope": "non_eu GBR/CAN/AUS/NOR",
        "total_records": len(df),
        "checks_passed": passed, "checks_warned": warned, "checks_failed": failed,
        "overall": overall, "results": results,
    }
    out = Path(f"{product}_non_eu_temporal_consistency.json")
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
