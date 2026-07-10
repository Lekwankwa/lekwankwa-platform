"""
Stage 1 — PIT Validation: GBR / CAN.

Both countries use RELEASE_DATE_ONLY ingestion — release dates are estimated
from obs_date + lag. Checks validate bitemporal field completeness and ordering.

Usage:
  python validations/non_eu/pit_validation_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import load, ALL_PRODUCTS, active_countries

# Publication lag bounds (months): RELEASE_DATE_ONLY estimates, wider than EU27
PUB_LAG = {
    "wages_and_employment":                (1, 6),
    "Housing_Supply_and_Shelter_Inflation": (1, 6),
    "trade_flows":                         (1, 5),
    "global_macro":                        (1, 24),
    "food_micropricing":                   (1, 3),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _np_safe(obj):
    if isinstance(obj, np.integer): return int(obj)
    if isinstance(obj, np.floating): return float(obj)
    if isinstance(obj, np.bool_): return bool(obj)
    if isinstance(obj, np.ndarray): return obj.tolist()
    if isinstance(obj, dict): return {k: _np_safe(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_np_safe(v) for v in obj]
    return obj


def _r(status, check, message, details=None):
    return {"status": status, "check": check, "message": message, "details": details or {}}


def check_vintage_id_unique(df: pd.DataFrame):
    col = "data_vintage_id"
    nulls = int(df[col].isna().sum()) if col in df.columns else len(df)
    dupes = int(df[col].duplicated().sum()) if col in df.columns else 0
    if nulls == 0 and dupes == 0:
        return _r("PASS", "Unique Vintage IDs", f"All {len(df):,} data_vintage_ids unique and non-null")
    return _r("FAIL", "Unique Vintage IDs",
              f"{nulls} null, {dupes} duplicate data_vintage_ids",
              {"null_count": nulls, "duplicate_count": dupes})


def check_published_date_complete(df: pd.DataFrame):
    issues = {}
    for col in ["published_date", "as_of_date", "official_release_date"]:
        if col in df.columns:
            n = int(df[col].isna().sum())
            if n > 0:
                issues[col] = n
    if not issues:
        return _r("PASS", "Release Date Completeness",
                  f"official_release_date, published_date, as_of_date all fully populated ({len(df):,} records)")
    return _r("FAIL", "Release Date Completeness", f"Null values found: {issues}", issues)


def check_reporting_before_published(df: pd.DataFrame):
    if "reporting_date" not in df.columns or "official_release_date" not in df.columns:
        return _r("SKIP", "Temporal Ordering", "Required columns missing")
    primary = df[df["confidence_tier"] == "PRIMARY"] if "confidence_tier" in df.columns else df
    rd  = pd.to_datetime(primary["reporting_date"], errors="coerce")
    pub = pd.to_datetime(primary["official_release_date"], errors="coerce")
    violations = int((rd > pub).sum())
    if violations == 0:
        return _r("PASS", "Temporal Ordering",
                  f"All reporting_date <= official_release_date ({len(primary):,} PRIMARY rows)")
    return _r("FAIL", "Temporal Ordering",
              f"{violations} rows where reporting_date > official_release_date",
              {"violation_count": violations})


def check_publication_lag(df: pd.DataFrame, bounds: tuple[int, int]):
    if "reporting_date" not in df.columns or "official_release_date" not in df.columns:
        return _r("SKIP", "Publication Lag", "Required columns missing")
    primary = df[df["confidence_tier"] == "PRIMARY"] if "confidence_tier" in df.columns else df
    rd  = pd.to_datetime(primary["reporting_date"], errors="coerce")
    pub = pd.to_datetime(primary["official_release_date"], errors="coerce")
    lag = ((pub.dt.year - rd.dt.year) * 12 + (pub.dt.month - rd.dt.month))
    lo, hi = bounds
    violations = int(((lag < lo) | (lag > hi)).sum())
    med = round(float(lag.median()), 1) if len(lag) > 0 else float("nan")
    if violations == 0:
        return _r("PASS", "Publication Lag",
                  f"All lags in [{lo}, {hi}] months (median={med}m, {len(primary):,} rows)")
    return _r("WARN", "Publication Lag",
              f"{violations} rows outside expected lag [{lo}, {hi}] months (median={med}m)",
              {"violation_count": violations, "median_lag": med})


def check_no_future_data(df: pd.DataFrame):
    today = pd.Timestamp.utcnow().normalize()
    if "data_timestamp" not in df.columns:
        return _r("SKIP", "Anti-Retroactive Ingestion", "data_timestamp missing")
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    future = int((ts > today + pd.Timedelta(days=1)).sum())
    if future == 0:
        return _r("PASS", "Anti-Retroactive Ingestion",
                  f"No future data_timestamp values ({len(df):,} rows)")
    return _r("FAIL", "Anti-Retroactive Ingestion",
              f"{future} records have data_timestamp in the future",
              {"future_count": future})


def check_revision_integrity(df: pd.DataFrame):
    if "revision_number" not in df.columns:
        return _r("SKIP", "Revision Integrity", "revision_number missing")
    neg = int((pd.to_numeric(df["revision_number"], errors="coerce") < 0).sum())
    if neg == 0:
        return _r("PASS", "Revision Integrity", f"All revision_number >= 0 ({len(df):,} rows)")
    return _r("FAIL", "Revision Integrity", f"{neg} negative revision_number values",
              {"negative_count": neg})


def check_confidence_tier(df: pd.DataFrame):
    if "confidence_tier" not in df.columns:
        return _r("SKIP", "Confidence Tier", "Column missing")
    valid = {"PRIMARY", "SECONDARY", "DERIVED"}
    invalid = set(df["confidence_tier"].dropna().unique()) - valid
    counts = df["confidence_tier"].value_counts().to_dict()
    if not invalid:
        return _r("PASS", "Confidence Tier", f"All confidence_tier values valid: {counts}")
    return _r("FAIL", "Confidence Tier", f"Invalid values: {invalid}")


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU PIT VALIDATION — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    df = load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False
    logger.info(f"  Loaded {len(df):,} rows from {df['iso_alpha3'].nunique()} countries")

    bounds = PUB_LAG.get(product, (1, 6))
    results = [
        check_vintage_id_unique(df),
        check_published_date_complete(df),
        check_reporting_before_published(df),
        check_publication_lag(df, bounds),
        check_no_future_data(df),
        check_revision_integrity(df),
        check_confidence_tier(df),
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
        "overall": overall, "results": [_np_safe(r) for r in results],
    }
    out = Path(f"{product}_non_eu_pit_report.json")
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    args = p.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
