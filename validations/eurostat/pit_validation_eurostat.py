"""
Bitemporal PIT validation — EU27 Eurostat products.

Uses bitemporal_core checks adapted for Eurostat schema (no record_id;
uses data_vintage_id as the unique identifier instead).

Usage:
  python validations/eurostat/pit_validation_eurostat.py --product wages_and_employment
  python validations/eurostat/pit_validation_eurostat.py --product Housing_Supply_and_Shelter_Inflation
  python validations/eurostat/pit_validation_eurostat.py --product trade_flows
  python validations/eurostat/pit_validation_eurostat.py --product global_macro
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT     = Path("lekwankwa-historical-vault")
SOURCE    = "eurostat_sdmx"
EU27      = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

# Eurostat publication lag bounds (months after reference period).
# wages uses (1, 24) because annual D11 wages are published ~14 months after year-end.
PUB_LAG = {
    "wages_and_employment":               (1, 24),
    "Housing_Supply_and_Shelter_Inflation": (1, 6),
    "trade_flows":                         (1, 5),
    "global_macro":                        (1, 24),
    "food_micropricing":                   (1, 3),   # HICP food index published ~30-45 days after month-end
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _load(product: str) -> pd.DataFrame:
    base = VAULT / f"product={product}"
    frames = []
    for iso in EU27:
        src_dir = base / f"country={iso}" / f"source={SOURCE}"
        if not src_dir.exists():
            continue
        for f in sorted(src_dir.rglob("*.parquet")):
            if "outlier" in f.name or "changelog" in f.name:
                continue
            try:
                frames.append(pd.read_parquet(f))
            except Exception as e:
                logger.warning(f"Skip {f}: {e}")
    if not frames:
        raise FileNotFoundError(f"No data found for product={product} source={SOURCE}")
    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Loaded {len(df):,} records from {product} (EU27)")
    return df


def _normalize_schema(df: pd.DataFrame, product: str) -> pd.DataFrame:
    """Alias food_micropricing non-standard columns to the names expected by all checks."""
    if product != "food_micropricing":
        return df
    rename = {}
    if "observation_period" in df.columns and "reporting_date" not in df.columns:
        rename["observation_period"] = "reporting_date"
    if "observed_price_local" in df.columns and "observed_value" not in df.columns:
        rename["observed_price_local"] = "observed_value"
    if "standard_name" in df.columns and "macro_metric_name" not in df.columns:
        rename["standard_name"] = "macro_metric_name"
    return df.rename(columns=rename) if rename else df


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


# ── Individual checks ─────────────────────────────────────────────────────────

def check_vintage_id_unique(df: pd.DataFrame):
    col = "data_vintage_id"
    nulls = int(df[col].isna().sum()) if col in df.columns else 0
    dupes = int(df[col].duplicated().sum()) if col in df.columns else 0
    if nulls == 0 and dupes == 0:
        return _r("PASS", "Unique Vintage IDs", f"All {len(df):,} data_vintage_ids unique and non-null")
    return _r("FAIL", "Unique Vintage IDs",
              f"{nulls} null, {dupes} duplicate data_vintage_ids",
              {"null_count": nulls, "duplicate_count": dupes})


def check_published_date_complete(df: pd.DataFrame):
    for col in ["published_date", "as_of_date"]:
        n = int(df[col].isna().sum()) if col in df.columns else len(df)
        if n > 0:
            return _r("FAIL", "Knowledge Time Completeness",
                      f"{n} null values in {col}")
    return _r("PASS", "Knowledge Time Completeness",
              f"published_date and as_of_date fully populated ({len(df):,} records)")


def check_reporting_before_published(df: pd.DataFrame):
    if "reporting_date" not in df.columns or "published_date" not in df.columns:
        return _r("SKIP", "Temporal Ordering", "Required columns missing")
    # DERIVED rows carry-forward values beyond their published_date by design — only check PRIMARY
    if "confidence_tier" in df.columns:
        primary = df[df["confidence_tier"] == "PRIMARY"]
    else:
        primary = df
    n_derived = len(df) - len(primary)
    rd = pd.to_datetime(primary["reporting_date"], errors="coerce")
    pd_ = pd.to_datetime(primary["published_date"], errors="coerce")
    violations = int((rd > pd_).sum())
    if violations == 0:
        return _r("PASS", "Temporal Ordering",
                  f"All PRIMARY reporting_date <= published_date ({len(primary):,} PRIMARY, {n_derived} DERIVED skipped)")
    return _r("FAIL", "Temporal Ordering",
              f"{violations} PRIMARY records where reporting_date > published_date",
              {"violation_count": violations})


def check_publication_lag(df: pd.DataFrame, bounds: tuple[int, int]):
    """published_date should be N–M months after reporting_date (PRIMARY rows only).
    DERIVED carry-forward rows have lag=0 or lag=-1 by design and are excluded.
    """
    if "reporting_date" not in df.columns or "published_date" not in df.columns:
        return _r("SKIP", "Publication Lag", "Required columns missing")
    primary = df[df["confidence_tier"] == "PRIMARY"] if "confidence_tier" in df.columns else df
    n_derived = len(df) - len(primary)
    rd  = pd.to_datetime(primary["reporting_date"], errors="coerce")
    pub = pd.to_datetime(primary["published_date"], errors="coerce")
    lag_months = ((pub.dt.year - rd.dt.year) * 12 + (pub.dt.month - rd.dt.month))
    lo, hi = bounds
    violations = int(((lag_months < lo) | (lag_months > hi)).sum())
    if violations == 0:
        med = round(float(lag_months.median()), 1)
        return _r("PASS", "Publication Lag",
                  f"All PRIMARY lags in [{lo}, {hi}] months (median={med}m, {n_derived} DERIVED skipped)")
    return _r("WARN", "Publication Lag",
              f"{violations} PRIMARY records outside expected lag [{lo}, {hi}] months",
              {"violation_count": violations, "median_lag": float(lag_months.median())})


def check_no_future_data(df: pd.DataFrame):
    today = pd.Timestamp.utcnow().normalize()
    if "data_timestamp" not in df.columns:
        return _r("SKIP", "Anti-Retroactive Ingestion", "data_timestamp column missing")
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    future = int((ts > today + pd.Timedelta(days=1)).sum())
    if future == 0:
        return _r("PASS", "Anti-Retroactive Ingestion",
                  f"No data_timestamp values beyond today ({len(df):,} records)")
    return _r("FAIL", "Anti-Retroactive Ingestion",
              f"{future} records have data_timestamp in the future",
              {"future_count": future})


def check_revision_integrity(df: pd.DataFrame):
    if "revision_number" not in df.columns:
        return _r("SKIP", "Revision Integrity", "revision_number column missing")
    neg = int((pd.to_numeric(df["revision_number"], errors="coerce") < 0).sum())
    if neg == 0:
        return _r("PASS", "Revision Integrity",
                  f"All revision_number >= 0 ({len(df):,} records)")
    return _r("FAIL", "Revision Integrity",
              f"{neg} negative revision_number values",
              {"negative_count": neg})


def check_confidence_tier_valid(df: pd.DataFrame):
    if "confidence_tier" not in df.columns:
        return _r("SKIP", "Confidence Tier", "Column missing")
    valid = {"PRIMARY", "SECONDARY", "DERIVED"}
    invalid = set(df["confidence_tier"].dropna().unique()) - valid
    if not invalid:
        counts = df["confidence_tier"].value_counts().to_dict()
        return _r("PASS", "Confidence Tier",
                  f"All confidence_tier values valid: {counts}")
    return _r("FAIL", "Confidence Tier",
              f"Invalid confidence_tier values: {invalid}")


# ── Runner ────────────────────────────────────────────────────────────────────

def run(product: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 PIT VALIDATION — {product.upper()}")
    logger.info("=" * 70)

    df = _load(product)
    df = _normalize_schema(df, product)
    lag_bounds = PUB_LAG.get(product, (1, 6))

    results = [
        check_vintage_id_unique(df),
        check_published_date_complete(df),
        check_reporting_before_published(df),
        check_publication_lag(df, lag_bounds),
        check_no_future_data(df),
        check_revision_integrity(df),
        check_confidence_tier_valid(df),
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
        "product": product,
        "scope": "EU27 eurostat_sdmx",
        "total_records": len(df),
        "checks_passed": passed,
        "checks_warned": warned,
        "checks_failed": failed,
        "overall": overall,
        "results": [_np_safe(r) for r in results],
    }
    out = Path(f"{product}_eu27_pit_report.json")
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="EU27 Eurostat PIT validation")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    args = parser.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
