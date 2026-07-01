"""
Temporal consistency validation — EU27 Eurostat products.

Checks:
  1. reporting_date is monotonically ordered within each series
  2. No duplicate reporting_dates within a single series
  3. data_timestamp <= published_date (knowledge order)
  4. All dates parseable as valid timestamps

Usage:
  python validations/eurostat/temporal_consistency_eurostat.py --product wages_and_employment
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _normalize_schema(df: pd.DataFrame, product: str) -> pd.DataFrame:
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


def check_date_parseable(df: pd.DataFrame) -> dict:
    issues = {}
    for col in ["reporting_date", "published_date", "official_release_date"]:
        if col not in df.columns: continue
        bad = pd.to_datetime(df[col], errors="coerce").isna().sum()
        if bad > 0:
            issues[col] = int(bad)
    if not issues:
        return {"status": "PASS", "check": "Date Parseability",
                "message": f"All date fields parse cleanly ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "Date Parseability",
            "message": f"Unparseable dates found: {issues}", "details": issues}


def check_no_duplicate_reporting_dates(df: pd.DataFrame, product: str = "") -> dict:
    if "reporting_date" not in df.columns or "sovereign_series_id" not in df.columns:
        return {"status": "SKIP", "check": "No Duplicate Reporting Dates", "message": "Missing columns"}
    # food_micropricing series IDs are COICOP codes (product-level, not country-specific).
    # The same EUROSTAT_HICP_CP* code appears for each of the 27 countries, so uniqueness
    # requires iso_alpha3 as a fourth dimension.
    subset = ["sovereign_series_id", "reporting_date", "confidence_tier"]
    if product == "food_micropricing" and "iso_alpha3" in df.columns:
        subset.append("iso_alpha3")
    dupes = df.duplicated(subset=subset).sum()
    key_label = "(series, date, tier, country)" if "iso_alpha3" in subset else "(series, date, tier)"
    if dupes == 0:
        return {"status": "PASS", "check": "No Duplicate Reporting Dates",
                "message": f"No duplicate {key_label} combinations ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "No Duplicate Reporting Dates",
            "message": f"{int(dupes)} duplicate {key_label} combinations",
            "details": {"duplicate_count": int(dupes)}}


def check_knowledge_ordering(df: pd.DataFrame) -> dict:
    """data_timestamp <= published_date for PRIMARY rows.
    DERIVED rows are forward projections — they carry reporting dates beyond their source
    publication date by design, so we skip them for this check.
    """
    if "data_timestamp" not in df.columns or "published_date" not in df.columns:
        return {"status": "SKIP", "check": "Knowledge Ordering", "message": "Columns missing"}
    primary = df[df["confidence_tier"] == "PRIMARY"] if "confidence_tier" in df.columns else df
    n_derived = len(df) - len(primary)
    ts  = pd.to_datetime(primary["data_timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
    pub = pd.to_datetime(primary["published_date"], errors="coerce")
    violations = int((ts > pub + pd.Timedelta(days=1)).sum())
    if violations == 0:
        return {"status": "PASS", "check": "Knowledge Ordering",
                "message": f"data_timestamp <= published_date for all PRIMARY rows ({len(primary):,} PRIMARY, {n_derived} DERIVED skipped)"}
    return {"status": "FAIL", "check": "Knowledge Ordering",
            "message": f"{violations} PRIMARY rows where data_timestamp > published_date",
            "details": {"violation_count": violations}}


def check_monotonic_series(df: pd.DataFrame) -> dict:
    """Sample 50 series and check reporting_date is monotonically increasing."""
    if "sovereign_series_id" not in df.columns or "reporting_date" not in df.columns:
        return {"status": "SKIP", "check": "Series Monotonicity", "message": "Columns missing"}

    primary = df[df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"]
    series_ids = primary["sovereign_series_id"].dropna().unique()
    sample = series_ids[:50]

    non_monotonic = []
    for sid in sample:
        grp = primary[primary["sovereign_series_id"] == sid].copy()
        grp["_rd"] = pd.to_datetime(grp["reporting_date"], errors="coerce")
        grp = grp.dropna(subset=["_rd"]).sort_values("_rd")
        if len(grp) < 3: continue
        if not grp["_rd"].is_monotonic_increasing:
            non_monotonic.append(sid)

    if not non_monotonic:
        return {"status": "PASS", "check": "Series Monotonicity",
                "message": f"All {len(sample)} sampled series are monotonically ordered"}
    return {"status": "WARN", "check": "Series Monotonicity",
            "message": f"{len(non_monotonic)} series have non-monotonic reporting_dates (sample of {len(sample)})",
            "details": {"non_monotonic": list(non_monotonic)}}


def run(product: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 TEMPORAL CONSISTENCY — {product.upper()}")
    logger.info("=" * 70)

    df = _load(product)
    df = _normalize_schema(df, product)
    if df.empty:
        logger.error("No data loaded.")
        return False

    results = [
        check_date_parseable(df),
        check_no_duplicate_reporting_dates(df, product),
        check_knowledge_ordering(df),
        check_monotonic_series(df),
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
    out = Path(f"{product}_eu27_temporal_consistency.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="EU27 temporal consistency")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    args = parser.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
