"""
Sanity checks — EU27 Eurostat products.

Per-product value range checks, MoM change spike detection, and
country/metric coverage completeness.

Usage:
  python validations/eurostat/sanity_check_eurostat.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT  = Path("lekwankwa-historical-vault")
SOURCE = "eurostat_sdmx"
EU27   = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
          "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

# Per-product: metric keyword → (min_value, max_value, max_mom_pct_change)
PRODUCT_THRESHOLDS: dict[str, list[tuple[str, float, float, float]]] = {
    "wages_and_employment": [
        ("UNEMPLOYMENT",   0.0,    35.0,   50.0),   # unemployment rate %
        ("EMPLOYMENT",     0.0, 60000.0,   15.0),   # thousands of persons
        ("WAGES",          0.0, 5000000.0, 30.0),   # millions EUR (annual D11)
    ],
    "Housing_Supply_and_Shelter_Inflation": [
        ("HOUSE PRICE",    10.0,  500.0,  25.0),    # HPI index 2015=100
        ("PERMIT",          0.0, 200000.0, 200.0),  # units authorized (count or index)
        ("RENT",           10.0,  300.0,  10.0),    # HICP rent index 2015=100; Eastern EU early data can reach ~18
        ("CPI",            10.0,  300.0,  10.0),    # CPI-based shelter index
    ],
    "trade_flows": [
        ("EXPORT",          0.0, 1500000.0, 30.0),  # million EUR quarterly
        ("IMPORT",          0.0, 1500000.0, 30.0),
    ],
    "global_macro": [
        ("GDP",          -500.0, 15000000.0, 20.0),  # million EUR
        ("GROSS FIXED",  -500.0, 5000000.0,  25.0),  # GFCF
        ("HICP",          -10.0,      70.0,  None),  # HICP annual rate of change %; negatives = deflation; max ~57% seen during 2022 energy crisis; MoM disabled (rate-of-change pct_change swings are not meaningful)
        ("RATE",          -50.0,    100.0,   None),  # interest/growth rates
    ],
    "food_micropricing": [
        ("",             10.0,   600.0,  80.0),  # any HICP food subcategory index 2015=100; oils/fruit/veg volatile
    ],
}

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


def _load_product(product: str) -> pd.DataFrame:
    base = VAULT / f"product={product}"
    frames = []
    for iso in EU27:
        src = base / f"country={iso}" / f"source={SOURCE}"
        if not src.exists(): continue
        for f in sorted(src.rglob("*.parquet")):
            if "outlier" in f.name or "changelog" in f.name: continue
            try:
                frames.append(pd.read_parquet(f))
            except Exception: pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def check_value_ranges(df: pd.DataFrame, product: str) -> dict:
    thresholds = PRODUCT_THRESHOLDS.get(product, [])
    if not thresholds or "observed_value" not in df.columns or "macro_metric_name" not in df.columns:
        return {"status": "SKIP", "check": "Value Ranges", "message": "No thresholds defined or missing columns"}

    violations: list[dict] = []
    for keyword, vmin, vmax, _ in thresholds:
        mask = df["macro_metric_name"].str.upper().str.contains(keyword.upper(), na=False)
        subset = df[mask]
        if subset.empty: continue
        vals = pd.to_numeric(subset["observed_value"], errors="coerce")
        out_of_range = subset[(vals < vmin) | (vals > vmax)]
        if not out_of_range.empty:
            violations.append({
                "metric_keyword": keyword,
                "expected": f"[{vmin}, {vmax}]",
                "violation_count": len(out_of_range),
                "min_seen": float(vals.min()),
                "max_seen": float(vals.max()),
            })

    if not violations:
        return {"status": "PASS", "check": "Value Ranges",
                "message": f"All observed_value within expected ranges ({len(df):,} rows checked)"}
    return {"status": "FAIL", "check": "Value Ranges",
            "message": f"{len(violations)} metric groups have out-of-range values",
            "details": {"violations": violations}}


def check_mom_spikes(df: pd.DataFrame, product: str) -> dict:
    thresholds = PRODUCT_THRESHOLDS.get(product, [])
    if not thresholds or "observed_value" not in df.columns or "reporting_date" not in df.columns:
        return {"status": "SKIP", "check": "MoM Spike Detection", "message": "Missing columns"}

    spike_count = 0
    checked_series = 0
    for keyword, _, _, max_mom in thresholds:
        if max_mom is None: continue
        mask = (df["macro_metric_name"].str.upper().str.contains(keyword.upper(), na=False) &
                (df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"))
        subset = df[mask].copy()
        if subset.empty: continue

        subset["reporting_date"] = pd.to_datetime(subset["reporting_date"], errors="coerce")
        for _, grp in subset.groupby(["iso_alpha3", "sovereign_series_id"]):
            grp = grp.sort_values("reporting_date")
            vals = pd.to_numeric(grp["observed_value"], errors="coerce")
            if len(vals) < 3: continue
            checked_series += 1
            pct_chg = vals.pct_change().abs() * 100
            spikes = int((pct_chg > max_mom).sum())
            spike_count += spikes

    if spike_count == 0:
        return {"status": "PASS", "check": "MoM Spike Detection",
                "message": f"No MoM spikes detected across {checked_series} series"}
    return {"status": "WARN", "check": "MoM Spike Detection",
            "message": f"{spike_count} MoM spike(s) detected across {checked_series} series",
            "details": {"spike_count": spike_count, "series_checked": checked_series}}


def check_country_coverage(df: pd.DataFrame) -> dict:
    if "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "Country Coverage", "message": "iso_alpha3 missing"}
    found = set(df["iso_alpha3"].dropna().unique())
    missing = sorted(set(EU27) - found)
    pct = round(len(found) / len(EU27) * 100, 1)
    if not missing:
        return {"status": "PASS", "check": "Country Coverage",
                "message": f"All 27 EU countries present ({pct}%)"}
    return {"status": "WARN", "check": "Country Coverage",
            "message": f"{len(missing)} EU countries missing from data ({pct}% coverage)",
            "details": {"missing_countries": missing}}


def check_no_null_observed_values(df: pd.DataFrame) -> dict:
    if "observed_value" not in df.columns:
        return {"status": "SKIP", "check": "Null Observed Values", "message": "Column missing"}
    nulls = int(df["observed_value"].isna().sum())
    if nulls == 0:
        return {"status": "PASS", "check": "Null Observed Values",
                "message": f"No null observed_value ({len(df):,} rows)"}
    pct = round(nulls / len(df) * 100, 2)
    return {"status": "FAIL", "check": "Null Observed Values",
            "message": f"{nulls:,} null observed_value ({pct}%)",
            "details": {"null_count": nulls, "null_pct": pct}}


def run(product: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 SANITY CHECKS — {product.upper()}")
    logger.info("=" * 70)

    df = _load_product(product)
    df = _normalize_schema(df, product)
    if df.empty:
        logger.error("No data loaded.")
        return False

    logger.info(f"  Loaded {len(df):,} rows from {df.get('iso_alpha3', pd.Series()).nunique()} countries")

    results = [
        check_value_ranges(df, product),
        check_mom_spikes(df, product),
        check_country_coverage(df),
        check_no_null_observed_values(df),
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
        "results": results,
    }
    out = Path(f"{product}_eu27_sanity_check.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="EU27 Eurostat sanity checks")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    args = parser.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
