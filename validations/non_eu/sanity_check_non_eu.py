"""
Stage 3 — Sanity Checks: GBR / CAN.

Value range checks and MoM spike detection calibrated for
RELEASE_DATE_ONLY sources (no vintage revision triangles).

Usage:
  python validations/non_eu/sanity_check_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import load, active_countries, ALL_PRODUCTS

# metric keyword → (min_value, max_value, max_mom_pct_change or None)
THRESHOLDS: dict[str, list[tuple]] = {
    "food_micropricing": [
        ("", 5.0, 800.0, 80.0),           # CPI food index; early-period values can dip below 10
    ],
    "wages_and_employment": [
        ("UNEMPLOYMENT", 0.0,  30.0,   50.0),
        ("EMPLOYMENT",   0.0,  60000.0, 15.0),
        ("EARNINGS",     0.0,  5000.0,  30.0),
        ("AWE",          0.0,  5000.0,  30.0),
        ("WAGE",         0.0,  200.0,   30.0),
        ("LFS",          0.0,  60000.0, 30.0),
    ],
    "Housing_Supply_and_Shelter_Inflation": [
        ("HPI",       10.0, 1000.0,  25.0),
        ("RPPI",      10.0, 1000.0,  25.0),
        ("NHPI",       0.0, 1000.0,  25.0),
        ("CPI",       10.0,  600.0,  20.0),
        ("HOUSING",   10.0, 1000.0,  25.0),
    ],
    "trade_flows": [
        ("EXPORT",  0.0,  1e9, 50.0),
        ("IMPORT",  -5e8, 1e9, 50.0),     # BOP imports can appear negative in some presentations
        ("BALANCE", -5e8, 5e8, None),
        ("BOP",     -5e8, 1e9, None),     # BOP balance can be negative (deficit)
        ("MERCH",   0.0,  1e9, 50.0),
        ("TRADE",   -5e8, 1e9, None),
    ],
    "global_macro": [
        ("GDP",    -1000.0, 5e9,   20.0),
        ("CPI",      -5.0, 600.0, None),  # lower bound relaxed: historical indices pre-1960 can be below 10
        ("HICP",     -5.0, 600.0, None),
        ("_RATE",   -20.0, 100.0, None),  # underscore prefix avoids matching "Annual Rates" in GDP series names
    ],
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def check_value_ranges(df: pd.DataFrame, product: str) -> dict:
    rules = THRESHOLDS.get(product, [])
    if not rules or "observed_value" not in df.columns:
        return {"status": "SKIP", "check": "Value Ranges", "message": "No thresholds or missing columns"}

    name_col = "macro_metric_name" if "macro_metric_name" in df.columns else None
    violations = []

    for keyword, vmin, vmax, _ in rules:
        if name_col and keyword:
            mask = df[name_col].str.upper().str.contains(keyword.upper(), na=False)
            subset = df[mask]
        else:
            subset = df

        if subset.empty:
            continue
        vals = pd.to_numeric(subset["observed_value"], errors="coerce")
        oob = subset[(vals < vmin) | (vals > vmax)]
        if not oob.empty:
            violations.append({
                "keyword": keyword or "(all)",
                "expected": f"[{vmin}, {vmax}]",
                "violation_count": len(oob),
                "min_seen": float(vals.min()),
                "max_seen": float(vals.max()),
            })

    if not violations:
        return {"status": "PASS", "check": "Value Ranges",
                "message": f"All values within expected ranges ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "Value Ranges",
            "message": f"{len(violations)} metric groups have out-of-range values",
            "details": {"violations": violations}}


def check_mom_spikes(df: pd.DataFrame, product: str) -> dict:
    rules = THRESHOLDS.get(product, [])
    if not rules or "observed_value" not in df.columns or "reporting_date" not in df.columns:
        return {"status": "SKIP", "check": "MoM Spike Detection", "message": "Missing columns"}

    name_col = "macro_metric_name" if "macro_metric_name" in df.columns else None
    spike_count = checked = 0

    for keyword, _, _, max_mom in rules:
        if max_mom is None:
            continue
        if name_col and keyword:
            mask = df[name_col].str.upper().str.contains(keyword.upper(), na=False)
            primary = df[mask & (df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY")]
        else:
            primary = df[df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"]

        if primary.empty:
            continue

        primary = primary.copy()
        primary["reporting_date"] = pd.to_datetime(primary["reporting_date"], errors="coerce")

        group_cols = ["iso_alpha3", "sovereign_series_id"] if "sovereign_series_id" in primary.columns else ["iso_alpha3"]
        for _, grp in primary.groupby(group_cols):
            grp = grp.sort_values("reporting_date")
            vals = pd.to_numeric(grp["observed_value"], errors="coerce")
            if len(vals) < 3:
                continue
            checked += 1
            pct = vals.pct_change().abs() * 100
            spike_count += int((pct > max_mom).sum())

    if spike_count == 0:
        return {"status": "PASS", "check": "MoM Spike Detection",
                "message": f"No MoM spikes detected across {checked} series"}
    return {"status": "WARN", "check": "MoM Spike Detection",
            "message": f"{spike_count} MoM spike(s) across {checked} series",
            "details": {"spike_count": spike_count, "series_checked": checked}}


def check_country_coverage(df: pd.DataFrame, product: str) -> dict:
    if "iso_alpha3" not in df.columns:
        return {"status": "SKIP", "check": "Country Coverage", "message": "iso_alpha3 missing"}
    from validations.non_eu._loader import active_countries
    expected = set(active_countries(product).keys())
    found    = set(df["iso_alpha3"].dropna().unique())
    missing  = sorted(expected - found)
    pct = round(len(found) / len(expected) * 100, 1) if expected else 0.0
    if not missing:
        return {"status": "PASS", "check": "Country Coverage",
                "message": f"All {len(expected)} countries present ({pct}%): {sorted(found)}"}
    return {"status": "WARN", "check": "Country Coverage",
            "message": f"{len(missing)} countries missing ({pct}% coverage)",
            "details": {"missing": missing}}


def check_no_null_values(df: pd.DataFrame) -> dict:
    if "observed_value" not in df.columns:
        return {"status": "SKIP", "check": "Null Values", "message": "observed_value missing"}
    nulls = int(df["observed_value"].isna().sum())
    if nulls == 0:
        return {"status": "PASS", "check": "Null Values",
                "message": f"No null observed_value ({len(df):,} rows)"}
    return {"status": "FAIL", "check": "Null Values",
            "message": f"{nulls:,} null observed_value ({nulls/len(df)*100:.2f}%)",
            "details": {"null_count": nulls}}


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU SANITY CHECKS — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    df = load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False
    logger.info(f"  Loaded {len(df):,} rows")

    results = [
        check_value_ranges(df, product),
        check_mom_spikes(df, product),
        check_country_coverage(df, product),
        check_no_null_values(df),
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
    out = Path(f"{product}_non_eu_sanity_check.json")
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
