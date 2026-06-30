"""
Non-EU Country Vault Validation Scorecard
Covers: GBR (ONS), CAN (StatCan CSV), AUS (ABS SDMX), NOR (SSB PX-Web)

Checks per country × dataset:
  1. Schema compliance  — PIT mandatory fields present?
  2. PIT compliance     — official_release_date populated and lag reasonable?
  3. Data quality       — value column non-null %, row counts
  4. Confidence tier    — % rows with confidence_tier == PRIMARY

Usage:
    cd backtesting
    python scrapers/validate_non_eu.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_VAULT_BASE = Path(__file__).resolve().parents[1] / "lekwankwa-historical-vault"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Country → source mapping
# ---------------------------------------------------------------------------

COUNTRIES = [
    ("GBR", "United Kingdom",  "ons_api"),
    ("CAN", "Canada",          "statcan_csv"),
    ("AUS", "Australia",       "abs_sdmx"),
    ("NOR", "Norway",          "ssb_statbank"),
]

# vault_product → (friendly name, parquet filename, value_col, skip_if_missing)
DATASETS: dict[str, tuple[str, str, str, bool]] = {
    "food_micropricing":              ("FOOD",   "food_pricing_data.parquet",   "observed_value", False),
    "wages_and_employment":           ("WAGES",  "wages_employment_data.parquet","observed_value", False),
    "Housing_Supply_and_Shelter_Inflation": ("HOUSE", "housing_data.parquet",   "observed_value", True),
    "trade_flows":                    ("TRADE",  "trade_flows_data.parquet",    "observed_value", False),
    "global_macro":                   ("MACRO",  "global_macro_data.parquet",   "observed_value", False),
}

PIT_MANDATORY = [
    "data_vintage_id",
    "sovereign_series_id",
    "data_timestamp",
    "official_release_date",
    "revision_number",
    "is_revised_figure",
]

SCHEMA_COMMON = PIT_MANDATORY + [
    "confidence_tier",
    "macro_metric_name",
    "reporting_date",
    "as_of_date",
    "observed_value",
    "unit_of_measure",
    "iso_alpha3",
    "source",
    "source_agency",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(product: str, iso3: str, source: str, filename: str) -> pd.DataFrame:
    partition = _VAULT_BASE / f"product={product}" / f"country={iso3}" / f"source={source}"
    if not partition.exists():
        return pd.DataFrame()
    files = sorted(partition.rglob(filename))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            log.warning("Could not read %s: %s", f, exc)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _pit_metrics(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"pit_ok": False, "null_rd": 100.0, "median_lag": float("nan")}
    n = len(df)
    null_rd  = df["official_release_date"].isna().sum() / n * 100 if "official_release_date" in df.columns else 100.0
    null_ts  = df["data_timestamp"].isna().sum() / n * 100 if "data_timestamp" in df.columns else 100.0
    null_vid = df["data_vintage_id"].isna().sum() / n * 100 if "data_vintage_id" in df.columns else 100.0

    lag_ok = True
    median_lag = float("nan")
    if "official_release_date" in df.columns and "data_timestamp" in df.columns:
        rd  = pd.to_datetime(df["official_release_date"], errors="coerce").dt.tz_localize(None)
        ts  = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
        lag = (rd - ts).dt.days.dropna()
        if len(lag) > 0:
            median_lag = float(lag.median())
            lag_ok = 0 < median_lag < 365
        else:
            lag_ok = False

    return {
        "pit_ok":    null_rd < 1.0 and null_ts < 1.0 and null_vid < 1.0 and lag_ok,
        "null_rd":   null_rd,
        "median_lag": median_lag,
    }


def _pct_primary(df: pd.DataFrame) -> float:
    if df.empty or "confidence_tier" not in df.columns:
        return 0.0
    return float((df["confidence_tier"] == "PRIMARY").sum() / len(df) * 100)


def _pct_nonnull(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(df[col].notna().sum() / len(df) * 100)


# ---------------------------------------------------------------------------
# Scorecard
# ---------------------------------------------------------------------------

def run() -> bool:
    all_pass = True
    grand_total = 0
    coverage_table: list[tuple] = []  # (iso3, product, rows, pit_ok, lag, schema_ok)

    print()
    print("=" * 80)
    print("  NON-EU VAULT VALIDATION SCORECARD  (GBR / CAN / AUS / NOR)")
    print("=" * 80)

    for product, (friendly, filename, vcol, skip_missing) in DATASETS.items():
        print(f"\n  Dataset: {friendly}  (product={product})")
        print(f"  {'Country':<6}  {'Rows':>8}  {'PIT':>5}  {'Lag(d)':>7}  "
              f"{'Value%':>7}  {'PRIMARY%':>9}  {'Schema':>7}")
        print("  " + "-" * 62)

        ds_rows = 0
        ds_ok = 0
        ds_fail = 0

        for iso3, _name, source in COUNTRIES:
            df = _load(product, iso3, source, filename)

            if df.empty:
                if not skip_missing:
                    print(f"  {iso3:<6}  {'0':>8}  {'N/A':>5}  {'N/A':>7}  {'N/A':>7}  {'N/A':>9}  NO DATA")
                    coverage_table.append((iso3, friendly, 0, False, float("nan"), False))
                continue

            missing  = [f for f in SCHEMA_COMMON if f not in df.columns]
            pit      = _pit_metrics(df)
            pct_val  = _pct_nonnull(df, vcol)
            pct_prim = _pct_primary(df)
            n        = len(df)
            schema_ok = len(missing) == 0
            row_ok    = schema_ok and pit["pit_ok"]

            ds_rows += n
            grand_total += n
            if row_ok:
                ds_ok += 1
            else:
                ds_fail += 1
                all_pass = False

            lag_str = f"{pit['median_lag']:.0f}" if pit["median_lag"] == pit["median_lag"] else "N/A"
            schema_str = "OK" if schema_ok else f"MISS:{','.join(missing[:2])}"
            coverage_table.append((iso3, friendly, n, pit["pit_ok"], pit["median_lag"], schema_ok))

            print(
                f"  {iso3:<6}  {n:>8,}  {'OK' if pit['pit_ok'] else 'FAIL':>5}  "
                f"{lag_str:>7}  {pct_val:>6.1f}%  {pct_prim:>8.1f}%  {schema_str}"
            )

        total_with_data = ds_ok + ds_fail
        print("  " + "-" * 62)
        print(f"  TOTAL   {ds_rows:>8,}  {ds_ok}/{total_with_data} countries OK")

    print()
    print("=" * 80)
    print("  PIT COVERAGE SUMMARY  (4 new countries × 5 datasets)")
    print("=" * 80)
    print(f"  {'Country':<6}  {'Dataset':<8}  {'Rows':>8}  {'PIT':>5}  {'Lag(d)':>7}  {'Schema':>7}")
    print("  " + "-" * 55)
    for iso3, ds, rows, pit_ok, lag, schema_ok in coverage_table:
        lag_str = f"{lag:.0f}" if lag == lag else "N/A"
        print(f"  {iso3:<6}  {ds:<8}  {rows:>8,}  {'OK' if pit_ok else 'FAIL':>5}  "
              f"{lag_str:>7}  {'OK' if schema_ok else 'FAIL':>7}")
    print("  " + "-" * 55)
    print(f"  Grand total rows: {grand_total:,}")
    print()
    print("=" * 80)
    print(f"  OVERALL: {'PASS' if all_pass else 'FAIL'}")
    print("=" * 80)
    print()

    return all_pass


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
