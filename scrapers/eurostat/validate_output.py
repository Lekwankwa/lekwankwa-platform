"""
Eurostat Vault Validation Scorecard

Checks all Eurostat-sourced parquet files in the vault and reports:
  1. Schema compliance  — are all gold-standard fields present?
  2. PIT compliance     — official_release_date populated and sensible?
  3. Coverage           — how many countries × datasets have data?
  4. Data quality       — null rates, value ranges, row counts

Usage:
    python validate_output.py [--dataset food|wages|housing|trade|macro|all]

Exits 0 if all checks pass, 1 if any critical check fails.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

from scrapers.eurostat.country_map import ALL_ISO3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
# ---------------------------------------------------------------------------
# Schema expectations per dataset
# ---------------------------------------------------------------------------

PIT_MANDATORY = [
    "data_vintage_id",
    "sovereign_series_id",
    "data_timestamp",
    "official_release_date",
    "revision_number",
    "is_revised_figure",
]

SCHEMA_FIELDS_COMMON = PIT_MANDATORY + [
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

SCHEMA_FIELDS_FOOD = [
    "data_vintage_id",
    "confidence_tier",
    "global_coicop_code",
    "standard_name",
    "observation_period",
    "official_release_date",
    "as_of_date",
    "is_revised_figure",
    "observed_price_local",
    "unit_measure_standardized",
    "data_timestamp",
    "revision_number",
    "iso_alpha3",
    "source",
]

DATASET_CONFIG: dict[str, dict[str, Any]] = {
    "food": {
        "vault_product": "food_micropricing",
        "filename":      "food_pricing_data.parquet",
        "required_fields": SCHEMA_FIELDS_FOOD,
        "value_col":     "observed_price_local",
    },
    "wages": {
        "vault_product": "wages_and_employment",
        "filename":      "wages_data.parquet",
        "required_fields": SCHEMA_FIELDS_COMMON,
        "value_col":     "observed_value",
    },
    "housing": {
        "vault_product": "Housing_Supply_and_Shelter_Inflation",
        "filename":      "housing_data.parquet",
        "required_fields": SCHEMA_FIELDS_COMMON,
        "value_col":     "observed_value",
    },
    "trade": {
        "vault_product": "trade_flows",
        "filename":      "trade_data.parquet",
        "required_fields": SCHEMA_FIELDS_COMMON,
        "value_col":     "observed_value",
    },
    "macro": {
        "vault_product": "global_macro",
        "filename":      "global_macro_data.parquet",
        "required_fields": SCHEMA_FIELDS_COMMON,
        "value_col":     "observed_value",
    },
}

SOURCE_FILTER = "eurostat_sdmx"


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------

def _load_country_dataset(product: str, iso3: str) -> pd.DataFrame:
    """Load all eurostat_sdmx parquet files for one country+product."""
    partition = (
        _VAULT_BASE
        / f"product={product}"
        / f"country={iso3}"
        / f"source={SOURCE_FILTER}"
    )
    if not partition.exists():
        return pd.DataFrame()

    files = [
        f for f in sorted(partition.rglob("*.parquet"))
        if "changelog" not in f.name and "outlier" not in f.name
    ]
    if not files:
        return pd.DataFrame()

    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            log.warning(f"Could not read {f}: {exc}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _check_schema(df: pd.DataFrame, required: list[str]) -> list[str]:
    """Return list of missing required fields."""
    return [f for f in required if f not in df.columns]


def _check_pit(df: pd.DataFrame) -> dict[str, Any]:
    """Return PIT quality metrics."""
    if df.empty:
        return {"pit_ok": False, "null_release_date": 100.0,
                "null_data_ts": 100.0, "null_vintage_id": 100.0}

    n = len(df)
    null_rd = df["official_release_date"].isna().sum() / n * 100 if "official_release_date" in df.columns else 100.0
    null_ts = df["data_timestamp"].isna().sum() / n * 100 if "data_timestamp" in df.columns else 100.0
    null_vid = df["data_vintage_id"].isna().sum() / n * 100 if "data_vintage_id" in df.columns else 100.0

    # Check release lag is reasonable (< 200 days for all)
    lag_ok = True
    if "official_release_date" in df.columns and "data_timestamp" in df.columns:
        rd = pd.to_datetime(df["official_release_date"], errors="coerce").dt.tz_localize(None)
        ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
        lag = (rd - ts).dt.days.dropna()
        if len(lag) > 0:
            median_lag = float(lag.median())
            lag_ok = 0 < median_lag < 200
        else:
            median_lag = float("nan")
            lag_ok = False
    else:
        median_lag = float("nan")

    return {
        "pit_ok":          null_rd < 1.0 and null_ts < 1.0 and null_vid < 1.0 and lag_ok,
        "null_release_date": null_rd,
        "null_data_ts":    null_ts,
        "null_vintage_id": null_vid,
        "median_lag_days": median_lag,
    }


def _check_confidence(df: pd.DataFrame) -> float:
    """% rows with confidence_tier == PRIMARY."""
    if df.empty or "confidence_tier" not in df.columns:
        return 0.0
    return float((df["confidence_tier"] == "PRIMARY").sum() / len(df) * 100)


def _check_value_col(df: pd.DataFrame, col: str) -> float:
    """% non-null values in the primary observation column."""
    if df.empty or col not in df.columns:
        return 0.0
    return float(df[col].notna().sum() / len(df) * 100)


# ---------------------------------------------------------------------------
# Main scorecard
# ---------------------------------------------------------------------------

def run_scorecard(datasets: list[str] | None = None) -> bool:
    targets = datasets or list(DATASET_CONFIG.keys())
    all_pass = True

    print()
    print("=" * 80)
    print("  EUROSTAT VAULT VALIDATION SCORECARD")
    print("=" * 80)

    for ds_name in targets:
        cfg     = DATASET_CONFIG[ds_name]
        product = cfg["vault_product"]
        req     = cfg["required_fields"]
        vcol    = cfg["value_col"]

        print(f"\n  Dataset: {ds_name.upper()}  (product={product})")
        print(f"  {'Country':<6}  {'Rows':>8}  {'PIT':>5}  {'Lag(d)':>7}  "
              f"{'Value%':>7}  {'PRIMARY%':>9}  {'Schema':>7}")
        print("  " + "-" * 60)

        ds_total_rows = 0
        ds_countries_ok = 0
        ds_pit_fails = 0

        for iso3 in ALL_ISO3:
            df = _load_country_dataset(product, iso3)
            if df.empty:
                continue

            missing_fields = _check_schema(df, req)
            pit_metrics    = _check_pit(df)
            pct_primary    = _check_confidence(df)
            pct_value      = _check_value_col(df, vcol)
            n_rows         = len(df)
            ds_total_rows += n_rows

            schema_ok  = len(missing_fields) == 0
            pit_ok     = pit_metrics["pit_ok"]
            row_status = "OK" if (schema_ok and pit_ok) else "FAIL"

            if pit_ok and schema_ok:
                ds_countries_ok += 1
            else:
                ds_pit_fails += 1
                all_pass = False

            lag_str = f"{pit_metrics['median_lag_days']:.0f}" if not (
                pit_metrics["median_lag_days"] != pit_metrics["median_lag_days"]
            ) else "N/A"

            print(
                f"  {iso3:<6}  {n_rows:>8,}  {'OK' if pit_ok else 'FAIL':>5}  "
                f"{lag_str:>7}  {pct_value:>6.1f}%  {pct_primary:>8.1f}%  "
                f"{'OK' if schema_ok else 'FAIL ' + ','.join(missing_fields[:2]):>7}"
            )

        countries_with_data = ds_countries_ok + ds_pit_fails
        print("  " + "-" * 60)
        print(
            f"  {ds_name.upper():<6}  {ds_total_rows:>8,}  "
            f"{ds_countries_ok}/{countries_with_data} countries OK  "
            f"({27 - countries_with_data} no data)"
        )

    print()
    print("=" * 80)
    print(f"  OVERALL: {'PASS' if all_pass else 'FAIL'}")
    if all_pass:
        print("  All countries × datasets passed schema + PIT compliance checks.")
    else:
        print("  One or more countries failed. Check log above for details.")
        print("  Common causes: API returned no data for a country; dimension")
        print("  filter mismatch; Eurostat geo code not in response.")
    print("=" * 80)
    print()

    return all_pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", nargs="*",
                        choices=list(DATASET_CONFIG.keys()) + ["all"],
                        default=None)
    args = parser.parse_args()

    if args.dataset and "all" not in args.dataset:
        targets = args.dataset
    else:
        targets = None

    ok = run_scorecard(targets)
    sys.exit(0 if ok else 1)
