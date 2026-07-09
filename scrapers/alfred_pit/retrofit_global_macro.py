"""
ALFRED PIT Retrofit — Global Macro Baseline

Fetches complete vintage/revision history from ALFRED for core FRED macro series:
  GDP       Nominal GDP (quarterly, from 1991)
  GDPC1     Real GDP chained 2017$ (quarterly, from 1991)
  INDPRO    Industrial Production Index (monthly, from 1927)
  CPIAUCSL  CPI All Items SA (monthly, from 1972)
  CPIAUCNS  CPI All Items NSA (monthly)
  UNRATE    Unemployment Rate (monthly, from 1960)
  PAYEMS    Total Nonfarm Payrolls (monthly, from 1955)
  FEDFUNDS  Federal Funds Rate (monthly)
  GS10      10-Year Treasury Yield (monthly)
  PCEPI     PCE Price Index (monthly)

Also patches the existing IMF WEO records with official_release_date,
is_revised_figure, and as_of_date which are currently missing.

Writes FRED-sourced vintage rows to:
  lekwankwa-historical-vault/product=global_macro/
    country=USA/source=alfred_vintage/year=YYYY/month=MM/global_macro_data.parquet

Patches IMF WEO records in-place (adds the 3 missing columns).

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.vault_io import get_vault_root
from scrapers.alfred_pit.alfred_client import fetch_all_vintages, build_vintage_rows
from scrapers.alfred_pit.series_map import GLOBAL_MACRO_SERIES

# ---------------------------------------------------------------------------
# Release-date correction for ALFRED floor dates
# ---------------------------------------------------------------------------
# ALFRED did not backfill true historical release dates for all series.
# For several series, the earliest ALFRED vintage date is the date ALFRED
# first started tracking revisions (the "ALFRED floor"), not the date the
# original observation was published by BEA/BLS/Fed.  This causes the v1
# row for every pre-floor observation to carry an implausibly late
# official_release_date (e.g. 1991-12-04 for a 1960 GDP observation).
#
# Known ALFRED floors by series:
#   GDP/GDPC1   : 1991-12-04  (pre-1991 obs)
#   CPIAUCSL    : 1994-02-17  (pre-1975 obs)
#   FEDFUNDS/GS10: 1996-12-03 (pre-1997 obs)
#   PCEPI       : 2000-08-01  (all pre-2000 obs)
#   PAYEMS      : 1961-11-03  (pre-1960 obs)
#   UNRATE      : 1960-03-15  (pre-1960 obs)
#   INDPRO/CPIAUCNS: genuine history back to 1950 -- not affected
#
# Fix: any v1 row with lag > 180 days gets its official_release_date replaced
# with obs_date + series-specific BEA/BLS release schedule estimate.
# Revision rows (n > 1) are never modified.

_RELEASE_DATE_THRESHOLD_DAYS = 180

_ESTIMATED_INITIAL_LAG_DAYS: dict = {
    "GDP":      120,   # BEA advance GDP: quarter-end + 30 d = quarter-start + 120 d
    "GDPC1":    120,   # same BEA advance GDP release
    "CPIAUCSL":  47,   # BLS CPI: month-end + 16 d
    "CPIAUCNS":  47,   # same (already correct; threshold protects them)
    "UNRATE":    38,   # BLS Employment Situation: month-end + 7 d
    "PAYEMS":    38,   # same BLS Employment Situation
    "FEDFUNDS":  35,   # Fed H.15 monthly average
    "GS10":      35,   # Fed H.15 10-year Treasury
    "PCEPI":     60,   # BEA Personal Income and Outlays: month-end + 30 d
    "INDPRO":    47,   # Fed G.17 (already correct; threshold protects it)
}


def _fix_initial_release_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Repair ALFRED floor dates in v1 rows of a freshly-fetched vintage DataFrame.

    Operates on the output of build_vintage_rows() before it is written to
    the vault.  Modifies official_release_date, published_date, and as_of_date
    for rows where revision_number == 1 and the computed lag exceeds the
    threshold.  All other rows are returned unchanged.
    """
    if df.empty:
        return df

    df = df.copy()
    obs_dt  = pd.to_datetime(df["data_timestamp"],       errors="coerce")
    rel_dt  = pd.to_datetime(df["official_release_date"], errors="coerce")
    lag     = (rel_dt - obs_dt).dt.days

    mask = (df["revision_number"].astype(float) == 1.0) & (lag > _RELEASE_DATE_THRESHOLD_DAYS)
    if not mask.any():
        return df

    for idx in df.index[mask]:
        sid     = str(df.at[idx, "sovereign_series_id"])
        obs     = obs_dt[idx]
        est_lag = _ESTIMATED_INITIAL_LAG_DAYS.get(sid, 45)
        est     = obs + pd.Timedelta(days=est_lag)
        df.at[idx, "official_release_date"] = est.strftime("%Y-%m-%d")
        if "published_date" in df.columns:
            df.at[idx, "published_date"] = est.strftime("%Y-%m-%d")
        if "as_of_date" in df.columns:
            df.at[idx, "as_of_date"] = est.isoformat() + "Z"

    n_fixed = int(mask.sum())
    logger.debug(f"_fix_initial_release_dates: corrected {n_fixed} v1 rows")
    return df

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_global_macro.log")],
)
logger = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
VAULT_ROOT_ALFRED = _VAULT_BASE / "product=global_macro/country=USA/source=alfred_vintage"
VAULT_ROOT_IMF    = _VAULT_BASE / "product=global_macro/country=USA/source=imf_weo"
FILE_NAME = "global_macro_data.parquet"

SCHEMA_CONSTANTS = {
    "iso_alpha3":          "USA",
    "country_name":        "United States",
    "country_code":        "US",
    "market_tier":         "Developed",
    "source":              "alfred_vintage",
    "source_agency":       "FRED_BEA_BLS_FRB",
    "source_sub_category": "NATIONAL_ACCOUNTS",
    "portal_url":          "https://alfred.stlouisfed.org/",
    "extraction_method":   "api",
    "product":             "global_macro",
    "sdmx_frequency":      "M",
    "is_forecast":         False,
    "data_quality_certified": True,
}

# GDP/GDPC1 are quarterly — mark frequency accordingly
QUARTERLY_SERIES = {"GDP", "GDPC1"}


def _write_alfred_partition(df: pd.DataFrame, year: int, month: int) -> None:
    part = VAULT_ROOT_ALFRED / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / FILE_NAME
    if out.exists():
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")
    combined.to_parquet(out, index=False, engine="pyarrow")
    logger.info(f"  Written {len(combined)} rows -> {out}")


def retrofit_fred_series(vault_id: str, alfred_id: str, metric: str, unit: str) -> int:
    freq = "Q" if alfred_id in QUARTERLY_SERIES else "M"
    constants = {**SCHEMA_CONSTANTS, "sdmx_frequency": freq}

    logger.info(f"Fetching ALFRED: {alfred_id} ({metric})")
    fred_df = fetch_all_vintages(alfred_id)
    if fred_df.empty:
        logger.warning(f"  No data for {alfred_id}")
        return 0

    vintage_rows = build_vintage_rows(
        series_id=     alfred_id,
        fred_df=       fred_df,
        source_prefix= "FRED",
        schema_fields= {
            **constants,
            "sovereign_series_id": alfred_id,
            "macro_metric_name":   metric,
            "unit_of_measure":     unit,
        },
        vintage_id_fn=lambda sid, date, n: f"FRED-{alfred_id}-{date.strftime('%Y-%m')}-v{n}",
    )
    if vintage_rows.empty:
        return 0

    # Correct ALFRED floor dates on v1 rows before writing to vault.
    vintage_rows = _fix_initial_release_dates(vintage_rows)

    vintage_rows["data_timestamp"] = pd.to_datetime(vintage_rows["data_timestamp"])
    for (year, month), grp in vintage_rows.groupby([
        vintage_rows["data_timestamp"].dt.year,
        vintage_rows["data_timestamp"].dt.month,
    ]):
        _write_alfred_partition(grp.copy(), int(year), int(month))

    logger.info(f"  {len(vintage_rows)} vintage rows for {alfred_id}")
    return len(vintage_rows)


def patch_imf_weo_records() -> int:
    """
    Add official_release_date, is_revised_figure, as_of_date to existing
    IMF WEO vault records that are currently missing these 3 PIT columns.

    IMF WEO release schedule:
      - April edition:   official_release_date = April 1 of that year
      - October edition: official_release_date = October 1 of that year
    The IMF DataMapper only publishes one value per year, so v1 only,
    is_revised_figure = False for all.
    """
    if not VAULT_ROOT_IMF.exists():
        logger.info("No IMF WEO vault found — skipping patch")
        return 0

    files = list(VAULT_ROOT_IMF.rglob("*_data.parquet"))
    if not files:
        logger.info("No IMF WEO parquet files found")
        return 0

    patched = 0
    now_utc = datetime.now(timezone.utc).isoformat()

    for f in files:
        df = pd.read_parquet(f)
        changed = False

        if "official_release_date" not in df.columns:
            # Derive from data_timestamp year — use April 1 as default WEO edition
            if "data_timestamp" in df.columns:
                years = pd.to_datetime(df["data_timestamp"], errors="coerce").dt.year
                df["official_release_date"] = years.apply(
                    lambda y: f"{int(y)}-04-01" if pd.notna(y) else None
                )
            else:
                df["official_release_date"] = None
            changed = True

        if "is_revised_figure" not in df.columns:
            df["is_revised_figure"] = False
            changed = True

        if "as_of_date" not in df.columns:
            df["as_of_date"] = df.get("official_release_date", None)
            changed = True

        if "published_date" not in df.columns:
            df["published_date"] = df.get("official_release_date", None)
            changed = True

        if changed:
            df.to_parquet(f, index=False, engine="pyarrow")
            patched += len(df)
            logger.info(f"  Patched {len(df)} IMF WEO rows in {f}")

    logger.info(f"IMF WEO patch complete: {patched} rows updated")
    return patched


def run() -> None:
    logger.info("=" * 70)
    logger.info("GLOBAL MACRO — ALFRED PIT RETROFIT")
    logger.info("=" * 70)
    total = 0
    for vault_id, (alfred_id, metric, unit) in GLOBAL_MACRO_SERIES.items():
        total += retrofit_fred_series(vault_id, alfred_id, metric, unit)

    logger.info("\nPatching IMF WEO records with missing PIT columns...")
    total += patch_imf_weo_records()
    logger.info(f"\nDone. {total} rows written/patched")


if __name__ == "__main__":
    run()
