"""
EU27 Monthly Gap Fill — Quarterly Carry-Forward Interpolation

Problem: Both housing (sts_cobp_q / prc_hpi_q) and trade (namq_10_gdp)
are quarterly sources. Monthly building permits (sts_cobp_m) fill months
for 2000-2023 for most countries, but 2024+ and Poland have no monthly data.
Trade has NO monthly source at all.

Fix: For each quarterly vault row (month = 01/04/07/10), if months +1 and +2
of that quarter are absent, generate carry-forward rows from the quarterly value.

Carry-forward row properties:
  reporting_date        carry month  (YYYY-MM-01)
  observed_value        copied from source quarterly row
  official_release_date copied from source quarterly row (unchanged lag)
  data_vintage_id       source VID with YYYY-MM → carry month, -CF appended
  interpolation_method  "QUARTERLY_CARRY_FORWARD"
  is_interpolated       True
  data_quality_certified False  (derived, not primary survey value)

Output files (written into same Hive tree):
  Housing vault  -> permits_monthly_fill.parquet
  Trade vault    -> trade_monthly_fill.parquet

Idempotent: write_partition deduplicates on data_vintage_id so re-runs are safe.
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import pandas as pd

from scrapers.utilities.vault_io import get_vault_root
_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))

from scrapers.eurostat.country_map import ALL_ISO3
from scrapers.eurostat.revision_tracker import write_partition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
SOURCE      = "eurostat_sdmx"
QUARTER_MONTHS = {1, 4, 7, 10}

# Vintage-ID date pattern: capture the YYYY-MM at end of VID before -vN
_VID_DATE_RE = re.compile(r"^(.*-)(\d{4}-\d{2})(-v\d+)$")


def _make_carry_vid(source_vid: str, carry_date: pd.Timestamp) -> str:
    """
    Replace the YYYY-MM portion in a vintage ID with carry_date and append -CF.

    Example:
        EUROSTAT-PERMIT-DEU-2024-01-v1  ->  EUROSTAT-PERMIT-DEU-2024-02-CF-v1
        EUROSTAT-DEU-EXPORTS_EUR-2024-01-v1  ->  EUROSTAT-DEU-EXPORTS_EUR-2024-02-CF-v1
    """
    m = _VID_DATE_RE.match(str(source_vid))
    if m:
        return f"{m.group(1)}{carry_date.strftime('%Y-%m')}-CF{m.group(3)}"
    # fallback: just append carry month
    return f"{source_vid}-{carry_date.strftime('%Y-%m')}-CF"


def _load_all_parquet(vault_root: Path, filenames: list[str]) -> pd.DataFrame:
    """Load all parquet files matching filenames under vault_root, return combined DataFrame."""
    frames: list[pd.DataFrame] = []
    if not vault_root.exists():
        return pd.DataFrame()
    for fname in filenames:
        for fpath in vault_root.rglob(fname):
            try:
                frames.append(pd.read_parquet(fpath))
            except Exception:
                pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _generate_carry_rows(df: pd.DataFrame, existing_months: set[str]) -> pd.DataFrame:
    """
    For each quarterly row in df, emit carry-forward rows for +1 and +2 months
    if those YYYY-MM-DD strings are absent from existing_months.

    Groups by sovereign_series_id so each series carries forward independently.
    """
    carry_rows: list[dict] = []
    required_cols = {"reporting_date", "data_vintage_id", "sovereign_series_id"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame()

    df = df.copy()
    df["_rd"] = pd.to_datetime(df["reporting_date"], errors="coerce")
    df = df.dropna(subset=["_rd"])

    # Only source from true quarter-start months
    df = df[df["_rd"].dt.month.isin(QUARTER_MONTHS)]

    for _, row in df.iterrows():
        q_date = row["_rd"]
        for offset in [1, 2]:
            carry_month = q_date.month + offset
            carry_year  = q_date.year
            if carry_month > 12:
                carry_month -= 12
                carry_year  += 1
            carry_date    = pd.Timestamp(carry_year, carry_month, 1)
            carry_date_str = carry_date.strftime("%Y-%m-%d")

            if carry_date_str in existing_months:
                continue   # already have data for this month (monthly source)

            new_vid = _make_carry_vid(row["data_vintage_id"], carry_date)

            new_row = dict(row)
            new_row.pop("_rd", None)
            new_row["reporting_date"]        = carry_date_str
            new_row["data_vintage_id"]       = new_vid
            new_row["data_timestamp"]        = carry_date.isoformat() + "Z"
            new_row["interpolation_method"]  = "QUARTERLY_CARRY_FORWARD"
            new_row["is_interpolated"]       = True
            new_row["data_quality_certified"] = False
            carry_rows.append(new_row)

    return pd.DataFrame(carry_rows)


# ---------------------------------------------------------------------------
# Housing fill
# ---------------------------------------------------------------------------

def _fill_housing_country(iso3: str) -> int:
    vault_root = (
        _VAULT_BASE
        / f"product=Housing_Supply_and_Shelter_Inflation"
        / f"country={iso3}"
        / f"source={SOURCE}"
    )
    if not vault_root.exists():
        return 0

    # Load all three housing file types
    all_source_files = [
        "housing_data.parquet",
        "permits_eu27_data.parquet",
        "hpi_purchase_data.parquet",
    ]
    df_all = _load_all_parquet(vault_root, all_source_files)
    if df_all.empty:
        return 0

    # Build set of all existing reporting_date × sovereign_series_id pairs
    # (so monthly sts_cobp_m data prevents carry-forward for the same series+month)
    existing_by_series: dict[str, set[str]] = {}
    if "sovereign_series_id" in df_all.columns:
        for sid, grp in df_all.groupby("sovereign_series_id"):
            existing_by_series[str(sid)] = set(grp["reporting_date"].dropna().astype(str))
    else:
        existing_by_series["_ALL"] = set(df_all["reporting_date"].dropna().astype(str))

    # Source quarterly rows: only from housing_data.parquet (the quarterly series)
    df_quarterly = _load_all_parquet(vault_root, ["housing_data.parquet"])
    if df_quarterly.empty:
        return 0

    total_written = 0

    if "sovereign_series_id" in df_quarterly.columns:
        for sid, grp in df_quarterly.groupby("sovereign_series_id"):
            existing = existing_by_series.get(str(sid), set())
            carry_df = _generate_carry_rows(grp, existing)
            if carry_df.empty:
                continue
            carry_df["_obs_ts"] = pd.to_datetime(
                carry_df["data_timestamp"], errors="coerce", utc=True
            )
            for (yr, mo), period_grp in carry_df.groupby([
                carry_df["_obs_ts"].dt.year, carry_df["_obs_ts"].dt.month
            ]):
                out = period_grp.drop(columns=["_obs_ts"])
                write_partition(out, vault_root, int(yr), int(mo),
                                "permits_monthly_fill.parquet")
                total_written += len(out)
    else:
        existing = set(df_all["reporting_date"].dropna().astype(str))
        carry_df = _generate_carry_rows(df_quarterly, existing)
        if not carry_df.empty:
            carry_df["_obs_ts"] = pd.to_datetime(
                carry_df["data_timestamp"], errors="coerce", utc=True
            )
            for (yr, mo), period_grp in carry_df.groupby([
                carry_df["_obs_ts"].dt.year, carry_df["_obs_ts"].dt.month
            ]):
                out = period_grp.drop(columns=["_obs_ts"])
                write_partition(out, vault_root, int(yr), int(mo),
                                "permits_monthly_fill.parquet")
                total_written += len(out)

    return total_written


# ---------------------------------------------------------------------------
# Trade fill
# ---------------------------------------------------------------------------

def _fill_trade_country(iso3: str) -> int:
    vault_root = (
        _VAULT_BASE
        / f"product=trade_flows"
        / f"country={iso3}"
        / f"source={SOURCE}"
    )
    if not vault_root.exists():
        return 0

    df_quarterly = _load_all_parquet(vault_root, ["trade_data.parquet"])
    if df_quarterly.empty:
        return 0

    # Trade has no monthly source — all non-quarter months are missing
    existing_months: set[str] = set()
    if "reporting_date" in df_quarterly.columns:
        existing_months = set(df_quarterly["reporting_date"].dropna().astype(str))

    carry_df = _generate_carry_rows(df_quarterly, existing_months)
    if carry_df.empty:
        return 0

    total_written = 0
    carry_df["_obs_ts"] = pd.to_datetime(
        carry_df["data_timestamp"], errors="coerce", utc=True
    )
    for (yr, mo), period_grp in carry_df.groupby([
        carry_df["_obs_ts"].dt.year, carry_df["_obs_ts"].dt.month
    ]):
        out = period_grp.drop(columns=["_obs_ts"])
        write_partition(out, vault_root, int(yr), int(mo),
                        "trade_monthly_fill.parquet")
        total_written += len(out)

    return total_written


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run() -> dict[str, int]:
    log.info("=" * 70)
    log.info("EU27 Monthly Gap Fill — Quarterly Carry-Forward")
    log.info("=" * 70)

    total_housing = 0
    total_trade   = 0

    log.info("\n-- Housing monthly fill (all 27 countries) --")
    for iso3 in ALL_ISO3:
        n = _fill_housing_country(iso3)
        if n > 0:
            log.info(f"  {iso3}: +{n:,} carry-forward rows (permits_monthly_fill.parquet)")
        total_housing += n

    log.info(f"  Housing total: {total_housing:,} carry-forward rows")

    log.info("\n-- Trade monthly fill (all 27 countries) --")
    for iso3 in ALL_ISO3:
        n = _fill_trade_country(iso3)
        if n > 0:
            log.info(f"  {iso3}: +{n:,} carry-forward rows (trade_monthly_fill.parquet)")
        total_trade += n

    log.info(f"  Trade total: {total_trade:,} carry-forward rows")

    log.info("")
    log.info("=" * 70)
    log.info(f"Monthly fill complete:")
    log.info(f"  Housing: {total_housing:,} rows written")
    log.info(f"  Trade:   {total_trade:,} rows written")
    log.info(f"  Grand total: {total_housing + total_trade:,} rows")
    log.info("=" * 70)

    return {"housing_carry_rows": total_housing, "trade_carry_rows": total_trade}


if __name__ == "__main__":
    run()
