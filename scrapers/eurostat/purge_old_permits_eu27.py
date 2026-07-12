"""
Purge Old-Format Permits Records — EU27 Housing Vault Cleanup

Deletes all records with sovereign_series_id starting with PERMITS_RESI_Q_
from every housing parquet file across all 27 EU countries.

These records were created by the old ingest_housing_credit.py code path using
HOUSING_CONFIGS from series_map.py. The correct records use EUROSTAT_PERMIT_*
prefix with vintage_id EUROSTAT-PERMIT-{ISO3}-{YYYY-MM}-v{N}, written by
ingest_permits_eu27_v2.py.

Also purges old-format carry-forward rows from permits_monthly_fill.parquet
and regenerates clean carry-forward rows from the correct EUROSTAT_PERMIT_*
data.

Reports:
  - How many rows removed per country
  - How many correct rows remain
  - Countries where correct data is missing (should be 0 after v2 run)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

from scrapers.utilities.vault_io import get_vault_root
_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_ROOT / "backtesting"))

from scrapers.eurostat.revision_tracker import write_partition

VAULT = _ROOT / "lekwankwa-historical-vault"
SOURCE = "eurostat_sdmx"
PRODUCT = "Housing_Supply_and_Shelter_Inflation"
FILL_FILENAME = "permits_monthly_fill.parquet"

EU27_ISO3 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE",
]

QUARTER_MONTHS = {1, 4, 7, 10}
_VID_DATE_RE = re.compile(r"^(.*?)(\d{4}-\d{2})(-CF-v\d+|-v\d+)$")

BAD_PREFIX = "PERMITS_RESI_Q_"
GOOD_PREFIX = "EUROSTAT_PERMIT_"
CORRECT_METRIC = "AUTHORIZED_PERMITS_TOTAL_UNITS"


def _make_carry_vid(source_vid: str, carry_date: pd.Timestamp) -> str:
    m = _VID_DATE_RE.match(str(source_vid))
    if m:
        return f"{m.group(1)}{carry_date.strftime('%Y-%m')}-CF-v1"
    return f"{source_vid}-{carry_date.strftime('%Y-%m')}-CF"


def _purge_and_rewrite_parquet(path: Path, verbose: bool = False) -> tuple[int, int]:
    """Read parquet, remove BAD_PREFIX rows, write back. Returns (removed, kept)."""
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"    [WARN] Cannot read {path.name}: {e}")
        return 0, 0

    if "sovereign_series_id" not in df.columns:
        return 0, len(df)

    bad_mask = df["sovereign_series_id"].str.startswith(BAD_PREFIX, na=False)
    n_bad = bad_mask.sum()
    if n_bad == 0:
        return 0, len(df)

    df_clean = df[~bad_mask].copy()
    n_kept = len(df_clean)

    if df_clean.empty:
        # Remove the file entirely if nothing remains
        path.unlink()
        if verbose:
            print(f"    Deleted empty file: {path.name}")
    else:
        df_clean.to_parquet(path, index=False, compression="snappy")

    return n_bad, n_kept


def _generate_clean_fill(df_correct: pd.DataFrame) -> pd.DataFrame:
    """Generate carry-forward rows from EUROSTAT_PERMIT_* quarterly records."""
    # Only rows from quarterly source (sts_cobp_q)
    q_mask = df_correct.get("sdmx_frequency", pd.Series(dtype=str)).eq("Q")
    if q_mask.sum() == 0:
        # Try detecting from reporting_date month
        df_correct["_rd"] = pd.to_datetime(df_correct["reporting_date"], errors="coerce")
        q_mask = df_correct["_rd"].dt.month.isin(QUARTER_MONTHS)
        # Only carry-forward if there are genuinely quarterly-only records
        # (monthly data doesn't need carry-forward)
        all_months = set(df_correct["_rd"].dt.month.dropna())
        if all_months >= {1,2,3,4,5,6,7,8,9,10,11,12}:
            return pd.DataFrame()  # monthly data present — no carry-forward needed

    df_q = df_correct[q_mask].copy()
    if "_rd" not in df_q.columns:
        df_q["_rd"] = pd.to_datetime(df_q["reporting_date"], errors="coerce")
    df_q = df_q[df_q["_rd"].dt.month.isin(QUARTER_MONTHS)]
    if df_q.empty:
        return pd.DataFrame()

    existing_dates = set(df_correct["reporting_date"].astype(str))
    carry_rows = []

    for _, row in df_q.iterrows():
        q_date = row["_rd"]
        if pd.isna(q_date):
            continue
        for offset in (1, 2):
            mo = q_date.month + offset
            yr = q_date.year
            if mo > 12:
                mo -= 12
                yr += 1
            carry_date = pd.Timestamp(yr, mo, 1)
            carry_date_str = carry_date.strftime("%Y-%m-%d")
            if carry_date_str in existing_dates:
                continue
            new_row = {k: v for k, v in row.items() if k != "_rd"}
            new_row["reporting_date"] = carry_date_str
            new_row["data_timestamp"] = carry_date.isoformat() + "Z"
            new_row["data_vintage_id"] = _make_carry_vid(row["data_vintage_id"], carry_date)
            new_row["interpolation_method"] = "QUARTERLY_CARRY_FORWARD"
            new_row["is_interpolated"] = True
            new_row["data_quality_certified"] = False
            new_row["macro_metric_name"] = CORRECT_METRIC
            carry_rows.append(new_row)
            existing_dates.add(carry_date_str)

    return pd.DataFrame(carry_rows) if carry_rows else pd.DataFrame()


def run() -> None:
    print("=" * 70)
    print("EU27 HOUSING — PURGE OLD PERMITS_RESI_Q_* RECORDS")
    print("=" * 70)
    print()

    report = []

    for iso3 in EU27_ISO3:
        base = VAULT / f"product={PRODUCT}" / f"country={iso3}" / f"source={SOURCE}"
        if not base.exists():
            print(f"  {iso3}: vault directory missing — skipped")
            continue

        # Purge all parquet files in this country's vault
        all_files = sorted(base.rglob("*.parquet"))
        total_removed = 0
        total_kept = 0

        for fpath in all_files:
            removed, kept = _purge_and_rewrite_parquet(fpath)
            total_removed += removed
            total_kept += kept

        # Count surviving correct rows
        frames_correct = []
        for fpath in sorted(base.rglob("*.parquet")):
            if "monthly_fill" in fpath.name:
                continue
            try:
                df = pd.read_parquet(fpath)
                frames_correct.append(df)
            except Exception:
                pass

        df_correct = pd.concat(frames_correct, ignore_index=True) if frames_correct else pd.DataFrame()
        good_rows = 0
        if not df_correct.empty and "sovereign_series_id" in df_correct.columns:
            good_rows = df_correct["sovereign_series_id"].str.startswith(GOOD_PREFIX, na=False).sum()

        # Regenerate clean monthly fill from correct data
        fill_written = 0
        if not df_correct.empty and good_rows > 0:
            df_good = df_correct[
                df_correct["sovereign_series_id"].str.startswith(GOOD_PREFIX, na=False)
            ].copy()
            df_fill = _generate_clean_fill(df_good)

            if not df_fill.empty:
                df_fill["_obs_ts"] = pd.to_datetime(
                    df_fill["data_timestamp"], errors="coerce", utc=True
                )
                vault_root = base
                for (yr, mo), grp in df_fill.groupby([
                    df_fill["_obs_ts"].dt.year,
                    df_fill["_obs_ts"].dt.month,
                ]):
                    out = grp.drop(columns=["_obs_ts"])
                    write_partition(out, vault_root, int(yr), int(mo), FILL_FILENAME)
                    fill_written += len(out)

        status = "OK " if good_rows > 0 else "WARN"
        print(f"  [{status}] {iso3}: removed {total_removed:>5} bad rows | "
              f"{good_rows:>5} correct rows remain | {fill_written:>5} fill rows written")
        report.append({
            "iso3": iso3,
            "removed": total_removed,
            "correct_remaining": good_rows,
            "fill_rows_written": fill_written,
            "status": status,
        })

    print()
    print("=" * 70)
    total_rm = sum(r["removed"] for r in report)
    total_good = sum(r["correct_remaining"] for r in report)
    total_fill = sum(r["fill_rows_written"] for r in report)
    warn_countries = [r["iso3"] for r in report if r["status"] != "OK "]
    print(f"TOTAL removed:   {total_rm:,}")
    print(f"TOTAL correct:   {total_good:,}")
    print(f"TOTAL fill rows: {total_fill:,}")
    if warn_countries:
        print(f"WARN countries (no correct data after purge): {warn_countries}")
    else:
        print("All 27 countries have correct permits data.")
    print("=" * 70)


if __name__ == "__main__":
    run()
