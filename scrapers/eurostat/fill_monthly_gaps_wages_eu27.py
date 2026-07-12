"""
EU27 Wages Monthly Gap Fill — Quarterly Carry-Forward for lfsi_emp_q

The quarterly employment data (lfsi_emp_q) only has observations for months
1, 4, 7, 10 (quarter start months). This generates carry-forward rows for
months +1 and +2 within each quarter so every calendar month is represented.

Logic mirrors fill_monthly_gaps_eu27.py used for housing permits.

Only generates carry-forward rows for months that are absent in the vault.
is_interpolated=True, interpolation_method=QUARTERLY_CARRY_FORWARD,
data_quality_certified=False on all carry-forward rows.

Output: wages_and_employment/{ISO3}/source=eurostat_sdmx/year=YYYY/month=MM/
        wages_empl_monthly_fill.parquet
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
PRODUCT = "wages_and_employment"
FILENAME = "wages_empl_monthly_fill.parquet"

EU27_ISO3 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE",
]

QUARTER_MONTHS = {1, 4, 7, 10}

_VID_DATE_RE = re.compile(r"^(.*?)(\d{4}-\d{2})(-CF-v\d+|-v\d+)$")


def _make_carry_vid(source_vid: str, carry_date: pd.Timestamp) -> str:
    m = _VID_DATE_RE.match(str(source_vid))
    if m:
        return f"{m.group(1)}{carry_date.strftime('%Y-%m')}-CF-v1"
    return f"{source_vid}-{carry_date.strftime('%Y-%m')}-CF"


def _load_employment_df(iso3: str) -> pd.DataFrame:
    base = VAULT / f"product={PRODUCT}" / f"country={iso3}" / f"source={SOURCE}"
    frames = []
    for f in sorted(base.rglob("*.parquet")):
        if "monthly_fill" in f.name:
            continue  # skip existing fill files
        try:
            df = pd.read_parquet(f)
            frames.append(df)
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _generate_employment_carry(df_all: pd.DataFrame) -> pd.DataFrame:
    # Only quarterly employment series
    q_mask = df_all["macro_metric_name"].str.contains("Quarterly Employment", case=False, na=False)
    df_q = df_all[q_mask].copy()
    if df_q.empty:
        return pd.DataFrame()

    df_q["_rd"] = pd.to_datetime(df_q["reporting_date"], errors="coerce")
    df_q = df_q[df_q["_rd"].dt.month.isin(QUARTER_MONTHS)]
    if df_q.empty:
        return pd.DataFrame()

    carry_rows = []
    # Track generated carry dates per sovereign_series_id to prevent double-gen
    generated: dict[str, set] = {}

    for _, row in df_q.iterrows():
        q_date = row["_rd"]
        if pd.isna(q_date):
            continue
        sid = str(row.get("sovereign_series_id", ""))
        if sid not in generated:
            # Seed with existing dates for this specific series only
            sid_dates = set(
                df_q[df_q["sovereign_series_id"] == sid]["_rd"]
                .dropna()
                .dt.strftime("%Y-%m-%d")
            )
            generated[sid] = sid_dates

        for offset in (1, 2):
            mo = q_date.month + offset
            yr = q_date.year
            if mo > 12:
                mo -= 12
                yr += 1
            carry_date = pd.Timestamp(yr, mo, 1)
            carry_date_str = carry_date.strftime("%Y-%m-%d")
            if carry_date_str in generated[sid]:
                continue
            new_row = row.drop(labels=["_rd"]).to_dict()
            new_row["reporting_date"] = carry_date_str
            new_row["data_timestamp"] = carry_date.isoformat() + "Z"
            new_row["data_vintage_id"] = _make_carry_vid(row["data_vintage_id"], carry_date)
            new_row["interpolation_method"] = "QUARTERLY_CARRY_FORWARD"
            new_row["is_interpolated"] = True
            new_row["data_quality_certified"] = False
            carry_rows.append(new_row)
            generated[sid].add(carry_date_str)

    return pd.DataFrame(carry_rows) if carry_rows else pd.DataFrame()


def run() -> int:
    print("=" * 70)
    print("EU27 Wages Employment Monthly Fill — Quarterly Carry-Forward")
    print("=" * 70)

    total_written = 0

    for iso3 in EU27_ISO3:
        df_all = _load_employment_df(iso3)
        if df_all.empty:
            print(f"  {iso3}: no data — skipped")
            continue

        df_fill = _generate_employment_carry(df_all)
        if df_fill.empty:
            print(f"  {iso3}: no quarterly employment rows — skipped")
            continue

        vault_root = (
            VAULT
            / f"product={PRODUCT}"
            / f"country={iso3}"
            / f"source={SOURCE}"
        )

        df_fill["_obs_ts"] = pd.to_datetime(
            df_fill["data_timestamp"], errors="coerce", utc=True
        )

        country_written = 0
        for (yr, mo), grp in df_fill.groupby([
            df_fill["_obs_ts"].dt.year,
            df_fill["_obs_ts"].dt.month,
        ]):
            out = grp.drop(columns=["_obs_ts"])
            write_partition(out, vault_root, int(yr), int(mo), FILENAME)
            country_written += len(out)

        total_written += country_written
        print(f"  {iso3}: {country_written:,} carry-forward rows written")

    print(f"\nTotal carry-forward rows written: {total_written:,}")
    print("=" * 70)
    return total_written


if __name__ == "__main__":
    run()
