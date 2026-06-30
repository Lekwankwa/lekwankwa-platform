"""
Backfill official_release_date for global_macro vault rows where ALFRED's
realtime_start is an ingestion-floor date rather than the true BEA/BLS
publication date.

Root cause
----------
ALFRED did not track revision history for every series from the beginning.
For series where ALFRED's earliest vintage is later than the true initial
publication, the v1 row for every pre-floor observation carries the ALFRED
floor date as its official_release_date. This inflates the measured release
lag from the actual 30-120 days to 10-44 years:

  GDP/GDPC1  floor: 1991-12-04 (pre-1991 obs)
  CPIAUCSL   floor: 1994-02-17 (pre-1975 obs)
  FEDFUNDS   floor: 1996-12-03 (pre-1997 obs)
  GS10       floor: 1996-12-03 (pre-1997 obs)
  PCEPI      floor: 2000-08-01 (pre-2000 obs)
  PAYEMS     floor: 1961-11-03 (pre-1960 obs only)
  UNRATE     floor: 1960-03-15 (pre-1960 obs only)
  INDPRO     CORRECT back to 1950 -- not touched
  CPIAUCNS   CORRECT back to 1950 -- not touched

Fix
---
For rows where revision_number == 1 AND lag > THRESHOLD_DAYS (180):
  Replace official_release_date with an estimated publication date
  using BEA/BLS known release schedules:
    GDP/GDPC1   obs_date + 120 d  (~30 d after quarter-end)
    CPIAUCSL    obs_date +  47 d  (~16 d after month-end)
    UNRATE      obs_date +  38 d  (~7 d after month-end)
    PAYEMS      obs_date +  38 d  (same BLS Employment Situation)
    FEDFUNDS    obs_date +  35 d  (Fed H.15)
    GS10        obs_date +  35 d  (Fed H.15)
    PCEPI       obs_date +  60 d  (~30 d after month-end)

Revision rows (revision_number > 1) are never touched -- their
realtime_start dates are genuine ALFRED vintage dates.

Runs in-place on the Hive vault partitions.
The original data can be regenerated at any time by re-running
scrapers/alfred_pit/retrofit_global_macro.py.

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.vault_io import get_vault_root

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VAULT_ROOT = (
    Path(__file__).resolve().parents[2]
    / "lekwankwa-historical-vault"
    / "product=global_macro"
    / "country=USA"
    / "source=alfred_vintage"
)

# Any v1 row whose (official_release_date - data_timestamp) > this threshold
# is treated as carrying an ALFRED floor date rather than the true release date.
THRESHOLD_DAYS = 180

# Estimated days from the START of the observation period to initial publication.
# Monthly series: from the 1st of the reference month.
# Quarterly series: from the 1st of the first month of the quarter.
_ESTIMATED_LAG_DAYS: dict = {
    "GDP":      120,   # BEA advance GDP: quarter-end + 30 d = quarter-start + 120 d
    "GDPC1":    120,   # same BEA advance GDP release
    "CPIAUCSL":  47,   # BLS CPI: month-end + 16 d = month-start + 47 d
    "CPIAUCNS":  47,   # same (already correct; threshold protects them)
    "UNRATE":    38,   # BLS Employment Situation: month-end + 7 d
    "PAYEMS":    38,   # same BLS Employment Situation
    "FEDFUNDS":  35,   # Fed H.15 monthly average
    "GS10":      35,   # Fed H.15 10-year Treasury
    "PCEPI":     60,   # BEA Personal Income and Outlays: month-end + 30 d
    "INDPRO":    47,   # Fed G.17 (already correct; threshold protects it)
}


def _estimate_initial_release(series_id: str, obs_date: pd.Timestamp) -> pd.Timestamp:
    """Return estimated true initial publication date for a series observation."""
    lag = _ESTIMATED_LAG_DAYS.get(series_id, 45)
    return obs_date + pd.Timedelta(days=lag)


def _fix_partition(path: Path) -> tuple:
    """
    Read one parquet file, apply the release-date fix, write back in-place.

    Returns (rows_read, rows_fixed).
    """
    df = pd.read_parquet(path)
    if df.empty:
        return 0, 0

    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], errors="coerce")
    df["official_release_date"] = pd.to_datetime(
        df["official_release_date"], errors="coerce"
    )

    lag = (df["official_release_date"] - df["data_timestamp"]).dt.days

    mask = (
        df["revision_number"].astype(float) == 1.0
    ) & (
        lag > THRESHOLD_DAYS
    )

    n_fix = int(mask.sum())
    if n_fix == 0:
        return len(df), 0

    for idx in df.index[mask]:
        sid = df.at[idx, "sovereign_series_id"]
        obs = df.at[idx, "data_timestamp"]
        est = _estimate_initial_release(str(sid), obs)
        df.at[idx, "official_release_date"] = est
        if "published_date" in df.columns:
            df.at[idx, "published_date"] = est.strftime("%Y-%m-%d")
        # as_of_date: update to match estimated release (isoformat with Z suffix)
        if "as_of_date" in df.columns:
            df.at[idx, "as_of_date"] = est.isoformat() + "Z"

    df.to_parquet(path, index=False, engine="pyarrow")
    return len(df), n_fix


def _compute_lag_stats(df: pd.DataFrame) -> dict:
    """Compute per-series and overall lag statistics for v1 rows."""
    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], errors="coerce")
    df["official_release_date"] = pd.to_datetime(
        df["official_release_date"], errors="coerce"
    )
    df["lag_days"] = (df["official_release_date"] - df["data_timestamp"]).dt.days
    v1 = df[df["revision_number"].astype(float) == 1.0].dropna(subset=["lag_days"])

    stats: dict = {}
    for sid, grp in v1.groupby("sovereign_series_id"):
        stats[str(sid)] = {
            "median": float(grp["lag_days"].median()),
            "p25":    float(grp["lag_days"].quantile(0.25)),
            "p75":    float(grp["lag_days"].quantile(0.75)),
            "max":    float(grp["lag_days"].max()),
            "n":      int(len(grp)),
        }
    overall = float(v1["lag_days"].median()) if not v1.empty else float("nan")
    return {"by_series": stats, "overall_median": overall}


def run() -> None:
    log.info("=" * 70)
    log.info("GLOBAL MACRO -- Release Date Backfill")
    log.info(f"Vault: {VAULT_ROOT}")
    log.info(f"Threshold: {THRESHOLD_DAYS} days  |  Scope: revision_number=1 only")
    log.info("=" * 70)

    files = sorted(VAULT_ROOT.rglob("*.parquet"))
    if not files:
        log.error("No parquet files found under vault root. Exiting.")
        sys.exit(1)

    log.info(f"Found {len(files)} parquet files")

    # --- Snapshot pre-fix stats ---
    log.info("Computing pre-fix lag statistics (sampling all v1 rows)...")
    pre_frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                pre_frames.append(df[df["revision_number"].astype(float) == 1.0])
        except Exception as exc:
            log.warning(f"Could not read {f}: {exc}")

    pre_df = pd.concat(pre_frames, ignore_index=True) if pre_frames else pd.DataFrame()
    pre_stats = _compute_lag_stats(pre_df) if not pre_df.empty else {}

    # --- Apply fix ---
    total_rows = 0
    total_fixed = 0
    file_errors = 0

    for f in files:
        try:
            n_rows, n_fixed = _fix_partition(f)
            total_rows += n_rows
            total_fixed += n_fixed
            if n_fixed:
                log.info(f"  Fixed {n_fixed:>4} rows in {f.relative_to(VAULT_ROOT)}")
        except Exception as exc:
            file_errors += 1
            log.error(f"ERROR processing {f}: {exc}")

    log.info(
        f"\nFix complete: {total_fixed} v1 rows updated across {total_rows} total rows"
        f" ({file_errors} file errors)"
    )

    # --- Snapshot post-fix stats ---
    log.info("Computing post-fix lag statistics...")
    post_frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                post_frames.append(df[df["revision_number"].astype(float) == 1.0])
        except Exception as exc:
            pass

    post_df = pd.concat(post_frames, ignore_index=True) if post_frames else pd.DataFrame()
    post_stats = _compute_lag_stats(post_df) if not post_df.empty else {}

    # --- Report ---
    print()
    print("=" * 70)
    print("  Global Macro Release-Date Backfill -- Results")
    print("=" * 70)
    print(f"  Rows scanned : {total_rows:>8,}")
    print(f"  v1 rows fixed: {total_fixed:>8,}")
    if file_errors:
        print(f"  File errors  : {file_errors:>8}")
    print()

    pre_med  = pre_stats.get("overall_median",  float("nan"))
    post_med = post_stats.get("overall_median", float("nan"))
    print(f"  Overall median release lag (v1 rows)")
    print(f"    Before fix: {pre_med:>8.0f} days  (~{pre_med/365:.1f} years)")
    print(f"    After fix : {post_med:>8.0f} days  (~{post_med:.0f} days)")
    print()

    print("  Per-series median lag (v1 rows, days):")
    print(f"  {'Series':<12} {'Before':>10} {'After':>10}  {'n_v1':>6}")
    print("  " + "-" * 44)
    pre_by  = pre_stats.get("by_series",  {})
    post_by = post_stats.get("by_series", {})
    all_series = sorted(set(pre_by) | set(post_by))
    for sid in all_series:
        pre_m  = pre_by.get(sid,  {}).get("median", float("nan"))
        post_m = post_by.get(sid, {}).get("median", float("nan"))
        n      = post_by.get(sid, {}).get("n", 0)
        flag   = " [FIXED]" if pre_m > THRESHOLD_DAYS and post_m <= THRESHOLD_DAYS else ""
        print(f"  {sid:<12} {pre_m:>10.0f} {post_m:>10.0f}  {n:>6}{flag}")

    print()
    print("  Note: revision_number > 1 rows are unchanged -- those are genuine")
    print("  ALFRED vintage dates and remain correct.")
    print("=" * 70)


if __name__ == "__main__":
    run()
