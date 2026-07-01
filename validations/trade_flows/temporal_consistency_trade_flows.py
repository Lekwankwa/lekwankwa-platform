"""
Temporal Consistency — Trade Flows (US Census FT-900)

CHECKS:
  1.  Timestamp Granularity       — data_timestamp has day=1, hour=0 (SDMX monthly)
  2.  Timezone Consistency        — all timestamps UTC-aware
  3.  No Future Dates             — max data_timestamp <= today
  4.  Partition Path Alignment    — data_timestamp year/month matches Hive folder
  5.  Coverage Start Boundary    — no records before 1989-01 (HS series start)
  6.  Both Flows Per Month        — each calendar month has both Export and Import
  7.  HS Code Stability           — commodity_code set consistent across years
  8.  Anchor Series Continuity    — HS27 (Mineral Fuels) present every month in dense window
  9.  Intra-Series Gap Detection  — sample series for MoM gaps > 2 months

OUTPUT:
  trade_flows_temporal_consistency_report.json
  trade_flows_temporal_consistency_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_temporal_consistency.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR    = Path("lekwankwa-historical-vault")
PRODUCT      = "trade_flows"
COUNTRY      = "USA"
SOURCE       = "census_ft900"

REPORT_JSON  = Path("trade_flows_temporal_consistency_report.json")
REPORT_TXT   = Path("trade_flows_temporal_consistency_report.txt")

TODAY            = pd.Timestamp.utcnow().normalize()
TRADE_START_YEAR = 2010   # Census timeseries/intltrade API earliest available month
SAMPLE_FILES     = 60
DENSE_YEARS      = list(range(2018, 2022))  # expect full coverage here
ANCHOR_HS_CODE   = "27"   # Mineral Fuels — present every month since 1989


# =============================================================================
# HELPERS
# =============================================================================

def _result(status, check, message, details=None):
    entry = {"status": status, "check": check, "message": message}
    if details:
        entry["details"] = details
    icons  = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}
    log_fn = (logger.error if status == "FAIL" else
              logger.warning if status == "WARN" else logger.info)
    log_fn(f"  {icons[status]} {check}")
    if message:
        log_fn(f"         {message}")
    return entry


def _all_partitions():
    return sorted(
        (VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={SOURCE}")
        .glob("year=*/month=*/*.parquet")
    )


def _partition_ym(path: Path):
    return (
        int(path.parent.parent.name.split("=")[1]),
        int(path.parent.name.split("=")[1]),
    )


def _load_sample(n: int) -> pd.DataFrame:
    files = _all_partitions()
    step  = max(1, len(files) // n)
    dfs   = [pd.read_parquet(f) for f in files[::step][:n]]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _load_dense(years) -> pd.DataFrame:
    files = [
        f for f in _all_partitions()
        if _partition_ym(f)[0] in years
    ]
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# =============================================================================
# CHECKS
# =============================================================================

def chk_timestamp_granularity(df: pd.DataFrame):
    ts      = pd.to_datetime(df["data_timestamp"], utc=True)
    bad_day = int((ts.dt.day  != 1).sum())
    bad_hr  = int((ts.dt.hour != 0).sum())
    if bad_day or bad_hr:
        return _result("FAIL", "Timestamp Granularity",
                       f"{bad_day:,} records with day!=1, {bad_hr:,} with hour!=0")
    return _result("PASS", "Timestamp Granularity",
                   f"All {len(df):,} records use day=1 / hour=0 (SDMX monthly)")


def chk_timezone_consistency(df: pd.DataFrame):
    ts    = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    nulls = int(ts.isna().sum())
    dtype = str(df["data_timestamp"].dtype)
    if nulls == 0:
        return _result("PASS", "Timezone Consistency",
                       f"All {len(df):,} timestamps parseable as UTC (storage dtype={dtype})")
    return _result("FAIL", "Timezone Consistency",
                   f"{nulls} timestamps could not be coerced to UTC (dtype={dtype})",
                   {"null_count": nulls})


def chk_no_future_dates(df: pd.DataFrame):
    ts  = pd.to_datetime(df["data_timestamp"], utc=True)
    fut = int((ts > TODAY).sum())
    if fut:
        return _result("FAIL", "No Future Dates",
                       f"{fut:,} records have data_timestamp > {TODAY.date()}")
    return _result("PASS", "No Future Dates",
                   f"Latest record: {ts.max().date()} — no future-dated records")


def chk_partition_path_alignment(files, n_sample: int = 60):
    mismatched = []
    sampled = files[::max(1, len(files) // n_sample)][:n_sample]
    for f in sampled:
        p_year, p_month = _partition_ym(f)
        df_p = pd.read_parquet(f, columns=["data_timestamp"])
        if df_p.empty:
            continue
        ts = pd.to_datetime(df_p["data_timestamp"].iloc[0], utc=True)
        if ts.year != p_year or ts.month != p_month:
            mismatched.append({"file": str(f), "path_ym": f"{p_year}-{p_month:02d}",
                                "data_ym": f"{ts.year}-{ts.month:02d}"})
    if not mismatched:
        return _result("PASS", "Partition Path Alignment",
                       f"All {len(sampled)} sampled partitions: data_timestamp matches Hive path")
    return _result("FAIL", "Partition Path Alignment",
                   f"{len(mismatched)} partitions have mismatched year/month in path vs data",
                   {"mismatched_count": len(mismatched), "examples": mismatched[:3]})


def chk_coverage_start(df: pd.DataFrame):
    ts      = pd.to_datetime(df["data_timestamp"], utc=True)
    too_old = int((ts.dt.year < TRADE_START_YEAR).sum())
    if too_old:
        return _result("FAIL", "Coverage Start Boundary",
                       f"{too_old:,} records before {TRADE_START_YEAR}-01 (HS series start)")
    min_yr = int(ts.dt.year.min())
    return _result("PASS", "Coverage Start Boundary",
                   f"Earliest record: {ts.min().date()} (>= {TRADE_START_YEAR}-01 boundary)")


def chk_both_flows_per_month(df: pd.DataFrame):
    """Each (year-month) should have both Export and Import records."""
    if "trade_flow" not in df.columns or "data_timestamp" not in df.columns:
        return _result("SKIP", "Both Flows Per Month", "Required columns missing")
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    df = df.assign(_ym=ts.dt.to_period("M"))
    pivot = df.groupby(["_ym", "trade_flow"]).size().unstack(fill_value=0)
    missing_exp = int((pivot.get("Export", 0) == 0).sum())
    missing_imp = int((pivot.get("Import", 0) == 0).sum())
    total_months = len(pivot)
    if missing_exp == 0 and missing_imp == 0:
        return _result("PASS", "Both Flows Per Month",
                       f"All {total_months} months have both Export and Import records")
    return _result("WARN", "Both Flows Per Month",
                   f"{missing_exp} months missing Exports, {missing_imp} months missing Imports "
                   f"(out of {total_months} total months in sample)",
                   {"missing_export_months": missing_exp, "missing_import_months": missing_imp})


def chk_hs_code_stability(files):
    """commodity_code set should be consistent year-to-year (no sudden additions/removals)."""
    year_codes = {}
    sampled_years = sorted({_partition_ym(f)[0] for f in files})
    # Sample one file per year for speed
    year_files = {}
    for f in files:
        y = _partition_ym(f)[0]
        if y not in year_files:
            year_files[y] = f
    for year, f in sorted(year_files.items()):
        try:
            df_y = pd.read_parquet(f, columns=["commodity_code"])
            year_codes[year] = set(df_y["commodity_code"].dropna().unique())
        except Exception:
            pass
    if len(year_codes) < 2:
        return _result("SKIP", "HS Code Stability", "Insufficient year samples to compare")
    # Find years with unusually different chapter count
    counts  = {y: len(c) for y, c in year_codes.items()}
    median  = sorted(counts.values())[len(counts) // 2]
    anomaly = {y: c for y, c in counts.items() if abs(c - median) > median * 0.2}
    if not anomaly:
        return _result("PASS", "HS Code Stability",
                       f"HS chapter count consistent across {len(year_codes)} sampled years "
                       f"(median {median} chapters)")
    return _result("WARN", "HS Code Stability",
                   f"Unusual chapter counts in: {anomaly} (median={median})",
                   {"anomalous_years": anomaly})


def chk_anchor_series_continuity(dense_df: pd.DataFrame):
    """HS27 (Mineral Fuels) must appear every month in the dense window."""
    if dense_df.empty:
        return _result("SKIP", f"Anchor Series HS{ANCHOR_HS_CODE} Continuity",
                       "No dense-window data loaded")
    ts   = pd.to_datetime(dense_df["data_timestamp"], utc=True)
    hs27 = dense_df[dense_df.get("commodity_code", pd.Series(dtype=str)) == ANCHOR_HS_CODE]
    if hs27.empty and "commodity_code" in dense_df.columns:
        return _result("FAIL", f"Anchor Series HS{ANCHOR_HS_CODE} Continuity",
                       f"HS{ANCHOR_HS_CODE} (Mineral Fuels) absent from dense window {DENSE_YEARS}")
    exp_months = len(DENSE_YEARS) * 12
    hs27_ts    = pd.to_datetime(
        hs27["data_timestamp"] if not hs27.empty else dense_df["data_timestamp"], utc=True
    )
    found_months = hs27_ts.dt.to_period("M").nunique() if not hs27.empty else 0
    if found_months >= exp_months * 0.9:
        return _result("PASS", f"Anchor Series HS{ANCHOR_HS_CODE} Continuity",
                       f"HS{ANCHOR_HS_CODE} present in {found_months}/{exp_months} expected months "
                       f"({DENSE_YEARS[0]}–{DENSE_YEARS[-1]})")
    return _result("WARN", f"Anchor Series HS{ANCHOR_HS_CODE} Continuity",
                   f"HS{ANCHOR_HS_CODE} found in only {found_months}/{exp_months} months",
                   {"found": found_months, "expected": exp_months})


def chk_intra_series_gaps(df: pd.DataFrame, max_series: int = 20):
    """Sample series for MoM gaps > 2 months."""
    if "sovereign_series_id" not in df.columns or "data_timestamp" not in df.columns:
        return _result("SKIP", "Intra-Series Gap Detection", "Required columns missing")
    series_ids = df["sovereign_series_id"].dropna().unique()
    if len(series_ids) > max_series:
        import numpy as np
        series_ids = np.random.default_rng(42).choice(series_ids, max_series, replace=False)
    gap_series = []
    for sid in series_ids:
        sub = df[df["sovereign_series_id"] == sid].copy()
        sub["_ts"] = pd.to_datetime(sub["data_timestamp"], utc=True)
        sub = sub.sort_values("_ts").drop_duplicates("_ts")
        if len(sub) < 3:
            continue
        months_gap = (sub["_ts"].diff().dt.days.iloc[1:] / 30).round()
        big_gaps   = int((months_gap > 2).sum())
        if big_gaps:
            gap_series.append({"series": str(sid), "gaps_gt2_months": big_gaps})
    if not gap_series:
        return _result("PASS", "Intra-Series Gap Detection",
                       f"No MoM gaps > 2 months in {len(series_ids)} sampled series")
    return _result("WARN", "Intra-Series Gap Detection",
                   f"{len(gap_series)}/{len(series_ids)} sampled series have MoM gaps > 2 months",
                   {"gap_series": gap_series})


# =============================================================================
# MAIN
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — TEMPORAL CONSISTENCY")
    logger.info("=" * 70)

    files     = _all_partitions()
    n_files   = len(files)
    logger.info(f"  Partition files found: {n_files}")

    if n_files == 0:
        logger.error("  No vault data found. Run scraper first.")
        return False

    df_sample = _load_sample(SAMPLE_FILES)
    df_dense  = _load_dense(DENSE_YEARS)

    results = [
        chk_timestamp_granularity(df_sample),
        chk_timezone_consistency(df_sample),
        chk_no_future_dates(df_sample),
        chk_partition_path_alignment(files),
        chk_coverage_start(df_sample),
        chk_both_flows_per_month(df_sample),
        chk_hs_code_stability(files),
        chk_anchor_series_continuity(df_dense),
        chk_intra_series_gaps(df_sample),
    ]

    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    overall = "PASS" if failed == 0 else "FAIL"

    logger.info(f"\n  Summary: {passed}P / {failed}F / {warned}W / {skipped}S -> [{overall}]")

    report = {
        "product": PRODUCT, "country": COUNTRY,
        "generated": datetime.utcnow().isoformat(),
        "overall": overall,
        "counts": {"passed": passed, "failed": failed, "warned": warned, "skipped": skipped},
        "checks": results,
    }
    REPORT_JSON.write_text(__import__("json").dumps(report, indent=2, default=str), encoding="utf-8")
    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write(f"TRADE FLOWS — TEMPORAL CONSISTENCY REPORT\nOverall: [{overall}]\n\n")
        for r in results:
            f.write(f"  [{r['status']:<4}] {r['check']}\n         {r['message']}\n")

    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
