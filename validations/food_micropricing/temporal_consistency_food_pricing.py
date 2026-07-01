"""
Temporal Consistency Check — Food Micropricing

Verifies the internal time-series coherence of the food_micropricing vault.

SECTION A — BLS (monthly CPI average prices):
  1.  Timestamp Granularity         — every data_timestamp has day=1, hour=0, min=0, sec=0 (SDMX monthly)
  2.  Timezone Consistency          — all timestamps are UTC-aware (datetime64[ns, UTC])
  3.  No Future Dates               — max data_timestamp <= today
  4.  Partition Path Alignment      — data_timestamp year/month matches the Hive partition folder
  5.  Intra-Series Gap Detection    — per-series check: flag series with >2 consecutive missing months
  6.  pct_change_mom Accuracy       — stored pct_change_mom == (v_t - v_{t-1}) / v_{t-1} * 100 ± 0.05pp
  7.  First Observation Null pct_change — earliest record per series must have null pct_change_mom
  8.  Temporal Bounds               — no records before 1980-01; no records in the future

  Note: USDA source removed from this dataset (June 2026 schema refactoring).

OUTPUT:
  - food_pricing_temporal_consistency_report.json
  - food_pricing_temporal_consistency_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("food_pricing_temporal_consistency.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "food_micropricing"
COUNTRY   = "USA"

REPORT_JSON = Path("food_pricing_temporal_consistency_report.json")
REPORT_TXT  = Path("food_pricing_temporal_consistency_report.txt")

TODAY = pd.Timestamp.utcnow().normalize()

BLS_START_YEAR  = 1980

SAMPLE_PARTITIONS  = 50   # for bulk checks
MAX_SERIES_CHECKED = 40   # for intra-series gap detection
PCT_CHANGE_TOL     = 0.05  # pp — tolerance for pct_change_mom accuracy

# =============================================================================
# HELPERS
# =============================================================================

def _result(status, check, message, details=None):
    entry = {"status": status, "check": check, "message": message}
    if details:
        entry["details"] = details
    icon   = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}[status]
    log_fn = logger.warning if status == "WARN" else (logger.error if status == "FAIL" else logger.info)
    log_fn(f"  {icon} {check}")
    if message:
        log_fn(f"         {message}")
    return entry


SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}


def _all_partitions(source, file_glob=None):
    if file_glob is None:
        file_glob = SOURCE_FILES.get(source, "*.parquet")
    return sorted(Path(".").glob(str(
        VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}"
        / f"source={source}" / "year=*" / "month=*" / file_glob
    )))


def _load_sample(source, n, file_glob=None):
    files = _all_partitions(source, file_glob)
    step  = max(1, len(files) // n)
    return pd.concat([pd.read_parquet(f) for f in files[::step][:n]], ignore_index=True)


def _partition_ym(path: Path):
    return int(path.parent.parent.name.split("=")[1]), int(path.parent.name.split("=")[1])


# =============================================================================
# SECTION A — BLS CHECKS
# =============================================================================

def chk_bls_timestamp_granularity(df):
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    bad_day  = (ts.dt.day  != 1).sum()
    bad_hour = (ts.dt.hour != 0).sum()
    bad_min  = (ts.dt.minute != 0).sum()
    if bad_day or bad_hour or bad_min:
        return _result("FAIL", "BLS Timestamp Granularity",
                       f"{int(bad_day):,} records with day!=1, "
                       f"{int(bad_hour):,} with hour!=0, {int(bad_min):,} with minute!=0")
    return _result("PASS", "BLS Timestamp Granularity",
                   f"All {len(df):,} BLS records have day=1, hour=0, minute=0 (SDMX monthly)")


def chk_bls_timezone_consistency(df):
    dtype = str(df["data_timestamp"].dtype)
    if "UTC" not in dtype and "tz" not in dtype.lower():
        return _result("FAIL", "BLS Timezone Consistency",
                       f"data_timestamp dtype is '{dtype}' — expected UTC-aware datetime64")
    naive_count = df["data_timestamp"].apply(
        lambda t: pd.isnull(t) or (pd.Timestamp(t).tzinfo is None)
    ).sum()
    if naive_count:
        return _result("FAIL", "BLS Timezone Consistency",
                       f"{int(naive_count):,} BLS records have tz-naive or null timestamps")
    return _result("PASS", "BLS Timezone Consistency",
                   f"All BLS timestamps are UTC-aware (dtype={dtype})")


def chk_bls_no_future_dates(df):
    ts  = pd.to_datetime(df["data_timestamp"], utc=True)
    fut = (ts > TODAY).sum()
    if fut:
        return _result("FAIL", "BLS No Future Dates",
                       f"{int(fut):,} records have data_timestamp > {TODAY.date()}")
    return _result("PASS", "BLS No Future Dates",
                   f"Latest BLS record: {ts.max().date()} — no future-dated records")


def chk_bls_partition_path_alignment(files, n_sample=60):
    """Check that data_timestamp year/month matches the Hive folder year/month."""
    mismatched = []
    sampled = files[::max(1, len(files) // n_sample)][:n_sample]
    for f in sampled:
        p_year, p_month = _partition_ym(f)
        df = pd.read_parquet(f)
        if df.empty:
            continue
        ts = pd.to_datetime(df["data_timestamp"], utc=True)
        bad = ((ts.dt.year != p_year) | (ts.dt.month != p_month)).sum()
        if bad:
            mismatched.append(f"{f.parent.parent.name}/{f.parent.name}: {int(bad)} records mismatch")
    if mismatched:
        return _result("FAIL", "BLS Partition Path Alignment",
                       f"{len(mismatched)} partitions have records whose year/month "
                       f"doesn't match the folder: {mismatched[:3]}",
                       {"mismatched_partitions": mismatched})
    return _result("PASS", "BLS Partition Path Alignment",
                   f"All {len(sampled)} sampled partitions: data_timestamp year/month "
                   f"matches Hive folder exactly")


def chk_bls_intra_series_gaps(files):
    """Sample series from a cross-partition view and check for gaps >2 consecutive months."""
    # Use all partitions from a 3-year dense window to get genuine per-series continuity
    dense = [f for f in files
             if 2017 <= int(f.parent.parent.name.split("=")[1]) <= 2019]
    if not dense:
        return _result("SKIP", "BLS Intra-Series Gap Detection", "No BLS data for 2017-2019")

    frames = [pd.read_parquet(f, columns=["source_series_id", "data_timestamp"])
              for f in dense]
    combined = pd.concat(frames, ignore_index=True)
    combined["_dt"] = pd.to_datetime(combined["data_timestamp"], utc=True)

    gap_series = []
    series_ids = combined["source_series_id"].dropna().unique()[:MAX_SERIES_CHECKED]
    for sid in series_ids:
        months = (combined[combined["source_series_id"] == sid]["_dt"]
                  .sort_values().dt.to_period("M").drop_duplicates())
        if len(months) < 3:
            continue
        expected = pd.period_range(months.min(), months.max(), freq="M")
        missing  = expected.difference(months.values)
        if len(missing) > 2:
            gap_series.append({"series": sid, "gap_months": len(missing)})

    if not gap_series:
        return _result("PASS", "BLS Intra-Series Gap Detection",
                       f"No series with >2 consecutive missing months in {len(series_ids)} "
                       f"sampled series (2017-2019 dense window)")
    return _result("WARN", "BLS Intra-Series Gap Detection",
                   f"{len(gap_series)}/{len(series_ids)} series have >2 missing months "
                   f"(may be discontinued BLS series)",
                   {"gap_series_sample": gap_series[:5]})


def chk_bls_pct_change_accuracy(all_files):
    """Verify stored pct_change_mom matches (v_t - v_{t-1}) / v_{t-1} * 100."""
    # Find two consecutive months in a mid-range year
    test_year = 2019
    month_pairs = [(test_year, m, test_year, m + 1) for m in range(1, 12)]
    month_pairs += [(test_year, 12, test_year + 1, 1)]

    checked = mismatch = 0
    examples = []

    for y0, m0, y1, m1 in month_pairs[:4]:
        f0_list = [f for f in all_files
                   if int(f.parent.parent.name.split("=")[1]) == y0
                   and int(f.parent.name.split("=")[1]) == m0]
        f1_list = [f for f in all_files
                   if int(f.parent.parent.name.split("=")[1]) == y1
                   and int(f.parent.name.split("=")[1]) == m1]
        if not f0_list or not f1_list:
            continue

        d0 = pd.read_parquet(f0_list[0], columns=["source_series_id", "observed_price_local"])
        d1 = pd.read_parquet(f1_list[0], columns=["source_series_id", "observed_price_local", "pct_change_mom"])
        d0 = d0.dropna(subset=["observed_price_local"]).set_index("source_series_id")
        d1 = d1.dropna(subset=["observed_price_local", "pct_change_mom"]).set_index("source_series_id")
        common = d0.index.intersection(d1.index)

        for sid in common[:25]:
            v0 = float(d0.loc[sid, "observed_price_local"])
            v1 = float(d1.loc[sid, "observed_price_local"])
            stored = float(d1.loc[sid, "pct_change_mom"])
            if v0 == 0:
                continue
            calc = (v1 - v0) / v0 * 100
            checked += 1
            if abs(calc - stored) > PCT_CHANGE_TOL:
                mismatch += 1
                examples.append({
                    "series": sid, "period": f"{y0}-{m0:02d}/{y1}-{m1:02d}",
                    "stored": round(stored, 6), "calculated": round(calc, 6),
                    "delta": round(abs(calc - stored), 6),
                })

    if not checked:
        return _result("SKIP", "BLS pct_change_mom Accuracy",
                       "Could not load consecutive monthly pairs for test year")
    if mismatch:
        return _result("FAIL", "BLS pct_change_mom Accuracy",
                       f"{mismatch}/{checked} pct_change_mom values deviate by >{PCT_CHANGE_TOL}pp "
                       f"from calculated (v_t-v_{{t-1}})/v_{{t-1}}*100",
                       {"mismatch_examples": examples[:5]})
    return _result("PASS", "BLS pct_change_mom Accuracy",
                   f"All {checked} sampled pct_change_mom values match calculated "
                   f"(v_t-v_{{t-1}})/v_{{t-1}}*100 within {PCT_CHANGE_TOL}pp tolerance")


def chk_bls_first_obs_null_pct(all_files):
    """First recorded month for each series should have null pct_change_mom."""
    earliest_files = sorted(all_files)[:30]
    frames = []
    for f in earliest_files:
        df = pd.read_parquet(f, columns=["source_series_id", "data_timestamp", "pct_change_mom"])
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    combined["_dt"] = pd.to_datetime(combined["data_timestamp"], utc=True)

    # For each series, find its globally earliest month and check pct_change_mom is null
    bad_series = []
    for sid, grp in combined.groupby("source_series_id"):
        earliest = grp.loc[grp["_dt"].idxmin()]
        if pd.notna(earliest["pct_change_mom"]):
            bad_series.append({
                "series": sid,
                "first_month": str(earliest["_dt"].date()),
                "pct_change_mom": float(earliest["pct_change_mom"]),
            })

    if bad_series:
        return _result("WARN", "BLS First Observation Null pct_change",
                       f"{len(bad_series)} series have non-null pct_change_mom on their "
                       f"earliest observed month (may be OK if series start pre-vault)",
                       {"examples": bad_series[:5]})
    return _result("PASS", "BLS First Observation Null pct_change",
                   f"All series in earliest {len(earliest_files)} partitions have "
                   f"null pct_change_mom on their first observed month")


def chk_bls_temporal_bounds(df, all_files):
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    min_ts = ts.min()
    max_ts = ts.max()
    issues = []
    if min_ts.year < BLS_START_YEAR:
        issues.append(f"Earliest record {min_ts.date()} predates declared start {BLS_START_YEAR}-01")
    if max_ts > TODAY:
        issues.append(f"Latest record {max_ts.date()} is in the future")
    if issues:
        return _result("FAIL", "BLS Temporal Bounds", "; ".join(issues))

    # Overall vault span check
    ym_set  = {_partition_ym(f) for f in all_files}
    min_year = min(y for y, m in ym_set)
    max_year = max(y for y, m in ym_set)
    return _result("PASS", "BLS Temporal Bounds",
                   f"BLS data spans {min_year}-{max_year}. "
                   f"Sample range: {min_ts.date()} to {max_ts.date()} — within declared bounds")


# =============================================================================
# RUNNER
# =============================================================================

def run_checks():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING — TEMPORAL CONSISTENCY CHECK")
    logger.info("=" * 70)

    results = []
    counts  = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}

    def record(r):
        results.append(r)
        counts[r["status"]] += 1

    bls_files  = _all_partitions("bls")

    if not bls_files:
        logger.error("No BLS parquet files found — aborting")
        return

    logger.info(f"\n  BLS partitions: {len(bls_files)}")

    # Load BLS sample
    logger.info("\nLoading BLS sample for bulk checks…")
    bls_df = _load_sample("bls", SAMPLE_PARTITIONS)
    logger.info(f"  {len(bls_df):,} BLS records loaded")

    # --- Section A: BLS ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION A — BLS TEMPORAL CONSISTENCY")
    logger.info("=" * 70)
    record(chk_bls_timestamp_granularity(bls_df))
    record(chk_bls_timezone_consistency(bls_df))
    record(chk_bls_no_future_dates(bls_df))
    record(chk_bls_partition_path_alignment(bls_files))
    record(chk_bls_intra_series_gaps(bls_files))
    record(chk_bls_pct_change_accuracy(bls_files))
    record(chk_bls_first_obs_null_pct(bls_files))
    record(chk_bls_temporal_bounds(bls_df, bls_files))

    # --- Summary ---
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    overall = "PASS" if counts["FAIL"] == 0 else "FAIL"
    logger.info(f"  {counts['PASS']} passed / {counts['FAIL']} failed / "
                f"{counts['WARN']} warned / {counts['SKIP']} skipped")
    logger.info(f"  Overall: [{overall}]")

    report = {
        "product":   PRODUCT,
        "country":   COUNTRY,
        "generated": datetime.utcnow().isoformat() + "Z",
        "overall":   overall,
        "counts":    counts,
        "checks":    results,
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    lines = [
        "FOOD MICROPRICING — TEMPORAL CONSISTENCY CHECK REPORT",
        f"Generated: {report['generated']}",
        f"Overall:   [{overall}]",
        f"Counts:    {counts['PASS']} passed / {counts['FAIL']} failed / "
        f"{counts['WARN']} warned / {counts['SKIP']} skipped",
        "",
        "SECTION A — BLS TEMPORAL CONSISTENCY",
        "-" * 60,
    ]
    for r in results:
        lines.append(f"  [{r['status']:4}] {r['check']}")
        lines.append(f"         {r['message']}")

    with open(REPORT_TXT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")
    return overall


if __name__ == "__main__":
    run_checks()
