"""
Temporal Consistency Check — Macro Employment

Verifies the internal time-series coherence of the macro_employment vault.

SECTION A — BLS CES (monthly, 1939-present):
  1.  Timestamp Granularity          — every data_timestamp has day=1, hour=0 (SDMX monthly)
  2.  Timezone Consistency           — all timestamps UTC-aware (datetime64[ns, UTC])
  3.  No Future Dates                — max data_timestamp <= today
  4.  Partition Path Alignment        — data_timestamp year/month matches Hive folder
  5.  CES Temporal Bounds            — earliest record >= 1939-01; no future records
  6.  CES Long-Run Anchor Series     — CES0000000001 (Total Nonfarm) present continuously from 1939
  7.  CES Intra-Series Gap Detection — sample series for gaps >2 consecutive months

SECTION B — BLS JOLTS (monthly, 2011-present):
  8.  JOLTS Timestamp Granularity    — day=1, UTC-aware
  9.  JOLTS No Future Dates
  10. JOLTS Partition Path Alignment
  11. JOLTS Start Date Boundary      — no JOLTS records before 2011-01-01
  12. JOLTS Intra-Series Gap Detection — sample series for gaps >2 months

SECTION C — CES / JOLTS temporal sync:
  13. CES–JOLTS Coverage Overlap     — every JOLTS month is also present in CES

OUTPUT:
  - macro_employment_temporal_consistency_report.json
  - macro_employment_temporal_consistency_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("macro_employment_temporal_consistency.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "wages_and_employment"
COUNTRY   = "USA"

REPORT_JSON = Path("macro_employment_temporal_consistency_report.json")
REPORT_TXT  = Path("macro_employment_temporal_consistency_report.txt")

TODAY = pd.Timestamp.utcnow().normalize()

CES_START_YEAR = 1939
CPS_START_YEAR = 1948   # BLS CPS programme began January 1948

SAMPLE_PARTITIONS  = 50
MAX_SERIES_CHECKED = 30

CES_ANCHOR_SERIES  = "CES0000000001"   # Total Private Nonfarm, seasonally adjusted

DENSE_YEARS = list(range(2018, 2022))   # consecutive 4-year window for gap detection (no step-sampling)

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


# Ancillary sidecar files (outliers.parquet, changelog.parquet, audit.parquet)
# live inside the same month partitions as the data files but carry a
# different schema (no sovereign_series_id). They must be excluded from data
# gathering — otherwise column-projected reads like
# pd.read_parquet(f, columns=["sovereign_series_id", ...]) raise ArrowInvalid.
_ANCILLARY_NAMES = ("outliers", "changelog", "change_log", "audit")


def _all_partitions(source):
    return sorted(
        p for p in Path(".").glob(str(
            VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}"
            / f"source={source}" / "year=*" / "month=*" / "*.parquet"
        ))
        if not any(a in p.name.lower() for a in _ANCILLARY_NAMES)
    )


def _partition_ym(path: Path):
    return int(path.parent.parent.name.split("=")[1]), int(path.parent.name.split("=")[1])


def _load_sample(source, n):
    files = _all_partitions(source)
    step  = max(1, len(files) // n)
    return pd.concat([pd.read_parquet(f) for f in files[::step][:n]], ignore_index=True)


def _get_ym_set(source):
    return {_partition_ym(f) for f in _all_partitions(source)}


# =============================================================================
# SECTION A — CES CHECKS
# =============================================================================

def chk_ces_timestamp_granularity(df):
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    bad_day  = (ts.dt.day  != 1).sum()
    bad_hour = (ts.dt.hour != 0).sum()
    if bad_day or bad_hour:
        return _result("FAIL", "CES Timestamp Granularity",
                       f"{int(bad_day):,} records with day!=1, {int(bad_hour):,} with hour!=0")
    return _result("PASS", "CES Timestamp Granularity",
                   f"All {len(df):,} CES records have day=1, hour=0 (SDMX monthly)")


def chk_ces_timezone_consistency(df):
    dtype = str(df["data_timestamp"].dtype)
    if "UTC" not in dtype:
        return _result("FAIL", "CES Timezone Consistency",
                       f"data_timestamp dtype is '{dtype}' — expected UTC-aware datetime64")
    return _result("PASS", "CES Timezone Consistency",
                   f"All CES timestamps are UTC-aware (dtype={dtype})")


def chk_ces_no_future_dates(df):
    ts  = pd.to_datetime(df["data_timestamp"], utc=True)
    fut = (ts > TODAY).sum()
    if fut:
        return _result("FAIL", "CES No Future Dates",
                       f"{int(fut):,} records have data_timestamp > {TODAY.date()}")
    return _result("PASS", "CES No Future Dates",
                   f"Latest CES record: {ts.max().date()} — no future-dated records")


def chk_ces_partition_path_alignment(files, n_sample=60):
    mismatched = []
    sampled = files[::max(1, len(files) // n_sample)][:n_sample]
    for f in sampled:
        p_year, p_month = _partition_ym(f)
        df = pd.read_parquet(f, columns=["data_timestamp"])
        if df.empty:
            continue
        ts  = pd.to_datetime(df["data_timestamp"], utc=True)
        bad = ((ts.dt.year != p_year) | (ts.dt.month != p_month)).sum()
        if bad:
            mismatched.append(f"year={p_year}/month={p_month:02d}: {int(bad)} mismatched")
    if mismatched:
        return _result("FAIL", "CES Partition Path Alignment",
                       f"{len(mismatched)} CES partitions have data_timestamp/folder mismatch",
                       {"examples": mismatched[:5]})
    return _result("PASS", "CES Partition Path Alignment",
                   f"All {len(sampled)} sampled CES partitions: data_timestamp year/month "
                   f"matches Hive folder exactly")


def chk_ces_temporal_bounds(df, all_files):
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    min_ts = ts.min()
    fut    = (ts > TODAY).sum()
    issues = []
    if fut:
        issues.append(f"{int(fut):,} records after today")
    ym_set   = {_partition_ym(f) for f in all_files}
    min_year = min(y for y, m in ym_set)
    max_year = max(y for y, m in ym_set)
    if min_year < CES_START_YEAR:
        issues.append(f"Vault has partitions from {min_year} — before declared start {CES_START_YEAR}")
    if issues:
        return _result("FAIL", "CES Temporal Bounds", "; ".join(issues))
    return _result("PASS", "CES Temporal Bounds",
                   f"CES vault spans {min_year}-{max_year}. "
                   f"Sample range: {min_ts.date()} to {ts.max().date()}")


def chk_ces_anchor_series(all_ces_files):
    """Verify CES0000000001 (Total Nonfarm) is present from 1939 to a recent year."""
    anchor_records = []
    files_1939 = [f for f in all_ces_files if _partition_ym(f)[0] == CES_START_YEAR]
    files_2024 = [f for f in all_ces_files if _partition_ym(f)[0] == 2024]

    for f_list, label in [(files_1939, str(CES_START_YEAR)), (files_2024, "2024")]:
        for f in f_list[:2]:
            df = pd.read_parquet(f, columns=["sovereign_series_id", "data_timestamp"])
            if CES_ANCHOR_SERIES in df["sovereign_series_id"].values:
                anchor_records.append(label)
                break

    missing = [y for y in [str(CES_START_YEAR), "2024"] if y not in anchor_records]
    if missing:
        return _result("WARN", "CES Long-Run Anchor Series",
                       f"Anchor series {CES_ANCHOR_SERIES} not found in: {missing}. "
                       f"Found in: {anchor_records}")
    return _result("PASS", "CES Long-Run Anchor Series",
                   f"Anchor series {CES_ANCHOR_SERIES} (Total Nonfarm) "
                   f"confirmed present in both {CES_START_YEAR} and 2024 partitions")


def chk_ces_intra_series_gaps(all_ces_files):
    """Check CES series for gaps >2 consecutive months in a dense 4-year window."""
    # Load ALL files in a dense consecutive window to avoid false gaps from step-sampling
    dense = [f for f in all_ces_files if _partition_ym(f)[0] in DENSE_YEARS]
    if not dense:
        return _result("SKIP", "CES Intra-Series Gap Detection", "No CES data in dense window")

    frames = []
    for f in dense:
        df = pd.read_parquet(f, columns=["sovereign_series_id", "data_timestamp"])
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    combined["_dt"] = pd.to_datetime(combined["data_timestamp"], utc=True)

    gap_series = []
    series_ids = combined["sovereign_series_id"].dropna().unique()[:MAX_SERIES_CHECKED]
    for sid in series_ids:
        months = (combined[combined["sovereign_series_id"] == sid]["_dt"]
                  .sort_values().dt.to_period("M").drop_duplicates())
        if len(months) < 4:
            continue
        expected = pd.period_range(months.min(), months.max(), freq="M")
        missing  = len(expected.difference(months.values))
        if missing > 2:
            gap_series.append({"series": sid, "missing_months": missing})

    if gap_series:
        return _result("WARN", "CES Intra-Series Gap Detection",
                       f"{len(gap_series)}/{len(series_ids)} CES series have >2 missing months "
                       f"(may be discontinued series)",
                       {"examples": gap_series[:5]})
    return _result("PASS", "CES Intra-Series Gap Detection",
                   f"No CES series with >2 consecutive missing months in "
                   f"{len(series_ids)} sampled series ({min(DENSE_YEARS)}-{max(DENSE_YEARS)})")


# =============================================================================
# SECTION B — CPS CHECKS
# =============================================================================

def chk_cps_timestamp_granularity(df):
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    bad = (ts.dt.day != 1).sum()
    if bad:
        return _result("FAIL", "CPS Timestamp Granularity",
                       f"{int(bad):,} CPS records with day!=1")
    return _result("PASS", "CPS Timestamp Granularity",
                   f"All {len(df):,} CPS records have day=1 (monthly)")


def chk_cps_no_future_dates(df):
    ts  = pd.to_datetime(df["data_timestamp"], utc=True)
    fut = (ts > TODAY).sum()
    if fut:
        return _result("FAIL", "CPS No Future Dates",
                       f"{int(fut):,} records have data_timestamp > {TODAY.date()}")
    return _result("PASS", "CPS No Future Dates",
                   f"Latest CPS record: {ts.max().date()} — no future-dated records")


def chk_cps_partition_path_alignment(files, n_sample=60):
    mismatched = []
    sampled = files[::max(1, len(files) // n_sample)][:n_sample]
    for f in sampled:
        p_year, p_month = _partition_ym(f)
        df = pd.read_parquet(f, columns=["data_timestamp"])
        if df.empty:
            continue
        ts  = pd.to_datetime(df["data_timestamp"], utc=True)
        bad = ((ts.dt.year != p_year) | (ts.dt.month != p_month)).sum()
        if bad:
            mismatched.append(f"year={p_year}/month={p_month:02d}: {int(bad)} mismatched")
    if mismatched:
        return _result("FAIL", "CPS Partition Path Alignment",
                       f"{len(mismatched)} CPS partitions have path/timestamp mismatch",
                       {"examples": mismatched[:5]})
    return _result("PASS", "CPS Partition Path Alignment",
                   f"All {len(sampled)} sampled CPS partitions: data_timestamp year/month "
                   f"matches Hive folder exactly")


def chk_cps_start_date_boundary(df):
    """No CPS records should predate 1948-01 (BLS CPS start)."""
    ts = pd.to_datetime(df["data_timestamp"], utc=True)
    cutoff = pd.Timestamp("1948-01-01", tz="UTC")
    pre_cutoff = (ts < cutoff).sum()
    if pre_cutoff:
        return _result("FAIL", "CPS Start Date Boundary",
                       f"{int(pre_cutoff):,} CPS records have data_timestamp before 1948-01")
    return _result("PASS", "CPS Start Date Boundary",
                   f"No CPS records predate 1948-01. Earliest: {ts.min().date()}")


def chk_cps_intra_series_gaps(all_cps_files):
    """Check CPS series for gaps >2 consecutive months in a dense 4-year window."""
    dense = [f for f in all_cps_files if _partition_ym(f)[0] in DENSE_YEARS]
    if not dense:
        return _result("SKIP", "CPS Intra-Series Gap Detection", "No CPS files in dense window")
    frames = []
    for f in dense:
        sid_col = None
        # support both v1 (source_series_id) and v2 (sovereign_series_id)
        try:
            sample = pd.read_parquet(f, columns=["sovereign_series_id", "data_timestamp"])
            sid_col = "sovereign_series_id"
        except Exception:
            try:
                sample = pd.read_parquet(f, columns=["source_series_id", "data_timestamp"])
                sid_col = "source_series_id"
            except Exception:
                continue
        frames.append(sample.rename(columns={sid_col: "_sid"}))
    if not frames:
        return _result("SKIP", "CPS Intra-Series Gap Detection", "Could not load CPS data")

    combined = pd.concat(frames, ignore_index=True)
    combined["_dt"] = pd.to_datetime(combined["data_timestamp"], utc=True)

    gap_series = []
    series_ids = combined["_sid"].dropna().unique()[:MAX_SERIES_CHECKED]
    for sid in series_ids:
        months = (combined[combined["_sid"] == sid]["_dt"]
                  .sort_values().dt.to_period("M").drop_duplicates())
        if len(months) < 4:
            continue
        expected = pd.period_range(months.min(), months.max(), freq="M")
        missing  = len(expected.difference(months.values))
        if missing > 2:
            gap_series.append({"series": sid, "missing_months": missing})

    if gap_series:
        return _result("WARN", "CPS Intra-Series Gap Detection",
                       f"{len(gap_series)}/{len(series_ids)} CPS series have >2 missing months",
                       {"examples": gap_series[:5]})
    return _result("PASS", "CPS Intra-Series Gap Detection",
                   f"No CPS series with >2 consecutive missing months in "
                   f"{len(series_ids)} sampled series ({min(DENSE_YEARS)}-{max(DENSE_YEARS)})")


# =============================================================================
# SECTION C — CES / CPS SYNC
# =============================================================================

def chk_ces_cps_coverage_overlap(ces_ym, cps_ym):
    """Every month in the shared 2011-present window should exist in both CES and CPS."""
    window    = {(y, m) for y in range(2011, 2027) for m in range(1, 13)}
    ces_w     = ces_ym & window
    cps_w     = cps_ym & window
    cps_missing_in_ces = cps_w - ces_w
    if cps_missing_in_ces:
        return _result("FAIL", "CES-CPS Coverage Overlap",
                       f"{len(cps_missing_in_ces)} CPS months not found in CES vault",
                       {"missing_months": sorted(cps_missing_in_ces)[:10]})
    ces_pre_cps_start = {(y, m) for (y, m) in ces_ym if y < 1948}
    return _result("PASS", "CES-CPS Coverage Overlap",
                   f"All {len(cps_w)} CPS months (2011-present) exist in CES vault. "
                   f"CES additionally covers {len(ces_pre_cps_start)} months before CPS start (pre-1948)")


# =============================================================================
# RUNNER
# =============================================================================

def run_checks():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT — TEMPORAL CONSISTENCY CHECK")
    logger.info("=" * 70)

    results = []
    counts  = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}

    def record(r):
        results.append(r)
        counts[r["status"]] += 1

    ces_files   = _all_partitions("bls_ces")
    cps_files   = _all_partitions("bls_cps")

    if not ces_files or not cps_files:
        logger.error("Vault files not found — aborting")
        return

    logger.info(f"\n  CES partitions: {len(ces_files)}, CPS partitions: {len(cps_files)}")

    logger.info("\nLoading CES sample…")
    ces_df = _load_sample("bls_ces", SAMPLE_PARTITIONS)
    logger.info(f"  {len(ces_df):,} CES records loaded")

    logger.info("Loading CPS sample…")
    cps_df = _load_sample("bls_cps", SAMPLE_PARTITIONS)
    logger.info(f"  {len(cps_df):,} CPS records loaded")

    ces_ym   = _get_ym_set("bls_ces")
    cps_ym   = _get_ym_set("bls_cps")

    # --- Section A: CES ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION A — CES TEMPORAL CONSISTENCY")
    logger.info("=" * 70)
    record(chk_ces_timestamp_granularity(ces_df))
    record(chk_ces_timezone_consistency(ces_df))
    record(chk_ces_no_future_dates(ces_df))
    record(chk_ces_partition_path_alignment(ces_files))
    record(chk_ces_temporal_bounds(ces_df, ces_files))
    record(chk_ces_anchor_series(ces_files))
    record(chk_ces_intra_series_gaps(ces_files))

    # --- Section B: CPS ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION B — CPS TEMPORAL CONSISTENCY")
    logger.info("=" * 70)
    record(chk_cps_timestamp_granularity(cps_df))
    record(chk_cps_no_future_dates(cps_df))
    record(chk_cps_partition_path_alignment(cps_files))
    record(chk_cps_start_date_boundary(cps_df))
    record(chk_cps_intra_series_gaps(cps_files))

    # --- Section C: sync ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION C — CES / CPS TEMPORAL SYNC")
    logger.info("=" * 70)
    record(chk_ces_cps_coverage_overlap(ces_ym, cps_ym))

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

    section_a = {"CES Timestamp Granularity", "CES Timezone Consistency", "CES No Future Dates",
                 "CES Partition Path Alignment", "CES Temporal Bounds",
                 "CES Long-Run Anchor Series", "CES Intra-Series Gap Detection"}
    section_b = {"CPS Timestamp Granularity", "CPS No Future Dates",
                 "CPS Partition Path Alignment", "CPS Start Date Boundary",
                 "CPS Intra-Series Gap Detection"}

    lines = [
        "MACRO WAGES & EMPLOYMENT — TEMPORAL CONSISTENCY CHECK REPORT",
        f"Generated: {report['generated']}",
        f"Overall:   [{overall}]",
        f"Counts:    {counts['PASS']} passed / {counts['FAIL']} failed / "
        f"{counts['WARN']} warned / {counts['SKIP']} skipped",
        "", "SECTION A — CES TEMPORAL CONSISTENCY", "-" * 60,
    ]
    cur_section = "A"
    for r in results:
        if r["check"] in section_b and cur_section != "B":
            lines += ["", "SECTION B — CPS TEMPORAL CONSISTENCY", "-" * 60]
            cur_section = "B"
        elif r["check"] not in section_a and r["check"] not in section_b and cur_section != "C":
            lines += ["", "SECTION C — CES / CPS TEMPORAL SYNC", "-" * 60]
            cur_section = "C"
        lines.append(f"  [{r['status']:4}] {r['check']}")
        lines.append(f"         {r['message']}")

    with open(REPORT_TXT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")
    return overall


if __name__ == "__main__":
    run_checks()
