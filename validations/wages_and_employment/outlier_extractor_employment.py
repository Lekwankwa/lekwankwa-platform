"""
Outlier Extractor for Macro Employment Hive Vault

Extracts outliers from BLS CES and JOLTS data and writes outliers.parquet
for each year partition.

OUTLIER DETECTION RULES:
  1. Metric Spikes: Month-over-month changes > 20% (employment rarely swings >20%)
  2. Extreme Drops: Month-over-month drops > 30% (layoff events)
  3. Null Metric Values: Missing metric_value in a record
  4. Range Violations: Values outside plausible bounds

OUTPUT:
  - lekwankwa-historical-vault/product=macro_employment/.../year=XXXX/outliers.parquet

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, IS_GCS, vault_exists, vault_glob_paths as vault_glob, vault_subdirs, vault_read_parquet  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('employment_outlier_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = VAULT_ROOT
PRODUCT = "wages_and_employment"
COUNTRY = "USA"
SOURCES = ["bls_ces", "bls_cps"]

MOM_SPIKE_THRESHOLD_PCT = 20.0
MOM_DROP_THRESHOLD_PCT = 30.0
MAX_METRIC_VALUE = 1_000_000_000.0

OUTLIER_COLUMNS = [
    "record_id", "source", "industry_code", "metric_value",
    "data_timestamp", "series_id",
    "outlier_type", "outlier_severity", "outlier_reason",
    "value_change_pct", "previous_value",
    "outlier_detected_at", "outlier_detection_method"
]

# Known major economic events (for context tagging)
KNOWN_EVENTS = {
    2009: "Global Financial Crisis - significant employment drops expected",
    2020: "COVID-19 Pandemic - extreme employment swings expected",
    2021: "COVID-19 Recovery - extreme employment spikes expected",
}


# =============================================================================
# DETECTION FUNCTIONS
# =============================================================================

def detect_metric_spikes(df: pd.DataFrame, year: int) -> List[Dict]:
    outliers = []
    if "metric_value" not in df.columns or len(df) < 2:
        return outliers

    df = df.copy()
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    df["_metric_numeric"] = pd.to_numeric(df["metric_value"], errors="coerce")
    df = df.sort_values([sid_col, "data_timestamp"])
    df["_pct_change"] = df.groupby(sid_col)["_metric_numeric"].pct_change() * 100

    spikes = df[
        (df["_pct_change"].abs() > MOM_SPIKE_THRESHOLD_PCT) &
        df["_pct_change"].notna()
    ]

    known_event = KNOWN_EVENTS.get(year, "")

    for _, row in spikes.iterrows():
        pct = row["_pct_change"]
        outlier_type = "metric_spike" if pct > 0 else "metric_drop"
        severity = "critical" if abs(pct) > 50 else "high" if abs(pct) > 30 else "medium"
        reason = f"MoM change of {pct:.1f}%"
        if known_event:
            reason += f" - {known_event}"

        outliers.append({
            "record_id": row.get("record_id"),
            "source": row.get("source"),
            "industry_code": row.get("industry_code"),
            "metric_value": row.get("metric_value"),
            "data_timestamp": row.get("data_timestamp"),
            "series_id": row.get("sovereign_series_id") or row.get("source_series_id"),
            "outlier_type": outlier_type,
            "outlier_severity": severity,
            "outlier_reason": reason,
            "value_change_pct": round(float(pct), 4),
            "previous_value": row.get("_metric_numeric"),
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "mom_pct_change"
        })

    return outliers


def detect_null_metrics(df: pd.DataFrame) -> List[Dict]:
    outliers = []
    if "metric_value" not in df.columns:
        return outliers

    null_rows = df[pd.to_numeric(df["metric_value"], errors="coerce").isna()]

    for _, row in null_rows.iterrows():
        outliers.append({
            "record_id": row.get("record_id"),
            "source": row.get("source"),
            "industry_code": row.get("industry_code"),
            "metric_value": None,
            "data_timestamp": row.get("data_timestamp"),
            "series_id": row.get("sovereign_series_id") or row.get("source_series_id"),
            "outlier_type": "null_metric",
            "outlier_severity": "medium",
            "outlier_reason": "metric_value is null or non-numeric",
            "value_change_pct": None,
            "previous_value": None,
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "null_check"
        })

    return outliers


def detect_range_violations(df: pd.DataFrame) -> List[Dict]:
    outliers = []
    if "metric_value" not in df.columns:
        return outliers

    numeric = pd.to_numeric(df["metric_value"], errors="coerce")
    violations = df[numeric > MAX_METRIC_VALUE]

    for _, row in violations.iterrows():
        outliers.append({
            "record_id": row.get("record_id"),
            "source": row.get("source"),
            "industry_code": row.get("industry_code"),
            "metric_value": row.get("metric_value"),
            "data_timestamp": row.get("data_timestamp"),
            "series_id": row.get("sovereign_series_id") or row.get("source_series_id"),
            "outlier_type": "range_violation",
            "outlier_severity": "high",
            "outlier_reason": f"metric_value > {MAX_METRIC_VALUE:,}",
            "value_change_pct": None,
            "previous_value": None,
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "range_check"
        })

    return outliers


# =============================================================================
# YEAR-LEVEL EXTRACTION
# =============================================================================

def extract_outliers_for_year(source: str, year_folder, year: int):
    """Load all month partitions for a year, detect outliers, write outliers.parquet."""
    month_folders = vault_subdirs(str(year_folder), "month=")

    dfs = []
    for mf in month_folders:
        data_files = [f for f in vault_glob(str(mf), "*.parquet")
                      if "outliers" not in f.name and "changelog" not in f.name]
        for f in data_files:
            try:
                dfs.append(vault_read_parquet(f))
            except Exception as e:
                logger.warning(f"Could not read {f}: {e}")

    if not dfs:
        logger.warning(f"No data found for {source}/year={year}")
        return 0

    df = pd.concat(dfs, ignore_index=True)

    all_outliers = []
    all_outliers.extend(detect_metric_spikes(df, year))
    all_outliers.extend(detect_null_metrics(df))
    all_outliers.extend(detect_range_violations(df))

    # Build outlier DataFrame (always write, even if empty)
    if all_outliers:
        outlier_df = pd.DataFrame(all_outliers)
        for col in OUTLIER_COLUMNS:
            if col not in outlier_df.columns:
                outlier_df[col] = None
        outlier_df = outlier_df[OUTLIER_COLUMNS]
    else:
        outlier_df = pd.DataFrame(columns=OUTLIER_COLUMNS)

    # Group by month and write to month partitions
    if not outlier_df.empty:
        outlier_df['_month'] = pd.to_datetime(outlier_df['data_timestamp']).dt.month
        for month, group in outlier_df.groupby('_month'):
            output_path = f"{year_folder}/month={month:02d}/outliers.parquet"
            if not IS_GCS:
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            group.drop(columns=['_month']).to_parquet(output_path, compression="snappy", index=False)
    else:
        output_path = f"{year_folder}/month=01/outliers.parquet"
        if not IS_GCS:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        outlier_df.to_parquet(output_path, compression="snappy", index=False)

    return len(all_outliers)


# =============================================================================
# MAIN
# =============================================================================

def run_outlier_extraction():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT - OUTLIER EXTRACTION")
    logger.info("=" * 70)

    total_outliers = 0
    total_years = 0

    for source in SOURCES:
        source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
        if not vault_exists(source_path):
            logger.error(f"Path not found: {source_path}")
            continue

        year_folders = vault_subdirs(source_path, "year=")
        logger.info(f"\nSource: {source} | Years: {len(year_folders)}")

        for year_folder in year_folders:
            year = int(year_folder.name.split("=")[1])
            count = extract_outliers_for_year(source, year_folder, year)
            total_outliers += count
            total_years += 1

            if count > 0:
                logger.info(f"  year={year}: {count} outliers detected")

    logger.info("\n" + "=" * 70)
    logger.info("OUTLIER EXTRACTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Years processed: {total_years}")
    logger.info(f"Total outliers extracted: {total_outliers:,}")
    logger.info("[PASS] Outlier extraction complete")
    return True


if __name__ == "__main__":
    success = run_outlier_extraction()
    exit(0 if success else 1)
