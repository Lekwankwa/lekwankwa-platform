"""
Outlier Extractor for Hive Vault — Housing Supply & Shelter Inflation

Extracts outliers from base_data.parquet based on validation rules and creates
outliers.parquet for each year. Always creates the file (empty if no outliers).

OUTLIER DETECTION RULES:
  1. Value Spikes:       Month-over-month changes > 10% for CPI index
  2. Value Spikes (BPS): Month-over-month changes > 80% for building permits
  3. Null Values:        Missing critical fields (series_id, value)
  4. Range Violations:   Values outside domain-appropriate bounds
     - BLS CPI shelter:  index value 1 – 1500
     - Census BPS:       units 0 – 5 000 000

OUTPUT:
  - lekwankwa-historical-vault/.../year=XXXX/outliers.parquet
  - Always created (empty DataFrame if no outliers)

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outlier_extraction_housing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, IS_GCS, vault_exists, vault_glob_paths as vault_glob, vault_subdirs, vault_read_parquet  # noqa: E402

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

VAULT_DIR = VAULT_ROOT
PRODUCT   = "Housing_Supply_and_Shelter_Inflation"
COUNTRY   = "USA"
SOURCES   = ["bls_cpi_shelter", "census_bps"]

# Detection thresholds
CPI_MOM_THRESHOLD_PCT      = 10.0     # CPI rarely moves >10% month-over-month
BPS_MOM_THRESHOLD_PCT      = 80.0     # Permits can be more volatile
MIN_CPI_VALUE              = 1.0
MAX_CPI_VALUE              = 1500.0
MIN_BPS_VALUE              = 0.0
MAX_BPS_VALUE              = 5_000_000.0

# Per-source thresholds and ranges
SOURCE_THRESHOLDS = {
    "bls_cpi_shelter": {
        "mom_threshold": CPI_MOM_THRESHOLD_PCT,
        "min_val": MIN_CPI_VALUE,
        "max_val": MAX_CPI_VALUE,
    },
    "census_bps": {
        "mom_threshold": BPS_MOM_THRESHOLD_PCT,
        "min_val": MIN_BPS_VALUE,
        "max_val": MAX_BPS_VALUE,
    },
}

# Actual column names in the housing parquet files
VALUE_COL  = "observed_value"   # NOT "value"
SERIES_COL = "sovereign_series_id"  # NOT "series_id"
Z_SCORE_THRESHOLD = 3.5

# Outlier schema
OUTLIER_COLUMNS = [
    "series_id", "variable_name", "value", "unit_of_measure",
    "adjustment_type", "data_timestamp",
    "outlier_type", "outlier_severity", "outlier_reason",
    "price_change_pct", "previous_value", "next_value",
    "outlier_detected_at", "outlier_detection_method", "source"
]


# ══════════════════════════════════════════════════════════════════════════════
#  OUTLIER DETECTION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def _build_mom_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force numeric types, sort chronologically, compute month-over-month
    percentage change per series, then compute per-series Z-scores on that
    rate of change (not on the raw level index).

    Grouping by series prevents cross-series contamination (e.g. mixing
    CPI index magnitudes with permit counts would distort the std).
    """
    ts_col = "data_timestamp"
    if ts_col not in df.columns:
        ts_col = "reporting_date"

    # Force observed_value to numeric — silently drops placeholder strings
    df = df.copy()
    df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce")

    df_sorted = df.sort_values([SERIES_COL, ts_col]).reset_index(drop=True)

    # MoM % change — evaluated per series so year-boundary transitions are captured
    df_sorted["_prev"] = df_sorted.groupby(SERIES_COL)[VALUE_COL].shift(1)
    df_sorted["_next"] = df_sorted.groupby(SERIES_COL)[VALUE_COL].shift(-1)
    df_sorted["_mom_pct"] = (
        (df_sorted[VALUE_COL] - df_sorted["_prev"])
        / df_sorted["_prev"].replace(0, np.nan)
    ) * 100

    # Z-score calculated on the rate-of-change column, per series
    df_sorted["_z"] = df_sorted.groupby(SERIES_COL)["_mom_pct"].transform(
        lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0.0
    )
    df_sorted["_ts_col"] = ts_col
    return df_sorted


def detect_value_spikes(df: pd.DataFrame, source: str) -> List[Dict]:
    """
    Flag records where the month-over-month % change in observed_value
    exceeds both the hard threshold AND 3.5 standard deviations from
    that series' own historical norm.
    """
    outliers = []
    if VALUE_COL not in df.columns or SERIES_COL not in df.columns:
        logger.warning(f"  [SKIP] {source}: missing {VALUE_COL!r} or {SERIES_COL!r}")
        return outliers

    threshold = SOURCE_THRESHOLDS.get(source, {}).get("mom_threshold", CPI_MOM_THRESHOLD_PCT)
    df_m = _build_mom_df(df)

    extreme = df_m[
        df_m["_mom_pct"].notna() &
        (
            (df_m["_mom_pct"].abs() > threshold) |
            (df_m["_z"].abs() > Z_SCORE_THRESHOLD)
        )
    ]

    for _, row in extreme.iterrows():
        ts_col = row["_ts_col"]
        mom = row["_mom_pct"]
        z   = row["_z"]
        severity = "critical" if abs(mom) > threshold * 2 or abs(z) > 5 else "high"
        reason = f"MoM change: {mom:.2f}% (threshold: ±{threshold}%), Z-score: {z:.2f}"
        outliers.append({
            "series_id":              row.get(SERIES_COL),
            "variable_name":          row.get("macro_metric_name"),
            "value":                  row.get(VALUE_COL),
            "unit_of_measure":        row.get("unit_of_measure"),
            "adjustment_type":        row.get("seasonal_adjustment"),
            "data_timestamp":         row.get(ts_col),
            "outlier_type":           "value_spike",
            "outlier_severity":       severity,
            "outlier_reason":         reason,
            "price_change_pct":       mom,
            "previous_value":         row["_prev"],
            "next_value":             row["_next"],
            "outlier_detected_at":    datetime.now().isoformat(),
            "outlier_detection_method": "mom_pct_change_z_score",
            "source":                 source,
        })

    return outliers


def detect_null_values(df: pd.DataFrame, source: str) -> List[Dict]:
    """Detect rows with null critical values in the actual column names."""
    outliers = []
    ts_col = "data_timestamp" if "data_timestamp" in df.columns else "reporting_date"

    for col in (VALUE_COL, SERIES_COL):
        if col not in df.columns:
            continue
        null_rows = df[df[col].isnull()]
        for _, row in null_rows.iterrows():
            outliers.append({
                "series_id":            row.get(SERIES_COL),
                "variable_name":        row.get("macro_metric_name"),
                "value":                row.get(VALUE_COL),
                "unit_of_measure":      row.get("unit_of_measure"),
                "adjustment_type":      row.get("seasonal_adjustment"),
                "data_timestamp":       row.get(ts_col),
                "outlier_type":         "null_value",
                "outlier_severity":     "critical",
                "outlier_reason":       f"Null value in critical column: {col}",
                "price_change_pct":     None,
                "previous_value":       None,
                "next_value":           None,
                "outlier_detected_at":  datetime.now().isoformat(),
                "outlier_detection_method": "null_value_detection",
                "source":               source,
            })

    return outliers


def detect_range_violations(df: pd.DataFrame, source: str) -> List[Dict]:
    """Detect values outside domain-appropriate bounds."""
    outliers = []
    if VALUE_COL not in df.columns:
        return outliers

    ts_col     = "data_timestamp" if "data_timestamp" in df.columns else "reporting_date"
    thresholds = SOURCE_THRESHOLDS.get(source, {})
    min_val    = thresholds.get("min_val", 0)
    max_val    = thresholds.get("max_val", float("inf"))

    numeric_vals = pd.to_numeric(df[VALUE_COL], errors="coerce")
    invalid = df[(numeric_vals < min_val) | (numeric_vals > max_val)]

    for _, row in invalid.iterrows():
        val = pd.to_numeric(row[VALUE_COL], errors="coerce")
        outliers.append({
            "series_id":            row.get(SERIES_COL),
            "variable_name":        row.get("macro_metric_name"),
            "value":                val,
            "unit_of_measure":      row.get("unit_of_measure"),
            "adjustment_type":      row.get("seasonal_adjustment"),
            "data_timestamp":       row.get(ts_col),
            "outlier_type":         "range_violation",
            "outlier_severity":     "high",
            "outlier_reason":       f"Value {val} outside range [{min_val}, {max_val}]",
            "price_change_pct":     None,
            "previous_value":       None,
            "next_value":           None,
            "outlier_detected_at":  datetime.now().isoformat(),
            "outlier_detection_method": "range_validation",
            "source":               source,
        })

    return outliers


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def _load_source_df(vault_path: Path, source: str) -> pd.DataFrame:
    """
    Load ALL parquet files for a source across all years/months.
    This is required so MoM calculations are never broken at year boundaries.
    """
    # Match data files by the standard *_data.parquet convention rather than a
    # hardcoded name. USA housing files are written with source-specific names
    # (housing_hicp_rent_data.parquet, permits_eu27_data.parquet) that never
    # matched the old hardcoded bls/census names, so this stage silently loaded
    # nothing. Ancillary sidecars (outliers.parquet, changelog.parquet) don't
    # end in _data.parquet and are naturally excluded.
    dfs = []
    for parquet_file in sorted(vault_glob(vault_path, "*_data.parquet")):
        try:
            dfs.append(vault_read_parquet(parquet_file))
        except Exception as e:
            logger.warning(f"Could not read {parquet_file}: {e}")

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def extract_outliers_for_source(vault_path: Path, source: str) -> Dict[int, pd.DataFrame]:
    """
    Load complete source history, detect outliers across the full timeline,
    then split results back into per-year DataFrames for vault storage.
    """
    logger.info(f"  Loading full history for {source}...")
    full_df = _load_source_df(vault_path, source)

    if full_df.empty:
        logger.warning(f"  No data loaded for {source}")
        return {}

    logger.info(f"  Loaded {len(full_df)} total records across all years")

    # Run all detection methods on the complete history
    spikes = detect_value_spikes(full_df, source)
    nulls  = detect_null_values(full_df, source)
    ranges = detect_range_violations(full_df, source)

    logger.info(f"    Value spikes (MoM Z-score): {len(spikes)}")
    logger.info(f"    Null values:                {len(nulls)}")
    logger.info(f"    Range violations:           {len(ranges)}")

    all_outliers = spikes + nulls + ranges

    if all_outliers:
        outliers_df = pd.DataFrame(all_outliers)
        for col in OUTLIER_COLUMNS:
            if col not in outliers_df.columns:
                outliers_df[col] = None
        outliers_df = outliers_df[OUTLIER_COLUMNS]
    else:
        outliers_df = pd.DataFrame(columns=OUTLIER_COLUMNS)

    # Extract year from data_timestamp to split into per-year files
    ts_col = "data_timestamp" if "data_timestamp" in outliers_df.columns else None
    if ts_col and len(outliers_df) > 0:
        ts = pd.to_datetime(outliers_df[ts_col], errors="coerce", utc=True)
        outliers_df["_year"] = ts.dt.year
    else:
        outliers_df["_year"] = pd.Series(dtype=int)

    # Gather the set of actual years with data in the vault
    year_folders = vault_subdirs(vault_path, "year=")

    year_map: Dict[int, pd.DataFrame] = {}
    for yf in year_folders:
        year = int(yf.name.split("=")[1])
        if len(outliers_df) > 0:
            year_outliers = outliers_df[outliers_df["_year"] == year].drop(columns=["_year"])
        else:
            year_outliers = pd.DataFrame(columns=OUTLIER_COLUMNS)
        year_map[year] = year_outliers

    return year_map


def run_outlier_extraction():
    logger.info("=" * 70)
    logger.info("HOUSING — OUTLIER EXTRACTION PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Product: {PRODUCT}")
    logger.info(f"Country: {COUNTRY}")
    logger.info(f"Sources: {', '.join(SOURCES)}")
    logger.info(f"Z-score threshold: {Z_SCORE_THRESHOLD} std-dev | MoM thresholds: CPI {CPI_MOM_THRESHOLD_PCT}%, BPS {BPS_MOM_THRESHOLD_PCT}%")
    logger.info("=" * 70)

    total_years    = 0
    total_outliers = 0
    any_success    = False

    for source in SOURCES:
        vault_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"

        if not vault_exists(vault_path):
            logger.warning(f"Vault path not found for source '{source}': {vault_path}")
            continue

        logger.info(f"\n{'-' * 70}")
        logger.info(f"Processing SOURCE: {source.upper()}")
        logger.info(f"{'-' * 70}")

        # Load full history first — required for correct cross-year MoM Z-scores
        year_map = extract_outliers_for_source(vault_path, source)

        if not year_map:
            logger.warning(f"  No year data produced for {source}")
            continue

        source_outliers = 0
        for year, outliers_df in sorted(year_map.items()):
            year_folder   = f"{vault_path}/year={year}"
            # Group by month and write to month partitions
            if not outliers_df.empty:
                outliers_df['_month'] = pd.to_datetime(outliers_df['data_timestamp']).dt.month
                for month, group in outliers_df.groupby('_month'):
                    outliers_file = f"{year_folder}/month={month:02d}/outliers.parquet"
                    if not IS_GCS:
                        Path(outliers_file).parent.mkdir(parents=True, exist_ok=True)
                    group.drop(columns=['_month']).to_parquet(
                        outliers_file, engine="pyarrow", compression="snappy", index=False
                    )
            else:
                outliers_file = f"{year_folder}/month=01/outliers.parquet"
                if not IS_GCS:
                    Path(outliers_file).parent.mkdir(parents=True, exist_ok=True)
                outliers_df.to_parquet(
                    outliers_file, engine="pyarrow", compression="snappy", index=False
                )
            n = len(outliers_df)
            source_outliers += n
            if n > 0:
                logger.info(f"  year={year}: {n} outliers written to month partitions")

        logger.info(f"\n{source.upper()} Summary: {len(year_map)} years, {source_outliers} total outliers")
        total_years    += len(year_map)
        total_outliers += source_outliers
        any_success     = True

    logger.info("\n" + "=" * 70)
    logger.info("EXTRACTION SUMMARY (ALL SOURCES)")
    logger.info("=" * 70)
    logger.info(f"Total years processed: {total_years}")
    logger.info(f"Total outliers found:  {total_outliers}")
    logger.info("=" * 70)

    return any_success


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_outlier_extraction() else 1)
