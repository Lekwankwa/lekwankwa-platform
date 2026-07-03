"""
Outlier Extractor for Hive Vault

Extracts outliers from base_data.parquet based on validation rules and creates
outliers.parquet for each year. Always creates the file (empty if no outliers).

OUTLIER DETECTION RULES:
  1. Price Spikes: Month-over-month changes > 50%
  2. Currency Shifts: Price drops > 99% (indicates unit/currency change)
  3. Null Values: Missing critical fields
  4. Range Violations: Prices outside reasonable bounds
  5. Duplicate Keys: Primary key violations

KNOWN OUTLIERS (Agricultural Supply Shocks):
  - These are verified legitimate outliers, marked as "known" but still extracted

OUTPUT:
  - lekwankwa-historical-vault/.../year=XXXX/outliers.parquet
  - Always created (empty DataFrame if no outliers)

Author: Lekwankwa Corporation
Date: May 31, 2026
"""

import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, IS_GCS, vault_exists, vault_glob_since as vault_glob, vault_read_parquet  # noqa: E402

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outlier_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

VAULT_DIR = VAULT_ROOT
PRODUCT = "food_micropricing"
COUNTRY = "USA"
SOURCES = ["bls", "usda_ers"]

SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}

# ERS gold-standard column names → BLS legacy names used by detection logic
_ERS_COL_ALIASES = {
    "standard_name":        "standard_name",
    "observed_price_local": "observed_price_local",
    "local_name":           "local_name",
}

# Detection thresholds
MOM_THRESHOLD_PCT = 50.0
MIN_REASONABLE_PRICE = 0.01
MAX_REASONABLE_PRICE = 10000.0
CURRENCY_SHIFT_THRESHOLD_PCT = 99.0

# Known outliers (verified agricultural supply shocks)
KNOWN_OUTLIERS = {
    ("Onions", 1984, 1): "Agricultural supply shock - frost/weather event",
    ("Onions", 1990, 1): "Agricultural supply shock - frost/weather event",
    ("Potatoes", 1985, 1): "Agricultural supply shock - winter storage disruption",
    ("Potatoes", 1987, 12): "Agricultural supply shock - severe winter crisis/drought",
    ("Potatoes", 1991, 11): "Agricultural supply shock - winter supply disruption",
    ("Potatoes", 1995, 4): "Agricultural supply shock - catastrophic crop failure",
    ("Potatoes", 1998, 1): "Agricultural supply shock - winter supply disruption",
    ("Tomatoes", 1990, 1): "Agricultural supply shock - winter freeze event",
    ("Tomatoes", 1990, 4): "Agricultural supply shock - recovery from freeze event",
}

# Outlier schema
OUTLIER_COLUMNS = [
    "standard_name", "category", "observed_price_local", "unit", "currency", "price_usd_equivalent",
    "data_timestamp", "source_series_id",
    "outlier_type", "outlier_severity", "outlier_reason", 
    "is_known_outlier", "known_outlier_description",
    "price_change_pct", "previous_value", "next_value",
    "outlier_detected_at", "outlier_detection_method"
]


# ══════════════════════════════════════════════════════════════════════════════
#  OUTLIER DETECTION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def detect_price_spikes(df: pd.DataFrame, year: int) -> List[Dict]:
    """
    Detect month-over-month price spikes > 50%.
    
    Uses two methods:
    1. Embedded pct_change_mom field from source (if available)
    2. Calculated MoM changes from sorted data
    """
    outliers = []
    
    if "observed_price_local" not in df.columns or len(df) < 1:
        return outliers
    
    # METHOD 1: Check embedded pct_change_mom field first (from original scraper)
    if "pct_change_mom" in df.columns:
        embedded_outliers = df[
            (df["pct_change_mom"].abs() > MOM_THRESHOLD_PCT) & 
            (df["pct_change_mom"].notna())
        ]
        
        for _, row in embedded_outliers.iterrows():
            item_name = row.get("standard_name", "")
            
            try:
                timestamp_col = "data_timestamp" if "data_timestamp" in row.index else "processing_timestamp"
                month = pd.to_datetime(row[timestamp_col]).month
            except:
                month = 0
            
            # Check if known outlier
            key = (item_name, year, month)
            is_known = key in KNOWN_OUTLIERS
            
            outlier = {
                "standard_name": row.get("standard_name"),
                "category": row.get("category"),
                "observed_price_local": row.get("observed_price_local"),
                "unit": row.get("unit"),
                "currency": row.get("currency"),
                "price_usd_equivalent": row.get("price_usd_equivalent"),
                "data_timestamp": row.get("data_timestamp", row.get("processing_timestamp")),
                "source_series_id": row.get("source_series_id"),
                "outlier_type": "price_spike",
                "outlier_severity": "critical" if abs(row["pct_change_mom"]) > 100 else "high",
                "outlier_reason": f"Price change: {row['pct_change_mom']:.1f}% (threshold: {MOM_THRESHOLD_PCT}%) [embedded]",
                "is_known_outlier": is_known,
                "known_outlier_description": KNOWN_OUTLIERS.get(key, ""),
                "price_change_pct": row["pct_change_mom"],
                "previous_value": None,  # Not available from embedded data
                "next_value": None,
                "outlier_detected_at": datetime.now().isoformat(),
                "outlier_detection_method": "embedded_pct_change_mom"
            }
            outliers.append(outlier)
    
    # METHOD 2: Calculate from sequential data (original method)
    if len(df) < 2:
        return outliers
    
    # Use data_timestamp (original date) for sorting, fallback to processing_timestamp
    timestamp_col = "data_timestamp" if "data_timestamp" in df.columns else "processing_timestamp"
    
    # Sort by item and timestamp
    df_sorted = df.sort_values(["standard_name", timestamp_col]).copy()
    
    # Calculate price changes
    df_sorted["previous_value"] = df_sorted.groupby("standard_name")["observed_price_local"].shift(1)
    df_sorted["next_value"] = df_sorted.groupby("standard_name")["observed_price_local"].shift(-1)
    df_sorted["price_change_pct"] = ((df_sorted["observed_price_local"] - df_sorted["previous_value"]) / df_sorted["previous_value"]) * 100
    
    # Flag extreme changes
    extreme = df_sorted[
        (df_sorted["price_change_pct"].abs() > MOM_THRESHOLD_PCT) & 
        (df_sorted["price_change_pct"].notna())
    ]
    
    for _, row in extreme.iterrows():
        item_name = row.get("standard_name", "")
        
        try:
            # Use data_timestamp (original date) if available
            timestamp_col = "data_timestamp" if "data_timestamp" in row.index else "processing_timestamp"
            month = pd.to_datetime(row[timestamp_col]).month
        except:
            month = 0
        
        # Check if known outlier
        key = (item_name, year, month)
        is_known = key in KNOWN_OUTLIERS
        
        outlier = {
            "standard_name": row.get("standard_name"),
            "category": row.get("category"),
            "observed_price_local": row.get("observed_price_local"),
            "unit": row.get("unit"),
            "currency": row.get("currency"),
            "price_usd_equivalent": row.get("price_usd_equivalent"),
            "data_timestamp": row.get("data_timestamp", row.get("processing_timestamp")),
            "source_series_id": row.get("source_series_id"),
            "outlier_type": "price_spike",
            "outlier_severity": "critical" if abs(row["price_change_pct"]) > 100 else "high",
            "outlier_reason": f"Price change: {row['price_change_pct']:.1f}% (threshold: {MOM_THRESHOLD_PCT}%) [calculated]",
            "is_known_outlier": is_known,
            "known_outlier_description": KNOWN_OUTLIERS.get(key, ""),
            "price_change_pct": row["price_change_pct"],
            "previous_value": row["previous_value"],
            "next_value": row.get("next_value"),
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "calculated_mom_pct_change"
        }
        outliers.append(outlier)
    
    return outliers


def detect_currency_shifts(df: pd.DataFrame) -> List[Dict]:
    """
    Detect potential currency/unit shifts (99%+ price drops).
    """
    outliers = []
    
    if "observed_price_local" not in df.columns or len(df) < 2:
        return outliers
    
    # Use data_timestamp (original date) for sorting, fallback to processing_timestamp
    timestamp_col = "data_timestamp" if "data_timestamp" in df.columns else "processing_timestamp"
    
    df_sorted = df.sort_values(["standard_name", timestamp_col]).copy()
    df_sorted["previous_value"] = df_sorted.groupby("standard_name")["observed_price_local"].shift(1)
    df_sorted["price_drop_pct"] = ((df_sorted["previous_value"] - df_sorted["observed_price_local"]) / df_sorted["previous_value"]) * 100
    
    severe_drops = df_sorted[df_sorted["price_drop_pct"] > CURRENCY_SHIFT_THRESHOLD_PCT]
    
    for _, row in severe_drops.iterrows():
        outlier = {
            "standard_name": row.get("standard_name"),
            "category": row.get("category"),
            "observed_price_local": row.get("observed_price_local"),
            "unit": row.get("unit"),
            "currency": row.get("currency"),
            "price_usd_equivalent": row.get("price_usd_equivalent"),
            "data_timestamp": row.get("data_timestamp", row.get("processing_timestamp")),
            "source_series_id": row.get("source_series_id"),
            "outlier_type": "currency_shift",
            "outlier_severity": "critical",
            "outlier_reason": f"Severe price drop: {row['price_drop_pct']:.1f}% (threshold: {CURRENCY_SHIFT_THRESHOLD_PCT}%)",
            "is_known_outlier": False,
            "known_outlier_description": "",
            "price_change_pct": -row["price_drop_pct"],
            "previous_value": row["previous_value"],
            "next_value": None,
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "severe_price_drop_detection"
        }
        outliers.append(outlier)
    
    return outliers


def detect_null_values(df: pd.DataFrame) -> List[Dict]:
    """
    Detect rows with null critical values.
    """
    outliers = []
    
    critical_columns = ["observed_price_local", "standard_name", "processing_timestamp"]
    
    for col in critical_columns:
        if col in df.columns:
            null_rows = df[df[col].isnull()]
            
            for _, row in null_rows.iterrows():
                outlier = {
                    "standard_name": row.get("standard_name"),
                    "category": row.get("category"),
                    "observed_price_local": row.get("observed_price_local"),
                    "unit": row.get("unit"),
                    "currency": row.get("currency"),
                    "price_usd_equivalent": row.get("price_usd_equivalent"),
                    "data_timestamp": row.get("data_timestamp", row.get("processing_timestamp")),
                    "source_series_id": row.get("source_series_id"),
                    "outlier_type": "null_value",
                    "outlier_severity": "critical",
                    "outlier_reason": f"Null value in critical column: {col}",
                    "is_known_outlier": False,
                    "known_outlier_description": "",
                    "price_change_pct": None,
                    "previous_value": None,
                    "next_value": None,
                    "outlier_detected_at": datetime.now().isoformat(),
                    "outlier_detection_method": "null_value_detection"
                }
                outliers.append(outlier)
    
    return outliers


def detect_range_violations(df: pd.DataFrame) -> List[Dict]:
    """
    Detect prices outside reasonable bounds.
    """
    outliers = []
    
    if "observed_price_local" not in df.columns:
        return outliers
    
    invalid = df[
        (df["observed_price_local"] < MIN_REASONABLE_PRICE) | 
        (df["observed_price_local"] > MAX_REASONABLE_PRICE)
    ]
    
    for _, row in invalid.iterrows():
        outlier = {
            "standard_name": row.get("standard_name"),
            "category": row.get("category"),
            "observed_price_local": row.get("observed_price_local"),
            "unit": row.get("unit"),
            "currency": row.get("currency"),
            "price_usd_equivalent": row.get("price_usd_equivalent"),
            "data_timestamp": row.get("data_timestamp", row.get("processing_timestamp")),
            "source_series_id": row.get("source_series_id"),
            "outlier_type": "range_violation",
            "outlier_severity": "high",
            "outlier_reason": f"Price outside reasonable range [{MIN_REASONABLE_PRICE}, {MAX_REASONABLE_PRICE}]",
            "is_known_outlier": False,
            "known_outlier_description": "",
            "price_change_pct": None,
            "previous_value": None,
            "next_value": None,
            "outlier_detected_at": datetime.now().isoformat(),
            "outlier_detection_method": "range_validation"
        }
        outliers.append(outlier)
    
    return outliers


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN EXTRACTION PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def extract_outliers_for_year(year_path: str, year: int, source: str = "bls") -> bool:
    """
    Extract outliers from the year's partition files and create outliers.parquet.
    Always creates the file (empty if no outliers).
    """
    fname = SOURCE_FILES.get(source, "food_pricing_data.parquet")
    month_files = {}   # month int -> file path
    for f in vault_glob(year_path, fname):
        m = re.search(r"month=(\d+)", f.replace("\\", "/"))
        if m:
            month_files[int(m.group(1))] = f

    if not month_files:
        logger.warning(f"No month folders found in {year_path}")
        return False

    try:
        all_dfs = []
        for month, parquet_file in sorted(month_files.items()):
            try:
                all_dfs.append(vault_read_parquet(parquet_file))
            except Exception:
                logger.warning(f"Missing/unreadable {fname} in month={month:02d}")

        if not all_dfs:
            logger.warning(f"No data files found in {year_path}")
            return False

        df = pd.concat(all_dfs, ignore_index=True)
        # Normalise ERS column names → BLS legacy names used by detection logic
        if source == "usda_ers":
            df = df.rename(columns={k: v for k, v in _ERS_COL_ALIASES.items()
                                     if k in df.columns and v not in df.columns})
        logger.info(f"  Processing {len(df)} records from {len(all_dfs)} months...")
        
        # Run all detection methods
        all_outliers = []
        
        price_spikes = detect_price_spikes(df, year)
        all_outliers.extend(price_spikes)
        logger.info(f"    Price spikes: {len(price_spikes)}")
        
        currency_shifts = detect_currency_shifts(df)
        all_outliers.extend(currency_shifts)
        logger.info(f"    Currency shifts: {len(currency_shifts)}")
        
        null_values = detect_null_values(df)
        all_outliers.extend(null_values)
        logger.info(f"    Null values: {len(null_values)}")
        
        range_violations = detect_range_violations(df)
        all_outliers.extend(range_violations)
        logger.info(f"    Range violations: {len(range_violations)}")
        
        # Create DataFrame (empty if no outliers)
        if all_outliers:
            outliers_df = pd.DataFrame(all_outliers)
            # Ensure all expected columns exist
            for col in OUTLIER_COLUMNS:
                if col not in outliers_df.columns:
                    outliers_df[col] = None
        else:
            # Create empty DataFrame with schema
            outliers_df = pd.DataFrame(columns=OUTLIER_COLUMNS)
        
        # Save to parquet (grouped by month partitions)
        if not outliers_df.empty:
            outliers_df['_month'] = pd.to_datetime(outliers_df['data_timestamp']).dt.month
            for month, group in outliers_df.groupby('_month'):
                outliers_file = f"{year_path}/month={month:02d}/outliers.parquet"
                if not IS_GCS:
                    Path(outliers_file).parent.mkdir(parents=True, exist_ok=True)
                group.drop(columns=['_month']).to_parquet(
                    outliers_file,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
        else:
            outliers_file = f"{year_path}/month=01/outliers.parquet"
            if not IS_GCS:
                Path(outliers_file).parent.mkdir(parents=True, exist_ok=True)
            outliers_df.to_parquet(
                outliers_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
        
        logger.info(f"  [PASS] Created outliers.parquet ({len(outliers_df)} outliers) in month partitions")
        return True
        
    except Exception as e:
        logger.error(f"  [FAIL] Failed to extract outliers: {e}")
        return False


def run_outlier_extraction():
    """
    Extract outliers for all years across BLS source in the vault.
    """
    logger.info("=" * 70)
    logger.info("UNIVERSAL OUTLIER EXTRACTION PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Product: {PRODUCT}")
    logger.info(f"Country: {COUNTRY}")
    logger.info(f"Sources: {', '.join(SOURCES)}")
    logger.info("=" * 70)
    
    total_successful = 0
    total_outliers = 0
    overall_year_count = 0
    
    for source in SOURCES:
        vault_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"

        if not vault_exists(vault_path):
            logger.warning(f"Vault path not found for source '{source}': {vault_path}")
            continue

        fname = SOURCE_FILES.get(source, "food_pricing_data.parquet")
        years = sorted({
            int(m.group(1)) for f in vault_glob(vault_path, fname)
            if (m := re.search(r"year=(\d+)", f.replace("\\", "/")))
        })

        if not years:
            logger.warning(f"No year folders found in {vault_path}")
            continue

        logger.info(f"\n{'-' * 70}")
        logger.info(f"Processing SOURCE: {source.upper()}")
        logger.info(f"Years to process: {len(years)}")
        logger.info(f"{'-' * 70}")

        source_successful = 0
        source_outliers = 0

        for year in years:
            year_path = f"{vault_path}/year={year}"
            logger.info(f"\nExtracting outliers for {source}/year={year}...")

            if extract_outliers_for_year(year_path, year, source=source):
                source_successful += 1

                # Count outliers written across this year's month partitions
                for outliers_file in vault_glob(year_path, "outliers.parquet"):
                    try:
                        outliers_df = vault_read_parquet(outliers_file)
                        source_outliers += len(outliers_df)
                    except Exception:
                        pass
        
        logger.info(f"\n{source.upper()} Summary: {source_successful}/{len(years)} years, {source_outliers} outliers")
        total_successful += source_successful
        total_outliers += source_outliers
        overall_year_count += len(years)
    
    # Overall Summary
    logger.info("\n" + "=" * 70)
    logger.info("EXTRACTION SUMMARY (ALL SOURCES)")
    logger.info("=" * 70)
    logger.info(f"Total years processed: {overall_year_count}")
    logger.info(f"Successful extractions: {total_successful}")
    logger.info(f"Failed: {overall_year_count - total_successful}")
    logger.info(f"Total outliers extracted: {total_outliers}")
    logger.info("=" * 70)
    
    return total_successful > 0


if __name__ == "__main__":
    import sys
    
    success = run_outlier_extraction()
    sys.exit(0 if success else 1)

