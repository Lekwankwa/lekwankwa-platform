"""
Changelog Generator for Hive Vault

Generates changelog.parquet for each year documenting:
  - Data ingestion events
  - Schema changes and evolution
  - Validation results
  - Data quality metrics
  - Methodology changes

Always creates changelog.parquet (minimum: data ingestion record).

CHANGELOG ENTRY TYPES:
  1. data_ingestion - Record of data conversion/loading
  2. schema_tracking - Column count, data types
  3. validation_result - GX/sanity check outcomes
  4. quality_metrics - Row counts, completeness, outlier stats
  5. methodology_change - Algorithm or process updates
  6. schema_evolution - Column additions/removals/renames

OUTPUT:
  - lekwankwa-historical-vault/.../year=XXXX/changelog.parquet
  - Always created (minimum 1 entry per year)

Author: Lekwankwa Corporation
Date: May 31, 2026
"""

import pandas as pd
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging
import hashlib

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, IS_GCS, vault_exists, vault_glob_since as vault_glob, vault_read_parquet  # noqa: E402

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('changelog_generation.log'),
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

_ERS_COL_ALIASES = {
    "standard_name":        "standard_name",
    "observed_price_local": "observed_price_local",
}

# Changelog schema
CHANGELOG_COLUMNS = [
    "change_id", "year", "change_date", "change_type", 
    "change_category", "change_description", "change_severity",
    "records_affected", "columns_affected", "validation_status",
    "change_author", "change_metadata"
]

# Track schema fingerprints to detect changes
SCHEMA_REGISTRY = {}


# ══════════════════════════════════════════════════════════════════════════════
#  CHANGELOG GENERATION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def generate_change_id(year: int, change_type: str, timestamp: str) -> str:
    """Generate unique change ID."""
    content = f"{year}_{change_type}_{timestamp}"
    return hashlib.md5(content.encode()).hexdigest()[:12]


def log_data_ingestion(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    """
    Log data ingestion event (always created).
    """
    timestamp = datetime.now().isoformat()
    
    return {
        "change_id": generate_change_id(year, "data_ingestion", timestamp),
        "year": year,
        "change_date": timestamp,
        "change_type": "data_ingestion",
        "change_category": "pipeline",
        "change_description": f"Converted and loaded {len(df)} records from JSON to Parquet (Hive partition format)",
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": "",
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"source_format=json;target_format=parquet;compression=snappy"
    }


def log_schema_tracking(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    """
    Log schema structure for tracking evolution.
    """
    timestamp = datetime.now().isoformat()
    
    # Create schema fingerprint
    schema_str = ";".join([f"{col}:{dtype}" for col, dtype in df.dtypes.items()])
    schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]
    
    # Check for schema changes
    if schema_hash in SCHEMA_REGISTRY:
        change_desc = f"Schema unchanged: {len(df.columns)} columns (fingerprint: {schema_hash})"
    else:
        SCHEMA_REGISTRY[schema_hash] = year
        change_desc = f"Schema registered: {len(df.columns)} columns (fingerprint: {schema_hash})"
    
    return {
        "change_id": generate_change_id(year, "schema_tracking", timestamp),
        "year": year,
        "change_date": timestamp,
        "change_type": "schema_tracking",
        "change_category": "metadata",
        "change_description": change_desc,
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": ",".join(df.columns.tolist()[:5]) + "...",
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"schema_hash={schema_hash};column_count={len(df.columns)}"
    }


def log_quality_metrics(df: pd.DataFrame, year: int, outliers_count: int) -> Dict[str, Any]:
    """
    Log data quality metrics.
    """
    timestamp = datetime.now().isoformat()
    
    # Calculate metrics
    null_count = df.isnull().sum().sum()
    duplicate_count = df.duplicated().sum()
    completeness_pct = ((df.size - null_count) / df.size * 100) if df.size > 0 else 0
    
    metrics_desc = (
        f"Quality check: {len(df)} rows, "
        f"{completeness_pct:.1f}% complete, "
        f"{outliers_count} outliers, "
        f"{duplicate_count} duplicates"
    )
    
    return {
        "change_id": generate_change_id(year, "quality_metrics", timestamp),
        "year": year,
        "change_date": timestamp,
        "change_type": "quality_metrics",
        "change_category": "validation",
        "change_description": metrics_desc,
        "change_severity": "info" if outliers_count == 0 else "warning",
        "records_affected": len(df),
        "columns_affected": "",
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"null_count={null_count};duplicates={duplicate_count};outliers={outliers_count};completeness_pct={completeness_pct:.2f}"
    }


def log_validation_result(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    """
    Log validation check results.
    """
    timestamp = datetime.now().isoformat()
    
    # Basic validation checks
    checks_passed = []
    checks_failed = []
    
    # Check 1: Minimum row count
    if len(df) >= 10:
        checks_passed.append("row_count")
    else:
        checks_failed.append("row_count")
    
    # Check 2: Required columns (universal schema)
    required_cols = ["standard_name", "observed_price_local", "data_timestamp", "conversion_timestamp"]
    if all(col in df.columns for col in required_cols):
        checks_passed.append("required_columns")
    else:
        checks_failed.append("required_columns")
    
    # Check 3: No null prices
    if "observed_price_local" in df.columns:
        if df["observed_price_local"].isnull().sum() == 0:
            checks_passed.append("no_null_prices")
        else:
            checks_failed.append("no_null_prices")
    
    status = "passed" if len(checks_failed) == 0 else "failed"
    severity = "info" if status == "passed" else "critical"
    
    validation_desc = f"Validation {status}: {len(checks_passed)} checks passed, {len(checks_failed)} failed"
    
    return {
        "change_id": generate_change_id(year, "validation_result", timestamp),
        "year": year,
        "change_date": timestamp,
        "change_type": "validation_result",
        "change_category": "validation",
        "change_description": validation_desc,
        "change_severity": severity,
        "records_affected": len(df),
        "columns_affected": "",
        "validation_status": status,
        "change_author": "automated_pipeline",
        "change_metadata": f"checks_passed={','.join(checks_passed)};checks_failed={','.join(checks_failed)}"
    }


def detect_schema_evolution(df: pd.DataFrame, year: int, previous_schema: Dict) -> List[Dict[str, Any]]:
    """
    Detect schema changes compared to previous year.
    """
    changes = []
    timestamp = datetime.now().isoformat()
    
    if not previous_schema:
        return changes
    
    current_cols = set(df.columns)
    previous_cols = set(previous_schema.get("columns", []))
    
    # New columns
    added_cols = current_cols - previous_cols
    if added_cols:
        changes.append({
            "change_id": generate_change_id(year, "schema_evolution_add", timestamp),
            "year": year,
            "change_date": timestamp,
            "change_type": "schema_evolution",
            "change_category": "schema",
            "change_description": f"New columns added: {', '.join(sorted(added_cols))}",
            "change_severity": "warning",
            "records_affected": len(df),
            "columns_affected": ",".join(sorted(added_cols)),
            "validation_status": "detected",
            "change_author": "automated_pipeline",
            "change_metadata": f"evolution_type=column_addition;count={len(added_cols)}"
        })
    
    # Removed columns
    removed_cols = previous_cols - current_cols
    if removed_cols:
        changes.append({
            "change_id": generate_change_id(year, "schema_evolution_remove", timestamp),
            "year": year,
            "change_date": timestamp,
            "change_type": "schema_evolution",
            "change_category": "schema",
            "change_description": f"Columns removed: {', '.join(sorted(removed_cols))}",
            "change_severity": "critical",
            "records_affected": len(df),
            "columns_affected": ",".join(sorted(removed_cols)),
            "validation_status": "detected",
            "change_author": "automated_pipeline",
            "change_metadata": f"evolution_type=column_removal;count={len(removed_cols)}"
        })
    
    return changes


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CHANGELOG GENERATION PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def generate_changelog_for_year(year_path: str, year: int,
                                previous_schema: Dict = None,
                                source: str = "bls") -> Dict:
    """
    Generate changelog.parquet for a year.
    Always creates the file (minimum 1 entry).
    """
    fname = SOURCE_FILES.get(source, "food_pricing_data.parquet")
    month_files = {}
    for f in vault_glob(year_path, fname):
        m = re.search(r"month=(\d+)", f.replace("\\", "/"))
        if m:
            month_files[int(m.group(1))] = f

    if not month_files:
        logger.warning(f"No month folders found in {year_path}")
        return {"success": False, "schema": None}

    try:
        all_dfs = []
        for month, parquet_file in sorted(month_files.items()):
            try:
                all_dfs.append(vault_read_parquet(parquet_file))
            except Exception:
                logger.warning(f"Missing/unreadable {fname} in month={month:02d}")

        if not all_dfs:
            logger.warning(f"No data files found in {year_path}")
            return {"success": False, "schema": None}

        df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"  Processing {len(df)} records from {len(all_dfs)} months...")

        # Load outliers count (sum across month partitions written by outlier_extractor_food.py)
        outliers_count = 0
        for outliers_file in vault_glob(year_path, "outliers.parquet"):
            try:
                outliers_count += len(vault_read_parquet(outliers_file))
            except Exception:
                pass
        
        # Generate changelog entries
        changelog_entries = []
        
        # Entry 1: Data ingestion (always)
        changelog_entries.append(log_data_ingestion(df, year))
        
        # Entry 2: Schema tracking (always)
        changelog_entries.append(log_schema_tracking(df, year))
        
        # Entry 3: Quality metrics (always)
        changelog_entries.append(log_quality_metrics(df, year, outliers_count))
        
        # Entry 4: Validation result (always)
        changelog_entries.append(log_validation_result(df, year))
        
        # Entry 5: Schema evolution (if detected)
        if previous_schema:
            evolution_entries = detect_schema_evolution(df, year, previous_schema)
            changelog_entries.extend(evolution_entries)
        
        # Create DataFrame
        changelog_df = pd.DataFrame(changelog_entries)
        
        # Ensure all expected columns exist
        for col in CHANGELOG_COLUMNS:
            if col not in changelog_df.columns:
                changelog_df[col] = None
        
        # Save to parquet
        changelog_file = f"{year_path}/changelog.parquet"
        if not IS_GCS:
            Path(changelog_file).parent.mkdir(parents=True, exist_ok=True)
        changelog_df.to_parquet(
            changelog_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        logger.info(f"  [PASS] Created changelog.parquet ({len(changelog_df)} entries)")
        
        # Return schema for next year
        current_schema = {
            "year": year,
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.to_dict()
        }
        
        return {"success": True, "schema": current_schema, "entries": len(changelog_df)}
        
    except Exception as e:
        logger.error(f"  [FAIL] Failed to generate changelog: {e}")
        return {"success": False, "schema": None}


def run_changelog_generation():
    """
    Generate changelogs for all years across BLS source in the vault.
    """
    logger.info("=" * 70)
    logger.info("UNIVERSAL CHANGELOG GENERATION PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Product: {PRODUCT}")
    logger.info(f"Country: {COUNTRY}")
    logger.info(f"Sources: {', '.join(SOURCES)}")
    logger.info("=" * 70)
    
    total_successful = 0
    total_entries_all = 0
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
        source_entries = 0
        previous_schema = None

        for year in years:
            year_path = f"{vault_path}/year={year}"
            logger.info(f"\nGenerating changelog for {source}/year={year}...")

            result = generate_changelog_for_year(year_path, year, previous_schema, source=source)

            if result["success"]:
                source_successful += 1
                source_entries += result.get("entries", 0)
                previous_schema = result.get("schema")

        logger.info(f"\n{source.upper()} Summary: {source_successful}/{len(years)} years, {source_entries} changelog entries")
        total_successful += source_successful
        total_entries_all += source_entries
        overall_year_count += len(years)
    
    # Overall Summary
    logger.info("\n" + "=" * 70)
    logger.info("GENERATION SUMMARY (ALL SOURCES)")
    logger.info("=" * 70)
    logger.info(f"Total years processed: {overall_year_count}")
    logger.info(f"Successful: {total_successful}")
    logger.info(f"Failed: {overall_year_count - total_successful}")
    logger.info(f"Total changelog entries: {total_entries_all}")
    logger.info("=" * 70)
    
    return total_successful > 0


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

def _run_eu27_changelog() -> bool:
    import hashlib
    logger.info("=" * 70)
    logger.info("EU27 FOOD MICROPRICING — CHANGELOG GENERATION (eurostat_sdmx)")
    logger.info("=" * 70)
    base = VAULT_DIR / "product=food_micropricing"
    run_ts = datetime.now().isoformat()
    ok, total = 0, 0

    for iso in EU27_ISO3:
        src = base / f"country={iso}" / "source=eurostat_sdmx"
        if not src.exists(): continue
        year_dirs = sorted({f.parent.parent for f in src.rglob("*.parquet")
                            if "outlier" not in f.name and "changelog" not in f.name})
        for yr_dir in year_dirs:
            year = int(yr_dir.name.replace("year=", ""))
            files = list(yr_dir.rglob("food_pricing_data.parquet"))
            n_records = 0
            for f in files:
                try: n_records += len(vault_read_parquet(f))
                except Exception: pass
            if n_records == 0: continue
            cid = hashlib.md5(f"{iso}_{year}_{run_ts}".encode()).hexdigest()[:12]
            entry = {
                "change_id": cid, "year": year, "source": "eurostat_sdmx",
                "change_date": run_ts, "change_type": "data_ingestion", "change_category": "pipeline",
                "change_description": f"{iso}: {n_records:,} Eurostat HICP food records for year={year}",
                "change_severity": "info", "records_affected": n_records, "columns_affected": "",
                "validation_status": "completed", "change_author": "eurostat_sdmx_ingestion",
                "change_metadata": f"country={iso};source=eurostat_sdmx;year={year}",
            }
            part = yr_dir / "month=1"
            part.mkdir(parents=True, exist_ok=True)
            out = part / "changelog.parquet"
            pd.DataFrame([entry]).to_parquet(out, index=False, engine="pyarrow")
            ok += 1
            total += 1

    logger.info("  %d / %d year-country partitions written", ok, total)
    logger.info("  EU27 Changelog: %d year-country partitions, %d entries", total, total)
    logger.info("  [PASS] EU27 changelog generation complete")
    return True


if __name__ == "__main__":
    import sys, argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true")
    _args, _ = _parser.parse_known_args()
    success = _run_eu27_changelog() if _args.eu27 else run_changelog_generation()
    sys.exit(0 if success else 1)

