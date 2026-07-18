"""
Changelog Generator for Macro Employment Hive Vault

Generates changelog.parquet for each year documenting:
  - Data ingestion events
  - Schema tracking
  - Validation results
  - Quality metrics

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import hashlib
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_exists, vault_glob_paths as vault_glob, vault_subdirs, vault_read_parquet  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('employment_changelog_generation.log'),
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
SOURCES = ["bls_ces", "bls_jolts"]

CHANGELOG_COLUMNS = [
    "change_id", "year", "source", "change_date", "change_type",
    "change_category", "change_description", "change_severity",
    "records_affected", "columns_affected", "validation_status",
    "change_author", "change_metadata"
]

SCHEMA_REGISTRY = {}


def generate_change_id(year: int, source: str, change_type: str) -> str:
    content = f"{year}_{source}_{change_type}_{datetime.now().isoformat()}"
    return hashlib.md5(content.encode()).hexdigest()[:12]


# =============================================================================
# CHANGELOG ENTRY BUILDERS
# =============================================================================

def log_data_ingestion(df: pd.DataFrame, year: int, source: str) -> Dict[str, Any]:
    return {
        "change_id": generate_change_id(year, source, "data_ingestion"),
        "year": year,
        "source": source,
        "change_date": datetime.now().isoformat(),
        "change_type": "data_ingestion",
        "change_category": "pipeline",
        "change_description": f"Loaded {len(df):,} records from BLS FTP via requests API ({source})",
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": "",
        "validation_status": "completed",
        "change_author": "bls_pit_ingestion",
        "change_metadata": f"source={source};format=parquet;compression=snappy;pit_enabled=true"
    }


def log_schema_tracking(df: pd.DataFrame, year: int, source: str) -> Dict[str, Any]:
    schema_str = ";".join([f"{col}:{dtype}" for col, dtype in df.dtypes.items()])
    schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]

    if schema_hash in SCHEMA_REGISTRY:
        desc = f"Schema unchanged ({len(df.columns)} cols, hash={schema_hash})"
    else:
        SCHEMA_REGISTRY[schema_hash] = year
        desc = f"Schema registered: {len(df.columns)} columns (hash={schema_hash})"

    return {
        "change_id": generate_change_id(year, source, "schema_tracking"),
        "year": year,
        "source": source,
        "change_date": datetime.now().isoformat(),
        "change_type": "schema_tracking",
        "change_category": "metadata",
        "change_description": desc,
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": ",".join(df.columns.tolist()),
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"schema_hash={schema_hash};column_count={len(df.columns)}"
    }


def log_quality_metrics(df: pd.DataFrame, year: int, source: str) -> Dict[str, Any]:
    numeric_vals = pd.to_numeric(df.get("metric_value", pd.Series()), errors="coerce")
    null_count = int(numeric_vals.isna().sum())
    complete_pct = round((1 - null_count / max(len(df), 1)) * 100, 2)

    outlier_file = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}/year={year}/outliers.parquet"
    outlier_count = 0
    if vault_exists(outlier_file):
        try:
            outlier_count = len(vault_read_parquet(outlier_file))
        except Exception:
            pass

    return {
        "change_id": generate_change_id(year, source, "quality_metrics"),
        "year": year,
        "source": source,
        "change_date": datetime.now().isoformat(),
        "change_type": "quality_metrics",
        "change_category": "quality",
        "change_description": f"Quality metrics: {len(df):,} records, {complete_pct}% completeness, {outlier_count} outliers",
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": "",
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"null_count={null_count};completeness_pct={complete_pct};outlier_count={outlier_count}"
    }


def log_pit_status(df: pd.DataFrame, year: int, source: str) -> Dict[str, Any]:
    revision_zero = int((df.get("revision_number", pd.Series([0])) == 0).sum()) if "revision_number" in df.columns else len(df)
    has_record_ids = "record_id" in df.columns

    return {
        "change_id": generate_change_id(year, source, "pit_status"),
        "year": year,
        "source": source,
        "change_date": datetime.now().isoformat(),
        "change_type": "pit_status",
        "change_category": "pit",
        "change_description": f"PIT tracking: {revision_zero:,} records at revision=0, record_ids={'present' if has_record_ids else 'missing'}",
        "change_severity": "info",
        "records_affected": len(df),
        "columns_affected": "record_id,published_date,as_of_date,revision_number,superseded_by",
        "validation_status": "completed",
        "change_author": "automated_pipeline",
        "change_metadata": f"revision_zero_count={revision_zero};has_record_ids={has_record_ids}"
    }


# =============================================================================
# YEAR-LEVEL CHANGELOG GENERATION
# =============================================================================

def generate_changelog_for_year(source: str, year_folder, year: int):
    """Load all month data for a year and generate year-level changelog."""
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
        # Still create minimal changelog
        entries = [{
            "change_id": generate_change_id(year, source, "no_data"),
            "year": year,
            "source": source,
            "change_date": datetime.now().isoformat(),
            "change_type": "data_ingestion",
            "change_category": "pipeline",
            "change_description": f"No data files found for year={year}",
            "change_severity": "warning",
            "records_affected": 0,
            "columns_affected": "",
            "validation_status": "skipped",
            "change_author": "automated_pipeline",
            "change_metadata": ""
        }]
    else:
        df = pd.concat(dfs, ignore_index=True)
        entries = [
            log_data_ingestion(df, year, source),
            log_schema_tracking(df, year, source),
            log_quality_metrics(df, year, source),
            log_pit_status(df, year, source),
        ]

    changelog_df = pd.DataFrame(entries)
    for col in CHANGELOG_COLUMNS:
        if col not in changelog_df.columns:
            changelog_df[col] = None
    changelog_df = changelog_df[CHANGELOG_COLUMNS]

    output_path = year_folder / "changelog.parquet"
    changelog_df.to_parquet(output_path, compression="snappy", index=False)
    return len(entries)


# =============================================================================
# MAIN
# =============================================================================

def run_changelog_generation():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT - CHANGELOG GENERATION")
    logger.info("=" * 70)

    total_entries = 0
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
            count = generate_changelog_for_year(source, year_folder, year)
            total_entries += count
            total_years += 1

    logger.info("\n" + "=" * 70)
    logger.info("CHANGELOG SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Years processed: {total_years}")
    logger.info(f"Total changelog entries: {total_entries:,}")
    logger.info("[PASS] Changelog generation complete")
    return True


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]


def _eu27_changelog_entry(year: int, iso: str, n_records: int) -> list:
    import hashlib
    ts = datetime.now().isoformat()
    cid = hashlib.md5(f"{iso}_{year}_{ts}".encode()).hexdigest()[:12]
    return [{
        "change_id": cid,
        "year": year,
        "source": "eurostat_sdmx",
        "change_date": ts,
        "change_type": "data_ingestion",
        "change_category": "pipeline",
        "change_description": f"{iso}: {n_records:,} Eurostat SDMX records for year={year}",
        "change_severity": "info",
        "records_affected": n_records,
        "columns_affected": "",
        "validation_status": "completed",
        "change_author": "eurostat_sdmx_ingestion",
        "change_metadata": f"country={iso};source=eurostat_sdmx;year={year}",
    }]


def _run_eu27_changelog():
    logger.info("=" * 70)
    logger.info(f"{PRODUCT.upper()} — EU27 CHANGELOG GENERATION")
    logger.info("=" * 70)
    total_years = total_entries = 0
    for iso in EU27_ISO3:
        src = VAULT_DIR / f"product={PRODUCT}" / f"country={iso}" / "source=eurostat_sdmx"
        if not src.exists():
            continue
        for year_dir in sorted(src.glob("year=*")):
            year = int(year_dir.name.split("=")[1])
            n_records = 0
            for f in year_dir.rglob("*.parquet"):
                if "outlier" in f.name or "changelog" in f.name:
                    continue
                try:
                    n_records += len(pd.read_parquet(f))
                except Exception:
                    pass
            entries = _eu27_changelog_entry(year, iso, n_records)
            cl_df = pd.DataFrame(entries)
            for col in CHANGELOG_COLUMNS:
                if col not in cl_df.columns:
                    cl_df[col] = None
            cl_df[CHANGELOG_COLUMNS].to_parquet(
                year_dir / "changelog.parquet", index=False, compression="snappy"
            )
            total_years += 1
            total_entries += len(entries)
    logger.info(f"  EU27 Changelog: {total_years} year-country partitions, {total_entries} entries")
    logger.info("[PASS] EU27 changelog generation complete")
    return True


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true", help="Generate changelogs for EU27 Eurostat data")
    _args, _ = _parser.parse_known_args()
    success = _run_eu27_changelog() if _args.eu27 else run_changelog_generation()
    exit(0 if success else 1)
