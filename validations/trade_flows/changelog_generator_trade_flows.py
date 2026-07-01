"""
Changelog Generator — Trade Flows (US Census FT-900)

Generates changelog.parquet for each year partition documenting:
  - Data ingestion events (row count, file info)
  - Schema tracking (column hash, column count)
  - Quality metrics (completeness, outlier count)

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import hashlib
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_changelog_generation.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "trade_flows"
COUNTRY   = "USA"
SOURCE    = "census_ft900"

CHANGELOG_COLUMNS = [
    "change_id", "year", "source", "change_date", "change_type",
    "change_category", "change_description", "change_severity",
    "records_affected", "columns_affected", "validation_status",
    "change_author", "change_metadata",
]

SCHEMA_REGISTRY: Dict[str, int] = {}


def _change_id(year: int, ctype: str) -> str:
    content = f"{year}_{SOURCE}_{ctype}_{datetime.now().isoformat()}"
    return hashlib.md5(content.encode()).hexdigest()[:12]


# =============================================================================
# CHANGELOG ENTRY BUILDERS
# =============================================================================

def log_data_ingestion(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    n_exp = int((df.get("trade_flow", pd.Series()) == "Export").sum())
    n_imp = int((df.get("trade_flow", pd.Series()) == "Import").sum())
    hs_chapters = df["commodity_code"].nunique() if "commodity_code" in df.columns else 0
    return {
        "change_id":         _change_id(year, "data_ingestion"),
        "year":              year,
        "source":            SOURCE,
        "change_date":       datetime.now().isoformat(),
        "change_type":       "data_ingestion",
        "change_category":   "pipeline",
        "change_description": (
            f"Loaded {len(df):,} records from Census FT-900 API via census_ft900_usa_scraper "
            f"({n_exp:,} exports + {n_imp:,} imports, {hs_chapters} HS chapters)"
        ),
        "change_severity":   "info",
        "records_affected":  len(df),
        "columns_affected":  "",
        "validation_status": "completed",
        "change_author":     "census_ft900_usa_scraper",
        "change_metadata":   f"source=census_ft900;format=parquet;compression=snappy;pit_enabled=true",
    }


def log_schema_tracking(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    schema_str  = ";".join(f"{c}:{t}" for c, t in df.dtypes.items())
    schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]
    if schema_hash in SCHEMA_REGISTRY:
        desc = f"Schema unchanged ({len(df.columns)} cols, hash={schema_hash})"
    else:
        SCHEMA_REGISTRY[schema_hash] = year
        desc = f"Schema registered: {len(df.columns)} columns (hash={schema_hash})"
    return {
        "change_id":         _change_id(year, "schema_tracking"),
        "year":              year,
        "source":            SOURCE,
        "change_date":       datetime.now().isoformat(),
        "change_type":       "schema_tracking",
        "change_category":   "metadata",
        "change_description": desc,
        "change_severity":   "info",
        "records_affected":  len(df),
        "columns_affected":  ",".join(df.columns.tolist()),
        "validation_status": "completed",
        "change_author":     "automated_pipeline",
        "change_metadata":   f"schema_hash={schema_hash};column_count={len(df.columns)}",
    }


def log_quality_metrics(df: pd.DataFrame, year: int) -> Dict[str, Any]:
    numeric   = pd.to_numeric(df.get("observed_value", pd.Series()), errors="coerce")
    null_ct   = int(numeric.isna().sum())
    complete  = round((1 - null_ct / max(len(df), 1)) * 100, 2)

    outlier_path = (VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}"
                    / f"source={SOURCE}" / f"year={year}" / "outliers.parquet")
    outlier_count = 0
    if outlier_path.exists():
        try:
            outlier_count = len(pd.read_parquet(outlier_path))
        except Exception:
            pass

    hs_chapters = df["commodity_code"].nunique() if "commodity_code" in df.columns else 0
    n_months    = pd.to_datetime(df.get("data_timestamp", pd.Series()), errors="coerce",
                                 utc=True).dt.month.nunique() if len(df) else 0

    return {
        "change_id":         _change_id(year, "quality_metrics"),
        "year":              year,
        "source":            SOURCE,
        "change_date":       datetime.now().isoformat(),
        "change_type":       "quality_metrics",
        "change_category":   "data_quality",
        "change_description": (
            f"Quality metrics: {complete:.1f}% completeness, "
            f"{outlier_count} outliers detected, "
            f"{hs_chapters} HS chapters, {n_months} months covered"
        ),
        "change_severity":   "info",
        "records_affected":  len(df),
        "columns_affected":  "observed_value",
        "validation_status": "completed",
        "change_author":     "automated_pipeline",
        "change_metadata":   (
            f"completeness_pct={complete};null_count={null_ct};"
            f"outlier_count={outlier_count};hs_chapters={hs_chapters};months={n_months}"
        ),
    }


# =============================================================================
# PER-YEAR GENERATOR
# =============================================================================

def generate_changelog_for_year(year_folder: Path, year: int) -> List[Dict[str, Any]]:
    entries = []
    data_files = [f for f in year_folder.glob("*.parquet")
                  if "outliers" not in f.name and "changelog" not in f.name]
    if not data_files:
        return entries

    dfs = []
    for f in data_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Could not read {f}: {exc}")
    if not dfs:
        return entries

    df = pd.concat(dfs, ignore_index=True)
    entries.append(log_data_ingestion(df, year))
    entries.append(log_schema_tracking(df, year))
    entries.append(log_quality_metrics(df, year))
    return entries


# =============================================================================
# MAIN
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — CHANGELOG GENERATION")
    logger.info("=" * 70)

    base = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={SOURCE}"
    year_dirs = sorted(base.glob("year=*"))

    if not year_dirs:
        logger.warning("  No year partitions found. Run scraper first.")
        return True

    total_years   = 0
    total_entries = 0

    for year_dir in year_dirs:
        year_str = year_dir.name.split("=")[1]
        try:
            year = int(year_str)
        except ValueError:
            continue

        entries = generate_changelog_for_year(year_dir, year)
        if not entries:
            continue

        df_changelog = pd.DataFrame(entries)
        for col in CHANGELOG_COLUMNS:
            if col not in df_changelog.columns:
                df_changelog[col] = None

        out_path = year_dir / "changelog.parquet"
        df_changelog[CHANGELOG_COLUMNS].to_parquet(out_path, engine="pyarrow", index=False)
        total_years   += 1
        total_entries += len(entries)
        logger.info(f"  year={year}: {len(entries)} changelog entries -> {out_path.name}")

    logger.info("")
    logger.info(f"  Changelog generation complete: {total_years} years, {total_entries} total entries")
    return True


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]


def _run_eu27_changelog():
    import hashlib
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
            ts = datetime.now().isoformat()
            cid = hashlib.md5(f"{iso}_{year}_{ts}".encode()).hexdigest()[:12]
            entries = [{
                "change_id": cid,
                "year": year,
                "source": "eurostat_sdmx",
                "change_date": ts,
                "change_type": "data_ingestion",
                "change_category": "pipeline",
                "change_description": f"{iso}: {n_records:,} Eurostat SDMX trade records for year={year}",
                "change_severity": "info",
                "records_affected": n_records,
                "columns_affected": "",
                "validation_status": "completed",
                "change_author": "eurostat_sdmx_ingestion",
                "change_metadata": f"country={iso};source=eurostat_sdmx;year={year}",
            }]
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
    success = _run_eu27_changelog() if _args.eu27 else run()
    sys.exit(0 if success else 1)
