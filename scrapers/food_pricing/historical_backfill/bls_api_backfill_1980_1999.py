"""
bls_api_backfill_1980_1999.py
Lekwankwa Corporation Pty Ltd

BLS API v2 Historical Backfill (1980-1999)
Memory-Efficient Series Discovery & Targeted Ingestion

WHY API > FTP FOR M1 MAC:
  - Targeted data: Only fetch series you already have (not entire BLS database)
  - Native JSON: No parsing tab-separated files, direct schema mapping
  - Lower memory: Fetch only what you need, not 500MB+ FTP files
  - Reliable: No HTML "Access Denied" errors

WORKFLOW:
  1. Discover series_ids from existing archive_v2 files
  2. Chunk requests (max 25 series per API call)
  3. Fetch 1980-1999 data from BLS API v2
  4. Map to 14-field schema (LOCKED)
  5. Merge with existing data, deduplicate
  6. Save to Hive-partitioned archive_v2

VERSION: 1.0
DATE: May 2026
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import psutil
import requests

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

# Import local config for conversion factors
from data_sets_config import COICOP_ITEMS, BLS_SERIES_IDS

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

OUTPUT_ROOT = Path("./archive_v2")

# Backfill range
BACKFILL_START_YEAR = 1980
BACKFILL_END_YEAR = 1999

# API limits
MAX_SERIES_PER_REQUEST = 25
MAX_YEARS_PER_REQUEST = 20
RATE_LIMIT_DELAY = 1.0  # seconds between API calls

# Reference schema (14 fields - LOCKED)
REFERENCE_SCHEMA_KEYS = [
    "country_code",
    "item_name",
    "item_description",
    "item_code",
    "category",
    "item_value",
    "unit",
    "currency",
    "usd_equivalent",
    "pct_change_mom",
    "bls_series_id",
    "source_url_direct",
    "processing_timestamp",
    "data_quality_certified"
]

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  MEMORY MONITORING
# ══════════════════════════════════════════════════════════════════════════════

def get_memory_usage_mb() -> float:
    """Get current process RSS memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def log_memory(stage: str):
    """Log memory usage at a specific processing stage."""
    mem_mb = get_memory_usage_mb()
    logger.info(f"[MEMORY] {stage}: {mem_mb:.2f} MB RSS")


# ══════════════════════════════════════════════════════════════════════════════
#  SERIES DISCOVERY FROM EXISTING ARCHIVE
# ══════════════════════════════════════════════════════════════════════════════

def _get_conversion_factor_for_series(series_id: str) -> float:
    """
    Get unit conversion factor for a BLS series from data_sets_config.
    
    Returns:
        Conversion factor (default 1.0 if not found)
    """
    try:
        for item_code, meta in BLS_SERIES_IDS.items():
            if meta is None:
                continue
            if meta.get("series_id") == series_id:
                # Try both factor_to_kg and factor_to_litre
                factor = meta.get("factor_to_kg") or meta.get("factor_to_litre", 1.0)
                logger.debug(f"Conversion factor for {series_id}: {factor}")
                return factor
    except Exception as exc:
        logger.warning(f"Error getting conversion factor for {series_id}: {exc}")
    
    return 1.0


def discover_series_from_archive() -> Dict[str, dict]:
    """
    Discover all unique series IDs from existing archive_v2 files.
    Also extract metadata (item_name, item_description, etc.) for each series.
    
    Returns:
        Dict: series_id → metadata dict with all 14-field schema info
    """
    logger.info("=" * 70)
    logger.info("DISCOVERING SERIES FROM EXISTING ARCHIVE")
    logger.info("=" * 70)
    
    log_memory("Before series discovery")
    
    if not OUTPUT_ROOT.exists():
        raise FileNotFoundError(f"Archive not found: {OUTPUT_ROOT}")
    
    series_metadata: Dict[str, dict] = {}
    
    # Scan all JSON files in archive
    json_files = list(OUTPUT_ROOT.glob("**/*.json"))
    logger.info(f"Scanning {len(json_files):,} JSON files...")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            series_id = data.get("bls_series_id")
            if not series_id:
                continue
            
            # Store metadata for this series (use first occurrence)
            if series_id not in series_metadata:
                # Get conversion factor from config
                conversion_factor = _get_conversion_factor_for_series(series_id)
                
                series_metadata[series_id] = {
                    "item_name": data.get("item_name"),
                    "item_description": data.get("item_description"),
                    "item_code": data.get("item_code"),
                    "category": data.get("category"),
                    "unit": data.get("unit"),
                    "currency": data.get("currency", "USD"),
                    "conversion_factor": conversion_factor,
                }
        
        except Exception as exc:
            logger.warning(f"Error reading {json_file}: {exc}")
            continue
    
    logger.info(f"\n✅ Discovered {len(series_metadata)} unique series IDs")
    logger.info(f"Sample series: {list(series_metadata.keys())[:5]}")
    
    log_memory("After series discovery")
    
    return series_metadata


# ══════════════════════════════════════════════════════════════════════════════
#  BLS API V2 FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bls_api_batch(
    series_ids: List[str],
    start_year: int,
    end_year: int
) -> Dict[str, List[Tuple[int, int, float]]]:
    """
    Fetch BLS data for multiple series IDs via API v2.
    
    Args:
        series_ids: List of BLS series identifiers (max 25)
        start_year: Start year (inclusive)
        end_year: End year (inclusive, max start_year + 19)
    
    Returns:
        Dict mapping series_id → [(year, month, value), ...]
    """
    if not BLS_API_KEY:
        raise RuntimeError(
            "BLS_API_KEY not found in environment.\n"
            "Add to .env file: BLS_API_KEY=your_key_here\n"
            "Get key at: https://www.bls.gov/developers/home.htm"
        )
    
    if len(series_ids) > MAX_SERIES_PER_REQUEST:
        raise ValueError(f"BLS API allows max {MAX_SERIES_PER_REQUEST} series per request. Got {len(series_ids)}")
    
    if end_year - start_year > MAX_YEARS_PER_REQUEST - 1:
        raise ValueError(f"BLS API v2 allows max {MAX_YEARS_PER_REQUEST} years per request. Got {end_year - start_year + 1} years.")
    
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY,
        "catalog": False,
        "calculations": False,
        "annualaverage": False,
    }
    
    logger.info(f"  Fetching {len(series_ids)} series: {start_year}–{end_year}")
    
    try:
        resp = requests.post(BLS_API_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        logger.error(f"  ❌ API request failed: {exc}")
        return {}
    
    # Check for API errors
    status = data.get("status")
    
    if status == "REQUEST_FAILED":
        error_msg = data.get("message", ["Unknown error"])[0] if isinstance(data.get("message"), list) else data.get("message", "Unknown error")
        
        if "Daily Limit" in error_msg or "daily limit" in error_msg:
            logger.error(f"  ❌ BLS API DAILY LIMIT REACHED")
            logger.error(f"     Message: {error_msg}")
            logger.error(f"     Your account allows 500 requests/day (registered) or 25/day (unregistered)")
            logger.error(f"     Wait 24 hours or use a different API key")
            return {}
        else:
            logger.error(f"  ❌ BLS API error: {error_msg}")
            return {}
    
    if status != "REQUEST_SUCCEEDED":
        logger.error(f"  ❌ BLS API returned status: {status}")
        return {}
    
    # Parse response
    results: Dict[str, List[Tuple[int, int, float]]] = {}
    
    for series in data.get("Results", {}).get("series", []):
        sid = series["seriesID"]
        rows = []
        
        for obs in series.get("data", []):
            period = obs.get("period", "")
            
            # Skip annual averages (M13) and non-monthly periods
            if not period.startswith("M") or period == "M13":
                continue
            
            try:
                month_num = int(period[1:])
                year = int(obs["year"])
                value = float(obs["value"])
            except (ValueError, KeyError) as exc:
                logger.debug(f"  Skipping invalid observation: {obs} ({exc})")
                continue
            
            rows.append((year, month_num, value))
        
        results[sid] = sorted(rows)  # chronological order
        logger.info(f"    {sid}: {len(rows)} observations")
    
    return results


def fetch_all_series_chunked(
    series_ids: List[str],
    start_year: int,
    end_year: int
) -> Dict[str, List[Tuple[int, int, float]]]:
    """
    Fetch all series, chunking into batches of 25 (API limit).
    
    Returns:
        Combined results from all batches
    """
    logger.info(f"\nFetching {len(series_ids)} series in chunks of {MAX_SERIES_PER_REQUEST}...")
    
    all_results = {}
    
    # Chunk series_ids
    for i in range(0, len(series_ids), MAX_SERIES_PER_REQUEST):
        chunk = series_ids[i:i + MAX_SERIES_PER_REQUEST]
        chunk_num = i // MAX_SERIES_PER_REQUEST + 1
        total_chunks = (len(series_ids) + MAX_SERIES_PER_REQUEST - 1) // MAX_SERIES_PER_REQUEST
        
        logger.info(f"\nChunk {chunk_num}/{total_chunks} ({len(chunk)} series):")
        
        batch_results = fetch_bls_api_batch(chunk, start_year, end_year)
        all_results.update(batch_results)
        
        # Rate limiting between chunks
        if i + MAX_SERIES_PER_REQUEST < len(series_ids):
            logger.info(f"  Rate limiting: sleeping {RATE_LIMIT_DELAY}s...")
            time.sleep(RATE_LIMIT_DELAY)
    
    return all_results


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEMA MAPPING & FILE WRITING
# ══════════════════════════════════════════════════════════════════════════════

def map_to_schema(
    series_id: str,
    year: int,
    month: int,
    value: float,
    metadata: dict,
    prev_price: Optional[float] = None
) -> dict:
    """
    Map BLS API response to 14-field schema (LOCKED).
    
    Args:
        series_id: BLS series ID
        year: Observation year
        month: Observation month (1-12)
        value: RAW price value from API (may need unit conversion)
        metadata: Series metadata from archive (item_name, category, conversion_factor, etc.)
        prev_price: Previous month's price for pct_change_mom calculation
    
    Returns:
        dict with exactly 14 fields matching reference schema
    """
    # Apply conversion factor to normalize to standard units (kg or litre)
    conversion_factor = metadata.get("conversion_factor", 1.0)
    converted_value = round(value / conversion_factor, 4)
    
    # Calculate month-over-month % change
    pct_change_mom = None
    if prev_price is not None and prev_price > 0:
        pct_change_mom = round((converted_value - prev_price) / prev_price * 100, 4)
    
    # Build 14-field record (STRICT schema compliance)
    record = {
        "country_code": "US",
        "item_name": metadata.get("item_name"),
        "item_description": metadata.get("item_description"),
        "item_code": metadata.get("item_code"),
        "category": metadata.get("category"),
        "item_value": converted_value,
        "unit": metadata.get("unit"),
        "currency": metadata.get("currency", "USD"),
        "usd_equivalent": converted_value,
        "pct_change_mom": pct_change_mom,
        "bls_series_id": series_id,
        "source_url_direct": "https://www.bls.gov/cpi/data.htm",
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "data_quality_certified": True
    }
    
    # Verify exactly 14 fields (LOCKED)
    assert len(record) == 14, f"Schema violation: expected 14 fields, got {len(record)}"
    assert set(record.keys()) == set(REFERENCE_SCHEMA_KEYS), "Schema keys mismatch"
    
    return record


def write_record(
    record: dict,
    year: int,
    month: int,
    item_slug: str,
    series_id: str
) -> bool:
    """
    Write a record to Hive-partitioned directory.
    
    Output path: archive_v2/year=YYYY/month=MM/country=USA/USA_YYYY_MM_{item_slug}_{series_suffix}.json
    
    Returns:
        True if written, False if skipped (already exists)
    """
    # Create directory
    out_dir = OUTPUT_ROOT / f"year={year:04d}" / f"month={month:02d}" / "country=USA"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # File path with series_id suffix for uniqueness
    series_suffix = series_id[-6:] if len(series_id) >= 6 else series_id
    fname = out_dir / f"USA_{year:04d}_{month:02d}_{item_slug}_{series_suffix}.json"
    
    if fname.exists():
        return False  # Skip existing files
    
    # Write JSON
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    
    return True


def slug(name: str) -> str:
    """Convert item name to filename-safe slug."""
    if not name:
        return "unknown"
    s = name.lower()
    for ch in " (/,)'\"":
        s = s.replace(ch, "_")
    return s.replace("__", "_").strip("_")[:40]


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN BACKFILL ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def process_api_backfill_1980_1999() -> Dict[str, int]:
    """
    Main API backfill function for 1980-1999.
    
    Returns:
        Dict with statistics: {'series_count', 'api_calls', 'new_records', 'skipped', 'errors'}
    """
    logger.info("=" * 70)
    logger.info("  BLS API v2 HISTORICAL BACKFILL (1980-1999)")
    logger.info(f"  Time Range: {BACKFILL_START_YEAR} – {BACKFILL_END_YEAR}")
    logger.info(f"  Output: {OUTPUT_ROOT}")
    logger.info("=" * 70)
    
    log_memory("Start of backfill")
    
    # Step 1: Discover series from existing archive
    series_metadata = discover_series_from_archive()
    
    if not series_metadata:
        logger.error("No series found in archive. Cannot proceed with backfill.")
        return {'series_count': 0, 'api_calls': 0, 'new_records': 0, 'skipped': 0, 'errors': 0}
    
    series_ids = list(series_metadata.keys())
    
    # Step 2: Fetch data from BLS API (chunked)
    logger.info("\n" + "=" * 70)
    logger.info(f"FETCHING DATA FROM BLS API v2")
    logger.info(f"Series to fetch: {len(series_ids)}")
    logger.info(f"API calls required: {(len(series_ids) + MAX_SERIES_PER_REQUEST - 1) // MAX_SERIES_PER_REQUEST}")
    logger.info("=" * 70)
    
    api_data = fetch_all_series_chunked(
        series_ids,
        BACKFILL_START_YEAR,
        BACKFILL_END_YEAR
    )
    
    log_memory("After API data fetch")
    
    if not api_data:
        logger.error("No data returned from API. Check API key and rate limits.")
        return {'series_count': len(series_ids), 'api_calls': 0, 'new_records': 0, 'skipped': 0, 'errors': 0}
    
    # Step 3: Process and write records
    logger.info("\n" + "=" * 70)
    logger.info("PROCESSING & WRITING RECORDS")
    logger.info("=" * 70)
    
    stats = {
        'series_count': len(series_ids),
        'api_calls': (len(series_ids) + MAX_SERIES_PER_REQUEST - 1) // MAX_SERIES_PER_REQUEST,
        'new_records': 0,
        'skipped': 0,
        'errors': 0
    }
    
    for series_id, observations in api_data.items():
        if not observations:
            continue
        
        metadata = series_metadata.get(series_id, {})
        item_name = metadata.get("item_name", "Unknown")
        item_slug_str = slug(item_name)
        
        prev_price = None
        
        for (year, month, value) in observations:
            try:
                # Map to 14-field schema
                record = map_to_schema(
                    series_id,
                    year,
                    month,
                    value,
                    metadata,
                    prev_price
                )
                
                # Write file
                if write_record(record, year, month, item_slug_str, series_id):
                    stats['new_records'] += 1
                else:
                    stats['skipped'] += 1
                
                # Update prev_price for next month
                prev_price = record['item_value']
                
                # Progress logging
                total_processed = stats['new_records'] + stats['skipped']
                if total_processed % 500 == 0:
                    logger.info(f"  Progress: {total_processed:,} records processed ({stats['new_records']:,} new, {stats['skipped']:,} skipped)")
            
            except Exception as exc:
                logger.error(f"Error processing {series_id} {year}-{month:02d}: {exc}")
                stats['errors'] += 1
    
    log_memory("After file writing")
    
    # Step 4: Summary
    logger.info("\n" + "=" * 70)
    logger.info("✅  BLS API BACKFILL COMPLETE (1980-1999)")
    logger.info("=" * 70)
    logger.info(f"Series discovered    : {stats['series_count']}")
    logger.info(f"API calls made       : {stats['api_calls']}")
    logger.info(f"New records added    : {stats['new_records']:,}")
    logger.info(f"Records skipped      : {stats['skipped']:,} (already exist)")
    logger.info(f"Errors               : {stats['errors']}")
    logger.info(f"Output directory     : {OUTPUT_ROOT}")
    logger.info(f"Schema compliance    : ✅ 14-field LOCKED schema")
    logger.info("=" * 70)
    
    # Breakdown by year
    logger.info("\n📊 RECORDS ADDED BY DECADE:")
    if stats['new_records'] > 0:
        logger.info(f"  1980-1989: Check archive_v2/year=198* for details")
        logger.info(f"  1990-1999: Check archive_v2/year=199* for details")
    
    return stats


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler("./api_backfill_1980_1999.log", mode='w'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Run backfill
        stats = process_api_backfill_1980_1999()
        
        print("\n" + "=" * 70)
        print("BACKFILL STATISTICS:")
        print(json.dumps(stats, indent=2))
        print("=" * 70)
        
    except KeyboardInterrupt:
        logger.warning("\n\nBackfill interrupted by user")
    except Exception as exc:
        logger.exception(f"Fatal error during backfill: {exc}")
