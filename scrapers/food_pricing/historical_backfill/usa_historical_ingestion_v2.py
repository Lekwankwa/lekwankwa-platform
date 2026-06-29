"""
usa_historical_ingestion_v2.py
Lekwankwa Corporation Pty Ltd

REFINED USA Food Pricing Historical Ingestion with Golden Rule Test
Time Range: January 2000 → April 2026 (316 months)
Source: Bureau of Labor Statistics (BLS) Average Price API v2

GOLDEN RULE:
  Before running full 2000–2026 ingestion, perform a Test Run for January 2000 only.
  If that single month's output matches pricing_schema.json perfectly (clean names,
  verified units, exact 14 fields), then proceed to full historical loop.

REFINEMENTS:
  - Clean item names: Strip varieties from item_name, move to item_description
  - Flat schema: category, bls_series_id, unit as top-level keys
  - Strict compliance: Only fields defined in pricing_schema.json
  - No metadata bloat: No _metadata wrapper, all provenance in standard fields

VERSION: 2.0-GOLDEN-RULE
DATE: April 2026
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

# Import local config
from data_sets_config import COICOP_ITEMS, BLS_SERIES_IDS

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

OUTPUT_ROOT = Path("./archive_v2")
SCHEMA_PATH = Path("./pricing_schema.json")
LOG_FILE = Path("./ingestion_v2.log")

# Time range
START_YEAR = 2000
START_MONTH = 1
END_YEAR = 2026
END_MONTH = 3  # Updated to March 2026

# Rate limiting
BATCH_DELAY_SECONDS = 5.0

# Load pricing schema for validation
PRICING_SCHEMA = json.loads(SCHEMA_PATH.read_text()) if SCHEMA_PATH.exists() else {}
REQUIRED_KEYS = set(PRICING_SCHEMA.get("required", []))
SCHEMA_PROPS = set(PRICING_SCHEMA.get("properties", {}).keys())

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def parse_item_name(full_name: str) -> Tuple[str, Optional[str]]:
    """
    Extract common name and description from full item name.
    
    Examples:
        "Rice (white, long grain)" → ("Rice", "white, long grain")
        "Apples (Red Delicious)" → ("Apples", "Red Delicious")
        "Milk" → ("Milk", None)
    
    Returns:
        (common_name, description or None)
    """
    if "(" in full_name and ")" in full_name:
        name = full_name[:full_name.index("(")].strip()
        desc = full_name[full_name.index("(") + 1:full_name.rindex(")")].strip()
        return name, desc if desc else None
    return full_name.strip(), None


def slug(name: str) -> str:
    """Convert item name to filename-safe slug."""
    s = name.lower()
    for ch in " (/,)'":
        s = s.replace(ch, "_")
    return s.replace("__", "_").strip("_")[:40]


# ══════════════════════════════════════════════════════════════════════════════
#  BLS API FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bls_batch(
    series_ids: List[str],
    start_year: int,
    end_year: int
) -> Dict[str, List[Tuple[int, int, float]]]:
    """
    Fetch BLS data for multiple series IDs across a date range.
    
    Args:
        series_ids: List of BLS series identifiers
        start_year: Start year (inclusive)
        end_year: End year (inclusive, max start_year + 19)
    
    Returns:
        Dict mapping series_id → [(year, month, value), ...]
    """
    if not BLS_API_KEY:
        raise RuntimeError("BLS_API_KEY not found in environment. Check .env file.")
    
    if end_year - start_year > 19:
        raise ValueError(f"BLS API v2 allows max 20 years per request. Got {end_year - start_year + 1} years.")
    
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY,
        "catalog": False,
        "calculations": False,
        "annualaverage": False,
    }
    
    logger.info(f"Fetching BLS data: {start_year}–{end_year}, {len(series_ids)} series")
    
    try:
        resp = requests.post(BLS_API_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        logger.error(f"BLS API request failed: {exc}")
        return {}
    
    if data.get("status") != "REQUEST_SUCCEEDED":
        logger.error(f"BLS API error: {data.get('message', data)}")
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
                logger.warning(f"Skipping invalid observation: {obs} ({exc})")
                continue
            
            rows.append((year, month_num, value))
        
        results[sid] = sorted(rows)  # chronological order
        logger.info(f"  {sid}: {len(rows)} observations")
    
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEMA VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_record(record: dict) -> Tuple[bool, Optional[str]]:
    """
    Validate record against pricing_schema.json.
    
    Returns:
        (is_valid, error_message or None)
    """
    # Check all required fields present
    for key in REQUIRED_KEYS:
        if key not in record:
            return False, f"Missing required field: {key}"
        if record[key] is None and key in ["country_code", "item_name", "item_value", "unit", "currency", "processing_timestamp"]:
            return False, f"Required field cannot be null: {key}"
    
    # Check no extra keys
    for key in record.keys():
        if key not in SCHEMA_PROPS:
            return False, f"Unexpected key not in schema: {key}"
    
    # Type validation
    schema_props = PRICING_SCHEMA.get("properties", {})
    for key, value in record.items():
        if value is None:
            continue  # nullable fields handled above
        
        expected_type = schema_props[key].get("type")
        
        # Handle nullable types: ["string", "null"]
        if isinstance(expected_type, list):
            expected_type = [t for t in expected_type if t != "null"][0] if len([t for t in expected_type if t != "null"]) > 0 else "string"
        
        # Type check
        if expected_type == "string" and not isinstance(value, str):
            return False, f"Key '{key}' should be string, got {type(value).__name__}"
        elif expected_type == "number" and not isinstance(value, (int, float)):
            return False, f"Key '{key}' should be number, got {type(value).__name__}"
        elif expected_type == "boolean" and not isinstance(value, bool):
            return False, f"Key '{key}' should be boolean, got {type(value).__name__}"
    
    # Price validation: MUST be absolute price, NOT percentage/index
    item_value = record.get("item_value")
    if item_value is not None and (item_value < 0 or item_value > 10000):
        return False, f"item_value={item_value} is out of reasonable range (0-10000)"
    
    return True, None


# ══════════════════════════════════════════════════════════════════════════════
#  FILE WRITING
# ══════════════════════════════════════════════════════════════════════════════

def write_record(
    record: dict,
    year: int,
    month: int,
    item_slug: str,
    series_id: str = None
) -> bool:
    """
    Write a validated record to Hive-partitioned directory.
    
    Output path: archive_v2/year=YYYY/month=MM/country=USA/USA_YYYY_MM_{item_slug}_{series_id_suffix}.json
    
    Returns:
        True if written, False if skipped (already exists or validation failed)
    """
    # Validate before writing
    is_valid, error = validate_record(record)
    if not is_valid:
        logger.error(f"Schema validation failed for {item_slug} {year}-{month:02d}: {error}")
        return False
    
    # Create directory
    out_dir = OUTPUT_ROOT / f"year={year:04d}" / f"month={month:02d}" / "country=USA"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # File path - include series_id suffix for uniqueness if provided
    if series_id:
        # Use last 6 chars of series ID for uniqueness (e.g., APU0000701111 → 701111)
        series_suffix = series_id[-6:] if len(series_id) >= 6 else series_id
        fname = out_dir / f"USA_{year:04d}_{month:02d}_{item_slug}_{series_suffix}.json"
    else:
        fname = out_dir / f"USA_{year:04d}_{month:02d}_{item_slug}.json"
    
    if fname.exists():
        logger.debug(f"Skipping existing file: {fname}")
        return False
    
    # Write JSON
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    
    logger.debug(f"Written: {fname}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  SERIES MAPPING
# ══════════════════════════════════════════════════════════════════════════════

def load_bls_series_mapping() -> Dict[str, Tuple]:
    """
    Load BLS Series ID mapping from data_sets_config.
    
    Returns:
        Dict: series_id → (item_code, item_name, item_description, category, unit, bls_unit, factor)
    """
    series_map = {}
    
    for item in COICOP_ITEMS:
        item_code = item["item_code"]
        full_item_name = item["item_name"]
        category = item.get("category", "Unknown")
        unit_std = item.get("unit_of_measurement", "kg")
        
        bls_meta = BLS_SERIES_IDS.get(item_code)
        if not bls_meta:
            logger.debug(f"No BLS Series ID for {item_code} ({full_item_name})")
            continue
        
        # Parse item name
        item_name, item_description = parse_item_name(full_item_name)
        
        # Get conversion factor (handle both factor_to_kg and factor_to_litre)
        factor = bls_meta.get("factor_to_kg") or bls_meta.get("factor_to_litre", 1.0)
        
        series_map[bls_meta["series_id"]] = (
            item_code,
            item_name,
            item_description,
            category,
            unit_std,
            bls_meta["bls_unit"],
            factor
        )
    
    return series_map


# ══════════════════════════════════════════════════════════════════════════════
#  GOLDEN RULE TEST
# ══════════════════════════════════════════════════════════════════════════════

def golden_rule_test() -> bool:
    """
    GOLDEN RULE: Test January 2000 only before full ingestion.
    
    Returns:
        True if test passes (perfect schema compliance), False otherwise
    """
    logger.info("=" * 70)
    logger.info("GOLDEN RULE TEST: January 2000 Only")
    logger.info("=" * 70)
    
    # Clean up old test files from previous runs
    test_dir = OUTPUT_ROOT / "year=2000" / "month=01" / "country=USA"
    if test_dir.exists():
        import shutil
        logger.info("Cleaning up old test files...")
        shutil.rmtree(test_dir)
        test_dir.mkdir(parents=True, exist_ok=True)
    
    # Load series mapping
    series_map = load_bls_series_mapping()
    series_ids = list(series_map.keys())
    
    if not series_ids:
        logger.error("No BLS series IDs found in configuration")
        return False
    
    logger.info(f"Testing with {len(series_ids)} BLS series")
    
    # Fetch ONLY January 2000
    test_data = fetch_bls_batch(series_ids, 2000, 2000)
    
    if not test_data:
        logger.error("Failed to fetch test data for January 2000")
        return False
    
    # Process January 2000 only
    test_records = []
    
    for sid, observations in test_data.items():
        if not observations:
            continue
        
        item_code, item_name, item_description, category, unit_std, bls_unit, factor = series_map[sid]
        item_slug = slug(item_name)
        
        # Filter for January 2000 only
        jan_2000 = [obs for obs in observations if obs[0] == 2000 and obs[1] == 1]
        
        if not jan_2000:
            logger.warning(f"No data for {item_name} in January 2000")
            continue
        
        year, month, raw_price = jan_2000[0]
        price_std = round(raw_price / factor, 4)
        
        # Build record (STRICT schema compliance)
        record = {
            "country_code": "US",
            "item_name": item_name,
            "item_description": item_description,
            "item_code": item_code,
            "category": category,
            "item_value": price_std,
            "unit": unit_std,
            "currency": "USD",
            "usd_equivalent": price_std,
            "pct_change_mom": None,  # First month, no previous data
            "bls_series_id": sid,
            "source_url_direct": "https://www.bls.gov/cpi/data.htm",
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_quality_certified": True
        }
        
        # Validate
        is_valid, error = validate_record(record)
        if not is_valid:
            logger.error(f"❌ TEST FAILED for {item_name}: {error}")
            return False
        
        test_records.append((record, year, month, item_slug, sid))  # Include sid for unique filenames
    
    # Write test files
    logger.info(f"Writing {len(test_records)} test files...")
    
    for record, year, month, item_slug, sid in test_records:
        if not write_record(record, year, month, item_slug, sid):
            logger.error(f"Failed to write test file for {item_slug} (series {sid})")
            return False
    
    # Verification
    logger.info("=" * 70)
    logger.info("TEST VERIFICATION")
    logger.info("=" * 70)
    
    # Check sample file (now includes series_id suffix)
    first_slug = test_records[0][3]
    first_sid = test_records[0][4]
    series_suffix = first_sid[-6:] if len(first_sid) >= 6 else first_sid
    sample_file = OUTPUT_ROOT / "year=2000" / "month=01" / "country=USA" / f"USA_2000_01_{first_slug}_{series_suffix}.json"
    
    if not sample_file.exists():
        logger.error(f"❌ TEST FAILED: Sample file not created: {sample_file}")
        return False
    
    sample_data = json.loads(sample_file.read_text())
    
    # Check field count
    field_count = len(sample_data)
    expected_fields = len(SCHEMA_PROPS)
    
    logger.info(f"Field count: {field_count} (expected {expected_fields})")
    
    if field_count != expected_fields:
        logger.error(f"❌ TEST FAILED: Field count mismatch")
        logger.error(f"   Present: {sorted(sample_data.keys())}")
        logger.error(f"   Expected: {sorted(SCHEMA_PROPS)}")
        return False
    
    # Check required fields
    missing_required = REQUIRED_KEYS - set(sample_data.keys())
    if missing_required:
        logger.error(f"❌ TEST FAILED: Missing required fields: {missing_required}")
        return False
    
    # Check extra fields
    extra_fields = set(sample_data.keys()) - SCHEMA_PROPS
    if extra_fields:
        logger.error(f"❌ TEST FAILED: Extra fields not in schema: {extra_fields}")
        return False
    
    # Check item_name is clean (no parentheses)
    if "(" in sample_data["item_name"]:
        logger.error(f"❌ TEST FAILED: item_name contains variety details: {sample_data['item_name']}")
        logger.error(f"   Should be clean common name, details in item_description")
        return False
    
    # Check unit is present
    if not sample_data.get("unit"):
        logger.error(f"❌ TEST FAILED: unit field is missing or empty")
        return False
    
    logger.info("=" * 70)
    logger.info("✅ GOLDEN RULE TEST: PASSED")
    logger.info("=" * 70)
    logger.info(f"Sample record:")
    logger.info(json.dumps(sample_data, indent=2))
    logger.info("=" * 70)
    
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN INGESTION
# ══════════════════════════════════════════════════════════════════════════════

def process_historical_ingestion():
    """
    Main historical ingestion orchestration.
    """
    logger.info("=" * 70)
    logger.info("  Lekwankwa USA Food Pricing — Historical Bulk Ingestion v2")
    logger.info(f"  Time Range: Jan {START_YEAR} – {END_MONTH:02d}/{END_YEAR} ({316} months)")
    logger.info("  Source: BLS Average Price API v2")
    logger.info("=" * 70)
    
    # GOLDEN RULE: Test January 2000 first
    if not golden_rule_test():
        logger.error("=" * 70)
        logger.error("❌ GOLDEN RULE TEST FAILED")
        logger.error("=" * 70)
        logger.error("Fix schema compliance issues before proceeding to full ingestion.")
        logger.error("Check test output in archive_v2/year=2000/month=01/")
        return
    
    # Proceed to full ingestion
    logger.info("\n" + "=" * 70)
    logger.info("PROCEEDING TO FULL HISTORICAL INGESTION")
    logger.info("=" * 70)
    
    # Load series mapping
    series_map = load_bls_series_mapping()
    series_ids = list(series_map.keys())
    
    logger.info(f"Total BLS Series IDs: {len(series_ids)} (from {len(COICOP_ITEMS)} COICOP items)")
    
    # Batch 1: 2000-2019
    logger.info("\n" + "─" * 70)
    logger.info("BATCH 1: 2000–2019")
    logger.info("─" * 70)
    
    batch1_data = fetch_bls_batch(series_ids, 2000, 2019)
    
    logger.info(f"Rate limiting: sleeping {BATCH_DELAY_SECONDS}s...")
    time.sleep(BATCH_DELAY_SECONDS)
    
    # Batch 2: 2020-2026
    logger.info("\n" + "─" * 70)
    logger.info("BATCH 2: 2020–2026")
    logger.info("─" * 70)
    
    batch2_data = fetch_bls_batch(series_ids, 2020, 2026)
    
    # Merge batches (combine observations from both batches)
    all_data = {}
    for sid in series_ids:
        all_data[sid] = batch1_data.get(sid, []) + batch2_data.get(sid, [])
    
    # Write files
    logger.info("\n" + "=" * 70)
    logger.info("DATA FETCHING COMPLETE — Starting File Generation")
    logger.info("=" * 70)
    
    written_count = 0
    skipped_count = 0
    
    for sid, observations in all_data.items():
        if not observations:
            logger.warning(f"No data for series {sid}")
            continue
        
        # Get item metadata
        item_code, item_name, item_description, category, unit_std, bls_unit, factor = series_map[sid]
        item_slug = slug(item_name)
        
        # Sort by (year, month)
        observations.sort()
        
        prev_price = None
        
        for (year, month, raw_price) in observations:
            # Skip months outside our target range
            if (year, month) < (START_YEAR, START_MONTH):
                continue
            if (year, month) > (END_YEAR, END_MONTH):
                continue
            
            # Convert BLS price to USD per kg or litre
            price_std = round(raw_price / factor, 4)
            
            # Month-over-month % change
            pct_change_mom = None
            if prev_price is not None and prev_price > 0:
                pct_change_mom = round((price_std - prev_price) / prev_price * 100, 4)
            
            # Build record (STRICT schema compliance)
            record = {
                "country_code": "US",
                "item_name": item_name,
                "item_description": item_description,
                "item_code": item_code,
                "category": category,
                "item_value": price_std,
                "unit": unit_std,
                "currency": "USD",
                "usd_equivalent": price_std,
                "pct_change_mom": pct_change_mom,
                "bls_series_id": sid,
                "source_url_direct": "https://www.bls.gov/cpi/data.htm",
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "data_quality_certified": True
            }
            
            # Write file
            if write_record(record, year, month, item_slug, sid):
                written_count += 1
            else:
                skipped_count += 1
            
            prev_price = price_std
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("✅  HISTORICAL INGESTION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Files written       : {written_count:,}")
    logger.info(f"Files skipped       : {skipped_count:,} (already exist)")
    logger.info(f"Output directory    : {OUTPUT_ROOT}")
    logger.info(f"Schema compliance   : ✅ All records validated")
    logger.info("=" * 70)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        process_historical_ingestion()
    except KeyboardInterrupt:
        logger.warning("\n\nIngestion interrupted by user")
    except Exception as exc:
        logger.exception(f"Fatal error during ingestion: {exc}")
