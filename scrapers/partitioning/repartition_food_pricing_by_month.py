"""
FOOD PRICING DATASET - MONTH-LEVEL REPARTITIONING SCRIPT
==========================================================

This script reorganizes the existing USA Food Pricing dataset from year-only 
partitioning to year+month Hive partitioning.

OLD STRUCTURE:
    lekwankwa-historical-vault/product=food_micropricing/country=USA/source={bls|usda}/year=YYYY/base_data.parquet

NEW STRUCTURE:
    vault/product=food_micropricing/country=USA/source={bls|usda}/year=YYYY/month=MM/base_data.parquet

FEATURES:
- Reads all existing year-partitioned Parquet files
- Extracts month from data_timestamp field
- Repartitions by year AND month
- Maintains all 22 Schema v4.0 fields (17 core + 5 PIT)
- Preserves compression (snappy)
- Creates proper Hive directory structure
- Logs progress and summary statistics

USAGE:
    python repartition_food_pricing_by_month.py
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

OLD_VAULT_PATH = Path("lekwankwa-historical-vault/product=food_micropricing")
NEW_VAULT_PATH = Path("lekwankwa-historical-vault/product=food_micropricing")
COUNTRY = "USA"

# =============================================================================
# MAIN LOGIC
# =============================================================================

def extract_month_from_timestamp(df):
    """
    Extract year and month from data_timestamp column.
    
    Args:
        df: DataFrame with data_timestamp column
        
    Returns:
        DataFrame with added 'year' and 'month' columns
    """
    # Convert to datetime if not already
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
    
    # Extract year and month
    df['year'] = df['data_timestamp'].dt.year
    df['month'] = df['data_timestamp'].dt.month.apply(lambda x: f"{x:02d}")
    
    return df


def repartition_by_month(source: str):
    """
    Repartition a single source (bls or usda) by month.
    
    Args:
        source: 'bls' or 'usda'
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"REPARTITIONING SOURCE: {source.upper()}")
    logger.info(f"{'='*80}")
    
    source_path = OLD_VAULT_PATH / f"country={COUNTRY}" / f"source={source}"
    
    # Find all year folders
    year_folders = sorted([d for d in source_path.glob("year=*") if d.is_dir()])
    
    if not year_folders:
        logger.warning(f"No year folders found for source: {source}")
        return 0
    
    logger.info(f"Found {len(year_folders)} year folders")
    
    total_records = 0
    months_created = 0
    
    for year_folder in year_folders:
        year = year_folder.name.split("=")[1]
        parquet_file = year_folder / "base_data.parquet"
        
        if not parquet_file.exists():
            logger.warning(f"No base_data.parquet in {year_folder}")
            continue
        
        # Read the year file
        logger.info(f"Processing {year}...")
        df = pd.read_parquet(parquet_file)
        
        # Extract month information
        df = extract_month_from_timestamp(df)
        
        # Group by month and save
        for month, month_df in df.groupby('month'):
            # Drop the temporary year and month columns before saving
            output_df = month_df.drop(columns=['year', 'month'])
            
            # Create output path
            output_path = (
                NEW_VAULT_PATH / 
                f"country={COUNTRY}" / 
                f"source={source}" / 
                f"year={year}" / 
                f"month={month}"
            )
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Save to Parquet
            output_file = output_path / "base_data.parquet"
            output_df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            total_records += len(output_df)
            months_created += 1
            
            logger.info(f"  ✓ Created {year}/month={month} with {len(output_df)} records")
    
    logger.info(f"\nSource {source.upper()} complete:")
    logger.info(f"  - Total records: {total_records:,}")
    logger.info(f"  - Month partitions created: {months_created}")
    
    return total_records


def verify_repartitioning():
    """
    Verify the new partitioning structure and compare with original.
    """
    logger.info(f"\n{'='*80}")
    logger.info("VERIFICATION")
    logger.info(f"{'='*80}")
    
    verification = {
        'sources': {},
        'total_records': 0,
        'total_files': 0
    }
    
    for source in ['bls', 'usda']:
        source_path = NEW_VAULT_PATH / f"country={COUNTRY}" / f"source={source}"
        
        if not source_path.exists():
            continue
        
        # Count files and records
        parquet_files = list(source_path.glob("**/base_data.parquet"))
        
        records = 0
        for pf in parquet_files:
            df = pd.read_parquet(pf)
            records += len(df)
        
        verification['sources'][source] = {
            'files': len(parquet_files),
            'records': records
        }
        verification['total_files'] += len(parquet_files)
        verification['total_records'] += records
        
        logger.info(f"\n{source.upper()}:")
        logger.info(f"  - Files: {len(parquet_files)}")
        logger.info(f"  - Records: {records:,}")
    
    logger.info(f"\nTOTAL:")
    logger.info(f"  - Files: {verification['total_files']}")
    logger.info(f"  - Records: {verification['total_records']:,}")
    
    return verification


def main():
    """
    Main execution function.
    """
    start_time = datetime.now()
    
    logger.info("="*80)
    logger.info("FOOD PRICING - MONTH-LEVEL REPARTITIONING")
    logger.info("="*80)
    logger.info(f"Start time: {start_time}")
    logger.info(f"Old path: {OLD_VAULT_PATH}")
    logger.info(f"New path: {NEW_VAULT_PATH}")
    
    # Repartition both sources
    bls_records = repartition_by_month('bls')
    usda_records = repartition_by_month('usda')
    
    # Verify
    verification = verify_repartitioning()
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"\n{'='*80}")
    logger.info("REPARTITIONING COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info(f"Total records processed: {bls_records + usda_records:,}")
    logger.info(f"Total month partitions: {verification['total_files']}")
    logger.info(f"New vault location: {NEW_VAULT_PATH.absolute()}")
    
    # Save verification report
    report_path = Path("food_pricing_repartitioning_report.json")
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': duration,
            'records_processed': bls_records + usda_records,
            'verification': verification,
            'old_structure': str(OLD_VAULT_PATH),
            'new_structure': str(NEW_VAULT_PATH)
        }, f, indent=2)
    
    logger.info(f"Verification report saved: {report_path}")


if __name__ == "__main__":
    main()
