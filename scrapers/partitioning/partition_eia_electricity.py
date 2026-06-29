"""
EIA ELECTRICITY DATASET - HIVE PARTITIONING SCRIPT
===================================================

This script takes the extracted EIA electricity data and partitions it into 
proper Hive structure with year and month levels.

INPUT:
    eia_electricity_data/electricity_retail_sales_2001_2026.parquet
    eia_electricity_data/electricity_generation_2001_2026.parquet

OUTPUT STRUCTURE:
    vault/product=electricity/country=USA/source=eia/year=YYYY/month=MM/base_data.parquet

FEATURES:
- Reads flat Parquet files from EIA extraction
- Extracts year and month from data_timestamp field
- Creates Hive-partitioned structure matching food pricing format
- Combines retail sales and generation datasets
- Maintains compression (snappy)
- Logs progress and statistics

USAGE:
    python partition_eia_electricity.py
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

INPUT_PATH = Path("eia_electricity_data")
OUTPUT_PATH = Path("lekwankwa-historical-vault/product=electricity")
COUNTRY = "USA"
SOURCE = "eia"

# Dataset files
DATASETS = [
    {
        'file': 'electricity_retail_sales_2001_2026.parquet',
        'name': 'retail_sales',
        'description': 'Electricity retail sales, revenue, and customer data'
    },
    {
        'file': 'electricity_generation_2001_2026.parquet',
        'name': 'generation',
        'description': 'Electricity generation by source (coal, gas, nuclear, renewables)'
    }
]

# =============================================================================
# MAIN LOGIC
# =============================================================================

def extract_year_month(df):
    """
    Extract year and month from data_timestamp column.
    
    Args:
        df: DataFrame with data_timestamp column
        
    Returns:
        DataFrame with added 'year' and 'month' columns
    """
    # Convert to datetime if not already
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp'], errors='coerce')
    
    # Extract year and month
    df['year'] = df['data_timestamp'].dt.year.astype(str)
    df['month'] = df['data_timestamp'].dt.month.apply(lambda x: f"{x:02d}" if pd.notna(x) else "00")
    
    return df


def partition_dataset(dataset_info):
    """
    Partition a single dataset by year and month.
    
    Args:
        dataset_info: Dictionary with file, name, and description
        
    Returns:
        Total records processed
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"PARTITIONING: {dataset_info['name'].upper()}")
    logger.info(f"{'='*80}")
    logger.info(f"Description: {dataset_info['description']}")
    
    input_file = INPUT_PATH / dataset_info['file']
    
    if not input_file.exists():
        logger.warning(f"Input file not found: {input_file}")
        logger.warning(f"Skipping {dataset_info['name']}")
        return 0
    
    # Read the input file
    logger.info(f"Reading: {input_file}")
    df = pd.read_parquet(input_file)
    logger.info(f"Loaded {len(df):,} records")
    
    # Add dataset type column
    df['dataset_type'] = dataset_info['name']
    
    # Extract year and month
    df = extract_year_month(df)
    
    # Check for invalid dates
    invalid_dates = df[df['year'].isna() | df['month'].isna()]
    if len(invalid_dates) > 0:
        logger.warning(f"Found {len(invalid_dates)} records with invalid dates")
        logger.warning("Dropping records with invalid dates")
        df = df[df['year'].notna() & df['month'].notna()]
    
    # Group by year and month, then save
    total_records = 0
    partitions_created = 0
    
    for (year, month), group_df in df.groupby(['year', 'month']):
        # Drop temporary columns
        output_df = group_df.drop(columns=['year', 'month'])
        
        # Create output path
        output_path = (
            OUTPUT_PATH /
            f"country={COUNTRY}" /
            f"source={SOURCE}" /
            f"year={year}" /
            f"month={month}"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Use dataset_type in filename to avoid schema conflicts
        dataset_suffix = dataset_info['name']
        output_file = output_path / f"{dataset_suffix}_data.parquet"
        
        logger.info(f"  ✓ Creating {year}/month={month}/{dataset_suffix} with {len(output_df)} records")
        
        # Save to Parquet
        output_df.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        total_records += len(group_df)
        partitions_created += 1
    
    logger.info(f"\nDataset {dataset_info['name']} complete:")
    logger.info(f"  - Records processed: {total_records:,}")
    logger.info(f"  - Partitions created: {partitions_created}")
    
    return total_records


def verify_partitioning():
    """
    Verify the partitioning structure.
    """
    logger.info(f"\n{'='*80}")
    logger.info("VERIFICATION")
    logger.info(f"{'='*80}")
    
    source_path = OUTPUT_PATH / f"country={COUNTRY}" / f"source={SOURCE}"
    
    if not source_path.exists():
        logger.error(f"Output path not found: {source_path}")
        return None
    
    # Count files and records
    parquet_files = list(source_path.glob("**/base_data.parquet"))
    
    total_records = 0
    dataset_counts = {}
    year_month_summary = {}
    
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        records = len(df)
        total_records += records
        
        # Extract year and month from path
        parts = pf.parts
        year = [p for p in parts if p.startswith('year=')][0].split('=')[1]
        month = [p for p in parts if p.startswith('month=')][0].split('=')[1]
        
        year_month_key = f"{year}-{month}"
        year_month_summary[year_month_key] = year_month_summary.get(year_month_key, 0) + records
        
        # Count by dataset type
        if 'dataset_type' in df.columns:
            for dtype, count in df['dataset_type'].value_counts().items():
                dataset_counts[dtype] = dataset_counts.get(dtype, 0) + count
    
    logger.info(f"\nPartition Files: {len(parquet_files)}")
    logger.info(f"Total Records: {total_records:,}")
    
    logger.info(f"\nRecords by Dataset:")
    for dtype, count in sorted(dataset_counts.items()):
        logger.info(f"  - {dtype}: {count:,}")
    
    logger.info(f"\nYear-Month Coverage:")
    for ym in sorted(year_month_summary.keys()):
        logger.info(f"  - {ym}: {year_month_summary[ym]:,} records")
    
    verification = {
        'total_files': len(parquet_files),
        'total_records': total_records,
        'dataset_counts': dataset_counts,
        'year_month_summary': year_month_summary
    }
    
    return verification


def main():
    """
    Main execution function.
    """
    start_time = datetime.now()
    
    logger.info("="*80)
    logger.info("EIA ELECTRICITY - HIVE PARTITIONING")
    logger.info("="*80)
    logger.info(f"Start time: {start_time}")
    logger.info(f"Input path: {INPUT_PATH}")
    logger.info(f"Output path: {OUTPUT_PATH}")
    
    # Process each dataset
    total_processed = 0
    for dataset in DATASETS:
        records = partition_dataset(dataset)
        total_processed += records
    
    # Verify
    verification = verify_partitioning()
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"\n{'='*80}")
    logger.info("PARTITIONING COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info(f"Records processed: {total_processed:,}")
    
    if verification:
        logger.info(f"Partition files created: {verification['total_files']}")
        logger.info(f"Vault location: {OUTPUT_PATH.absolute()}")
        
        # Save verification report
        report_path = Path("eia_electricity_partitioning_report.json")
        with open(report_path, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': duration,
                'records_processed': total_processed,
                'verification': verification,
                'input_path': str(INPUT_PATH),
                'output_structure': str(OUTPUT_PATH)
            }, f, indent=2)
        
        logger.info(f"Verification report saved: {report_path}")


if __name__ == "__main__":
    main()
