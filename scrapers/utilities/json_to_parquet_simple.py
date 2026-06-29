"""
Simple JSON to Parquet Converter

Reads JSON files from a folder and converts them to Parquet format.
No complex dependencies - just Pandas and PyArrow.

Usage:
    python json_to_parquet_simple.py --input data_sample --output sample_parquet
"""

import pandas as pd
import json
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_json_files(input_folder: str):
    """Load all JSON files from folder into a list of records."""
    input_path = Path(input_folder)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input folder not found: {input_folder}")
    
    # Find all JSON files recursively
    json_files = list(input_path.rglob("*.json"))
    
    if len(json_files) == 0:
        logger.warning(f"No JSON files found in {input_folder}")
        return []
    
    logger.info(f"Found {len(json_files)} JSON files in {input_folder}")
    
    records = []
    errors = 0
    
    for idx, json_file in enumerate(json_files, 1):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both single dict and list of dicts
                if isinstance(data, dict):
                    records.append(data)
                elif isinstance(data, list):
                    records.extend(data)
            
            # Progress logging
            if idx % 500 == 0:
                logger.info(f"Loaded {idx}/{len(json_files)} files ({len(records)} records)")
                
        except Exception as e:
            logger.error(f"Error reading {json_file}: {e}")
            errors += 1
            continue
    
    logger.info(f"Loaded {len(records)} records from {len(json_files)} files ({errors} errors)")
    return records


def save_to_parquet(records: list, output_folder: str, table_name: str = "food_prices"):
    """Save records as Parquet file."""
    if len(records) == 0:
        logger.error("No records to save!")
        return False
    
    # Convert to DataFrame
    logger.info(f"Converting {len(records)} records to DataFrame...")
    df = pd.DataFrame(records)
    
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Create output directory
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save as Parquet
    output_file = output_path / f"{table_name}.parquet"
    
    logger.info(f"Saving to {output_file}...")
    df.to_parquet(
        output_file,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    # Get file size
    file_size = output_file.stat().st_size / (1024 * 1024)  # MB
    logger.info(f"Saved {len(df)} records to {output_file} ({file_size:.2f} MB)")
    
    return True


def convert_json_to_parquet(input_folder: str, output_folder: str, table_name: str = "food_prices"):
    """Main conversion function."""
    logger.info("=" * 70)
    logger.info("JSON TO PARQUET CONVERTER")
    logger.info("=" * 70)
    logger.info(f"Input:  {input_folder}")
    logger.info(f"Output: {output_folder}")
    logger.info(f"Table:  {table_name}")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # Load JSON files
        records = load_json_files(input_folder)
        
        if len(records) == 0:
            logger.error("No records loaded - aborting")
            return False
        
        # Save to Parquet
        success = save_to_parquet(records, output_folder, table_name)
        
        if success:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 70)
            logger.info(f"SUCCESS! Converted in {elapsed:.2f} seconds")
            logger.info(f"Output: {Path(output_folder) / f'{table_name}.parquet'}")
            logger.info("=" * 70)
            return True
        else:
            logger.error("Conversion failed")
            return False
            
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert JSON files to Parquet format")
    parser.add_argument("--input", default="data", help="Input folder with JSON files")
    parser.add_argument("--output", default="output_parquet", help="Output folder for Parquet file")
    parser.add_argument("--table", default="food_prices", help="Table name for Parquet file")
    
    args = parser.parse_args()
    
    success = convert_json_to_parquet(args.input, args.output, args.table)
    
    sys.exit(0 if success else 1)
