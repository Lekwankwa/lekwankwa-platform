"""
EIA Electricity Data Extractor

Extracts electricity generation, consumption, and pricing data from the
U.S. Energy Information Administration (EIA) API v2.

Sources electricity data from 2001 to present via REST API endpoints.
Handles pagination, flattens nested JSON, and outputs to compressed Parquet format.

Author: Lekwankwa Data Platform
Product: Electricity & Power Grid Utilities
Date: June 6, 2026
"""

import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import logging
from pathlib import Path
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# EIA API Configuration
EIA_API_KEY = os.getenv('EIA_API_KEY')
if not EIA_API_KEY:
    raise ValueError("EIA_API_KEY not found in .env file")
    
EIA_BASE_URL = "https://api.eia.gov/v2"
EIA_HEADERS = {
    "X-Params": json.dumps({"api_key": EIA_API_KEY})
}

class EIAElectricityExtractor:
    """
    Extracts electricity data from EIA API v2.
    
    Supports multiple electricity endpoints:
    - /electricity/retail-sales
    - /electricity/electric-power-operational-data
    - /electricity/rto
    - /electricity/state-electricity-profiles
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = EIA_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Lekwankwa-Data-Platform/1.0"
        })
        
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make API request with error handling and rate limiting.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        if params is None:
            params = {}
        
        params['api_key'] = self.api_key
        
        try:
            logger.info(f"Requesting: {endpoint}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'error' in data:
                logger.error(f"API Error: {data['error']}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None
            
    def _paginate_data(self, endpoint: str, params: Dict, start_date: str, end_date: str) -> List[Dict]:
        """
        Handle API pagination and collect all records.
        
        Args:
            endpoint: API endpoint path
            params: Base query parameters
            start_date: Start date (YYYY-MM format)
            end_date: End date (YYYY-MM format)
            
        Returns:
            List of all records across pages
        """
        all_records = []
        offset = 0
        length = 5000  # Max records per page
        
        params.update({
            'start': start_date,
            'end': end_date,
            'offset': offset,
            'length': length
        })
        
        while True:
            logger.info(f"Fetching page with offset={offset}")
            
            response = self._make_request(endpoint, params)
            
            if not response or 'response' not in response:
                logger.warning("No response data, stopping pagination")
                break
                
            data = response['response']
            
            # Extract records (varies by endpoint)
            if 'data' in data:
                records = data['data']
            else:
                logger.warning("No 'data' field in response")
                break
                
            if not records:
                logger.info("No more records, pagination complete")
                break
                
            all_records.extend(records)
            logger.info(f"Collected {len(records)} records (total: {len(all_records)})")
            
            # Check if more pages exist
            total = data.get('total', 0)
            # Convert total to int if it's a string
            if isinstance(total, str):
                try:
                    total = int(total)
                except ValueError:
                    total = 0
            
            if len(all_records) >= total or len(records) < length:
                logger.info(f"Reached end of data (total: {total})")
                break
                
            # Update offset for next page
            offset += length
            params['offset'] = offset
            
            # Rate limiting (1 second between requests)
            time.sleep(1)
            
        return all_records
    
    def extract_retail_sales(self, start_year: int = 2001, end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Extract electricity retail sales data.
        
        Includes sales, revenue, and customer count by state and sector.
        
        Args:
            start_year: Start year (default 2001)
            end_year: End year (default current year)
            
        Returns:
            DataFrame with retail sales data
        """
        if end_year is None:
            end_year = datetime.now().year
            
        logger.info(f"Extracting retail sales data: {start_year} to {end_year}")
        
        endpoint = "/electricity/retail-sales/data/"
        params = {
            'frequency': 'monthly',
            'data[0]': 'customers',
            'data[1]': 'price',
            'data[2]': 'revenue',
            'data[3]': 'sales',
            'facets[sectorid][]': ['ALL', 'RES', 'COM', 'IND', 'TRA'],  # All, Residential, Commercial, Industrial, Transportation
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc'
        }
        
        start_date = f"{start_year}-01"
        end_date = f"{end_year}-12"
        
        records = self._paginate_data(endpoint, params, start_date, end_date)
        
        if not records:
            logger.error("No retail sales data retrieved")
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        logger.info(f"Extracted {len(df)} retail sales records")
        
        return df
    
    def extract_generation_data(self, start_year: int = 2001, end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Extract electricity generation data by source (coal, gas, nuclear, renewables, etc.).
        
        Args:
            start_year: Start year (default 2001)
            end_year: End year (default current year)
            
        Returns:
            DataFrame with generation data
        """
        if end_year is None:
            end_year = datetime.now().year
            
        logger.info(f"Extracting generation data: {start_year} to {end_year}")
        
        endpoint = "/electricity/electric-power-operational-data/data/"
        params = {
            'frequency': 'monthly',
            'data[0]': 'generation',
            'facets[fueltypeid][]': ['ALL', 'COL', 'NG', 'NUC', 'HYC', 'WND', 'SUN', 'OOG'],  # All sources
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc'
        }
        
        start_date = f"{start_year}-01"
        end_date = f"{end_year}-12"
        
        records = self._paginate_data(endpoint, params, start_date, end_date)
        
        if not records:
            logger.error("No generation data retrieved")
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        logger.info(f"Extracted {len(df)} generation records")
        
        return df
    
    def extract_state_profiles(self, start_year: int = 2001, end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Extract state electricity profiles (comprehensive state-level data).
        
        Args:
            start_year: Start year (default 2001)
            end_year: End year (default current year)
            
        Returns:
            DataFrame with state profile data
        """
        if end_year is None:
            end_year = datetime.now().year
            
        logger.info(f"Extracting state profiles: {start_year} to {end_year}")
        
        endpoint = "/electricity/state-electricity-profiles/data/"
        params = {
            'frequency': 'annual',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc'
        }
        
        start_date = f"{start_year}"
        end_date = f"{end_year}"
        
        records = self._paginate_data(endpoint, params, start_date, end_date)
        
        if not records:
            logger.error("No state profile data retrieved")
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        logger.info(f"Extracted {len(df)} state profile records")
        
        return df
    
    def flatten_and_clean(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        """
        Flatten nested JSON structures and standardize column names.
        
        Args:
            df: Raw DataFrame from API
            dataset_name: Dataset identifier (retail-sales, generation, etc.)
            
        Returns:
            Cleaned and flattened DataFrame
        """
        if df.empty:
            return df
            
        logger.info(f"Cleaning {dataset_name} data: {len(df)} records")
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Flatten nested columns
        # Common pattern: period, stateDescription, sectorName, etc.
        for col in df.columns:
            if isinstance(df[col].iloc[0], dict):
                # Expand dictionary columns
                nested_df = pd.json_normalize(df[col])
                nested_df.columns = [f"{col}_{subcol}" for subcol in nested_df.columns]
                df = pd.concat([df.drop(columns=[col]), nested_df], axis=1)
        
        # Convert period to datetime
        if 'period' in df.columns:
            df['period'] = pd.to_datetime(df['period'], format='%Y-%m', errors='coerce')
            df.rename(columns={'period': 'data_timestamp'}, inplace=True)
        elif 'year' in df.columns:
            df['data_timestamp'] = pd.to_datetime(df['year'], format='%Y', errors='coerce')
            
        # Add metadata
        df['extraction_timestamp'] = pd.Timestamp.now(tz='UTC')
        df['source'] = 'eia'
        df['dataset'] = dataset_name
        df['data_quality_certified'] = False  # Will be set after validation
        
        # Standardize numeric columns
        numeric_cols = df.select_dtypes(include=['object']).columns
        for col in numeric_cols:
            if col not in ['source', 'dataset', 'stateDescription', 'sectorName', 'fuelTypeDescription']:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        
        logger.info(f"Cleaned data: {len(df)} records, {len(df.columns)} columns")
        
        return df
    
    def save_to_parquet(self, df: pd.DataFrame, output_path: str, compression: str = 'snappy'):
        """
        Save DataFrame to compressed Parquet file.
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            compression: Compression codec (snappy, gzip, brotli)
        """
        if df.empty:
            logger.warning("DataFrame is empty, skipping save")
            return
            
        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to PyArrow Table for better compression
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet
        pq.write_table(
            table,
            output_path,
            compression=compression,
            use_dictionary=True,
            write_statistics=True
        )
        
        # Get file size
        file_size = Path(output_path).stat().st_size / 1024 / 1024  # MB
        
        logger.info(f"Saved to: {output_path}")
        logger.info(f"File size: {file_size:.2f} MB")
        logger.info(f"Compression: {compression}")
        logger.info(f"Records: {len(df):,}")
        

def main():
    """
    Main extraction workflow.
    
    Extracts all electricity datasets and saves to Parquet files.
    """
    logger.info("="*80)
    logger.info("EIA ELECTRICITY DATA EXTRACTION")
    logger.info("="*80)
    logger.info(f"Start time: {datetime.now()}")
    logger.info("")
    
    # Initialize extractor
    extractor = EIAElectricityExtractor(api_key=EIA_API_KEY)
    
    # Define extraction parameters
    start_year = 2001
    end_year = datetime.now().year
    
    # Output directory
    output_dir = "eia_electricity_data"
    Path(output_dir).mkdir(exist_ok=True)
    
    # -------------------------------------------------------------------------
    # 1. Extract Retail Sales Data  [REMOVED — retail sales dropped from dataset]
    # -------------------------------------------------------------------------
    # Retail sales (prices/revenue/customers) are no longer part of the
    # Lekwankwa electricity product.  Only generation data is retained.

    # -------------------------------------------------------------------------
    # 2. Extract Generation Data
    # -------------------------------------------------------------------------
    logger.info("\n" + "="*80)
    logger.info("DATASET 2: GENERATION BY SOURCE")
    logger.info("="*80)
    
    try:
        generation_df = extractor.extract_generation_data(start_year, end_year)
        
        if not generation_df.empty:
            generation_df = extractor.flatten_and_clean(generation_df, 'generation')
            
            output_file = f"{output_dir}/electricity_generation_{start_year}_{end_year}.parquet"
            extractor.save_to_parquet(generation_df, output_file)
            
            # Show sample
            logger.info("\nSample data:")
            logger.info(f"\n{generation_df.head(3).to_string()}")
        else:
            logger.warning("No generation data extracted")
            
    except Exception as e:
        logger.error(f"Generation data extraction failed: {e}")
    
    # -------------------------------------------------------------------------
    # 3. Extract State Profiles (COMMENTED OUT - API endpoint not available)
    # -------------------------------------------------------------------------
    # logger.info("\n" + "="*80)
    # logger.info("DATASET 3: STATE ELECTRICITY PROFILES")
    # logger.info("="*80)
    # 
    # try:
    #     state_df = extractor.extract_state_profiles(start_year, end_year)
    #     
    #     if not state_df.empty:
    #         state_df = extractor.flatten_and_clean(state_df, 'state-profiles')
    #         
    #         output_file = f"{output_dir}/electricity_state_profiles_{start_year}_{end_year}.parquet"
    #         extractor.save_to_parquet(state_df, output_file)
    #         
    #         # Show sample
    #         logger.info("\nSample data:")
    #         logger.info(f"\n{state_df.head(3).to_string()}")
    #     else:
    #         logger.warning("No state profile data extracted")
    #         
    # except Exception as e:
    #     logger.error(f"State profile extraction failed: {e}")
    
    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    logger.info("\n" + "="*80)
    logger.info("EXTRACTION COMPLETE")
    logger.info("="*80)
    logger.info(f"End time: {datetime.now()}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Validate extracted data with universal_gx_validator.py")
    logger.info("  2. Run sanity checks and outlier detection")
    logger.info("  3. Generate changelog and quality metrics")
    logger.info("  4. Partition into Hive structure for vault storage")
    logger.info("="*80)


if __name__ == "__main__":
    main()
