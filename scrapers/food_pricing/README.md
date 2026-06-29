# Food Pricing Data Scrapers

Enterprise-grade data extraction scripts for food pricing data from BLS and USDA sources.

## Overview

This module contains all scrapers, extractors, and ingestion pipelines for food micropricing data used in the Lekwankwa vault system.

---

## Scripts

### 1. usa_food_scraper.py
**Purpose**: Main food pricing scraper for BLS and USDA data  
**Data Sources**:
- Bureau of Labor Statistics (BLS) API
- USDA Economic Research Service

**Features**:
- Multi-source data extraction
- Automatic schema validation
- Point-in-Time (PIT) field generation
- Error handling and retry logic

**Usage**:
```bash
python scrapers/food_pricing/usa_food_scraper.py
```

**Environment Variables Required**:
- `BLS_API_KEY`: BLS API authentication key
- `USDA_API_KEY`: USDA API authentication key

**Output**: JSON files → Parquet files in vault structure

---

### 2. bls_fetcher.py
**Purpose**: Dedicated BLS API fetcher with pagination and rate limiting  
**Features**:
- Handles BLS API v2 pagination
- Rate limiting (1 req/sec)
- Series ID management
- Automatic retries on failure

**Usage**:
```bash
python scrapers/food_pricing/bls_fetcher.py
```

**API Limits**:
- 500 daily queries (registered key)
- 50 series per query
- 20 years per query

---

### 3. bls_api_backfill_1980_1999.py
**Purpose**: Historical data backfill for 1980-1999 period  
**Features**:
- Targets pre-2000 data gaps
- Batch processing by year
- Deduplication logic
- Progress tracking

**Usage**:
```bash
python scrapers/food_pricing/bls_api_backfill_1980_1999.py
```

**Output**: Historical data appended to vault

---

### 4. download_bls_ftp.py
**Purpose**: Download bulk data files from BLS FTP server  
**Features**:
- FTP connection management
- Bulk file download
- Compressed file handling
- Incremental updates

**Usage**:
```bash
python scrapers/food_pricing/download_bls_ftp.py
```

**FTP Server**: ftp.bls.gov

---

### 5. usa_historical_ingestion.py
**Purpose**: Legacy historical data ingestion pipeline  
**Status**: Superseded by v2  
**Use Case**: Reference implementation

---

### 6. usa_historical_ingestion_v2.py
**Purpose**: Updated historical data ingestion pipeline  
**Features**:
- Schema v4.0 compliance (22 fields: 17 core + 5 PIT)
- Enhanced error handling
- Batch processing optimization
- Hive partitioning support

**Usage**:
```bash
python scrapers/food_pricing/usa_historical_ingestion_v2.py
```

**Output**: Parquet files in Hive-partitioned structure
```
vault/product=food_micropricing/country=USA/source={bls,usda}/year=YYYY/month=MM/base_data.parquet
```

---

## Data Pipeline Flow

```
┌─────────────────┐
│  BLS API / FTP  │
│   USDA API      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Scrapers      │
│  (Raw Extract)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Ingestion     │
│  (Transform)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Partitioning   │
│ (Month-level)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Vault       │
│  (Parquet)      │
└─────────────────┘
```

---

## Schema v4.0 (22 Fields)

### Core Fields (17)
- `country_code`: ISO country code (US)
- `item_name`: Food item name
- `item_description`: Detailed description
- `item_code`: BLS/USDA item code
- `category`: Food category
- `item_value`: Price value
- `unit`: Measurement unit
- `currency`: Currency code (USD)
- `usd_equivalent`: USD conversion
- `pct_change_mom`: Month-over-month % change
- `data_timestamp`: Original data date
- `conversion_timestamp`: Pipeline processing date
- `source`: Data source (bls, usda)
- `source_series_id`: Original series ID
- `source_url`: API endpoint
- `extraction_method`: api, scraper, manual
- `data_quality_certified`: Boolean flag

### PIT Fields (5)
- `record_id`: UUID unique identifier
- `published_date`: When published by source
- `as_of_date`: When version became valid
- `revision_number`: Version number (0 = original)
- `superseded_by`: Points to newer version (NULL if current)

---

## Configuration

### API Keys (.env)
```env
BLS_API_KEY=your_bls_key_here
USDA_API_KEY=your_usda_key_here
```

### Series IDs
Managed in: `data_sets_config.py`

---

## Data Quality

All scrapers integrate with validation pipeline:
- Schema compliance checks
- PIT field validation
- Outlier detection
- Sanity checks

See: `validations/food_micropricing/` for validation scripts

---

## Performance Metrics

**Historical Backfill (1980-1999)**:
- Records: ~5,000
- Runtime: ~45 minutes
- API calls: ~200

**Current Scraping (2000-present)**:
- Records: ~10,000
- Runtime: ~15 minutes
- API calls: ~150

**Output Size**:
- Raw JSON: ~15 MB
- Parquet (compressed): ~8.79 MB (65% reduction)

---

## Error Handling

### Common Issues
1. **API Rate Limits**: Automatic retry with exponential backoff
2. **Missing Series**: Logged to error file, processing continues
3. **Schema Mismatches**: Validation checks before vault ingestion
4. **Network Failures**: Retry up to 3 times per request

### Logs
- `food_scraper.log`: General scraping logs
- `bls_fetcher_errors.json`: BLS API errors
- `ingestion_errors.json`: Ingestion failures

---

## Maintenance

### Monthly Tasks
- Update series IDs in `data_sets_config.py`
- Review API usage limits
- Check for schema changes from BLS/USDA

### Quarterly Tasks
- Run historical backfill for new series
- Update known outliers list
- Review data quality metrics

---

## Related Components

**Partitioning**: `scrapers/partitioning/repartition_food_pricing_by_month.py`  
**Validation**: `validations/food_micropricing/`  
**Configs**: `configs/gx_config_food_micropricing_vault.json`

---

## Contact & Support

**Data Sources**:
- BLS API: https://www.bls.gov/developers/
- USDA ERS: https://www.ers.usda.gov/

**Internal**:
- Data Team: data-team@lekwankwa.com
- Schema Issues: schema-team@lekwankwa.com

---

Author: Lekwankwa Corporation  
Last Updated: 2026-06-07  
Version: 4.0
