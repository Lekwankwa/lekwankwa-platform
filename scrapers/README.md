# Scrapers & Data Extractors

Enterprise-grade data extraction and transformation scripts organized by data source.

## Overview

This module contains all data scrapers, extractors, partitioning scripts, and utilities used to populate the Lekwankwa vault with high-quality, validated data.

---

## Folder Structure

```
scrapers/
├── food_pricing/              # BLS & USDA food micropricing (live scraper + historical backfill)
│   ├── usa_food_scraper.py                    ← LIVE scraper
│   └── historical_backfill/                   ← one-time ingestion (not deployed)
│       ├── bls_api_backfill_1980_1999.py
│       ├── bls_fetcher.py
│       ├── download_bls_ftp.py
│       ├── usa_historical_ingestion.py
│       └── usa_historical_ingestion_v2.py
│
├── electricity/               # EIA electricity generation data
│   └── eia_electricity_extractor.py           ← LIVE scraper
│
├── wages_employment/          # BLS wages (CES) + unemployment (CPS)
│   └── bls_ces_cps_usa_scraper.py             ← LIVE scraper (both datasets)
│
├── housing/                   # US Census Bureau Building Permits Survey
│   └── census_housing_permits_usa_scraper.py  ← LIVE scraper
│
├── partitioning/              # Vault Hive-partitioning utilities
│   ├── repartition_food_pricing_by_month.py
│   └── partition_eia_electricity.py
│
└── utilities/                 # Shared helpers
    ├── historical_fx_rates.py
    └── json_to_parquet_simple.py
```

---

## Dataset → Scraper Map

| Dataset | Product Vault Path | Scraper | Source API |
|---|---|---|---|
| Food Micropricing | `product=food_micropricing` | `food_pricing/usa_food_scraper.py` | BLS API + USDA |
| Electricity Generation | `product=electricity` | `electricity/eia_electricity_extractor.py` | EIA Open Data API v2 |
| Wages (CES) | `product=macro_employment/source=bls_ces` | `wages_employment/bls_ces_cps_usa_scraper.py` | BLS Public API v2 |
| Unemployment (CPS) | `product=macro_employment/source=bls_cps` | `wages_employment/bls_ces_cps_usa_scraper.py` | BLS Public API v2 |
| Housing Permits | `product=housing/source=census_bps` | `housing/census_housing_permits_usa_scraper.py` | US Census BPS API |

---

## Environment Variables

| Variable | Required By | Description |
|---|---|---|
| `BLS_API_KEY` | wages_employment, food_pricing | BLS registration key (increases rate limits) |
| `CENSUS_API_KEY` | housing | Census Bureau API key (free — register at api.census.gov) |

---
|--------|---------|----------|
| `historical_fx_rates.py` | FX rate scraper | Currency conversion |
| `json_to_parquet_simple.py` | JSON → Parquet converter | Data transformation |

---

## Data Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│  BLS API │ USDA API │ EIA API v2 │ BLS FTP │ FX Providers   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   SCRAPERS                                   │
│  food_pricing/ │ energy/ │ utilities/                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              RAW DATA (JSON/Parquet)                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                 PARTITIONING                                 │
│  Month-level Hive partitioning (10-12x query speedup)       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     VAULT                                    │
│  product=X/country=Y/source=Z/year=YYYY/month=MM            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  VALIDATIONS                                 │
│  Schema │ PIT │ Sanity │ GX │ Outlier │ Changelog          │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Quality Standards

All scrapers adhere to enterprise data quality standards:

### 1. Schema Compliance
- **Food Pricing**: Schema v4.0 (22 fields: 17 core + 5 PIT)
- **Electricity**: 18 fields (retail sales), 14 fields (generation)
- All fields validated against config files

### 2. Point-in-Time (PIT) Fields (Food Pricing Only)
- `record_id`: UUID unique identifier
- `published_date`: When published by source
- `as_of_date`: When version became valid
- `revision_number`: Version tracking (0 = original)
- `superseded_by`: Version chain (NULL if current)

### 3. Hive Partitioning
- **5-level hierarchy**: product/country/source/year/month
- **Query optimization**: 10-12x faster for time-based queries
- **Partition pruning**: Skip irrelevant directories
- **Incremental updates**: Add new months without reprocessing

### 4. Compression
- **Format**: Snappy compression (pyarrow)
- **Reduction**: 60-70% size reduction
- **Performance**: Optimized for query speed

### 5. Validation Integration
- All scrapers output to validation pipeline
- See: `validations/` for comprehensive validation suite

---

## Performance Metrics

### Food Pricing
- **Historical Backfill (1980-1999)**: 45 minutes, ~5,000 records
- **Current Scraping (2000-present)**: 15 minutes, ~10,000 records
- **Output**: 8.79 MB (compressed)

### Electricity
- **Extraction Time**: 48.6 minutes
- **Total Records**: 1,044,070
- **API Calls**: ~210
- **Output**: 15.71 MB (compressed)

### Partitioning
- **Food Pricing**: 9.4 seconds, 581 files created
- **Electricity**: 9.8 seconds, 606 files created

---

## Configuration

### Environment Variables (.env)
```env
# Food Pricing
BLS_API_KEY=your_bls_api_key
USDA_API_KEY=your_usda_api_key

# Energy
EIA_API_KEY=your_eia_api_key
```

### Config Files
- `data_sets_config.py`: BLS series IDs
- `configs/gx_config_food_micropricing_vault.json`: Food validation rules
- `configs/gx_config_electricity_vault.json`: Electricity validation rules

---

## Usage Examples

### Food Pricing - Full Pipeline
```bash
# 1. Scrape current data
python scrapers/food_pricing/usa_food_scraper.py

# 2. Backfill historical gaps
python scrapers/food_pricing/bls_api_backfill_1980_1999.py

# 3. Ingest and transform
python scrapers/food_pricing/usa_historical_ingestion_v2.py

# 4. Partition by month
python scrapers/partitioning/repartition_food_pricing_by_month.py

# 5. Validate
python run_all_validations.py --product food_micropricing
```

### Electricity - Full Pipeline
```bash
# 1. Extract from EIA API
python scrapers/energy/eia_electricity_extractor.py

# 2. Partition by month
python scrapers/partitioning/partition_eia_electricity.py

# 3. Validate
python run_all_validations.py --product electricity
```

---

## Error Handling

All scrapers implement enterprise-grade error handling:

### API Issues
- **Rate Limits**: Automatic retry with exponential backoff
- **Timeouts**: Configurable timeout with retry logic
- **Invalid Responses**: Logged and skipped
- **Authentication Failures**: Clear error messages

### Data Issues
- **Schema Mismatches**: Validation before vault ingestion
- **Missing Fields**: Logged with null handling
- **Duplicate Records**: Deduplication logic
- **Invalid Values**: Flagged in data_quality_certified field

### Logs
- Detailed logs in `*.log` files
- Error reports in `*_errors.json` files
- Progress tracking in console output

---

## Maintenance

### Daily Tasks
- Monitor scraper logs for errors
- Check API usage limits

### Weekly Tasks
- Review data quality metrics
- Validate new data ingestion

### Monthly Tasks
- Run scrapers for latest data
- Update series IDs (food pricing)
- Execute validation pipeline

### Quarterly Tasks
- Review API documentation for changes
- Update schema definitions if needed
- Audit data completeness

---

## Related Components

**Validations**: `validations/`  
**Configs**: `configs/`  
**Vault**: `vault/`  
**Documentation**:
- `HIVE_PARTITIONING_COMPLETE.md`
- `VAULT_STRUCTURE_README.md`
- `DATA_DICTIONARY_v2.md`

---

## Contact & Support

**Data Sources**:
- BLS: https://www.bls.gov/developers/
- USDA: https://www.ers.usda.gov/
- EIA: https://www.eia.gov/opendata/

**Internal**:
- Data Team: data-team@lekwankwa.com
- API Issues: api-support@lekwankwa.com
- Schema Questions: schema-team@lekwankwa.com

---

Author: Lekwankwa Corporation  
Last Updated: 2026-06-07  
Version: 1.0
