# Utility Scripts

Supporting utility scripts for data transformation and currency conversion.

## Overview

This module contains utility scripts that support data scrapers and transformations across the pipeline.

---

## Scripts

### 1. historical_fx_rates.py
**Purpose**: Historical foreign exchange (FX) rate scraper  
**Data Source**: Multiple FX data providers

**Features**:
- Historical FX rate extraction
- Multi-currency support
- Date range queries
- Rate normalization to USD

**Usage**:
```bash
python scrapers/utilities/historical_fx_rates.py
```

**Output**: FX rates for currency conversion in food pricing pipeline

**Supported Currencies**:
- USD (baseline)
- EUR, GBP, JPY, CAD, etc.

---

### 2. json_to_parquet_simple.py
**Purpose**: Simple JSON to Parquet converter  
**Use Case**: Convert raw JSON scraper output to Parquet format

**Features**:
- Batch JSON processing
- Schema inference
- Snappy compression
- Progress tracking

**Usage**:
```bash
python scrapers/utilities/json_to_parquet_simple.py --input data.json --output data.parquet
```

**Benefits**:
- 60-70% size reduction
- Faster query performance
- Schema preservation
- Columnar storage format

---

## Integration

### FX Rates in Food Pricing
```python
# Used in usa_food_scraper.py
fx_rate = get_historical_fx_rate(date, currency)
usd_equivalent = item_value * fx_rate
```

### JSON to Parquet Pipeline
```
Raw Scraper → JSON Output → json_to_parquet_simple → Parquet Files → Vault
```

---

## Related Components

**Main Scrapers**:
- `scrapers/food_pricing/`
- `scrapers/energy/`

**Partitioning**:
- `scrapers/partitioning/`

---

Author: Lekwankwa Corporation  
Last Updated: 2026-06-07  
Version: 1.0
