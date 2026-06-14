# USA Food Micropricing Data Dictionary
## Schema Version 4.0 (Point-in-Time Enabled)

**Product**: USA Food Micropricing
**Coverage**: United States (1980–2026)
**Sources**: Bureau of Labor Statistics (BLS), United States Department of Agriculture (USDA)
**Approximate Records**: ~21,000 validated records
**Sample**: January–March 2022 (48 records)
**Update Frequency**: Monthly
**Last Updated**: June 14, 2026
**Sample File**: `sample_parquet_food_pricing/food_prices_v4.0_sample.parquet`

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Core Data Fields](#core-data-fields)
3. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
4. [Data Types and Constraints](#data-types-and-constraints)
5. [Sample Field Values (Jan–Mar 2022)](#sample-field-values)
6. [Data Sources](#data-sources)
7. [Coverage and Granularity](#coverage-and-granularity)
8. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US food micropricing data with full Point-in-Time (PIT) capability, enabling
accurate backtesting without look-ahead bias. All records carry actual BLS/USDA publication
timestamps and full revision history.

**Key Features**:
- No look-ahead bias: `data_timestamp` reflects measurement period; `published_date` reflects actual release
- Bitemporal model: separate valid-time and knowledge-time dimensions prevent retroactive bias
- Full revision tracking: `revision_number` and `superseded_by` columns
- 9/9 validation stages passing (PIT, sanity, schema, temporal, referential, lineage, GX, outlier, changelog)
- Hive-partitioned: `product=food_micropricing/country=USA/source={src}/year={yyyy}/month={m}/`
- UUID v4 primary key on every record

---

## Core Data Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `country_code` | STRING | No | ISO 3166-1 alpha-2; always `"US"` |
| `item_name` | STRING | No | Standardised food item name (e.g., `"Apples"`, `"Beef"`) |
| `item_description` | STRING | Yes | Detailed specification (e.g., `"red delicious or equiv."`) |
| `item_code` | STRING | No | Source-agency hierarchical code (BLS dot-notation: `"01.1.6.3"`) |
| `category` | STRING | No | High-level food category (see vocabulary below) |
| `item_value` | DOUBLE | No | Observed price in local currency per unit |
| `unit` | STRING | No | Unit of measurement (`"kg"`, `"lb"`, `"litre"`) |
| `currency` | STRING | No | ISO 4217 code; always `"USD"` |
| `usd_equivalent` | DOUBLE | No | Price normalised to USD per kg |
| `pct_change_mom` | DOUBLE | Yes | Month-on-month percentage change |
| `source` | STRING | No | Source identifier (`"bls"`, `"usda"`) |
| `source_series_id` | STRING | No | Source agency series code (e.g., `APU0000711211`) |
| `extraction_method` | STRING | No | Always `"api"` |
| `source_url` | STRING | Yes | API endpoint URL |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |

### `category` Vocabulary

`Fruits` | `Vegetables` | `Grains` | `Dairy` | `Meat` | `Poultry` |
`Seafood` | `Oils and Fats` | `Beverages` | `Eggs` | `Sugar and Sweets`

---

## Point-in-Time (PIT) Fields

| Field | Type | Description |
|-------|------|-------------|
| `data_timestamp` | TIMESTAMP (UTC) | First moment of the measurement period (e.g., `2022-01-01T00:00:00Z`) |
| `published_date` | TIMESTAMP (UTC) | Actual BLS release date (typically 2–4 weeks after measurement month) |
| `as_of_date` | TIMESTAMP (UTC) | Date this record version was ingested into the vault |
| `revision_number` | INTEGER | `0` = initial; incremented on each BLS revision |
| `superseded_by` | STRING | `record_id` of the newer version; `"N/A"` if current |
| `conversion_timestamp` | TIMESTAMP (UTC) | Timestamp when raw source data was converted to vault schema |

**PIT Guarantee**: `published_date >= data_timestamp` for every record (validated at Stage 1).

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `record_id` | UTF8 | UUID v4 format; unique; not null |
| `country_code` | UTF8 | Always `"US"` |
| `item_value` | DOUBLE | > 0 |
| `usd_equivalent` | DOUBLE | > 0 |
| `pct_change_mom` | DOUBLE | –100 to 1,000 |
| `revision_number` | INT64 | >= 0 |
| `data_timestamp` | TIMESTAMP(tz=UTC) | ISO 8601 UTC |
| `published_date` | TIMESTAMP(tz=UTC) | >= `data_timestamp` |
| `data_quality_certified` | BOOLEAN | `true` for all production records |

---

## Sample Field Values

**Sample period**: January–March 2022 | 48 records

| Field | Example Values |
|-------|----------------|
| `item_name` | `"Apples"`, `"Beef"`, `"Milk"`, `"Rice"`, `"Eggs"` |
| `item_code` | `"01.1.6.3"`, `"03.2.1.3"`, `"01.1.1.1"` |
| `category` | `"Fruits"`, `"Meat"`, `"Dairy"`, `"Grains"` |
| `item_value` | `1.3977` (Apples, Jan 2022, USD/kg) |
| `pct_change_mom` | `–1.40%` (Apples, Jan 2022) |
| `data_timestamp` | `2022-01-01T00:00:00Z` through `2022-03-01T00:00:00Z` |
| `published_date` | `2022-02-10T09:30:00Z` (CPI release, following month) |
| `revision_number` | `0` (initial release) |

---

## Data Sources

| Source | Agency | Frequency | Series Format | Historical Coverage |
|--------|---------|-----------|---------------|---------------------|
| BLS CPI Average Retail Food Prices | Bureau of Labor Statistics | Monthly | `APU{area}{item}` | 1980–2026 |
| USDA ERS Food Price Outlook | US Dept of Agriculture | Monthly | Product-specific | 2000–2026 |

---

## Coverage and Granularity

- **Geography**: National (US)
- **Temporal**: 1980–2026, monthly
- **Food items**: 40+ items across 11 categories
- **Vault partition**: `year={yyyy}/month={m}` (Hive)
- **Record count**: ~21,000 (growing monthly)

---

## Quality Metrics

| Stage | Check | Result |
|-------|-------|--------|
| 1 | PIT Validation (10 checks) | PASS |
| 2 | Sanity Checks | PASS |
| 3 | Schema Compliance (15 SDMX checks) | PASS |
| 4 | Temporal Consistency | PASS |
| 5 | Referential Integrity | PASS |
| 6 | Lineage | PASS |
| 7 | GX Universal Validation | PASS |
| 8 | Outlier Extraction | PASS |
| 9 | Changelog Generation | PASS |

**Overall: 9/9 PASS**
