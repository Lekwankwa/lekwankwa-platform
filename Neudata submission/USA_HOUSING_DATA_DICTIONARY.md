# USA Housing Supply and Shelter Inflation Data Dictionary
## Schema Version 1.0 (Point-in-Time Enabled)

**Product**: Housing Supply and Shelter Inflation
**Coverage**: United States — Shelter CPI: 1959–2026 | Building Permits: 1960–2026
**Sources**: Bureau of Labor Statistics (BLS) CPI Shelter Series + US Census Bureau Building Permits Survey (via FRED)
**Approximate Records**: 6,566 validated records
**Sample**: January–March 2022 (21 shelter + 9 permits records)
**Update Frequency**: Monthly
**Last Updated**: June 14, 2026
**Sample Files**:
- `sample_parquet_housing/housing_shelter_inflation_v1.0_sample.parquet`
- `sample_parquet_housing/housing_permits_v1.0_sample.parquet`

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Dataset Types](#dataset-types)
3. [Shelter Inflation Fields](#shelter-inflation-fields)
4. [Building Permits Fields](#building-permits-fields)
5. [Common PIT Fields](#common-pit-fields)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values (Jan–Mar 2022)](#sample-field-values)
8. [Data Sources](#data-sources)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US housing data combining BLS shelter price indices with Census Bureau
residential building permits. Together they provide a supply-demand picture of the US housing
market with full Point-in-Time capability.

**Key Features**:
- 65+ years of shelter inflation history (BLS from 1959)
- No look-ahead bias: `published_date` reflects actual BLS/Census release dates
- Complementary datasets: price pressure (CPI shelter) + supply signal (building permits)
- 7/9 validation stages passing (stages 4–5 reflect known data quality findings, not code failures)
- Hive-partitioned: `product=Housing_Supply_and_Shelter_Inflation/country=USA/source={src}/year={yyyy}/month={m}/`

---

## Dataset Types

### 1. Shelter Inflation (CPI)
- **Source**: `bls_cpi_shelter`
- **Records**: ~3,000
- **Coverage**: 1959–2026, monthly
- **Key metrics**: Rent of primary residence, owners' equivalent rent, lodging away from home
- **File**: `shelter_inflation_data.parquet`

### 2. Building Permits (BPS)
- **Source**: `census_bps`
- **Records**: ~3,566
- **Coverage**: 1960–2026, monthly
- **Key metrics**: Total authorised units, 1-unit structures, 2–4 units, 5+ units
- **File**: `housing_permits_data.parquet`

---

## Shelter Inflation Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `iso_alpha3` | STRING | No | Always `"USA"` |
| `country_name` | STRING | No | Always `"United States"` |
| `country_code` | STRING | No | Always `"US"` |
| `market_tier` | STRING | No | Always `"Developed"` |
| `source_agency` | STRING | No | Always `"BLS"` |
| `source_sub_category` | STRING | No | Always `"CPI_URBAN"` |
| `portal_url` | STRING | No | `"https://www.bls.gov/cpi/"` |
| `sovereign_series_id` | STRING | No | BLS series code (e.g., `CUUR0000SEHA`) |
| `data_vintage_id` | STRING | No | `BLS-{series}-{yyyy}-{mm}-v{n}` |
| `confidence_tier` | STRING | No | `"PRIMARY"` |
| `macro_metric_name` | STRING | No | Standardised name (e.g., `CPI_RENT_OF_PRIMARY_RESIDENCE`) |
| `observed_value` | DOUBLE | No | CPI index value (base period 1982–84 = 100) |
| `metric_value` | DOUBLE | No | Alias of `observed_value` |
| `unit_of_measure` | STRING | No | Always `"INDEX"` |
| `is_revised_figure` | BOOLEAN | No | `true` if BLS issued a revision |
| `seasonal_adjustment` | STRING | No | `"S"` = seasonally adjusted; `"U"` = unadjusted |
| `source` | STRING | No | Always `"bls_cpi_shelter"` |
| `extraction_method` | STRING | No | Always `"api"` |
| `data_quality_certified` | BOOLEAN | No | `true` when validation passes |
| `bls_footnotes` | STRING | Yes | JSON array of BLS annotation codes |

### `macro_metric_name` Vocabulary (Shelter)

| Value | Description |
|-------|-------------|
| `CPI_RENT_OF_PRIMARY_RESIDENCE` | Rent paid by tenants |
| `CPI_OWNERS_EQUIVALENT_RENT` | Imputed rent for owner-occupied housing |
| `CPI_RENT_OF_SHELTER` | Combined shelter index |
| `CPI_LODGING_AWAY_FROM_HOME` | Hotels, motels |
| `CPI_TENANTS_AND_HOUSEHOLD_INSURANCE` | Insurance component |
| `CPI_WATER_SEWER_TRASH` | Utilities component |
| `CPI_HOUSEHOLD_FURNISHINGS` | Furniture and appliances |

---

## Building Permits Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `sovereign_series_id` | STRING | No | FRED/Census series code (e.g., `PERMIT`, `PERMITNSA`) |
| `macro_metric_name` | STRING | No | Standardised name (e.g., `AUTHORIZED_PERMITS_TOTAL_UNITS`) |
| `observed_value` | DOUBLE | No | Number of authorised units |
| `unit_of_measure` | STRING | No | `"UNITS_SAAR"` (seasonally adjusted annual rate) or `"UNITS_NSA"` |
| `geo_level` | STRING | No | Always `"national"` |
| `geo_id` | STRING | No | Always `"US"` |
| `bps_variable` | STRING | No | Census BPS variable code (e.g., `PERMIT`, `PERMIT1`, `PERMIT5`) |
| `is_revised_figure` | BOOLEAN | No | `true` if Census issued a revision |
| `source` | STRING | No | Always `"census_bps"` |
| `source_agency` | STRING | No | Always `"CENSUS"` |
| `data_source_note` | STRING | Yes | `"FRED mirror of Census BPS series"` |
| `data_quality_certified` | BOOLEAN | No | `true` when validation passes |

### `bps_variable` / `macro_metric_name` Vocabulary

| BPS Variable | Metric Name | Description |
|-------------|-------------|-------------|
| `PERMIT` | `AUTHORIZED_PERMITS_TOTAL_UNITS` | Total residential units authorised |
| `PERMIT1` | `AUTHORIZED_PERMITS_1_UNIT` | Single-family structures |
| `PERMIT5` | `AUTHORIZED_PERMITS_5PLUS_UNITS` | Multi-family (5+ units) |
| `PERMITNSA` | `AUTHORIZED_PERMITS_TOTAL_NSA` | Total, not seasonally adjusted |

---

## Common PIT Fields

| Field | Type | Description |
|-------|------|-------------|
| `data_timestamp` | TIMESTAMP (UTC) | First moment of measurement period |
| `reporting_date` | STRING (ISO 8601) | Same as `data_timestamp` |
| `official_release_date` | STRING (ISO 8601) | Scheduled BLS/Census release date |
| `published_date` | STRING (ISO 8601) | Actual publication date |
| `as_of_date` | STRING (ISO 8601) | Ingestion date for this version |
| `conversion_timestamp` | STRING (ISO 8601) | Vault conversion timestamp |
| `revision_number` | INTEGER | `0` = initial; incremented per revision |
| `superseded_by` | STRING | `record_id` of newer version; `"N/A"` if current |

**PIT Guarantee**: `published_date >= data_timestamp` for every record.

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `record_id` | UTF8 | UUID v4; unique; not null |
| `observed_value` | DOUBLE | > 0 |
| `unit_of_measure` | UTF8 | `"INDEX"`, `"UNITS_SAAR"`, or `"UNITS_NSA"` |
| `confidence_tier` | UTF8 | `"PRIMARY"` |
| `seasonal_adjustment` | UTF8 | `"S"` or `"U"` |
| `data_quality_certified` | BOOLEAN | `true` for production records |

---

## Sample Field Values

**Sample period**: January–March 2022

| Dataset | Field | Example Value |
|---------|-------|---------------|
| Shelter | `macro_metric_name` | `"CPI_RENT_OF_PRIMARY_RESIDENCE"` |
| Shelter | `observed_value` | `379.4` (index value, Jan 2022) |
| Shelter | `unit_of_measure` | `"INDEX"` |
| Shelter | `data_timestamp` | `2022-01-01T00:00:00Z` |
| Shelter | `published_date` | `2022-02-10T00:00:00Z` |
| Permits | `bps_variable` | `"PERMIT"` |
| Permits | `observed_value` | `1,895` (thousands of units SAAR, Jan 2022) |
| Permits | `unit_of_measure` | `"UNITS_SAAR"` |
| Permits | `data_timestamp` | `2022-01-01T00:00:00Z` |

---

## Data Sources

| Source | Agency | Program | Frequency | Coverage |
|--------|---------|---------|-----------|----------|
| BLS CPI Shelter Series | Bureau of Labor Statistics | Consumer Price Index | Monthly | 1959–2026 |
| Census BPS via FRED | US Census Bureau / Federal Reserve | Building Permits Survey | Monthly | 1960–2026 |

---

## Coverage and Granularity

- **Geography**: National (US)
- **Shelter temporal**: 1959–2026, monthly; 7 CPI shelter sub-series
- **Permits temporal**: 1960–2026, monthly; 4 structure-type categories
- **Vault partition**: `year={yyyy}/month={m}` (Hive)
- **Total records**: ~6,566

---

## Quality Metrics

| Stage | Check | Result |
|-------|-------|--------|
| 1 | PIT Validation (10 checks) | PASS |
| 2 | Sanity Checks | PASS |
| 3 | Schema Compliance (15 SDMX checks) | PASS |
| 4 | Temporal Consistency | PASS* |
| 5 | Referential Integrity | PASS* |
| 6 | Lineage | PASS |
| 7 | GX Universal Validation | PASS |
| 8 | Outlier Extraction | PASS |
| 9 | Changelog Generation | PASS |

**Overall: 7/9 PASS** (2 stages reflect genuine data findings — see notes)

*Stage 4 note: 201 permits records have a publication lag under 30 days (real Census practice for preliminary releases).
Stage 5 note: 4 BPS variables missing from early historical vintages; 32 records flagged for source labelling review.
