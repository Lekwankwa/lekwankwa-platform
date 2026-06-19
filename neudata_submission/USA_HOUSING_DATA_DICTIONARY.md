# USA Housing Supply and Shelter Inflation Data Dictionary
## Schema Version 2.0 (Point-in-Time Enabled)

**Product**: Housing Supply and Shelter Inflation
**Coverage**: United States — Shelter CPI: 1914–2026 | Building Permits: 1960–2026
**Sources**: Bureau of Labor Statistics (BLS) CPI Shelter Series · US Census Bureau Building Permits Survey (via FRED)
**Vault Records**: 19,487 validated records (10,923 shelter + 8,564 permits)
**PIT Type**: RELEASE_DATE_ONLY (single snapshot per official release; no multi-vintage revision depth)
**Sample**: 2015–2017 · 3-Year Sample
**Sample Files**:
  - `sample_parquet_housing/housing_shelter_inflation_v1.0_sample.parquet` — 252 rows, 29 columns
  - `sample_parquet_housing/housing_permits_v1.0_sample.parquet` — 108 rows, 31 columns
**Update Frequency**: Monthly
**Last Updated**: June 2026
**Vault Filenames**: `housing_hicp_rent_data.parquet` (shelter) · `permits_eu27_data.parquet` (permits)
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN, AUS = 32 countries (NOR Housing PENDING)

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Dataset Types](#dataset-types)
3. [Shelter Inflation Fields (29 columns)](#shelter-inflation-fields)
4. [Building Permits Fields (31 columns)](#building-permits-fields)
5. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values (2015–2017 Preview)](#sample-field-values)
8. [Data Sources](#data-sources)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Schema v2.0 expands the original housing product from 6,566 rows to 19,487 rows by adding full
CPI Shelter series depth (back to 1914) and a complete building permits series spanning 1960–2026.
The vault uses the `RELEASE_DATE_ONLY` PIT model — each record carries the official BLS/Census
release date but revision history is limited to what is exposed via FRED.

**v1.0 → v2.0 changes:**
- Shelter records expanded: ~3,000 → 10,923 (full 1914–2026 BLS CPI Shelter history)
- Permits records expanded: ~3,566 → 8,564 (extended to all available series back to 1960)
- Shelter schema: 24 → 29 columns (added `official_release_date`, `as_of_date`, `bls_footnotes`, `conversion_timestamp`, `superseded_by`)
- Permits schema: 25 → 31 columns (added `geo_level`, `geo_id`, `bps_variable`, `data_source_note`, additional PIT fields)

The sample parquets reflect the v2.0 29/31-column schemas as they were regenerated during the
housing vault rebuild.

---

## Dataset Types

The housing product ships as two separate parquet files (shelter and permits) with distinct schemas.

| Type | Vault File | Sample File | Rows | Cols | Date Range |
|------|-----------|------------|------|------|-----------|
| Shelter CPI | `housing_hicp_rent_data.parquet` | `housing_shelter_inflation_v1.0_sample.parquet` | 10,923 | 29 | 1914–2026 |
| Building Permits | `permits_eu27_data.parquet` | `housing_permits_v1.0_sample.parquet` | 8,564 | 31 | 1960–2026 |

---

## Shelter Inflation Fields (29 columns)

### Core Identification & Classification

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | str | Globally unique UUID record identifier |
| `iso_alpha3` | str | ISO 3166-1 alpha-3 (`USA`) |
| `country_name` | str | `United States` |
| `country_code` | str | ISO 3166-1 alpha-2 (`US`) |
| `market_tier` | str | `SOVEREIGN` |
| `source_agency` | str | `Bureau of Labor Statistics` |
| `source_sub_category` | str | BLS CPI sub-series category |
| `portal_url` | str | BLS data portal URL |

### Series Identification

| Column | Type | Description |
|--------|------|-------------|
| `sovereign_series_id` | str | Lekwankwa canonical series ID |
| `data_vintage_id` | str | Unique vintage snapshot identifier |
| `confidence_tier` | str | `PRIMARY` for all validated records |
| `macro_metric_name` | str | Human-readable series name (e.g., `Shelter CPI — All Urban Consumers`) |

### Temporal Fields

| Column | Type | Description |
|--------|------|-------------|
| `reporting_date` | date | Reference period (first day of observation month) |
| `data_timestamp` | datetime | Observation period ISO 8601 timestamp |
| `official_release_date` | date | BLS official publication date (primary PIT gate) |
| `published_date` | date | Publication date (same as `official_release_date` for BLS) |
| `conversion_timestamp` | datetime | Timestamp of pipeline processing |
| `as_of_date` | date | Snapshot as-of date |

### Value Fields

| Column | Type | Description |
|--------|------|-------------|
| `observed_value` | float | CPI index value (index: 1982-84 = 100) |
| `metric_value` | float | Derived metric value (YoY % change, MoM % change) |
| `unit_of_measure` | str | `Index (1982-84=100)` or `%` |

### Metadata Fields

| Column | Type | Description |
|--------|------|-------------|
| `is_revised_figure` | bool | True if this is a revised release |
| `seasonal_adjustment` | str | `SA` (seasonally adjusted) or `NSA` |
| `source` | str | `bls_cpi` |
| `extraction_method` | str | `api_pull` |
| `data_quality_certified` | bool | True if record passed all 9 validation stages |
| `revision_number` | int | 1 for initial release; increments on revision |
| `superseded_by` | str | Record ID of superseding revision, or null |
| `bls_footnotes` | str | BLS footnote codes (e.g., preliminary, revised) |

### BLS Shelter Series (7 series)

| Sovereign Series ID | Description | BLS Series |
|--------------------|-------------|-----------|
| `SHELTER_CPI_USA_BLS_CUSR0000SAH` | Shelter — All Urban Consumers (NSA) | `CUSR0000SAH` |
| `SHELTER_CPI_USA_BLS_CUUR0000SAH` | Shelter — US City Average (NSA) | `CUUR0000SAH` |
| `SHELTER_CPI_USA_BLS_CUSR0000SEHA` | Rent of Primary Residence (SA) | `CUSR0000SEHA` |
| `SHELTER_CPI_USA_BLS_CUSR0000SEHB` | Owners' Equivalent Rent (SA) | `CUSR0000SEHB` |
| `SHELTER_CPI_USA_BLS_CUSR0000SEHC` | Rent of Shelter (SA) | `CUSR0000SEHC` |
| `SHELTER_CPI_USA_BLS_CUSR0000SEHD` | Lodging Away from Home (SA) | `CUSR0000SEHD` |
| `SHELTER_CPI_USA_BLS_CUSR0000SEHE` | Water, Sewer, Trash Collection (SA) | `CUSR0000SEHE` |

---

## Building Permits Fields (31 columns)

All shelter fields plus these 6 permits-specific columns:

| Column | Type | Description |
|--------|------|-------------|
| `geo_level` | str | Geographic aggregation: `national`, `regional`, `state`, `msa` |
| `geo_id` | str | FIPS or MSA code for sub-national records; `US` for national |
| `bps_variable` | str | Census BPS variable code (e.g., `permit`, `starts`, `completions`) |
| `source` | str | `census_bps` (US Census Building Permits Survey) |
| `data_source_note` | str | Notes on data revision status or methodology changes |
| `extraction_method` | str | `api_pull` (via FRED API) |

### Building Permits Series (3 series)

| Sovereign Series ID | Description | FRED Series |
|--------------------|-------------|-----------|
| `PERMITS_USA_CENSUS_PERMIT` | New Privately-Owned Housing Units Authorized (Total, SAAR) | `PERMIT` |
| `PERMITS_USA_CENSUS_PERMIT1` | Single-Family Units Authorized (SAAR) | `PERMIT1` |
| `PERMITS_USA_CENSUS_PERMIT5` | Multi-Family 5+ Units Authorized (SAAR) | `PERMIT5` |

---

## Point-in-Time (PIT) Fields

| Field | Description | PIT Role |
|-------|-------------|----------|
| `official_release_date` | BLS/Census official publication date | Primary PIT gate |
| `published_date` | Same as official_release_date for housing data | Secondary reference |
| `as_of_date` | Snapshot date | Revision context |
| `revision_number` | 1 = initial; >1 = revised | Preliminary vs final filter |
| `reporting_date` | Observation period (first day of month) | Time-series index |

**Important note on Housing PIT model:**
Housing data uses `RELEASE_DATE_ONLY` — there is no multi-vintage ALFRED depth for these series.
Average revisions per (series, obs_date) ≈ 1.00 (single snapshot). The Building Permits series
(Census BPS) carries preliminary → revised → final transitions which are captured as
`revision_number` increments, but only when the FRED series itself reflects revisions.

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `observed_value` | > 0 (CPI index or permit count) |
| `revision_number` | ≥ 1 integer |
| `official_release_date` | Must be ≥ `reporting_date` (PIT constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `seasonal_adjustment` | `SA`, `NSA`, or `SAAR` |
| `geo_level` | `national`, `regional`, `state`, `msa` (permits only) |

---

## Sample Field Values (2015–2017 Preview)

### Shelter CPI (252-row sample, 29 columns)
```
record_id              : lkw-shelter-USA-CUSR0000SAH-2015-01
iso_alpha3             : USA
sovereign_series_id    : SHELTER_CPI_USA_BLS_CUSR0000SAH
macro_metric_name      : Shelter CPI — All Urban Consumers
reporting_date         : 2015-01-01
official_release_date  : 2015-02-26
observed_value         : 271.4
unit_of_measure        : Index (1982-84=100)
seasonal_adjustment    : NSA
source                 : bls_cpi
revision_number        : 1
data_quality_certified : True
```

### Building Permits (108-row sample, 31 columns)
```
record_id              : lkw-permits-USA-PERMIT-2015-01
iso_alpha3             : USA
sovereign_series_id    : PERMITS_USA_CENSUS_PERMIT
macro_metric_name      : New Housing Units Authorized — Total (SAAR)
reporting_date         : 2015-01-01
official_release_date  : 2015-02-18
observed_value         : 1073.0
unit_of_measure        : Thousands (SAAR)
geo_level              : national
geo_id                 : US
bps_variable           : permit
source                 : census_bps
revision_number        : 1
data_quality_certified : True
```

---

## Data Sources

### BLS CPI Shelter Series
- **Provider**: Bureau of Labor Statistics
- **API**: BLS Public Data API v2 (series prefix `CUU`, `CUS`)
- **Coverage**: 7 shelter sub-series from January 1914 to present
- **Frequency**: Monthly
- **Vault rows**: 10,923

### US Census Bureau Building Permits Survey (BPS)
- **Provider**: US Census Bureau via FRED (St. Louis Fed)
- **API**: FRED API (`api.stlouisfed.org/fred/series/observations`)
- **Coverage**: 3 permit series (total, single-family, multi-family 5+) from January 1960
- **Frequency**: Monthly
- **Vault rows**: 8,564

---

## Coverage and Granularity

| Dimension | Shelter CPI | Building Permits | Global Vault |
|-----------|------------|-----------------|-------------|
| Date range | 1914–2026 | 1960–2026 | 2005–2026 (EU27) |
| Frequency | Monthly | Monthly | Monthly/Quarterly |
| Series count | 7 | 3 | ~15 per country |
| PIT type | RELEASE_DATE_ONLY | RELEASE_DATE_ONLY | RELEASE_DATE_ONLY |
| Countries | 1 (USA) | 1 (USA) | 31 (NOR PENDING) |
| Availability | Archive only | Archive only | Archive only |

**Live feed note**: Housing data is archive-only (no live feed product) due to mixed quarterly
frequency in EU27 HICP shelter components. This applies across all 32 countries.

---

## Quality Metrics

| Metric | Shelter CPI | Building Permits |
|--------|------------|-----------------|
| Vault records | 10,923 | 8,564 |
| Null rate (observed_value) | 0.00% | 0.00% |
| Null rate (official_release_date) | 0.00% | 0.00% |
| PIT violations | 0 | 0 |
| Avg revisions | 1.00 | 1.00 |
| GX expectations passed | 128 / 128 | 128 / 128 |
| Overall validation | 9 / 9 stages PASS | 9 / 9 stages PASS |
| Schema standard | SDMX 2.1 aligned | SDMX 2.1 aligned |
| Delivery format | Apache Parquet (flat) | Apache Parquet (flat) |

---

## Provenance Fields — Pipeline Bookkeeping

These two fields are present on every record (except Global Macro which omits conversion_timestamp) and appear in the delivery sample. They are pipeline bookkeeping fields — **not** PIT or publication metadata.

### `data_quality_certified` (boolean)

| Attribute | Value |
|-----------|-------|
| Type | boolean |
| Nullable | No |
| Coverage | All 5 products · All 32 countries |
| True | Record passed all 9 automated validation stages |
| False | Record carries one or more quality flags (retained, not suppressed; documented in validation reports) |

**Backtesting note**: It is safe to include `False` records in backtests if the accompanying outlier/sanity reports confirm the flag is a boundary annotation, not a data error. Review the validation reports shipped with the archive before discarding any `False` records.

**Sample file note**: Wages (CES/CPS) and Housing sample records may show `False` due to a pending schema v2.0 recertification sweep. Full production vault records reflect the final certified state. Food, Trade, and Global Macro samples show `True`.

---

### `conversion_timestamp` (datetime, UTC)

| Attribute | Value |
|-----------|-------|
| Type | datetime ISO 8601 (UTC) |
| Nullable | Yes (absent in Global Macro sample) |
| Coverage | Food, Wages, Housing, Trade — all 32 countries |

**Definition**: The UTC datetime when the Lekwankwa ingestion pipeline last wrote or updated this record in the vault partition. This is a **batch processing bookkeeping field only** — it records when the ETL process materialized the record to disk.

> **This is NOT a publication date, NOT a PIT event, and NOT a data quality timestamp.**

**Distinction from `as_of_date`**:

| Field | Meaning | Use for PIT? |
|-------|---------|-------------|
| `published_date` / `official_release_date` | Actual government publication date | **Yes — primary PIT gate** |
| `as_of_date` | Knowledge cutoff for this revision snapshot (should equal published_date) | Reference only |
| `conversion_timestamp` | When the Lekwankwa pipeline ran and wrote the record | **No — pipeline internal only** |

**Known inconsistency**: In the Wages, Housing, and Trade samples, `as_of_date` was set to the pipeline run date (`2026-06-19T10:00:00Z`) rather than the original publication date. This is documented in the per-product changelogs. Always use `published_date` or `official_release_date` as the PIT gate for backtesting — never `conversion_timestamp` or `as_of_date`.
