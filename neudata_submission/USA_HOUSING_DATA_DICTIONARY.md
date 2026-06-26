# USA Housing Supply and Shelter Inflation Data Dictionary
## Schema Version 5.0 (Point-in-Time Enabled)

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
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN = 30 countries active (AUS DISCONTINUED — ABS RPPI ceased 2021-Q4; NOR DISCONTINUED — no SSB source identified)

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
| `source` | str | `bls_cpi_shelter` |
| `extraction_method` | str | `api_pull` |
| `data_quality_certified` | bool | True if record passed all 9 validation stages |
| `revision_number` | int | 0 (fixed; RELEASE_DATE_ONLY — BLS/Census do not publish vintage dates) |
| `superseded_by` | str | Record ID of superseding revision, or null |
| `bls_footnotes` | str | BLS footnote codes (e.g., preliminary, revised) |

### BLS Shelter Series (7 series)

| `sovereign_series_id` (vault) | Description | Adjustment |
|-------------------------------|-------------|-----------|
| `CUSR0000SAH1` | Shelter — All Urban Consumers | SA |
| `CUSR0000SEHA` | Rent of Primary Residence | SA |
| `CUSR0000SEHB` | Owners' Equivalent Rent of Residences | SA |
| `CUUR0000SAH1` | Shelter — All Urban Consumers | NSA |
| `CUUR0000SEHA` | Rent of Primary Residence | NSA |
| `CUUR0000SEHB` | Owners' Equivalent Rent of Residences | NSA |
| `CUUR0000SEHC` | Rent of Shelter | NSA |

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

## EU27 Permits Schema — `is_interpolated` and `interpolation_method` Fields

The global vault file `permits_eu27_data.parquet` is shared between USA (Census BPS rows) and 27 EU
member states (Eurostat SDMX rows). The EU27 schema adds two fields that are absent from the USA
31-column schema:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `is_interpolated` | boolean | **No** | True for carry-forward fill records; False for genuine quarterly observations |
| `interpolation_method` | string | Yes | `QUARTERLY_CARRY_FORWARD` when `is_interpolated=True`; null when `is_interpolated=False` |

### Why these fields exist (EU27 quarterly publishers only)

Fourteen EU member states (AUT, BGR, CZE, DNK, EST, HRV, IRL, ITA, LTU, LUX, LVA, MLT, POL, SVK)
publish building permits at quarterly frequency via Eurostat `sts_cobp_q`. To maintain a consistent
monthly time-series across all 27 EU countries, the pipeline generates carry-forward fill records for
months 2 and 3 of each quarter (e.g., a Q1 observation is carried forward to February and March).

Genuine observations and fill records co-exist within a single `permits_eu27_data.parquet` file for
these 14 countries. `is_interpolated` is the authoritative flag distinguishing them:

- `is_interpolated = False` → genuine quarterly observation (use for analysis)
- `is_interpolated = True`  → carry-forward fill record (`confidence_tier = DERIVED`)

### Filtering

`is_interpolated` is always set explicitly True or False. Filtering genuine observations is safe
with a direct equality check:

```python
# pandas
genuine = df[df["is_interpolated"] == False]

# SQL / DuckDB
SELECT * FROM permits_eu27_data WHERE is_interpolated = False
```

No `IS NOT TRUE` or nullable-boolean workaround is needed. The field was made non-nullable in
June 2026 when a vault backfill corrected 1,322 rows that had been written as None due to the
scraper omitting the key for genuine rows (resolved by explicit `is_interpolated=False` in
`ingest_permits_quarterly_14.py`).

### Scope

`is_interpolated` and `interpolation_method` are present **only** in:
- `permits_eu27_data.parquet` for the 14 quarterly-publisher EU countries listed above
- `hpi_monthly_fill.parquet` — EU27 HPI forward-fill records
- `wages_empl_monthly_fill.parquet` — EU27 wages forward-fill records
- `trade_exports_fill.parquet` / `trade_imports_fill.parquet` — EU27 trade forward-fill records

These fields are intentionally absent from the USA partition of `permits_eu27_data.parquet` and
from all 13 monthly-publisher EU countries (BEL, CYP, DEU, ESP, FIN, FRA, GRC, HUN, NLD, PRT,
ROU, SVN, SWE), which publish at monthly frequency and require no fill.

---

## Point-in-Time (PIT) Fields

| Field | Description | PIT Role |
|-------|-------------|----------|
| `official_release_date` | BLS/Census official publication date | Primary PIT gate |
| `published_date` | Same as official_release_date for housing data | Secondary reference |
| `as_of_date` | Snapshot date | Revision context |
| `revision_number` | 0 (fixed; RELEASE_DATE_ONLY — no vintage tracking) | Not a meaningful filter for housing |
| `reporting_date` | Observation period (first day of month) | Time-series index |

**Important note on Housing PIT model:**
Housing data uses `RELEASE_DATE_ONLY` — neither BLS nor Census publishes vintage date histories for
these series. `revision_number = 0` for all records (both sources); it is a fixed placeholder, not
a meaningful revision counter. Use `official_release_date <= simulation_date` as the sole PIT gate.
Do not filter on `revision_number` for housing data.

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `observed_value` | > 0 (CPI index or permit count) |
| `revision_number` | 0 (fixed; no revision depth for RELEASE_DATE_ONLY sources) |
| `official_release_date` | Must be ≥ `reporting_date` (PIT constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `seasonal_adjustment` | `SA`, `NSA`, or `SAAR` |
| `geo_level` | `national`, `regional`, `state`, `msa` (permits only) |

---

## Sample Field Values (2015–2017 Preview)

### Shelter CPI (252-row sample, 29 columns)
```
record_id              : lkw-shelter-USA-CUUR0000SAH1-2015-01
iso_alpha3             : USA
sovereign_series_id    : CUUR0000SAH1
macro_metric_name      : Shelter CPI — All Urban Consumers
reporting_date         : 2015-01-01
official_release_date  : 2015-02-26
observed_value         : 271.4
unit_of_measure        : Index (1982-84=100)
seasonal_adjustment    : NSA
source                 : bls_cpi_shelter
revision_number        : 0
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
revision_number        : 0
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
| Countries | 1 (USA) | 1 (USA) | 30 active (AUS + NOR DISCONTINUED) |
| Availability | Archive only | Archive only | Archive only |

**Live feed note**: Housing data is archive-only (no live feed product) due to mixed quarterly
frequency in EU27 HICP shelter components. This applies across all 32 countries.

---

## Known Data Gaps

| Gap ID | Affected Series | Period | Reason | Client Action |
|--------|----------------|--------|--------|---------------|
| `BLS_2025_APPROPRIATIONS_LAPSE` | `CUUR0000SAH1`, `CUUR0000SEHA`, `CUUR0000SEHB`, `CUUR0000SEHC`, `CUSR0000SEHA`, `CUSR0000SEHB`, `CUSR0000SAH1` (all 7 BLS shelter series) | October 2025 only | US government appropriations lapse; BLS CPI not published (footnote code X). BLS does not retroactively backfill. November 2025 confirmed normal resumption. | Treat all 7 shelter series as `NaN` for October 2025. Do not flag as scraper error. Forward-fill from September 2025 for that single month if gap-free series is required. |
| `AUS_RPPI_DISCONTINUED` | `RPPI_HOUSES` (AUS only) | All periods after 2021-Q4 | ABS SDMX RPPI dataflow ceased. No `includeHistory` support; no mechanism to extend. | 74 vault rows available for 2003-Q3 to 2021-Q4 historical backtests only. Do not use for post-2021 strategies. |
| `NOR_HOUSING_NO_SOURCE` | All NOR housing series | All periods | No SSB Statbank table for residential property price index identified. No vault data exists. | Exclude NOR from all housing strategies. |

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

`data_quality_certified` is a universal vault field present on all records across all 5 products and all 32 countries (100% of data partitions). `conversion_timestamp` is a USA-only pipeline ingestion artifact, present only in the food_micropricing/USA and wages_and_employment/USA vault partitions (2 of 160 total); it is absent from all EU27 and non-EU country partitions and from all USA housing, trade, and global_macro partitions. Both fields are pipeline bookkeeping metadata — **not** PIT events or publication metadata.

### `data_quality_certified` (boolean)

| Attribute | Value |
|-----------|-------|
| Type | boolean |
| Nullable | No |
| Coverage | All 5 products · All 32 countries · 100% of vault data partitions. Value = True for all countries across all 5 products. USA food_micropricing, USA wages_and_employment, and USA housing (bls_cpi_shelter + census_bps rows) were all corrected to True June 2026 — scraper-placeholder False; confirmed 10/10 validation PASS for housing. |
| True | Record passed all 9 automated validation stages |
| False | Record carries one or more quality flags (retained, not suppressed; documented in validation reports) |

**Backtesting note**: It is safe to include `False` records in backtests if the accompanying outlier/sanity reports confirm the flag is a boundary annotation, not a data error. Review the validation reports shipped with the archive before discarding any `False` records.

**Sample file note**: All sample records now show `data_quality_certified = True`. The housing vault backfill was completed June 2026 — scraper-placeholder `False` values for `bls_cpi_shelter` and `census_bps` rows were corrected after confirming 10/10 validation stages PASS.

---

### `conversion_timestamp` (datetime, UTC)

| Attribute | Value |
|-----------|-------|
| Type | datetime ISO 8601 (UTC) |
| Nullable | Yes — absent in 158 of 160 vault partitions. Present only in food/USA and wages/USA partitions. |
| Coverage | Food micropricing/USA and wages_and_employment/USA only (2 of 160 vault partitions). Absent from all EU27 and non-EU partitions, and from USA housing, trade, and global_macro vault files. |

**Definition**: The UTC datetime when the Lekwankwa ingestion pipeline last wrote or updated this record in the vault partition. This is a **batch processing bookkeeping field only** — it records when the ETL process materialized the record to disk.

> **This is NOT a publication date, NOT a PIT event, and NOT a data quality timestamp.**

**Distinction from `as_of_date`**:

| Field | Meaning | Use for PIT? |
|-------|---------|-------------|
| `published_date` / `official_release_date` | Actual government publication date | **Yes — primary PIT gate** |
| `as_of_date` | Knowledge cutoff for this revision snapshot (should equal published_date) | Reference only |
| `conversion_timestamp` | When the Lekwankwa pipeline ran and wrote the record | **No — pipeline internal only** |

Always use `published_date` or `official_release_date` as the PIT gate for backtesting — not `conversion_timestamp` or `as_of_date`. The `as_of_date` pipeline contamination for housing `census_bps` rows (2,340 records that carried the 2026-06-15 scrape date instead of the historical publication date) was corrected June 2026: `as_of_date` is now set to `official_release_date[:10]` for all affected rows.
