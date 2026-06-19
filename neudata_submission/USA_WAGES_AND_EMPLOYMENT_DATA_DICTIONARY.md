# USA Wages and Employment Data Dictionary
## Schema Version 2.0 (Point-in-Time Enabled)

**Product**: US Wages & Labour
**Dataset Name**: `wages_and_employment`
**Coverage**: United States — CPS Unemployment: 1948–2026 | CES Payroll: 1939–2026
**Sources**: Bureau of Labor Statistics (BLS) Current Population Survey (CPS) · Current Employment Statistics (CES)
**Vault Records (CPS)**: 8,802 confirmed records (unemployment series, confirmed in vault)
**Vault Records (CES)**: Vault structure present; data parquets under review — schema documented via sample
**PIT Type**: RELEASE_DATE_ONLY (single official release date per observation; no ALFRED multi-vintage for wages)
**Sample**: 2015–2017 · 3-Year Sample
**Sample Files**:
  - `sample_parquet_wages_and_employment/wages_and_employment_cps_v1.0_sample.parquet` — 396 rows, 31 columns
  - `sample_parquet_wages_and_employment/wages_and_employment_ces_v1.0_sample.parquet` — 648 rows, 31 columns
**Update Frequency**: Monthly
**Last Updated**: June 2026
**CPS Vault File**: `unemployment_u3_data.parquet`
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN, AUS, NOR = 32 countries

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Dataset Types](#dataset-types)
3. [CPS Fields (31 columns)](#cps-fields)
4. [CES Fields (31 columns)](#ces-fields)
5. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values (2015–2017 Preview)](#sample-field-values)
8. [Data Sources](#data-sources)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

The Wages & Labour product covers two complementary BLS data series that together characterize
US labor market conditions from a household survey (CPS) and an establishment survey (CES)
perspective.

**Schema v1.0 → v2.0 changes:**
- CPS vault records confirmed: 8,802 rows spanning 11 unemployment/labor force series (1948–2026)
- CES vault structure audited; data parquet completeness under review (schema is fully documented)
- Both samples now reflect the 2015–2017 3-year period with the 31-column Golden Record Schema v5.0
- Added columns: `official_release_date`, `industry_code`, `industry_name`, `conversion_timestamp`,
  `superseded_by`, `bls_footnotes` (vs v1.0 schema)

**Important note on CPS scope**: The 8,802-row CPS vault covers headline unemployment series
(U-1 through U-6 + total labor force and participation metrics = 11 series). It does not include
the full CPS microdata (household-level survey records). This is a macro-level time-series product.

---

## Dataset Types

| Type | Survey | Vault File | Sample File | Confirmed Rows | Cols | Date Range |
|------|--------|-----------|-------------|---------------|------|-----------|
| CPS Unemployment | Current Population Survey | `unemployment_u3_data.parquet` | `wages_and_employment_cps_v1.0_sample.parquet` | 8,802 | 31 | 1948–2026 |
| CES Payroll | Current Employment Statistics | *(under review)* | `wages_and_employment_ces_v1.0_sample.parquet` | TBD | 31 | 1939–2026 |

---

## CPS Fields (31 columns)

### Identification & Geography

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | str | Globally unique UUID record identifier |
| `iso_alpha3` | str | ISO 3166-1 alpha-3 (`USA`) |
| `country_name` | str | `United States` |
| `country_code` | str | ISO 3166-1 alpha-2 (`US`) |
| `market_tier` | str | `SOVEREIGN` |
| `source_agency` | str | `Bureau of Labor Statistics` |
| `source_sub_category` | str | `CPS — Current Population Survey` |
| `portal_url` | str | BLS data portal URL |

### Series Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `sovereign_series_id` | str | Lekwankwa canonical series ID (e.g., `UNRATE_USA_BLS_LNS14000000`) |
| `data_vintage_id` | str | Unique vintage snapshot identifier |
| `confidence_tier` | str | `PRIMARY` for all validated records |
| `macro_metric_name` | str | Human-readable series name (e.g., `Unemployment Rate U-3 (SA)`) |

### Temporal Fields

| Column | Type | Description |
|--------|------|-------------|
| `reporting_date` | date | Observation period (first day of reference month) |
| `data_timestamp` | datetime | Observation period ISO 8601 timestamp |
| `official_release_date` | date | BLS official publication date (Employment Situation release) |
| `published_date` | date | Same as official_release_date |
| `conversion_timestamp` | datetime | Pipeline processing timestamp |
| `as_of_date` | date | Snapshot as-of date |

### Value Fields

| Column | Type | Description |
|--------|------|-------------|
| `observed_value` | float | Numeric series value (% for rates, thousands for levels) |
| `metric_value` | float | Derived metric (e.g., YoY change) |
| `unit_of_measure` | str | `%` (rates) or `Thousands` (level series) |

### Classification (CPS-specific)

| Column | Type | Description |
|--------|------|-------------|
| `industry_code` | str | `HOUSEHOLD` (CPS is a household survey) |
| `industry_name` | str | `All Civilian Noninstitutional Population` |

### Metadata Fields

| Column | Type | Description |
|--------|------|-------------|
| `is_revised_figure` | bool | True if this is a revised release |
| `seasonal_adjustment` | str | `SA` (seasonally adjusted) or `NSA` |
| `source` | str | `bls_cps` |
| `extraction_method` | str | `api_pull` |
| `data_quality_certified` | bool | True if passed all 9 validation stages |
| `revision_number` | int | 1 = initial; increments on revision |
| `superseded_by` | str | Record ID of superseding revision, or null |
| `bls_footnotes` | str | BLS footnote codes (P = preliminary, R = revised) |

### CPS Series (11 confirmed in vault)

| Sovereign Series ID | BLS Series ID | Description | Unit |
|--------------------|--------------|-------------|------|
| `UNRATE_USA_BLS_LNS14000000` | `LNS14000000` | Unemployment Rate U-3 (SA) | % |
| `UNRATE_U1_USA_BLS_LNS13023569` | `LNS13023569` | U-1: Jobless 15+ weeks (SA) | % |
| `UNRATE_U2_USA_BLS_LNS13023654` | `LNS13023654` | U-2: Job losers and completers (SA) | % |
| `UNRATE_U4_USA_BLS_LNS14000025` | `LNS14000025` | U-4: U-3 + discouraged workers | % |
| `UNRATE_U5_USA_BLS_LNS14000012` | `LNS14000012` | U-5: U-4 + marginally attached | % |
| `UNRATE_U6_USA_BLS_LNS13327709` | `LNS13327709` | U-6: Total underemployment (SA) | % |
| `CIVLF_USA_BLS_CLF16OV` | `CLF16OV` | Civilian Labor Force Level (SA) | Thousands |
| `LFPART_USA_BLS_LNS11300000` | `LNS11300000` | Labor Force Participation Rate (SA) | % |
| `EMPLOY_USA_BLS_CE16OV` | `CE16OV` | Civilian Employment Level (SA) | Thousands |
| `UNEMPLOY_USA_BLS_UNEMPLOY` | `UNEMPLOY` | Unemployed Persons Level (SA) | Thousands |
| `EMRAT_USA_BLS_LNS12300000` | `LNS12300000` | Employment-Population Ratio (SA) | % |

---

## CES Fields (31 columns)

The CES sample shares the same 31-column Golden Record Schema as the CPS sample. CES-specific
columns are:

| Column | Type | Description (CES context) |
|--------|------|--------------------------|
| `industry_code` | str | NAICS industry code (e.g., `10-14` = Mining; `31-33` = Manufacturing) |
| `industry_name` | str | NAICS industry description |
| `seasonal_adjustment` | str | `SA` (most CES series are seasonally adjusted) |
| `observed_value` | float | Payroll employment (thousands) or average hourly earnings |
| `unit_of_measure` | str | `Thousands` (employment) or `USD` (earnings) |
| `source` | str | `bls_ces` |
| `source_agency` | str | `Bureau of Labor Statistics` |
| `source_sub_category` | str | `CES — Current Employment Statistics` |

All other fields (temporal, identification, metadata) are identical to the CPS schema above.

**CES series coverage:**
- Total nonfarm payrolls (`PAYEMS` equivalent) + breakdowns by major NAICS sector
- Average hourly earnings (total private, by sector)
- Average weekly hours (total private, by sector)
- Production/nonsupervisory worker series

---

## Point-in-Time (PIT) Fields

| Field | Description | PIT Role |
|-------|-------------|----------|
| `official_release_date` | BLS Employment Situation report date | Primary PIT gate |
| `published_date` | Same as official_release_date | Secondary reference |
| `as_of_date` | Snapshot date | Revision context |
| `revision_number` | 1 = initial; >1 = revised | Preliminary vs final filter |
| `reporting_date` | Observation period (first day of month) | Time-series index |

**BLS release schedule**: The Employment Situation (CPS/CES combined) releases on the first
Friday of each month, ~5 weeks after the reference month end.

**Correct PIT query:**
```python
pit_view = df[df["official_release_date"] <= simulated_date]
latest = (
    pit_view.sort_values("revision_number")
    .drop_duplicates(["sovereign_series_id", "reporting_date"], keep="last")
)
```

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `observed_value` | > 0 for employment/earnings; 0–100 for rate series |
| `revision_number` | ≥ 1 integer |
| `official_release_date` | Must be ≥ `reporting_date` + 25 days (BLS lag constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `seasonal_adjustment` | `SA` or `NSA` |
| `unit_of_measure` | `%`, `Thousands`, `USD`, or `Hours` |
| `source` | `bls_cps` (CPS) or `bls_ces` (CES) |

---

## Sample Field Values (2015–2017 Preview)

### CPS Sample (396 rows, 31 columns)
```
record_id              : lkw-cps-USA-LNS14000000-2015-01
iso_alpha3             : USA
sovereign_series_id    : UNRATE_USA_BLS_LNS14000000
macro_metric_name      : Unemployment Rate U-3 (SA)
reporting_date         : 2015-01-01
official_release_date  : 2015-02-06
observed_value         : 5.7
unit_of_measure        : %
seasonal_adjustment    : SA
industry_code          : HOUSEHOLD
source                 : bls_cps
revision_number        : 1
data_quality_certified : True
```

### CES Sample (648 rows, 31 columns)
```
record_id              : lkw-ces-USA-PAYEMS-2015-01
iso_alpha3             : USA
sovereign_series_id    : PAYEMS_USA_BLS_CES0000000001
macro_metric_name      : Total Nonfarm Payrolls (SA)
reporting_date         : 2015-01-01
official_release_date  : 2015-02-06
observed_value         : 140742.0
unit_of_measure        : Thousands
seasonal_adjustment    : SA
industry_code          : 00-00
industry_name          : Total Nonfarm
source                 : bls_ces
revision_number        : 1
data_quality_certified : True
```

---

## Data Sources

### BLS Current Population Survey (CPS)
- **Provider**: Bureau of Labor Statistics
- **API**: BLS Public Data API v2 (`api.bls.gov/publicAPI/v2/timeseries/data`)
- **Coverage**: 11 headline unemployment and labor force series
- **Date range**: January 1948 to present (series-dependent)
- **Frequency**: Monthly
- **PIT type**: RELEASE_DATE_ONLY (Employment Situation release, first Friday of month)
- **Confirmed vault rows**: 8,802

### BLS Current Employment Statistics (CES)
- **Provider**: Bureau of Labor Statistics
- **Source**: BLS CES FTP bulk download + API
- **Coverage**: Total nonfarm + ~850 NAICS industry breakdowns; earnings; hours
- **Date range**: January 1939 to present (series-dependent)
- **Frequency**: Monthly
- **PIT type**: RELEASE_DATE_ONLY
- **Vault rows**: Under review (CES parquet completeness audit in progress)

---

## Coverage and Granularity

| Dimension | CPS | CES | Global Vault |
|-----------|-----|-----|-------------|
| Date range | 1948–2026 | 1939–2026 | 2000–2026 (non-EU) |
| Frequency | Monthly | Monthly | Monthly/Quarterly |
| Series count | 11 | 850+ NAICS | ~5–15 per country |
| Industry depth | Household (aggregate) | 850+ NAICS codes | Varies by country |
| PIT type | RELEASE_DATE_ONLY | RELEASE_DATE_ONLY | RELEASE_DATE_ONLY |
| Countries | 1 (USA) | 1 (USA) | 32 total |
| Live feed | Yes (monthly) | Yes (monthly) | Yes (monthly) |

---

## Quality Metrics

| Metric | CPS | CES |
|--------|-----|-----|
| Vault records | 8,802 | Under review |
| Null rate (observed_value) | 0.00% | 0.00% (sample) |
| Null rate (official_release_date) | 0.00% | 0.00% (sample) |
| PIT violations | 0 | 0 |
| Avg revisions | 1.00 | 1.00 |
| GX expectations passed | 128 / 128 | 128 / 128 (sample) |
| Overall validation | 9 / 9 stages PASS | Schema PASS; data audit ongoing |
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
