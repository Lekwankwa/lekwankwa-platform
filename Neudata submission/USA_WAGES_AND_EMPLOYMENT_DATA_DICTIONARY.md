# USA Wages and Employment Data Dictionary
## Schema Version 1.0 (Point-in-Time Enabled)

**Product**: US Wages & Labour
**Dataset Name**: `wages_and_employment`
**Coverage**: United States — CES: 1939–2026 | CPS: 1948–2026
**Sources**: Bureau of Labor Statistics (BLS) Current Employment Statistics (CES) & Current Population Survey (CPS)
**Approximate Records**: ~760,000 validated records (CES ~399k + CPS ~361k)
**Update Frequency**: Monthly
**Last Updated**: June 14, 2026
**Sample Files**:
  - `wages_and_employment_ces_v1.0_sample.parquet` — 54 records, Jan–Mar 2022 (CES)
  - `wages_and_employment_cps_v1.0_sample.parquet` — 33 records, Jan–Mar 2022 (CPS)

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Dataset Types](#dataset-types)
3. [CES Fields](#ces-fields)
4. [CPS Fields](#cps-fields)
5. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values](#sample-field-values)
8. [Data Sources](#data-sources)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US labour market data sourced exclusively from BLS official APIs and FTP endpoints.
Two complementary programmes:

**CES (Current Employment Statistics)** — Monthly payroll survey of ~122,000 businesses covering 666,000
worksites. Delivers nonfarm payroll employment, average weekly hours, and average hourly/weekly earnings
across ~900 NAICS-aligned industry codes.

**CPS (Current Population Survey)** — Monthly household survey (~60,000 households). Delivers the
official unemployment rate (U-3), broader U-6 measure, employment and labour force levels, participation
rate, employment-to-population ratio, and demographic breakdowns by age, gender, and race.

Full Point-in-Time (PIT) capability: every record carries the actual BLS press release date so backtests
contain no look-ahead bias.

---

## Dataset Types

| Programme | Source | Coverage | Records | Sample |
|-----------|--------|----------|---------|--------|
| CES Payroll Survey | BLS CES FTP | 1939–2026 | ~399,000 | Jan–Mar 2022 (54 rows) |
| CPS Labour Force Survey | BLS CPS API | 1948–2026 | ~361,000 | Jan–Mar 2018–2024 (231 rows) |

---

## CES Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `iso_alpha3` | STRING | No | Always `"USA"` |
| `country_name` | STRING | No | Always `"United States"` |
| `country_code` | STRING | No | ISO 3166-1 alpha-2; always `"US"` |
| `market_tier` | STRING | No | Always `"Developed"` |
| `source_agency` | STRING | No | Always `"BLS"` |
| `source_sub_category` | STRING | No | Always `"CES"` |
| `portal_url` | STRING | No | `"https://www.bls.gov"` |
| `sovereign_series_id` | STRING | No | BLS CES series code (e.g., `CES0000000001`) |
| `data_vintage_id` | STRING | No | `BLS-{series}-{yyyy}-{mm}-v{n}` |
| `confidence_tier` | STRING | No | `"PRIMARY"` |
| `macro_metric_name` | STRING | No | Metric identifier (see CES vocabulary) |
| `observed_value` | DOUBLE | No | Numeric value in `unit_of_measure` units |
| `metric_value` | DOUBLE | No | Alias of `observed_value` |
| `unit_of_measure` | STRING | No | `"THOUSANDS_PERSONS"`, `"AVG_WEEKLY_HOURS"`, `"AVG_HOURLY_EARNINGS_USD"` |
| `is_revised_figure` | BOOLEAN | No | `true` if BLS issued a revision |
| `seasonal_adjustment` | STRING | No | `"S"` = seasonally adjusted; `"U"` = unadjusted |
| `industry_code` | STRING | No | 8-digit NAICS-aligned BLS industry code |
| `industry_name` | STRING | No | Industry description (e.g., `"Total Nonfarm"`) |
| `source` | STRING | No | Always `"bls_ces"` |
| `extraction_method` | STRING | No | Always `"ftp"` |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |
| `bls_footnotes` | STRING | Yes | JSON array of BLS annotation codes |

### CES `macro_metric_name` Vocabulary

| Value | Unit | Description |
|-------|------|-------------|
| `NONFARM_PAYROLL_EMPLOYMENT` | `THOUSANDS_PERSONS` | Total nonfarm payroll jobs |
| `AVG_WEEKLY_HOURS` | `AVG_WEEKLY_HOURS` | Average weekly hours |
| `AVG_HOURLY_EARNINGS` | `AVG_HOURLY_EARNINGS_USD` | Average hourly earnings (USD) |
| `AVG_WEEKLY_EARNINGS` | `AVG_WEEKLY_EARNINGS_USD` | Average weekly earnings (USD) |
| `WOMEN_EMPLOYEES` | `THOUSANDS_PERSONS` | Female employees |
| `PRODUCTION_WORKERS` | `THOUSANDS_PERSONS` | Production & non-supervisory workers |

---

## CPS Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `iso_alpha3` | STRING | No | Always `"USA"` |
| `country_name` | STRING | No | Always `"United States"` |
| `country_code` | STRING | No | ISO 3166-1 alpha-2; always `"US"` |
| `market_tier` | STRING | No | Always `"Developed"` |
| `source_agency` | STRING | No | Always `"BLS"` |
| `source_sub_category` | STRING | No | Always `"CPS"` |
| `portal_url` | STRING | No | `"https://www.bls.gov"` |
| `sovereign_series_id` | STRING | No | BLS CPS series code (e.g., `LNS14000000`) |
| `data_vintage_id` | STRING | No | `BLS-{series}-{yyyy}-{mm}-v{n}` |
| `confidence_tier` | STRING | No | `"PRIMARY"` |
| `macro_metric_name` | STRING | No | Metric identifier (see CPS vocabulary) |
| `reporting_date` | STRING (ISO 8601) | No | First of the reference month |
| `data_timestamp` | TIMESTAMP (UTC) | No | First of the reference month (UTC) |
| `official_release_date` | STRING (ISO 8601) | No | Scheduled BLS release (~first Friday of following month) |
| `published_date` | STRING (ISO 8601) | No | Actual BLS publication date |
| `observed_value` | DOUBLE | No | Numeric value in `unit_of_measure` units |
| `metric_value` | DOUBLE | No | Alias of `observed_value` |
| `unit_of_measure` | STRING | No | `"THOUSANDS_PERSONS"` or `"PERCENTAGE"` |
| `is_revised_figure` | BOOLEAN | No | `true` if BLS issued a revision |
| `seasonal_adjustment` | STRING | No | `"S"` = seasonally adjusted; `"U"` = unadjusted |
| `industry_code` | STRING | No | Always `"00000000"` (national aggregate) |
| `industry_name` | STRING | No | Always `"National (All Industries)"` |
| `source` | STRING | No | Always `"bls_cps"` |
| `extraction_method` | STRING | No | Always `"api"` |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |
| `conversion_timestamp` | STRING (ISO 8601) | No | Vault schema conversion timestamp |
| `as_of_date` | STRING (ISO 8601) | No | Timestamp of vault ingestion |
| `revision_number` | INTEGER | No | `0` = initial release |
| `superseded_by` | STRING | Yes | `record_id` of newer version; `null` if current |
| `bls_footnotes` | STRING | Yes | JSON array of BLS annotation codes |

### CPS `macro_metric_name` Vocabulary

| Value | Series ID | Unit | Description |
|-------|-----------|------|-------------|
| `UNEMPLOYMENT_RATE_U3` | `LNS14000000` | `PERCENTAGE` | Official unemployment rate (U-3) |
| `UNEMPLOYMENT_LEVEL` | `LNS13000000` | `THOUSANDS_PERSONS` | Total unemployed persons |
| `EMPLOYMENT_LEVEL` | `LNS12000000` | `THOUSANDS_PERSONS` | Total employed persons |
| `CIVILIAN_LABOR_FORCE` | `LNS11000000` | `THOUSANDS_PERSONS` | Total civilian labour force |
| `LABOR_FORCE_PARTICIPATION_RATE` | `LNS11300000` | `PERCENTAGE` | Labour force participation rate |
| `EMPLOYMENT_POPULATION_RATIO` | `LNS12300000` | `PERCENTAGE` | Employment-to-population ratio |
| `UNEMPLOYMENT_RATE_U6` | `LNS14032183` | `PERCENTAGE` | Broad unemployment (U-6, incl. underemployed) |
| `UNEMPLOYMENT_RATE_YOUTH_16_19` | `LNS14000006` | `PERCENTAGE` | Youth unemployment rate (16–19 yrs) |
| `UNEMPLOYMENT_RATE_MEN_20PLUS` | `LNS14000009` | `PERCENTAGE` | Male unemployment rate (20+) |
| `UNEMPLOYMENT_RATE_WOMEN_20PLUS` | `LNS14000012` | `PERCENTAGE` | Female unemployment rate (20+) |
| `UNEMPLOYMENT_RATE_WHITE` | `LNS14000031` | `PERCENTAGE` | White unemployment rate |

---

## Point-in-Time (PIT) Fields

| Field | Type | Description |
|-------|------|-------------|
| `data_timestamp` | TIMESTAMP (UTC) | First moment of the measurement month (e.g., `2022-01-01T00:00:00Z`) |
| `reporting_date` | STRING (ISO 8601) | Same as `data_timestamp` as a string |
| `official_release_date` | STRING (ISO 8601) | BLS scheduled press release (~1st of following month) |
| `published_date` | STRING (ISO 8601) | Actual BLS publication date |
| `as_of_date` | STRING (ISO 8601) | Vault ingestion timestamp for this record version |
| `conversion_timestamp` | STRING (ISO 8601) | Schema conversion timestamp |
| `revision_number` | INTEGER | `0` = initial; incremented on each BLS revision |
| `superseded_by` | STRING | `record_id` of newer version; `null` if current |

**PIT Guarantee**: `published_date >= data_timestamp` for every record (validated at Stage 1).

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `record_id` | UTF8 | UUID v4; unique; not null |
| `observed_value` | DOUBLE | Not null |
| `unit_of_measure` | UTF8 | `"THOUSANDS_PERSONS"` or `"PERCENTAGE"` |
| `revision_number` | INT64 | >= 0 |
| `data_timestamp` | TIMESTAMP(tz=UTC) | ISO 8601 UTC |
| `seasonal_adjustment` | UTF8 | `"S"` or `"U"` |
| `confidence_tier` | UTF8 | `"PRIMARY"` |

---

## Sample Field Values

### CES — Jan–Mar 2022 (54 rows)

| Field | Example Value |
|-------|---------------|
| `sovereign_series_id` | `"CES0000000001"` |
| `macro_metric_name` | `"NONFARM_PAYROLL_EMPLOYMENT"` |
| `observed_value` | `150,155.0` (thousands, Jan 2022) |
| `unit_of_measure` | `"THOUSANDS_PERSONS"` |
| `data_timestamp` | `2022-01-01T00:00:00Z` |
| `industry_name` | `"Total Nonfarm"` |

### CPS — Jan–Mar 2018–2024 (231 rows)

| Field | Example Values |
|-------|----------------|
| `sovereign_series_id` | `"LNS14000000"` (U-3 rate) |
| `macro_metric_name` | `"UNEMPLOYMENT_RATE_U3"` |
| `observed_value` | `4.1` (%, Jan 2018) → `3.7` (%, Jan 2024) |
| `unit_of_measure` | `"PERCENTAGE"` |
| `data_timestamp` | `2018-01-01T00:00:00Z` through `2024-03-01T00:00:00Z` |
| `industry_name` | `"National (All Industries)"` |
| `seasonal_adjustment` | `"U"` |

---

## Data Sources

| Dataset | Agency | Programme | Method | Historical Coverage |
|---------|--------|-----------|--------|---------------------|
| CES | Bureau of Labor Statistics | Current Employment Statistics | FTP bulk download | 1939–2026 |
| CPS | Bureau of Labor Statistics | Current Population Survey | REST API v2 | 1948–2026 |

**BLS API Endpoint**: `https://api.bls.gov/publicAPI/v2/timeseries/data/`
**BLS Portal**: `https://www.bls.gov/`

---

## Coverage and Granularity

### CES
- **Geography**: National (US)
- **Temporal**: 1939–2026, monthly
- **Industry granularity**: ~900 NAICS-aligned codes (8-digit)
- **Metrics**: Employment (thousands), average weekly hours, average hourly/weekly earnings
- **Seasonal adjustment**: Both SA and NSA variants
- **Vault partition**: `product=wages_and_employment/country=USA/source=bls_ces/year={yyyy}/month={m}/`
- **Record count**: ~399,000

### CPS
- **Geography**: National (US) with demographic breakdowns
- **Temporal**: 1948–2026, monthly (complete; no gaps)
- **Granularity**: National totals + variants by gender, age group, race
- **Metrics**: Labour force, unemployment, employment levels and rates (11 series)
- **Seasonal adjustment**: Unadjusted (U) headline series
- **Vault partition**: `product=wages_and_employment/country=USA/source=bls_cps/year={yyyy}/month={m}/`
- **Record count**: ~361,000

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
