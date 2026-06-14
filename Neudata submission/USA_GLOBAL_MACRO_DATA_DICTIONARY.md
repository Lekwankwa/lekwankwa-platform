# USA Global Macro Data Dictionary
## Schema Version 1.0 (Point-in-Time Enabled)

**Product**: Global Macro (IMF WEO)
**Coverage**: United States (1980–2031, including IMF forecasts)
**Source**: International Monetary Fund World Economic Outlook (WEO) via IMF DataMapper API
**Records**: 4,488 validated records (8 indicators x 52 years x 12 months)
**Sample**: January–March 2022 (24 records — 8 indicators x 3 months)
**Update Frequency**: Bi-annual (April and October WEO releases); vault refreshed on each release
**Last Updated**: June 14, 2026
**Sample File**: `sample_parquet_global_macro/global_macro_imf_weo_v1.0_sample.parquet`

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Indicator Reference](#indicator-reference)
3. [Core Data Fields](#core-data-fields)
4. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
5. [Forecast vs Actual Logic](#forecast-vs-actual-logic)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values (Jan–Mar 2022)](#sample-field-values)
8. [Data Source](#data-source)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Annual macroeconomic indicators for the United States from the IMF World Economic Outlook database,
expanded to monthly granularity for consistent partitioning with other vault products. Each annual
WEO value is replicated across all 12 months of that year to enable time-series joins with monthly
datasets (CPI, trade flows, employment, etc.).

**Key Features**:
- 52-year span: 1980–2024 actuals, 2025–2031 IMF forecasts
- 8 key macro indicators: GDP, inflation, unemployment, current account, government debt
- No API key required: sourced from public IMF DataMapper API
- PIT-compliant: `published_date` = April 1 of the following year (WEO confirmation date)
- 9/9 validation stages passing
- SDMX-aligned: indicator codes, unit vocabulary, and vintage IDs follow SDMX conventions
- Hive-partitioned: `product=global_macro/country=USA/source=imf_weo/year={yyyy}/month={m}/`

---

## Indicator Reference

| Indicator Code | Metric Name | Unit | Coverage |
|----------------|-------------|------|----------|
| `PCPIPCH` | CPI Inflation (% change, annual) | `PERCENT` | 1980–2031 |
| `NGDP_RPCH` | Real GDP Growth (% change) | `PERCENT` | 1980–2031 |
| `NGDPD` | GDP at Current Prices (USD billions) | `USD_BILLIONS` | 1980–2031 |
| `PPPGDP` | GDP, PPP (International $ billions) | `INTL_DOLLAR_BN` | 1980–2031 |
| `LUR` | Unemployment Rate (% of labour force) | `PERCENT` | 1980–2031 |
| `BCA_NGDPD` | Current Account Balance (% of GDP) | `PERCENT` | 1980–2031 |
| `GGXWDG_NGDP` | Gross Government Debt (% of GDP) | `PERCENT` | 2001–2031 |
| `GGXCNL_NGDP` | Government Net Lending/Borrowing (% of GDP) | `PERCENT` | 2001–2031 |

---

## Core Data Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `product` | STRING | No | Always `"global_macro"` |
| `country_code` | STRING | No | ISO 3166-1 alpha-2; always `"US"` |
| `iso_alpha3` | STRING | No | Always `"USA"` |
| `source` | STRING | No | Always `"imf_weo"` |
| `source_agency` | STRING | No | Always `"IMF"` |
| `source_sub_category` | STRING | No | Always `"WEO"` |
| `sovereign_series_id` | STRING | No | IMF WEO indicator code (see table above) |
| `macro_metric_name` | STRING | No | Standardised metric name (e.g., `CPI_INFLATION_ANNUAL_PCT_CHANGE`) |
| `observed_value` | DOUBLE | No | Numeric value in `unit_of_measure` units |
| `unit_of_measure` | STRING | No | `"PERCENT"`, `"USD_BILLIONS"`, or `"INTL_DOLLAR_BN"` |
| `sdmx_frequency` | STRING | No | Always `"A"` (Annual; value replicated across months) |
| `market_tier` | STRING | No | Always `"Developed"` |
| `portal_url` | STRING | No | `"https://www.imf.org/external/datamapper/"` |
| `extraction_method` | STRING | No | Always `"api"` |
| `confidence_tier` | STRING | No | `"PRIMARY"` (actuals) or `"ESTIMATED"` (forecasts, year >= 2025) |
| `is_forecast` | BOOLEAN | No | `true` for year >= 2025; `false` for year <= 2024 |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |
| `data_vintage_id` | STRING | No | `IMF-{indicator}-USA-{yyyy}-v1` |
| `revision_number` | INTEGER | No | Always `1` (single WEO vintage per year) |
| `processing_timestamp` | STRING (ISO 8601) | No | Timestamp of vault ingestion run |

---

## Point-in-Time (PIT) Fields

| Field | Type | Description |
|-------|------|-------------|
| `data_timestamp` | STRING (ISO 8601) | First moment of the monthly reporting period (e.g., `2022-01-01T00:00:00Z`) |
| `published_date` | STRING (ISO 8601) | April 1 of the following year (WEO April release confirms all prior-year data) |

**PIT Design**: Because WEO data is annual but stored monthly, `published_date` is set to
`{year+1}-04-01T00:00:00Z` for all 12 months of a given year. This ensures every monthly
`data_timestamp` (Jan–Dec of year N) precedes the `published_date` (April 1 of year N+1).

**Forecast boundary**: Records with `year >= 2025` have `is_forecast = true` and
`confidence_tier = "ESTIMATED"`. Records with `year <= 2024` have `is_forecast = false`
and `confidence_tier = "PRIMARY"`.

---

## Forecast vs Actual Logic

| Year Range | `is_forecast` | `confidence_tier` | Source |
|-----------|---------------|-------------------|--------|
| 1980–2024 | `false` | `"PRIMARY"` | IMF WEO historical actuals |
| 2025–2031 | `true` | `"ESTIMATED"` | IMF WEO projections |

The forecast boundary `FORECAST_FROM = 2025` is defined in the scraper and aligned with the
April 2026 WEO vintage at time of ingestion.

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `record_id` | UTF8 | UUID v4; unique; not null |
| `observed_value` | DOUBLE | Not null; range varies by indicator (see table) |
| `sovereign_series_id` | UTF8 | Must be in the 8-indicator vocabulary |
| `unit_of_measure` | UTF8 | `"PERCENT"`, `"USD_BILLIONS"`, or `"INTL_DOLLAR_BN"` |
| `confidence_tier` | UTF8 | `"PRIMARY"` or `"ESTIMATED"` |
| `is_forecast` | BOOLEAN | Aligned with year >= 2025 |
| `data_quality_certified` | BOOLEAN | `true` for production records |
| `published_date` | STRING | ISO 8601 UTC; >= all 12 `data_timestamp` values for that year |

### Observed Value Ranges

| Indicator | Min | Max | Notes |
|-----------|-----|-----|-------|
| `PCPIPCH` | –10 | 100 | Annual CPI inflation % |
| `NGDP_RPCH` | –20 | 20 | Real GDP growth % |
| `NGDPD` | 0 | 50,000 | USD billions |
| `PPPGDP` | 0 | 50,000 | Int'l $ billions |
| `LUR` | 0 | 50 | Unemployment rate % |
| `BCA_NGDPD` | –30 | 30 | Current account % GDP |
| `GGXWDG_NGDP` | 0 | 200 | Govt debt % GDP |
| `GGXCNL_NGDP` | –30 | 10 | Net lending % GDP |

---

## Sample Field Values

**Sample period**: January–March 2022 | 24 records (8 indicators x 3 months)

| Field | Example Value |
|-------|---------------|
| `sovereign_series_id` | `"PCPIPCH"` |
| `macro_metric_name` | `"CPI_INFLATION_ANNUAL_PCT_CHANGE"` |
| `observed_value` | `8.0` (% annual CPI inflation, USA 2022) |
| `unit_of_measure` | `"PERCENT"` |
| `is_forecast` | `false` (2022 is a historical actual) |
| `confidence_tier` | `"PRIMARY"` |
| `data_timestamp` | `2022-01-01T00:00:00Z` |
| `published_date` | `2023-04-01T00:00:00Z` (April WEO year+1) |
| `data_vintage_id` | `"IMF-PCPIPCH-USA-2022-v1"` |
| `sdmx_frequency` | `"A"` (annual value replicated monthly) |

---

## Data Source

| Source | API | Auth | Frequency | URL |
|--------|-----|------|-----------|-----|
| IMF World Economic Outlook | IMF DataMapper REST API v1 | None (public) | Bi-annual (Apr + Oct) | `https://www.imf.org/external/datamapper/api/v1/{indicator}/{country}` |

---

## Coverage and Granularity

- **Geography**: USA (`country_code=US`, `iso_alpha3=USA`)
- **Temporal**: 1980–2031 (52 years)
  - Actuals: 1980–2024
  - Forecasts: 2025–2031 (IMF projections)
- **Indicators**: 8 (6 from 1980; 2 government finance indicators from 2001)
- **Monthly expansion**: Each annual value replicated to 12 monthly records
- **Vault partition**: `year={yyyy}/month={m}` (Hive)
- **Total records**: 4,488

---

## Quality Metrics

| Stage | Check | Result |
|-------|-------|--------|
| 1 | PIT Validation (10 checks) | PASS |
| 2 | Sanity Checks (624 files, 4,488 rows, 8/8 indicators) | PASS |
| 3 | Schema Compliance (15 SDMX checks) | PASS |
| 4 | Temporal Consistency (17 checks — no gaps 1980–2031) | PASS |
| 5 | Referential Integrity (all (indicator, year, month) triples unique) | PASS |
| 6 | Lineage | PASS |
| 7 | GX Universal Validation (14 required columns, no nulls) | PASS |
| 8 | Outlier Extraction (41 outliers across 23 years — documented) | PASS |
| 9 | Changelog Generation (52 entries) | PASS |

**Overall: 9/9 PASS**
