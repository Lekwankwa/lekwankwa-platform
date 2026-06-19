# USA Trade Flows Data Dictionary
## Schema Version 2.0 (Point-in-Time Enabled)

**Product**: USA Trade Flows
**Coverage**: United States (1992–2026)
**Sources**: US Census Bureau FT-900 · ALFRED Vintage Feed (St. Louis Fed — BOPGSTB)
**Vault Records**: 43,020 validated records
**Series Count**: 196 series (BOPGSTB ALFRED multi-vintage + HS01–HS99 EXP/IMP Census)
**PIT Type**: FULL VINTAGE (ALFRED component) · RELEASE_DATE_ONLY (Census HS-code component)
**Sample**: 2015–2017 · 3-Year Sample (7,020 rows, 35 columns)
**Sample File**: `sample_parquet_trade_flows/trade_flows_v1.0_sample.parquet`
**Update Frequency**: Monthly
**Last Updated**: June 2026
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN, AUS, NOR = 32 countries

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Series Reference](#series-reference)
3. [Core Data Fields (35 columns)](#core-data-fields)
4. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
5. [Data Types and Constraints](#data-types-and-constraints)
6. [Sample Field Values (2015–2017 Preview)](#sample-field-values)
7. [Data Sources](#data-sources)
8. [Coverage and Granularity](#coverage-and-granularity)
9. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Schema v2.0 expands US trade flows from ~38,122 Census-only records (2010–2026) to 43,020 records
(1992–2026) by adding ALFRED multi-vintage revision history for the aggregate trade balance
series (BOPGSTB) and extending the Census HS-code data to the full 1992 Census API start date.

**v1.0 → v2.0 changes:**
- Extended date range: 2010–2026 → 1992–2026
- Added ALFRED BOPGSTB multi-vintage source (full BOP revision history back to 1992)
- Series count: ~198 (HS-code) → 196 (consolidated: 2 ALFRED + 194 HS-code EXP/IMP)
- Schema expanded: 30 vault cols → 35 (added `partner_country_code`, `partner_country_name`,
  `commodity_code`, `commodity_name`, `trade_flow`, `source_url`)
- Vault records: ~38,122 → 43,020

The sample parquet (7,020 rows, 35 columns) covers 2015–2017 across all active HS chapters and
the BOPGSTB series.

---

## Series Reference

### ALFRED Component — Aggregate Trade Balance

| Series ID | Description | Source | Revisions |
|-----------|-------------|--------|-----------|
| `BOPGSTB` | U.S. Trade Balance — Goods and Services ($ Millions) | ALFRED | avg 8.80 |

The BOPGSTB series is sourced with full multi-vintage revision depth from ALFRED. Each
(obs_date, revision) pair represents the trade balance as officially published at that point in time.

### Census FT-900 Component — HS-Code Level

194 series covering Exports (EXP) and Imports (IMP) for HS-2 commodity chapters HS01–HS99
(minus excluded codes), plus the aggregate goods totals.

**Naming pattern**: `TRADE_{FLOW}_{HS_CODE}_USA_CENSUS`
- `TRADE_EXP_HS01_USA_CENSUS` — Exports: Live Animals (HS Chapter 01)
- `TRADE_IMP_HS01_USA_CENSUS` — Imports: Live Animals (HS Chapter 01)
- ... (97 export + 97 import = 194 HS-code series)

---

## Core Data Fields (35 columns)

### Identification & Geography

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | str | Globally unique UUID record identifier |
| `iso_alpha3` | str | Reporter country ISO 3166-1 alpha-3 (`USA`) |
| `country_name` | str | `United States` |
| `country_code` | str | ISO 3166-1 alpha-2 (`US`) |
| `market_tier` | str | `SOVEREIGN` |
| `partner_country_code` | str | ISO 3166-1 alpha-2 of trading partner (World = `WLD`; `null` for BOPGSTB) |
| `partner_country_name` | str | Trading partner name or `World Aggregate` |

### Commodity Classification

| Column | Type | Description |
|--------|------|-------------|
| `commodity_code` | str | HS-2 chapter code (e.g., `01`–`99`); null for BOPGSTB aggregate |
| `commodity_name` | str | HS chapter description (e.g., `Live Animals`); null for BOPGSTB |
| `trade_flow` | str | `EXP` (exports) / `IMP` (imports) / `BAL` (balance for BOPGSTB) |

### Series Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `sovereign_series_id` | str | Lekwankwa canonical series ID |
| `source_series_id` | str | Original source series identifier |
| `data_vintage_id` | str | Unique vintage snapshot identifier |
| `macro_metric_name` | str | Human-readable series name |

### Temporal Fields

| Column | Type | Description |
|--------|------|-------------|
| `reporting_date` | date | Observation period (first day of reference month) |
| `data_timestamp` | datetime | Observation period ISO 8601 timestamp |
| `official_release_date` | date | Official FT-900 / ALFRED publication date (primary PIT gate) |
| `published_date` | date | Publication date |
| `as_of_date` | date | Snapshot as-of date |
| `conversion_timestamp` | datetime | Pipeline processing timestamp |

### Value Fields

| Column | Type | Description |
|--------|------|-------------|
| `observed_value` | float | Trade value in USD millions |
| `trade_value` | float | Same as observed_value (alias for downstream compatibility) |
| `unit_of_measure` | str | `USD Millions` |
| `currency` | str | `USD` |

### PIT & Revision Metadata

| Column | Type | Description |
|--------|------|-------------|
| `is_revised_figure` | bool | True if revision_number > 1 |
| `confidence_tier` | str | `PRIMARY` for all validated records |
| `revision_number` | int | 1 = advance release; max = final revised (ALFRED only) |
| `superseded_by` | str | Record ID of superseding revision, or null |

### Source Provenance

| Column | Type | Description |
|--------|------|-------------|
| `source` | str | `alfred_vintage` or `census_ft900` |
| `source_agency` | str | `Federal Reserve Bank of St. Louis` or `US Census Bureau` |
| `source_sub_category` | str | Sub-classification (e.g., `FT-900 Monthly`, `ALFRED BOPGSTB`) |
| `portal_url` | str | Source portal URL |
| `source_url` | str | Direct API call URL |
| `extraction_method` | str | `api_pull` |
| `data_quality_certified` | bool | True if passed all 9 validation stages |

---

## Point-in-Time (PIT) Fields

| Field | ALFRED (BOPGSTB) | Census HS-Code |
|-------|-----------------|----------------|
| `official_release_date` | ALFRED vintage release date | FT-900 monthly release date |
| `revision_number` | 1–42 range (avg 8.80) | 1 (RELEASE_DATE_ONLY) |
| `is_revised_figure` | True for rev_num > 1 | False |
| `data_vintage_id` | Unique per ALFRED revision | Unique per monthly release |

**ALFRED PIT query (preliminary only):**
```python
pit_view = df[
    (df["source"] == "alfred_vintage") &
    (df["official_release_date"] <= simulated_date) &
    (df["revision_number"] == 1)
]
```

**Census FT-900 PIT query:**
```python
pit_view = df[
    (df["source"] == "census_ft900") &
    (df["official_release_date"] <= simulated_date)
]
```

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `observed_value` | Numeric (USD millions); can be negative for trade balance |
| `revision_number` | ≥ 1 integer |
| `official_release_date` | Must be ≥ `reporting_date` (PIT constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `trade_flow` | `EXP`, `IMP`, or `BAL` |
| `commodity_code` | `01`–`99` or null (for aggregate series) |
| `source` | `alfred_vintage` or `census_ft900` |

---

## Sample Field Values (2015–2017 Preview)

The 7,020-row sample covers January 2015–December 2017 across all active HS chapters.

```
# HS-Code record example
record_id              : lkw-trade-USA-HS01-EXP-2015-01
iso_alpha3             : USA
partner_country_code   : WLD
commodity_code         : 01
commodity_name         : Live Animals
trade_flow             : EXP
sovereign_series_id    : TRADE_EXP_HS01_USA_CENSUS
observed_value         : 158.3
unit_of_measure        : USD Millions
reporting_date         : 2015-01-01
official_release_date  : 2015-03-04
source                 : census_ft900
revision_number        : 1
data_quality_certified : True

# ALFRED BOPGSTB record example
record_id              : lkw-trade-USA-BOPGSTB-2015-01-v1
iso_alpha3             : USA
trade_flow             : BAL
sovereign_series_id    : BOPGSTB_USA_ALFRED
observed_value         : -40820.0
unit_of_measure        : USD Millions
reporting_date         : 2015-01-01
official_release_date  : 2015-03-06
source                 : alfred_vintage
revision_number        : 1
data_quality_certified : True
```

---

## Data Sources

### US Census Bureau FT-900 (Foreign Trade)
- **Provider**: US Census Bureau, Economic Indicators Division
- **API**: Census International Trade API (`api.census.gov/data/timeseries/intltrade`)
- **Coverage**: 97 HS-2 export chapters + 97 import chapters = 194 series
- **Date range**: January 1992 to present (monthly)
- **PIT type**: RELEASE_DATE_ONLY
- **Vault rows**: ~42,440 (Census component)

### ALFRED — Balance of Payments Trade Balance
- **Provider**: Federal Reserve Bank of St. Louis
- **Series**: `BOPGSTB` — U.S. International Trade in Goods and Services, Balance
- **API**: ALFRED vintage API
- **Revisions**: Average 8.80 per (obs_date); maximum 42 revisions
- **Date range**: January 1992 to present
- **PIT type**: FULL VINTAGE
- **Vault rows**: ~580 (ALFRED component across all revision vintages)

---

## Coverage and Granularity

| Dimension | Census FT-900 | ALFRED BOPGSTB | Combined |
|-----------|--------------|----------------|---------|
| Date range | 1992–2026 | 1992–2026 | 1992–2026 |
| Frequency | Monthly | Monthly | Monthly |
| Series count | 194 (HS EXP/IMP) | 2 (BAL + components) | 196 |
| Granularity | HS-2 chapter | Aggregate BOP | Mixed |
| PIT type | RELEASE_DATE_ONLY | FULL VINTAGE | Mixed |
| Countries (USA vault) | USA | USA | USA |
| Countries (global vault) | 32 countries | USA only | 32 countries |
| Vault records (USA) | ~42,440 | ~580 | 43,020 |
| Live feed | Yes (monthly FT-900) | Yes (monthly ALFRED) | Yes |

---

## Quality Metrics

| Metric | Value |
|--------|-------|
| Total validated records | 43,020 |
| Null rate (observed_value) | 0.00% |
| Null rate (official_release_date) | 0.00% |
| PIT violations | 0 |
| Avg revisions (ALFRED component) | 8.80 |
| Avg revisions (Census component) | 1.00 |
| GX expectations passed | 128 / 128 (100%) |
| Overall validation | 9 / 9 stages PASS |
| Schema standard | SDMX 2.1 aligned |
| Delivery format | Apache Parquet (flat schema, no nesting) |
