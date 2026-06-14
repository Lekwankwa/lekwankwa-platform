# USA Trade Flows Data Dictionary
## Schema Version 1.0 (Point-in-Time Enabled)

**Product**: USA Trade Flows
**Coverage**: United States (2010–2026)
**Sources**: US Census Bureau FT-900 (Foreign Trade) via Census International Trade API
**Approximate Records**: 38,122 validated records
**Sample**: January–March 2022 (585 records)
**Update Frequency**: Monthly
**Last Updated**: June 14, 2026
**Sample File**: `sample_parquet_trade_flows/trade_flows_v1.0_sample.parquet`

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Core Data Fields](#core-data-fields)
3. [Trade Classification Fields](#trade-classification-fields)
4. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
5. [Data Types and Constraints](#data-types-and-constraints)
6. [Sample Field Values (Jan–Mar 2022)](#sample-field-values)
7. [Data Sources](#data-sources)
8. [Coverage and Granularity](#coverage-and-granularity)
9. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US international trade data covering merchandise exports and imports at the
HS-2 commodity level. Sourced from the US Census Bureau FT-900 monthly trade report via the
official Census International Trade API.

**Key Features**:
- No look-ahead bias: `published_date` reflects actual FT-900 press release date (~5 weeks after reference month)
- HS-2 commodity granularity: 99 HS chapters covering all traded goods
- Bilateral trade: exports (FOB) and imports (CIF/customs value) separately
- 9/9 validation stages passing
- Hive-partitioned: `product=trade_flows/country=USA/source=census_ft900/year={yyyy}/month={m}/`

---

## Core Data Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `record_id` | STRING (UUID v4) | No | Unique record identifier |
| `iso_alpha3` | STRING | No | Always `"USA"` (reporting country) |
| `country_name` | STRING | No | Always `"United States"` |
| `country_code` | STRING | No | Always `"US"` |
| `market_tier` | STRING | No | Always `"Developed"` |
| `partner_country_code` | STRING | No | Census partner code (`"0000"` = World aggregate) |
| `partner_country_name` | STRING | No | Partner country or `"WORLD"` |
| `observed_value` | DOUBLE | No | Trade value in USD millions |
| `trade_value` | DOUBLE | No | Alias of `observed_value` |
| `unit_of_measure` | STRING | No | Always `"USD_MILLIONS"` |
| `currency` | STRING | No | Always `"USD"` |
| `is_revised_figure` | BOOLEAN | No | `true` if Census issued a revision |
| `confidence_tier` | STRING | No | `"PRIMARY"` for official Census data |
| `source` | STRING | No | Always `"census_ft900"` |
| `source_agency` | STRING | No | Always `"CENSUS"` |
| `source_sub_category` | STRING | No | Always `"TRADE"` |
| `portal_url` | STRING | No | `"https://www.census.gov/foreign-trade/"` |
| `source_url` | STRING | No | Census API endpoint used |
| `extraction_method` | STRING | No | Always `"api"` |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |

---

## Trade Classification Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `commodity_code` | STRING | No | HS-2 chapter code (`"01"`–`"99"`) |
| `commodity_name` | STRING | No | HS chapter description (e.g., `"Clocks and Watches"`) |
| `trade_flow` | STRING | No | `"Export"` or `"Import"` |
| `sovereign_series_id` | STRING | No | `HS{cc}_{EXP\|IMP}` (e.g., `HS91_EXP`) |
| `source_series_id` | STRING | No | Same as `sovereign_series_id` |
| `data_vintage_id` | STRING | No | `CENSUS-{sovereign_series_id}-{yyyy}-{mm}-v{n}` |
| `macro_metric_name` | STRING | No | e.g., `EXPORTS_FOB_CLOCKS_WATCHES_HS91` |

### `trade_flow` Vocabulary

| Value | Valuation | Description |
|-------|-----------|-------------|
| `"Export"` | FOB (Free on Board) | US goods leaving US customs territory |
| `"Import"` | Customs value | Foreign goods entering US customs territory |

---

## Point-in-Time (PIT) Fields

| Field | Type | Description |
|-------|------|-------------|
| `data_timestamp` | STRING (ISO 8601) | First moment of the reference trade month |
| `reporting_date` | STRING (ISO 8601) | Same as `data_timestamp` |
| `official_release_date` | STRING (ISO 8601) | Census FT-900 scheduled release date |
| `published_date` | STRING (ISO 8601) | Actual publication date (~5 weeks post reference month) |
| `as_of_date` | STRING (ISO 8601) | Ingestion timestamp |
| `conversion_timestamp` | STRING (ISO 8601) | Vault conversion timestamp |
| `revision_number` | INTEGER | `0` = initial; incremented per Census revision |
| `superseded_by` | STRING | `record_id` of newer version; `"N/A"` if current |

**PIT Guarantee**: `published_date >= data_timestamp` for every record.

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `record_id` | UTF8 | UUID v4; unique; not null |
| `observed_value` | DOUBLE | >= 0 (zero valid for zero-trade months) |
| `commodity_code` | UTF8 | 2-digit string `"01"` to `"99"` |
| `trade_flow` | UTF8 | `"Export"` or `"Import"` |
| `unit_of_measure` | UTF8 | Always `"USD_MILLIONS"` |
| `currency` | UTF8 | Always `"USD"` |
| `confidence_tier` | UTF8 | `"PRIMARY"` |
| `data_quality_certified` | BOOLEAN | `true` for production records |

---

## Sample Field Values

**Sample period**: January–March 2022 | 585 records

| Field | Example Values |
|-------|----------------|
| `commodity_code` | `"91"` (Clocks), `"84"` (Machinery), `"27"` (Mineral Fuels) |
| `commodity_name` | `"Clocks and Watches"`, `"Nuclear Reactors and Machinery"` |
| `trade_flow` | `"Export"`, `"Import"` |
| `observed_value` | `155.05` (USD millions, HS91 Exports, Jan 2022) |
| `partner_country_name` | `"WORLD"`, `"China"`, `"Canada"`, `"Mexico"` |
| `data_timestamp` | `2022-01-01T00:00:00Z` through `2022-03-01T00:00:00Z` |
| `published_date` | `2022-03-08T13:30:00Z` (FT-900 release for Jan 2022) |
| `revision_number` | `0` |

---

## Data Sources

| Source | Agency | Publication | Frequency | Coverage |
|--------|---------|------------|-----------|----------|
| FT-900 Monthly US Trade in Goods | US Census Bureau | FT-900 press release | Monthly | 2010–2026 |

---

## Coverage and Granularity

- **Geography**: US (reporter); World aggregate + 80+ partner countries
- **Temporal**: 2010–2026, monthly
- **Commodity granularity**: HS-2 (99 chapters)
- **Trade flows**: Export (FOB) + Import (customs value)
- **Vault partition**: `year={yyyy}/month={m}` (Hive)
- **Record count**: ~38,122

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
