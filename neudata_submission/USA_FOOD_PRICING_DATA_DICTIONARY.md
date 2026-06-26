# USA Food Micropricing Data Dictionary
## Schema Version 5.0 (Point-in-Time Enabled)

**Product**: USA Food Micropricing
**Coverage**: United States (1980–2026)
**Sources**: USDA Economic Research Service (ERS), Bureau of Labor Statistics (BLS), ALFRED Vintage Feed
**Vault Records**: 29,825 validated records
**Source Breakdown**: usda_ers — 18,629 | bls — 10,645 | alfred_vintage — 551
**Sample**: 2015–2017 · 3-Year Sample (576 records, 22 columns)
**Sample File**: `sample_parquet_food_pricing/food_prices_v4.0_sample.parquet`
**Update Frequency**: Monthly
**Last Updated**: June 2026
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN, AUS, NOR = 32 countries total

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Core Data Fields](#core-data-fields)
3. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
4. [Schema v4.0 → v5.0 Field Renames](#schema-renames)
5. [Data Types and Constraints](#data-types-and-constraints)
6. [Sample Field Values (2015–2017 Preview)](#sample-field-values)
7. [Data Sources](#data-sources)
8. [Coverage and Granularity](#coverage-and-granularity)
9. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US food micropricing data with full Point-in-Time (PIT) capability, enabling
accurate backtesting without look-ahead bias. All records carry actual BLS/USDA publication
timestamps and ALFRED vintage revision history. The 29,825-row USA vault is one component of a
32-country global food pricing archive covering EU27, GBR, CAN, AUS, and NOR via Eurostat SDMX
and national statistical offices.

**Key differentiators:**
- ALFRED multi-vintage: 551 food price observations with full revision history
- 46 vault columns (v5.0 schema); 22 sample columns (v4.0 sample parquet)
- Three independent source pipelines with unified schema
- Covers 40+ individual food items, 6 product categories

---

## Core Data Fields

### Sample Schema (v4.0 — 22 columns, as-is in sample parquet)

| Column | Type | Description |
|--------|------|-------------|
| `country_code` | str | ISO 3166-1 alpha-2 country code (e.g., `US`) |
| `item_name` | str | Food item label (e.g., `Eggs, grade A, large, per doz.`) |
| `item_description` | str | Extended description including unit specification |
| `item_code` | str | BLS/USDA internal item code |
| `category` | str | Product category (Dairy, Meat, Cereals, Fruit, Vegetables, Beverages) |
| `item_value` | float | Price in local currency (USD for US records) |
| `unit` | str | Unit of measure (per lb, per doz, per gallon, etc.) |
| `currency` | str | ISO 4217 currency code (`USD`) |
| `usd_equivalent` | float | USD-converted price (same as item_value for US records) |
| `pct_change_mom` | float | Month-over-month percentage change in price |
| `data_quality_certified` | bool | True if record passed all 9-stage validation |
| `data_timestamp` | datetime | ISO 8601 observation period (first day of reference month) |
| `conversion_timestamp` | datetime | Timestamp of USD conversion calculation |
| `source` | str | Source code: `bls`, `usda_ers`, or `alfred_vintage` |
| `source_series_id` | str | Source API series identifier |
| `extraction_method` | str | Ingestion method: `api_pull` or `bulk_download` |
| `source_url` | str | Source portal URL (v4.0 name; renamed to `portal_url` in v5.0) |
| `record_id` | str | Globally unique record identifier (UUID format) |
| `published_date` | date | Actual government publication date (PIT timestamp) |
| `as_of_date` | date | Snapshot as-of date for this record version |
| `revision_number` | int | Revision sequence number (1 = first release) |
| `superseded_by` | str | Record ID of the superseding revision, or null |

---

## Point-in-Time (PIT) Fields

| Field | Description | PIT Role |
|-------|-------------|----------|
| `published_date` | Actual BLS/USDA publication date | Primary PIT gate: no data visible before this date |
| `as_of_date` | Snapshot date of this revision | Secondary PIT anchor for revision-aware queries |
| `revision_number` | Revision sequence (1 = first release) | Filter to preliminary (`= 1`) or final (`= max`) |
| `superseded_by` | ID of the record that replaced this one | Identify latest revision per observation |
| `data_timestamp` | Observation month (first day) | Time-series index for backtests |

**Correct PIT query pattern:**
```python
# Get the data as it would have appeared on simulated_date
pit_view = df[df["published_date"] <= simulated_date]
latest = (
    pit_view.sort_values("revision_number")
    .drop_duplicates(["item_code", "data_timestamp"], keep="last")
)
```

---

## Schema v4.0 → v5.0 Field Renames

The vault was upgraded to schema v5.0 to align with the Golden Record Schema used across all
32 countries. The sample parquet shipped here retains v4.0 column names. The full vault parquets
use v5.0 names.

| v4.0 Column (sample) | v5.0 Column (vault) | Notes |
|---------------------|--------------------|----|
| `item_name` | `standard_name` / `macro_metric_name` | Unified with EU27/non-EU schema |
| `item_value` | `observed_price_local` | Explicit local-currency naming |
| `usd_equivalent` | `price_usd_equivalent` | Explicit USD naming |
| `source_url` | `portal_url` | Consistent with all other vault products |

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `item_value` | > 0 (price cannot be negative) |
| `revision_number` | ≥ 1 integer |
| `pct_change_mom` | −50% to +50% (outliers flagged and documented) |
| `published_date` | Must be ≥ `data_timestamp` (PIT constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `source` | One of: `bls`, `usda_ers`, `alfred_vintage` |
| `country_code` | ISO 3166-1 alpha-2 (`US` for USA records) |

---

## Sample Field Values (2015–2017 Preview)

The sample parquet covers January 2015 to December 2017 (36 months). It contains 576 records
across food items tracked during this period.

```
country_code       : US
item_name          : Eggs, grade A, large, per doz. (example)
category           : Dairy & Eggs
item_value         : 2.18
unit               : per doz.
currency           : USD
usd_equivalent     : 2.18
pct_change_mom     : -1.8%
data_timestamp     : 2015-01-01
published_date     : 2015-02-24
revision_number    : 1
source             : bls
data_quality_certified: True
```

---

## Data Sources

### Bureau of Labor Statistics (BLS) — CPI Average Retail Prices
- **Series coverage**: 50+ food items including meats, dairy, cereals, beverages, fruits, vegetables
- **Method**: BLS CPI API (JSON v2), series prefix `APU`
- **Frequency**: Monthly
- **Vault rows**: 10,645

### USDA Economic Research Service (ERS) — Food Price Outlook
- **Series coverage**: Commodity-level prices from ERS retail scanner data and survey
- **Method**: USDA ERS bulk download (CSV)
- **Frequency**: Monthly
- **Vault rows**: 18,629

### ALFRED Vintage Feed (St. Louis Fed)
- **Series coverage**: Key food commodity CPI series with full revision history
- **Method**: ALFRED API vintage snapshots
- **Average revisions per series**: 8.80 (max: 42)
- **Vault rows**: 551

---

## Coverage and Granularity

| Dimension | USA Coverage | Global Vault |
|-----------|-------------|-------------|
| Date range | 1980–2026 (monthly) | 2005–2026 (EU27), 2000–2026 (GBR/CAN/AUS/NOR) |
| Frequency | Monthly | Monthly |
| Food items | 40+ BLS categories; 80+ USDA commodities | 5 core categories (HICP-aligned) |
| PIT type | FULL VINTAGE (ALFRED) + RELEASE_DATE_ONLY (BLS/USDA) | RELEASE_DATE_ONLY |
| Countries | 1 (USA) | 32 total |
| Vault records | 29,825 | ~220,000+ combined |

---

## Known Data Gaps

| Gap ID | Affected Series | Period | Reason | Client Action |
|--------|----------------|--------|--------|---------------|
| `BLS_2025_APPROPRIATIONS_LAPSE` | All 11 BLS food price series (`APU0000701111`, `APU0000702111`, `APU0000702421`, `APU0000703112`, `APU0000706111`, `APU0000708111`, `APU0000709112`, `APU0000711412`, `APU0000717311`, `APU0000720111`, `APU0000720311`) | October 2025 only | US government appropriations lapse prevented BLS from collecting or publishing data. BLS does not retroactively backfill lapse-period data. November 2025 confirmed normal resumption. | Treat as `NaN` for October 2025. Do not flag as scraper error. Forward-fill from September 2025 for that single month if your model requires gap-free series. |

---

## Quality Metrics

| Metric | Value |
|--------|-------|
| Total validated records | 29,825 |
| Null rate (item_value) | 0.00% |
| Null rate (published_date) | 0.00% |
| PIT violations | 0 |
| Outliers flagged | Documented in `reports/food_micropricing/` |
| Outliers suppressed | 0 |
| GX expectations passed | 128 / 128 (100%) |
| Overall validation | 9 / 9 stages PASS |
| Schema standard | SDMX 2.1 aligned |
| Delivery format | Apache Parquet (flat schema, no nesting) |

---

## Provenance Fields — Pipeline Bookkeeping

`data_quality_certified` is a universal vault field present on all records across all 5 products and all 32 countries (100% of data partitions). `conversion_timestamp` is a USA-only pipeline ingestion artifact, present only in the food_micropricing/USA and wages_and_employment/USA vault partitions (2 of 160 total); it is absent from all EU27 and non-EU country partitions and from all USA housing, trade, and global_macro partitions. Both fields are pipeline bookkeeping metadata — **not** PIT events or publication metadata.

### `data_quality_certified` (boolean)

| Attribute | Value |
|-----------|-------|
| Type | boolean |
| Nullable | No |
| Coverage | All 5 products · All 32 countries · 100% of vault data partitions. Value = True for all countries across all 5 products. USA food_micropricing and USA wages_and_employment were corrected to True June 2026 (scraper-placeholder False; confirmed 9/9 validation PASS for both). |
| True | Record passed all 9 automated validation stages |
| False | Record carries one or more quality flags (retained, not suppressed; documented in validation reports) |

**Backtesting note**: It is safe to include `False` records in backtests if the accompanying outlier/sanity reports confirm the flag is a boundary annotation, not a data error. Review the validation reports shipped with the archive before discarding any `False` records.

**Sample file note**: Wages (CES/CPS) and Housing sample records may show `False` due to a pending schema v2.0 recertification sweep. Full production vault records reflect the final certified state. Food, Trade, and Global Macro samples show `True`.

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

**Known inconsistency**: In the Wages, Housing, and Trade samples, `as_of_date` was set to the pipeline run date (`2026-06-19T10:00:00Z`) rather than the original publication date. This is documented in the per-product changelogs. Always use `published_date` or `official_release_date` as the PIT gate for backtesting — never `conversion_timestamp` or `as_of_date`.
