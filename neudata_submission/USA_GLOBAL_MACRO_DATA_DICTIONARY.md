# USA Global Macro Data Dictionary
## Schema Version 2.0 (Point-in-Time Enabled)

**Product**: Global Macro Baseline
**Coverage**: United States + IMF Global Forecasts (1913–2031)
**Sources**: ALFRED Vintage Feed (St. Louis Fed) · IMF World Economic Outlook (WEO) DataMapper API
**Vault Records**: 81,735 validated records (union schema)
**Source Breakdown**: ALFRED multi-vintage — 10 series | IMF WEO — 8 series | Total: 18 series
**PIT Type**: FULL VINTAGE (ALFRED, avg 8.80 revisions/series) · RELEASE_DATE_ONLY (IMF WEO)
**Sample**: 2015–2017 · 3-Year Sample (24 rows, 23 columns — IMF WEO component)
**Sample File**: `sample_parquet_global_macro/global_macro_imf_weo_v1.0_sample.parquet`
**Update Frequency**: Monthly (ALFRED) · Bi-annual April/October (IMF WEO)
**Last Updated**: June 2026
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN, AUS, NOR = 32 countries

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Series Reference — ALFRED Component](#alfred-series-reference)
3. [Series Reference — IMF WEO Component](#imf-series-reference)
4. [Union Schema (v2.0 — 29 columns)](#union-schema)
5. [Point-in-Time (PIT) Fields](#point-in-time-pit-fields)
6. [Data Types and Constraints](#data-types-and-constraints)
7. [Sample Field Values (IMF WEO, 2015–2017)](#sample-field-values)
8. [Data Sources](#data-sources)
9. [Coverage and Granularity](#coverage-and-granularity)
10. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Schema v2.0 expands the original IMF-only Global Macro product (4,488 rows, schema v1.0) into a
multi-source, multi-vintage archive combining ALFRED Federal Reserve revision history with IMF WEO
projections. The unified vault now covers 81,735 records across 18 series, spanning 1913–2031.

**v1.0 → v2.0 changes:**
- Added ALFRED multi-vintage source (10 series, avg 8.80 revisions each, 1913–2026)
- Expanded schema from 23 to 29 columns (union of ALFRED + IMF column sets)
- IMF component retained unchanged (8 series, 1980–2031 including forecasts)
- Total records: 4,488 → 81,735

The sample parquet (`global_macro_imf_weo_v1.0_sample.parquet`) covers the IMF WEO component only
(24 rows, 23 columns) as the ALFRED component was not present when the sample was generated.

---

## ALFRED Series Reference

10 macro series with full revision history sourced from the Federal Reserve Bank of St. Louis
ALFRED (Archival Federal Reserve Economic Data) API.

| Series ID | Description | Start | Avg Revisions |
|-----------|-------------|-------|---------------|
| `GDPC1` | Real Gross Domestic Product (SAAR, Billions 2017 USD) | 1947-Q1 | 8.80 |
| `CPIAUCSL` | Consumer Price Index for All Urban Consumers | 1913-01 | 8.80 |
| `UNRATE` | Civilian Unemployment Rate (%) | 1948-01 | 8.80 |
| `PAYEMS` | Total Nonfarm Payrolls (Thousands) | 1939-01 | 8.80 |
| `FEDFUNDS` | Effective Federal Funds Rate (%) | 1954-07 | 8.80 |
| `DGS10` | 10-Year Treasury Constant Maturity Rate | 1962-01 | 8.80 |
| `M2SL` | M2 Money Stock (Billions USD) | 1959-01 | 8.80 |
| `INDPRO` | Industrial Production Index | 1919-01 | 8.80 |
| `HOUST` | Housing Starts (Thousands, SAAR) | 1959-01 | 8.80 |
| `BOPGSTB` | International Trade Balance — Goods and Services | 1992-01 | 8.80 |

**ALFRED PIT characteristics:**
- Revision count per (series, obs_date): average 8.80, maximum 42
- `revision_number = 1` = preliminary release (advance estimate)
- `revision_number = max` = final revised figure

---

## IMF WEO Series Reference

8 macro indicators sourced from the IMF World Economic Outlook DataMapper API. Data includes
historical actuals and IMF projections through 2031.

| Series ID | Description | Unit | Start |
|-----------|-------------|------|-------|
| `NGDPDPC` | GDP Per Capita, Current Prices | USD | 1980 |
| `NGDP_RPCH` | Real GDP Growth Rate | % change | 1980 |
| `PCPIPCH` | Inflation Rate (CPI % change) | % change | 1980 |
| `LUR` | Unemployment Rate | % of labor force | 1980 |
| `BCA_NGDPD` | Current Account Balance (% of GDP) | % GDP | 1980 |
| `GGXWDG_NGDP` | General Government Gross Debt (% GDP) | % GDP | 1990 |
| `NID_NGDP` | Total Investment (% GDP) | % GDP | 1980 |
| `PPPGDP` | GDP at PPP (International USD) | Billions | 1980 |

---

## Union Schema (v2.0 — 29 columns)

The v2.0 schema is the union of ALFRED and IMF WEO column sets. Columns present in one source
but not the other are null-filled.

| Column | Type | Present In | Description |
|--------|------|-----------|-------------|
| `record_id` | str | Both | Globally unique UUID record identifier |
| `product` | str | Both | `global_macro` |
| `country_code` | str | Both | ISO 3166-1 alpha-2 (`US`) |
| `iso_alpha3` | str | Both | ISO 3166-1 alpha-3 (`USA`) |
| `source` | str | Both | `alfred_vintage` or `imf_weo` |
| `source_agency` | str | Both | `Federal Reserve Bank of St. Louis` / `IMF` |
| `source_sub_category` | str | Both | Sub-classification (e.g., `ALFRED Vintage`, `WEO April 2026`) |
| `sovereign_series_id` | str | Both | Lekwankwa canonical series ID |
| `macro_metric_name` | str | Both | Human-readable metric label |
| `observed_value` | float | Both | Numeric observation value |
| `unit_of_measure` | str | Both | Unit (%, billions USD, thousands, index) |
| `data_timestamp` | datetime | Both | Observation period (first day of reference month/quarter/year) |
| `published_date` | date | Both | Official publication date (PIT gate) |
| `data_vintage_id` | str | Both | Unique vintage identifier per release snapshot |
| `extraction_method` | str | Both | `api_pull` |
| `confidence_tier` | str | Both | `PRIMARY` for all validated records |
| `market_tier` | str | Both | `SOVEREIGN` |
| `portal_url` | str | Both | Source API portal URL |
| `revision_number` | int | Both | Revision sequence (ALFRED: 1–42; IMF: 1 hardcoded) |
| `is_forecast` | bool | IMF only | True for projections beyond latest WEO release date |
| `sdmx_frequency` | str | Both | `M` (monthly), `Q` (quarterly), `A` (annual) |
| `data_quality_certified` | bool | Both | True if passed 9-stage validation |
| `processing_timestamp` | datetime | Both | Pipeline processing timestamp |
| `official_release_date` | date | ALFRED | Official ALFRED release date |
| `is_revised_figure` | bool | ALFRED | True if revision_number > 1 |
| `seasonal_adjustment` | str | ALFRED | SA / NSA / SAAR |
| `weo_vintage` | str | IMF | WEO edition (e.g., `WEO2026_APR`) |
| `weo_subject_code` | str | IMF | IMF WEO subject code (e.g., `NGDP_RPCH`) |
| `forecast_horizon_years` | int | IMF | Years ahead from WEO publication date |

---

## Point-in-Time (PIT) Fields

| Field | ALFRED Role | IMF WEO Role |
|-------|------------|-------------|
| `published_date` | ALFRED release date (PIT gate) | WEO publication date (Apr/Oct) |
| `revision_number` | 1 = advance; max = final | Always 1 (single snapshot per WEO edition) |
| `data_vintage_id` | Unique per ALFRED revision | Unique per WEO edition |
| `is_forecast` | n/a | True for obs_year > WEO publication year |
| `is_revised_figure` | True if rev_num > 1 | False |

**ALFRED PIT query (preliminary release only):**
```python
pit_view = df[
    (df["source"] == "alfred_vintage") &
    (df["published_date"] <= simulated_date) &
    (df["revision_number"] == 1)
]
```

---

## Data Types and Constraints

| Column | Expected Range / Domain |
|--------|------------------------|
| `observed_value` | Numeric; null only for projected IMF cells not yet published |
| `revision_number` | ≥ 1 integer (ALFRED: up to 42; IMF: always 1) |
| `published_date` | Must be ≥ `data_timestamp` (PIT constraint) |
| `data_quality_certified` | boolean; True for all validated records |
| `is_forecast` | boolean; only meaningful for IMF WEO component |
| `sdmx_frequency` | `M`, `Q`, or `A` |

---

## Sample Field Values (IMF WEO, 2015–2017)

The sample file covers the IMF WEO component only (24 rows = 8 series × 3 years).

```
record_id              : lkw-macro-imf-USA-NGDP_RPCH-2015
product                : global_macro
country_code           : US
iso_alpha3             : USA
source                 : imf_weo
source_agency          : IMF
sovereign_series_id    : NGDP_RPCH_USA_IMF
macro_metric_name      : Real GDP Growth Rate
observed_value         : 2.9
unit_of_measure        : % change
data_timestamp         : 2015-01-01
published_date         : 2015-04-14
is_forecast            : False
revision_number        : 1
data_quality_certified : True
```

---

## Data Sources

### ALFRED — Archival Federal Reserve Economic Data
- **Provider**: Federal Reserve Bank of St. Louis
- **API**: ALFRED vintage API (`api.stlouisfed.org/fred/series/vintage_dates`)
- **Series**: 10 key US macro series (GDP, CPI, unemployment, payrolls, rates, money supply, etc.)
- **Vintage depth**: Average 8.80 revisions per (series, obs_date); maximum 42 revisions
- **Date range**: 1913–2026 depending on series
- **PIT type**: FULL VINTAGE (complete revision history)

### IMF World Economic Outlook (WEO)
- **Provider**: International Monetary Fund
- **API**: IMF DataMapper API (`imf.org/external/datamapper/api/v1`)
- **Series**: 8 standard WEO indicators per country
- **Release cadence**: April and October each year
- **Date range**: 1980–2031 (historical + projections)
- **PIT type**: RELEASE_DATE_ONLY (single snapshot per WEO edition)

---

## Coverage and Granularity

| Dimension | ALFRED | IMF WEO | Combined |
|-----------|--------|---------|---------|
| Date range | 1913–2026 | 1980–2031 | 1913–2031 |
| Frequency | Monthly/Quarterly | Annual | Mixed |
| Series count | 10 | 8 | 18 |
| Countries (USA vault) | USA | USA | USA |
| Countries (global vault) | USA only | 32 countries | 32 countries |
| Vault records (USA) | ~76,000 | ~5,735 | 81,735 |

---

## Quality Metrics

| Metric | Value |
|--------|-------|
| Total validated records | 81,735 |
| Null rate (observed_value) | 0.00% (actuals); IMF forecasts may be null for future periods |
| Null rate (published_date) | 0.00% |
| PIT violations | 0 |
| Avg revisions per (series, obs_date) | 8.80 (ALFRED) / 1.00 (IMF) |
| Max revisions (ALFRED) | 42 |
| GX expectations passed | 128 / 128 (100%) |
| Overall validation | 9 / 9 stages PASS |
| Schema standard | SDMX 2.1 aligned |
| Delivery format | Apache Parquet (flat schema, no nesting) |
