# USA Global Macro Data Dictionary
## Schema Version 5.0 (Point-in-Time Enabled)

**Product**: Global Macro Baseline
**Coverage**: United States + IMF Global Forecasts (1913–2031)
**Sources**: ALFRED Vintage Feed (St. Louis Fed) · IMF World Economic Outlook (WEO) DataMapper API
**Vault Records**: 78,743 validated records (union schema)
**Source Breakdown**: ALFRED multi-vintage — 10 series | IMF WEO — 8 series | Total: 18 series
**PIT Type**: FULL VINTAGE (ALFRED, avg 8.80 revisions/series) · QUAD_VINTAGE (IMF WEO: January Update + April Final + July Update + October Preliminary)
**Sample**: 2015–2017 · 3-Year Sample (24 rows, 26 columns — IMF WEO April vintages only)
**Sample File**: `sample_parquet_global_macro/global_macro_imf_weo_v1.0_sample.parquet`
**Update Frequency**: Monthly (ALFRED) · Quarterly January/April/July/October (IMF WEO)
**Last Updated**: June 2026
**Vault Coverage (Global)**: USA + 27 EU Member States + GBR, CAN = 30 countries

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Series Reference — ALFRED Component](#alfred-series-reference)
3. [Series Reference — IMF WEO Component](#imf-series-reference)
4. [Union Schema (v2.0 — 26 columns)](#union-schema)
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
projections. The unified vault now covers 78,743 records across 18 series, spanning 1913–2031.

**v1.0 → v2.0 changes:**
- Added ALFRED multi-vintage source (10 series, avg 8.80 revisions each, 1913–2026)
- Expanded schema from 23 to 26 columns (union of ALFRED + IMF column sets)
- IMF component upgraded to quad-vintage (January Update + April Final + July Update + October Preliminary; 1,496 records, 8 series, 1980–2031 including forecasts)
- Total records: 4,488 → 78,743

The sample parquet (`global_macro_imf_weo_v1.0_sample.parquet`) covers the IMF WEO component only
(24 rows = 8 series × 3 years, April vintages only) as the ALFRED component was not present when the
sample was generated. Full production delivery includes all four vintages (1,496 IMF records).

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
| `NGDPD` | GDP, Current Prices | USD Billions | 1980 |
| `NGDP_RPCH` | Real GDP Growth Rate | % change | 1980 |
| `PCPIPCH` | Inflation Rate (CPI % change) | % change | 1980 |
| `LUR` | Unemployment Rate | % of labor force | 1980 |
| `BCA_NGDPD` | Current Account Balance (% of GDP) | % GDP | 1980 |
| `GGXWDG_NGDP` | General Government Gross Debt (% GDP) | % GDP | 2001 |
| `GGXCNL_NGDP` | Government Net Lending/Borrowing (% GDP) | % GDP | 2001 |
| `PPPGDP` | GDP at PPP (International USD) | Billions | 1980 |

**`BCA_NGDPD` and `GGXCNL_NGDP` are net/balance measures and are frequently negative** — `BCA_NGDPD` (current account balance) is negative whenever the US runs a current account deficit; `GGXCNL_NGDP` (government net lending/borrowing) is negative whenever the US runs a fiscal deficit. Both are negative for essentially the entire USA history in this vault (e.g. the 2015–2017 sample shows `BCA_NGDPD` -2.3 to -2.4 and `GGXCNL_NGDP` -3.2 to -3.6). **Do not treat negative values in these two series as data errors** — see `configs/known_anomalies.json` entry `BALANCE_METRICS_LEGITIMATELY_NEGATIVE` for the full explanation and other affected series across products.

---

## Union Schema (v2.0 — 26 columns)

The v2.0 schema is the union of ALFRED and IMF WEO column sets. Columns present in one source
but not the other are null-filled. All 26 columns are present in both ALFRED and IMF WEO partitions.

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
| `is_forecast` | bool | Both | True for IMF projections (year ≥ 2025); False for all ALFRED records |
| `sdmx_frequency` | str | Both | `M` (monthly), `Q` (quarterly), `A` (annual) |
| `data_quality_certified` | bool | Both | True if passed 9-stage validation |
| `processing_timestamp` | datetime | Both | Pipeline processing timestamp |
| `official_release_date` | date | Both | Official publication date (ALFRED release date; IMF: January YYYY+1-01-01, April YYYY+1-04-01, July YYYY-07-01, October YYYY-10-01) |
| `is_revised_figure` | bool | Both | True if revision_number > 1; always False for IMF records |
| `seasonal_adjustment` | str | ALFRED | SA / NSA / SAAR; null-filled for IMF records |
| `as_of_date` | date | Both | Knowledge cutoff for this revision snapshot; use `published_date` as PIT gate, not this field |

---

## Point-in-Time (PIT) Fields

| Field | ALFRED Role | IMF WEO Role |
|-------|------------|-------------|
| `published_date` | ALFRED release date (PIT gate) | WEO publication date (January YYYY+1, April YYYY+1, July YYYY, or October YYYY) |
| `official_release_date` | ALFRED official release date | January: `YYYY+1-01-01`; April: `YYYY+1-04-01`; July: `YYYY-07-01`; October: `YYYY-10-01` — use this as PIT gate |
| `revision_number` | 1 = advance; max = final | Always 1 (single snapshot per WEO edition) |
| `data_vintage_id` | Unique per ALFRED revision | `IMF-{IND}-USA-{YYYY}-Jan-v1` / `IMF-{IND}-USA-{YYYY}-v1` (April) / `IMF-{IND}-USA-{YYYY}-Jul-v1` / `IMF-{IND}-USA-{YYYY}-Oct-v1` |
| `is_forecast` | always False | True for years ≥ 2025 |
| `is_revised_figure` | True if rev_num > 1 | Always False |

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
- **Release cadence**: Quarterly — January Update (`official_release_date = YYYY+1-01-01`), April Final (`YYYY+1-04-01`), July Update (`YYYY-07-01`), October Preliminary (`YYYY-10-01`)
- **Date range**: 1980–2031 (historical + projections; years ≥ 2025 are IMF forecasts, `is_forecast=True`)
- **PIT type**: QUAD_VINTAGE — four vintages per observation year; backtest readers should filter on `official_release_date <= simulation_date` and take the most-recently published vintage per observation year. Months between publications use the most recently available vintage via forward-fill. PIT signal order per observation year: July → October → January (YYYY+1) → April (YYYY+1).
- **Vault partitions**: `year=YYYY/month=01/` (January Update), `year=YYYY/month=04/` (April Final), `year=YYYY/month=07/` (July Update), `year=YYYY/month=10/` (October Preliminary)
- **⚠ Synthetic PIT dates (January and July)**: The IMF DataMapper API exposes only current-vintage values — it does not archive historical snapshots from past publications. The January Update and July Update vintages in this vault are therefore stamped with their respective `official_release_date` values (e.g. `YYYY+1-01-01`, `YYYY-07-01`) but the underlying observed values are drawn from the current API response, not a preserved snapshot of that publication. Revision deltas between adjacent WEO editions are typically < 0.1pp for GDP growth. Clients running granular revision-delta analysis between January/July and April/October vintages will observe near-zero deltas for those pairs.

---

## Known Data Gaps

| Gap ID | Affected Component | Period | Reason | Client Action |
|--------|-------------------|--------|--------|---------------|
| `IMF_SYNTHETIC_JAN_JUL_VINTAGES` | IMF WEO `month=01` (January Update) and `month=07` (July Update) partitions | All years 1980–2031 | DataMapper API returns current-vintage values only; no historical snapshot archive exists. January and July `official_release_date` values are genuine but the data values are synthetic backdates. | Use April and October vintages for revision-delta analysis. January and July vintages are valid for PIT ordering (information availability gate) but not for measuring revision depth across WEO editions. |
| `BLS_2025_APPROPRIATIONS_LAPSE` | ALFRED series `UNRATE` (`LNS14000000`) and `PAYEMS` — note: these map to CPS/BLS, not ALFRED vintage | October 2025 only | US government appropriations lapse; BLS Employment Situation not published. ALFRED vintage series (`GDPC1`, `CPIAUCSL`, `FEDFUNDS`, `DGS10`, `M2SL`, `INDPRO`, `HOUST`, `BOPGSTB`, `CES0500000003`) are **not affected** — all confirmed present with real October 2025 values. | ALFRED multi-vintage component: no action needed. If combining with BLS CPS data from another product, treat CPS unemployment for October 2025 as `NaN`. |
| `IMF_WEO_FORECAST_ANNUAL_CADENCE` | IMF WEO component, forecast years 2027 and later | 2027–2031 | IMF WEO publishes one annual observation per forecast year beyond the ALFRED historical window, not one per month. A naive monthly-continuity check will report missing months for the other 11 months of each forecast year — this is expected, not a gap. | Filter on `source = "imf_weo"` and expect annual cadence for forecast years; do not run monthly gap-continuity checks against this window without accounting for source-specific frequency. See `configs/known_anomalies.json` entry `IMF_WEO_FORECAST_ANNUAL_CADENCE`. |

---

## Coverage and Granularity

| Dimension | ALFRED | IMF WEO | Combined |
|-----------|--------|---------|---------|
| Date range | 1913–2026 | 1980–2031 | 1913–2031 |
| Frequency | Monthly/Quarterly | Quarterly (observation: Annual) | Mixed |
| Series count | 10 | 8 | 18 |
| Countries (USA vault) | USA | USA | USA |
| Countries (global vault) | USA only | 30 countries | 30 countries |
| Vault records (USA) | 77,247 | 1,496 (374 × 4 vintages) | 78,743 |

---

## Quality Metrics

| Metric | Value |
|--------|-------|
| Total validated records | 78,743 |
| Null rate (observed_value) | 0.00% (actuals); IMF forecasts may be null for future periods |
| Null rate (published_date) | 0.00% |
| PIT violations | 0 |
| Avg revisions per (series, obs_date) | 8.80 (ALFRED) / 4.00 (IMF — 1 per January/April/July/October vintage) |
| Max revisions (ALFRED) | 42 |
| GX expectations passed | 128 / 128 (100%) |
| Overall validation | 9 / 9 stages PASS |
| Schema standard | SDMX 2.1 aligned |
| Delivery format | Apache Parquet (flat schema, no nesting) |

---

## Provenance Fields — Pipeline Bookkeeping

`data_quality_certified` is a universal vault field present on all records across all 5 products and all 30 countries (100% of data partitions). `conversion_timestamp` is a USA-only pipeline ingestion artifact, present only in the food_micropricing/USA and wages_and_employment/USA vault partitions (2 of 160 total); it is absent from all EU27 and non-EU country partitions and from all USA housing, trade, and global_macro partitions. Both fields are pipeline bookkeeping metadata — **not** PIT events or publication metadata.

### `data_quality_certified` (boolean)

| Attribute | Value |
|-----------|-------|
| Type | boolean |
| Nullable | No |
| Coverage | All 5 products · All 30 countries · 100% of vault data partitions. Value = True for all countries across all 5 products. USA food_micropricing and USA wages_and_employment were corrected to True June 2026 (scraper-placeholder False; confirmed 9/9 validation PASS for both). |
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
