# USA Electricity Data Dictionary
## Schema Version 1.0 (Point-in-Time Enabled)

**Product**: USA Electricity
**Dataset Name**: `electricity`
**Coverage**: United States (2001–2026)
**Source**: U.S. Energy Information Administration (EIA) API v2
**Approximate Records**: ~950,000 validated generation records
**Sample**: January–March 2022 (10,884 generation records)
**Update Frequency**: Monthly
**Last Updated**: June 14, 2026
**Sample File**: `sample_parquet_electricity/electricity_generation_v1.0_sample.parquet`

---

## Table of Contents
1. [Dataset Overview](#dataset-overview)
2. [Generation Fields](#generation-fields)
3. [Fuel Type Vocabulary](#fuel-type-vocabulary)
4. [Data Types and Constraints](#data-types-and-constraints)
5. [Sample Field Values (Jan–Mar 2022)](#sample-field-values)
6. [Data Source](#data-source)
7. [Coverage and Granularity](#coverage-and-granularity)
8. [Quality Metrics](#quality-metrics)

---

## Dataset Overview

Institutional-grade US electricity generation data covering production by fuel type and utility
sector across all 50 US states plus 12 territories. Sourced exclusively from EIA's official
API v2 (`/electricity/electric-power-operational-data/`).

**Key Features**:
- 25+ years of historical coverage (2001–2026)
- 9/9 validation stages passing
- Complete state coverage: 62 jurisdictions (50 states + DC + 11 territories)
- Full fuel-type breakdown: coal, natural gas, nuclear, hydro, wind, solar, petroleum
- Hive-partitioned: `product=electricity/country=USA/source=eia/year={yyyy}/month={m}/`
- PIT-enabled: `official_release_date` and `conversion_timestamp` tracked per record

---

## Generation Fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `data_timestamp` | TIMESTAMP | No | First moment of the reporting month (e.g., `2022-01-01`) |
| `state_code` | STRING | No | EIA state or census-division code (e.g., `"CA"`, `"TX"`, `"90"`) |
| `state_name` | STRING | No | Full state name or census division (e.g., `"California"`, `"Pacific"`) |
| `sectorid` | INTEGER | No | EIA generator sector (1 = Electric Utility, 2 = IPP, 3 = CHP, 99 = All) |
| `sector_name` | STRING | No | Sector description |
| `fueltypeid` | STRING | No | EIA fuel type code (see vocabulary below) |
| `energy_source_type` | STRING | No | Human-readable fuel description (e.g., `"natural gas"`, `"wind"`) |
| `generation_output_mwh` | DOUBLE | No | Electricity generated in thousand megawatt-hours |
| `generation_units` | STRING | No | Always `"thousand megawatthours"` |
| `observation_period` | STRING | No | Reporting month in `YYYY-MM` format |
| `sovereign_series_id` | STRING | No | Composite key: `EIA-ELEC-GEN-{state}-S{sector}-{fuel}` |
| `data_vintage_id` | STRING | No | `EIA-{sovereign_series_id}-{yyyy}-{mm}-v{n}` |
| `dataset` | STRING | No | Always `"generation"` |
| `dataset_type` | STRING | No | Always `"generation"` |
| `source` | STRING | No | Always `"eia"` |
| `source_agency` | STRING | No | Always `"EIA"` |
| `portal_url` | STRING | No | `"https://www.eia.gov/electricity/data.cfm"` |
| `iso_alpha3` | STRING | No | Always `"USA"` |
| `market_tier` | STRING | No | Always `"Developed"` |
| `confidence_tier` | STRING | No | `"PRIMARY"` for official EIA data |
| `is_revised_figure` | BOOLEAN | No | `true` if EIA issued a revision for this month |
| `official_release_date` | STRING | Yes | EIA scheduled release date (approximately 2 months after reference month) |
| `data_quality_certified` | BOOLEAN | No | `true` when all 9 validation checks pass |
| `conversion_timestamp` | TIMESTAMP (UTC) | No | Vault conversion timestamp |

---

## Fuel Type Vocabulary

| `fueltypeid` | `energy_source_type` | Description |
|-------------|----------------------|-------------|
| `ALL` | `"all fuels"` | All fuel types combined |
| `COW` | `"coal"` | Coal (bituminous, sub-bituminous, lignite) |
| `NG` | `"natural gas"` | Natural gas (including LNG) |
| `NUC` | `"nuclear"` | Nuclear fission |
| `HYC` | `"conventional hydroelectric"` | Conventional hydroelectric turbines |
| `WND` | `"wind"` | Onshore and offshore wind |
| `SUN` | `"solar"` | Utility-scale photovoltaic and thermal |
| `PET` | `"petroleum"` | Petroleum liquids and petroleum coke |
| `OTH` | `"other"` | Other fuels (biomass, geothermal, etc.) |
| `WAS` | `"other biomass"` | Waste / biomass |
| `GEO` | `"geothermal"` | Geothermal |

---

## Data Types and Constraints

| Field | Parquet Type | Constraints |
|-------|-------------|-------------|
| `generation_output_mwh` | DOUBLE | >= 0 |
| `sectorid` | INT64 | 1, 2, 3, or 99 |
| `data_timestamp` | TIMESTAMP | ISO 8601 |
| `fueltypeid` | UTF8 | From vocabulary table above |
| `confidence_tier` | UTF8 | `"PRIMARY"` or `"ESTIMATED"` |
| `data_quality_certified` | BOOLEAN | `true` for production records |
| `observation_period` | UTF8 | `YYYY-MM` format |

---

## Sample Field Values

**Sample period**: January–March 2022 | 10,884 records

| Field | Example Values |
|-------|----------------|
| `state_code` | `"CA"`, `"TX"`, `"NY"`, `"90"` (Pacific region) |
| `state_name` | `"California"`, `"Texas"`, `"Pacific"` |
| `fueltypeid` | `"ALL"`, `"NG"`, `"SUN"`, `"WND"`, `"NUC"` |
| `energy_source_type` | `"all fuels"`, `"natural gas"`, `"solar"`, `"wind"` |
| `generation_output_mwh` | `15,008` thousand MWh (Pacific, All fuels, Jan 2022) |
| `sectorid` | `1` (Electric Utility) |
| `sector_name` | `"Electric Utility"` |
| `observation_period` | `"2022-01"`, `"2022-02"`, `"2022-03"` |
| `data_timestamp` | `2022-01-01` through `2022-03-01` |
| `official_release_date` | `"2022-03-01"` (approx. 2 months after reference) |

---

## Data Source

| Source | Agency | API Version | Endpoint | Frequency | Coverage |
|--------|---------|------------|---------|-----------|----------|
| EIA Electric Power Operations | Energy Information Administration | v2 | `/electricity/electric-power-operational-data/` | Monthly | 2001–2026 |

**EIA Portal**: `https://www.eia.gov/electricity/data.cfm`
**API Base**: `https://api.eia.gov/v2/`

---

## Coverage and Granularity

- **Geography**: 62 US jurisdictions (50 states + DC + 11 territories/regions)
- **Census divisions also included**: New England, Middle Atlantic, Pacific, etc.
- **Temporal**: 2001–2026, monthly
- **Fuel types**: 11 categories including full renewables breakdown
- **Sectors**: Electric Utility (1), Independent Power Producers (2), CHP (3), All (99)
- **Vault partition**: `year={yyyy}/month={m}` (Hive)
- **Record count**: ~950,000

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
