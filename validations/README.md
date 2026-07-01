# Validation Suite

Organized validation scripts for all datasets in the vault.

## Folder Structure

```
validations/
├── food_micropricing/      # Food pricing validation scripts
│   ├── README.md
│   ├── pit_validation_food.py
│   ├── sanity_check_food.py
│   ├── outlier_extractor_food.py
│   └── changelog_generator_food.py
│
├── electricity/            # Electricity validation scripts
│   ├── README.md
│   ├── validate_schema_pit_electricity.py
│   ├── sanity_check_electricity.py
│   ├── outlier_extractor_electricity.py
│   └── changelog_generator_electricity.py
│
└── gx_universal/           # Universal GX validator
    ├── README.md
    └── universal_gx_validator.py
```

---

## Quick Start

### Food Micropricing Validation

```bash
# Full pipeline
python validations/food_micropricing/pit_validation_food.py
python validations/food_micropricing/sanity_check_food.py
python validations/gx_universal/universal_gx_validator.py configs/gx_config_food_micropricing_vault.json
python validations/food_micropricing/outlier_extractor_food.py
python validations/food_micropricing/changelog_generator_food.py
```

### Electricity Validation

```bash
# Full pipeline
python validations/electricity/validate_schema_pit_electricity.py
python validations/electricity/sanity_check_electricity.py
python validations/gx_universal/universal_gx_validator.py configs/gx_config_electricity_vault.json
python validations/electricity/outlier_extractor_electricity.py
python validations/electricity/changelog_generator_electricity.py
```

---

## Validation Types

### 1. Schema & PIT Validation
- **Food**: PIT fields (record_id, published_date, as_of_date, etc.)
- **Electricity**: Schema only (no PIT fields)

### 2. Sanity Checks
- Empty file detection
- Null value checks
- Duplicate detection
- Value range validation

### 3. GX Validation (Universal)
- Configurable validation rules
- Works with any dataset via config files
- See: `configs/gx_config_*.json`

### 4. Outlier Detection
- Dataset-specific thresholds
- Known outlier tracking (food)
- Creates outliers.parquet files

### 5. Changelog Generation
- Tracks data ingestion events
- Schema evolution
- Validation results
- Quality metrics

---

## Config Files

All GX validation configs are in: `configs/`
- `gx_config_food_micropricing_vault.json`
- `gx_config_electricity_vault.json`

---

## Reports Generated

### Food Micropricing
- `food_micropricing_pit_validation_report.json`
- `food_sanity_check_report.txt`
- `food_sanity_check_failures.json`
- `food_micropricing_gx_validation_report.json`
- `outliers.parquet` (in year folders)
- `changelog.parquet` (in year/month folders)

### Electricity
- `electricity_schema_validation_report.json`
- `electricity_sanity_check_report.txt`
- `electricity_sanity_check_failures.json`
- `electricity_gx_validation_report.json`
- `electricity_outlier_summary.json`
- `electricity_changelog_summary.json`

---

## Validation Pipeline Order

1. **Schema/PIT** - Validate structure and temporal fields
2. **Sanity** - Quick data quality checks
3. **GX** - Comprehensive validation rules
4. **Outlier** - Flag anomalies
5. **Changelog** - Document changes

Author: Lekwankwa Corporation  
Date: 2026-06-07
