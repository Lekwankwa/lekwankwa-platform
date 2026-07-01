# Universal GX Validator

This folder contains the universal GX-style validator that works with any product by accepting a config file.

## universal_gx_validator.py

**Purpose**: Parameterized validation engine for any dataset

**Features**:
- Config-driven validation rules
- Works with multiple file patterns (base_data.parquet, *_data.parquet)
- Supports PIT validation when applicable
- Comprehensive reporting

**Usage**:
```bash
python validations/gx_universal/universal_gx_validator.py <config_file>
```

**Examples**:
```bash
# Food Micropricing (with PIT fields)
python validations/gx_universal/universal_gx_validator.py configs/gx_config_food_micropricing_vault.json

# Electricity (without PIT fields)
python validations/gx_universal/universal_gx_validator.py configs/gx_config_electricity_vault.json
```

---

## Validation Checks

### Core Checks (All Datasets)
1. **Row Count**: Validate reasonable data volume
2. **Required Columns**: Ensure all expected columns present
3. **Non-Null Fields**: Check critical fields have no nulls
4. **Value Constraints**: Validate numeric ranges
5. **Categorical Constraints**: Verify allowed values
6. **Timestamp Checks**: Validate date ranges and formats

### PIT Checks (Food Micropricing Only)
7. **Temporal Consistency**: published_date >= data_timestamp
8. **Unique Record IDs**: All record_id values unique
9. **Validity Ordering**: as_of_date >= published_date
10. **Superseded Integrity**: superseded_by points to valid records

---

## Config File Structure

```json
{
  "product_name": "Product Name",
  "product_code": "product_code",
  "vault_path": "vault/product=X/country=Y",
  "schema_version": "1.0",
  "sources": ["source1", "source2"],
  
  "critical_checks": {
    "row_count": {"min": 100, "max": 1000000},
    "required_columns": ["col1", "col2", ...],
    "non_null_fields": ["field1", "field2", ...],
    "value_constraints": {
      "field_name": {"min": 0.01, "max": 1000}
    },
    "categorical_constraints": {
      "field_name": ["value1", "value2"]
    },
    "timestamp_checks": {
      "timestamp_field": {
        "min_year": 1980,
        "max_year": 2030
      }
    },
    "pit_validation_checks": {
      "check_11_temporal_consistency": {...},
      "check_13_unique_record_id": {...}
    }
  }
}
```

---

## Output

**Console**: Real-time progress with ✅/❌ indicators

**JSON Report**: `{product_code}_gx_validation_report.json`
```json
{
  "timestamp": "2026-06-07T...",
  "product": "product_code",
  "total_records": 10000,
  "checks_run": 15,
  "checks_passed": 14,
  "checks_failed": 1,
  "pass_rate": "93.3%",
  "status": "FAIL"
}
```

---

## Benefits of Universal Design

1. **Reusability**: One script validates all products
2. **Consistency**: Same validation logic across datasets
3. **Maintainability**: Update validation rules in config, not code
4. **Extensibility**: Add new products by creating new configs
5. **Documentation**: Configs serve as validation specifications

Author: Lekwankwa Corporation  
Date: 2026-06-07
