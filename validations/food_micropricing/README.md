# Food Micropricing Validation Suite

This folder contains validation scripts for the food micropricing dataset in the vault.

## Scripts

### 1. pit_validation_food.py
**Purpose**: Point-in-Time (PIT) field validation  
**Validates**:
- `record_id`: Unique identifiers
- `published_date`: When data was published
- `as_of_date`: When version became valid
- `revision_number`: Version tracking
- `superseded_by`: Version chain integrity

**Usage**:
```bash
python validations/food_micropricing/pit_validation_food.py
```

**Output**: `food_micropricing_pit_validation_report.json`

---

### 2. sanity_check_food.py
**Purpose**: Data quality sanity checks  
**Checks**:
1. Empty file detection
2. Null value detection in critical fields
3. Month-over-month price spike detection (>50%)
4. Duplicate record detection
5. Currency shift detection (>99% drops)
6. Value range violations

**Usage**:
```bash
python validations/food_micropricing/sanity_check_food.py
```

**Output**:
- `food_sanity_check_report.txt`
- `food_sanity_check_failures.json`

---

### 3. outlier_extractor_food.py
**Purpose**: Extract and flag outliers  
**Detects**:
- Price spikes (>50% MoM change)
- Currency shifts (>99% drops)
- Null values in critical fields
- Range violations
- Duplicate keys

**Known Outliers** (Agricultural supply shocks):
- Onions (1984, 1990)
- Potatoes (1985, 1987, 1991, 1995, 1998)
- Tomatoes (1990)

**Usage**:
```bash
python validations/food_micropricing/outlier_extractor_food.py
```

**Output**: `outliers.parquet` in each year folder

---

### 4. changelog_generator_food.py
**Purpose**: Generate changelog tracking  
**Tracks**:
- Data ingestion events
- Schema changes
- Validation results
- Quality metrics
- Methodology changes

**Usage**:
```bash
python validations/food_micropricing/changelog_generator_food.py
```

**Output**: `changelog.parquet` in each year/month folder

---

## GX Validation (Universal)

Food micropricing uses the universal GX validator:

```bash
python validations/gx_universal/universal_gx_validator.py configs/gx_config_food_micropricing_vault.json
```

See: `configs/gx_config_food_micropricing_vault.json` for field definitions.

---

## Validation Pipeline

Run validations in this order:

```bash
# 1. PIT Validation
python validations/food_micropricing/pit_validation_food.py

# 2. Sanity Checks
python validations/food_micropricing/sanity_check_food.py

# 3. GX Validation (Universal)
python validations/gx_universal/universal_gx_validator.py configs/gx_config_food_micropricing_vault.json

# 4. Outlier Extraction
python validations/food_micropricing/outlier_extractor_food.py

# 5. Changelog Generation
python validations/food_micropricing/changelog_generator_food.py
```

---

## Vault Structure

```
vault/product=food_micropricing/country=USA/
├── source=bls/
│   ├── year=1980/
│   │   ├── month=01/base_data.parquet
│   │   ├── month=02/base_data.parquet
│   │   └── ...
│   └── ...
└── source=usda/
    └── ...
```

---

## Expected Schema (v4.0)

**22 fields total**:
- **17 core fields**: item_name, item_value, currency, etc.
- **5 PIT fields**: record_id, published_date, as_of_date, revision_number, superseded_by

Author: Lekwankwa Corporation  
Date: 2026-06-07
