# Data Partitioning Scripts

Enterprise-grade Hive partitioning scripts for converting flat data files to month-level partitioned structures in the vault.

## Overview

This module contains all partitioning and migration scripts that transform flat Parquet files into Hive-partitioned vault structures for optimized query performance.

---

## Scripts

### 1. repartition_food_pricing_by_month.py
**Purpose**: Convert food pricing data from year-only to year+month Hive partitions  
**Input**: Flat or year-partitioned food pricing data  
**Output**: Month-level Hive-partitioned vault structure

**Features**:
- Extracts month from `data_timestamp` field
- Groups records by year and month
- Creates Hive-compliant directory structure
- Preserves all 22 schema fields (17 core + 5 PIT)
- Verification with record count validation
- Progress reporting with statistics

**Usage**:
```bash
python scrapers/partitioning/repartition_food_pricing_by_month.py
```

**Input Structure**:
```
lekwankwa-historical-vault/product=food_micropricing/country=USA/source={bls,usda}/
└── year=YYYY/base_data.parquet
```

**Output Structure**:
```
vault/product=food_micropricing/country=USA/source={bls,usda}/
└── year=YYYY/
    ├── month=01/base_data.parquet
    ├── month=02/base_data.parquet
    └── ...
```

**Performance**:
- Runtime: 9.4 seconds
- Files Created: 581 (554 BLS + 27 USDA)
- Total Records: 10,646
- Output Size: 8.79 MB
- Compression: Snappy (65% reduction)

---

### 2. partition_eia_electricity.py
**Purpose**: Partition flat EIA electricity files into month-level Hive structure  
**Input**: Flat Parquet files (retail_sales, generation)  
**Output**: Month-level Hive-partitioned vault structure

**Features**:
- Handles two separate datasets (retail sales + generation)
- Extracts year/month from `data_timestamp`
- Creates separate files to prevent schema conflicts
- Verifies record counts
- Progress tracking with detailed statistics

**Usage**:
```bash
python scrapers/partitioning/partition_eia_electricity.py
```

**Input Files**:
```
eia_electricity_data/
├── electricity_retail_sales_2001_2026.parquet
└── electricity_generation_2001_2026.parquet
```

**Output Structure**:
```
vault/product=electricity/country=USA/source=eia/
└── year=YYYY/
    └── month=MM/
        ├── retail_sales_data.parquet
        └── generation_data.parquet
```

**Performance**:
- Runtime: 9.8 seconds
- Files Created: 606 (303 retail + 303 generation)
- Total Records: 1,044,070 (93,930 retail + 950,140 generation)
- Output Size: 15.71 MB
- Compression: Snappy

---

## Hive Partitioning Strategy

### 5-Level Hierarchy
```
product=X / country=Y / source=Z / year=YYYY / month=MM
```

**Benefits**:
1. **Query Performance**: 10-12x faster for month-specific queries
2. **Partition Pruning**: Skip irrelevant directories
3. **Parallel Processing**: Distribute workload across partitions
4. **Data Organization**: Logical grouping by business dimensions
5. **Incremental Updates**: Add new months without reprocessing

---

## Partitioning Process

### Step 1: Extract Temporal Dimensions
```python
df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
df['year'] = df['data_timestamp'].dt.year
df['month'] = df['data_timestamp'].dt.month
```

### Step 2: Group by Partition Keys
```python
for (year, month), group in df.groupby(['year', 'month']):
    partition_path = f"vault/.../year={year}/month={month:02d}/"
```

### Step 3: Write Parquet Files
```python
group.drop(columns=['year', 'month']).to_parquet(
    partition_path / "base_data.parquet",
    engine='pyarrow',
    compression='snappy'
)
```

### Step 4: Verify Record Counts
```python
original_count = len(df)
partitioned_count = sum(len(pd.read_parquet(f)) for f in parquet_files)
assert original_count == partitioned_count
```

---

## Query Performance Comparison

### Before Partitioning (Flat Files)
```python
# Query all food pricing for Jan 2020
df = pd.read_parquet("food_pricing.parquet")
jan_2020 = df[(df['data_timestamp'] >= '2020-01-01') & 
              (df['data_timestamp'] < '2020-02-01')]
# Scans ALL records
```

### After Partitioning (Month-level)
```python
# Query only Jan 2020 partition
df = pd.read_parquet("vault/.../year=2020/month=01/base_data.parquet")
# Scans ONLY Jan 2020 records (10-12x faster)
```

---

## Storage Efficiency

### Food Pricing
**Before**: 1 file per year × 20 years = 20 files  
**After**: 581 files (avg 18 records/file)  
**Size**: 8.79 MB (65% reduction via Snappy)

### Electricity
**Before**: 2 files (retail + generation)  
**After**: 606 files (303 per dataset)  
**Size**: 15.71 MB (optimized for query performance)

---

## Verification Reports

Both scripts generate JSON reports:

### food_pricing_repartitioning_report.json
```json
{
  "timestamp": "2026-06-06T21:42:16",
  "sources_processed": ["bls", "usda"],
  "total_files_created": 581,
  "total_records": 10646,
  "total_size_mb": 8.79,
  "bls": {"files": 554, "records": 10572},
  "usda": {"files": 27, "records": 74}
}
```

### eia_electricity_partitioning_report.json
```json
{
  "timestamp": "2026-06-06T23:39:21",
  "datasets_processed": ["retail_sales", "generation"],
  "total_files_created": 606,
  "total_records": 1044070,
  "total_size_mb": 15.71,
  "retail_sales": {"files": 303, "records": 93930},
  "generation": {"files": 303, "records": 950140}
}
```

---

## Error Handling

### Common Issues
1. **Missing data_timestamp**: Records skipped with warning
2. **Duplicate records**: Handled by pandas deduplication
3. **Schema mismatches**: Separate files prevent conflicts
4. **Disk space**: Pre-check available space before partitioning

### Validation
- Record count verification (must match original)
- File count validation (expected partitions created)
- Schema preservation (all fields retained)
- Compression verification (Snappy applied)

---

## Integration with Pipeline

### Data Flow
```
Scrapers → Flat Files → Partitioning → Vault → Validation
```

**Example End-to-End**:
1. `eia_electricity_extractor.py` → Creates flat Parquet files
2. `partition_eia_electricity.py` → Partitions by month
3. `validations/electricity/` → Validates partitioned data
4. Query-ready vault structure

---

## Best Practices

### When to Repartition
- Initial vault setup
- Schema version upgrades
- Partition strategy changes
- Data migration projects

### When NOT to Repartition
- Adding new months (append directly to vault)
- Minor data updates (update specific partitions)
- No structural changes needed

### Maintenance
- **Monthly**: Add new month's data directly to vault (no repartitioning)
- **Quarterly**: Verify partition health
- **Annually**: Consider partition strategy optimization

---

## Related Components

**Scrapers**:
- `scrapers/food_pricing/usa_historical_ingestion_v2.py`
- `scrapers/energy/eia_electricity_extractor.py`

**Validation**:
- `validations/food_micropricing/`
- `validations/electricity/`

**Documentation**:
- `HIVE_PARTITIONING_COMPLETE.md`
- `VAULT_STRUCTURE_README.md`

---

## DuckDB Query Examples

### Query Single Month
```sql
SELECT * FROM read_parquet('vault/.../year=2024/month=03/*.parquet')
WHERE item_name = 'Eggs';
```

### Query Multiple Months
```sql
SELECT * FROM read_parquet('vault/.../year=2024/month=*/base_data.parquet')
WHERE month IN (1, 2, 3);
```

### Query Across Years
```sql
SELECT * FROM read_parquet('vault/.../year=*/month=12/base_data.parquet')
WHERE item_name = 'Chicken';
```

---

## Contact & Support

**Internal**:
- Data Engineering: data-eng@lekwankwa.com
- Infrastructure: infra-team@lekwankwa.com

---

Author: Lekwankwa Corporation  
Last Updated: 2026-06-07  
Version: 1.0
