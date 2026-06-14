# Food Pricing Sample Data - Schema v4.0 (PIT-Enabled)

## Overview
This folder contains a representative sample of the **USA Food Micropricing** dataset with full Point-in-Time (PIT) capability for institutional backtesting demonstrations.

## File Details
- **File**: `food_prices_v4.0_sample.parquet`
- **Records**: 15 (sampled from BLS and USDA sources)
- **Schema Version**: 4.0 (22 fields)
- **PIT Enabled**: ✅ Yes

## Schema Fields (22 total)

### Core Data Fields (17)
1. `country_code` - ISO country code (US)
2. `item_name` - Food item name
3. `item_description` - Detailed description
4. `item_code` - Item classification code
5. `category` - Food category
6. `item_value` - Price value (USD)
7. `unit` - Measurement unit
8. `currency` - Currency (USD)
9. `usd_equivalent` - USD conversion
10. `pct_change_mom` - Month-over-month % change
11. `data_timestamp` - Original data collection date
12. `conversion_timestamp` - Pipeline processing date
13. `source` - Data source (bls, usda)
14. `source_series_id` - Original series ID
15. `source_url` - Source URL
16. `extraction_method` - Collection method (api)
17. `data_quality_certified` - Validation flag

### Point-in-Time (PIT) Fields (5) ⭐
18. `record_id` - Unique UUID identifier
19. `published_date` - When data was released by agency
20. `as_of_date` - When version became valid
21. `revision_number` - Version number (0 = original)
22. `superseded_by` - Pointer to next version (null = current)

## PIT Backtesting Capabilities

### No Look-Ahead Bias
All records have `published_date` >= `data_timestamp`, ensuring algorithms train only on data that was actually available at decision time.

### As-Of Queries
Query data "as of" any historical date:
```python
# What data was available on April 20, 2020?
df[df['as_of_date'] <= '2020-04-20']
```

### Revision Tracking
Track changes over time (when revisions are captured):
```python
# Get all versions of a specific data point
df[(df['item_name'] == 'Rice') & (df['data_timestamp'] == '2020-03-01')]
```

## Publication Lags
- **BLS**: ~45 days (data published mid-month following collection)
- **USDA**: ~60 days (2-month lag)

## Sample Coverage
- **Sources**: BLS (Bureau of Labor Statistics), USDA
- **Years**: 2020, 2024
- **Items**: Beef, Milk, Bread, Pork, Tomatoes, Oranges, Rice, Pasta, etc.

## Use Cases
- Algorithm training demonstrations
- Backtesting pattern validation
- PIT query testing
- Data quality showcases
- Marketplace demonstrations

## Full Dataset
This is a **sample only**. The complete dataset contains:
- **10,646 records** across 74 years (1980-2026)
- **BLS**: 10,275 records (47 years)
- **USDA**: 371 records (27 years)
- **100% validation pass rate** (16 checks including 5 PIT-specific)

---

**Contact**: Lekwankwa Data Platform  
**Last Updated**: June 6, 2026  
**Schema Version**: 4.0
