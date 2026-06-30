"""
Statistics Canada NDM table + vector catalog for all 5 vault products.

pit_coverage_type: RELEASE_DATE_ONLY/accumulating
  StatCan NDM CSV downloads contain current values only.
  No historical vintage retrieval via this method.
  Release date estimated from obs_date + release_lag_days.

WDS REST API broken: /getCubeMetadata, /getDataFromVectorsAndLatestNPeriods,
  etc. return HTTP 404 for all PIDs/vectors (confirmed 2026-06-18).
  Using NDM ZIP/CSV download instead.

Vectors confirmed via CSV download probe 2026-06-18:
  Table 18100004 (CPI):
    v41690973 = All-items, Canada, 2002=100, from 1914-01
    v41690974 = Food, Canada, 2002=100, from 1949-01
  Table 14100017 (LFS SA):
    v2091072  = Employment, Canada, Total gender, 15+, SA
    v2091177  = Unemployment rate, Canada, Total gender, 15+, SA
  Table 18100205 (NHPI):
    v111955442 = Total (house and land), Canada, from 1981-01
  Table 12100011 (Merchandise Trade):
    v87008839 = Import, Balance of payments, SA, All countries
    v87008955 = Export, Balance of payments, SA, All countries
    v87008984 = Trade Balance, Balance of payments, SA, All countries
  Table 36100104 (GDP quarterly expenditure-based):
    v62305752 = GDP at market prices, Canada, Chained 2017$, SA annual rates

Release lag estimates:
  CPI monthly    : 21 days (released ~3rd week of following month)
  LFS monthly    : 21 days (released ~3rd Friday of following month)
  GDP quarterly  : 60 days (advance ~60d, final ~90d; use 60)
  NHPI monthly   : 45 days
  Trade monthly  : 45 days (~6 weeks lag)
"""

from __future__ import annotations

PIT_COVERAGE  = "RELEASE_DATE_ONLY/accumulating"
SOURCE        = "statcan_csv"
SOURCE_AGENCY = "STATCAN"
ISO3          = "CAN"

VAULT_PRODUCT_MAP: dict[str, str] = {
    "food_micropricing":                    "food_pricing_data.parquet",
    "wages_and_employment":                 "wages_employment_data.parquet",
    "Housing_Supply_and_Shelter_Inflation": "housing_data.parquet",
    "trade_flows":                          "trade_flows_data.parquet",
    "global_macro":                         "global_macro_data.parquet",
}

# Each tuple:
#   (table_id, vector_str, metric_code, vault_product, macro_metric_name,
#    unit, release_lag_days, freq, source_sub_category)
SERIES: list[tuple] = [
    # -----------------------------------------------------------------------
    # Food Pricing — CPI food (Table 18-10-0004-01)
    # -----------------------------------------------------------------------
    ("18100004", "v41690974", "CPI_FOOD_ALL", "food_micropricing",
     "Canada CPI Food, Monthly (2002=100)",
     "INDEX_2002_100", 21, "M", "CPI"),

    # -----------------------------------------------------------------------
    # Wages & Employment — LFS SA (Table 14-10-0017-01)
    # Both sexes, 15 years and over, Canada, seasonally adjusted
    # -----------------------------------------------------------------------
    ("14100017", "v2091177", "LFS_UNEMP_RATE", "wages_and_employment",
     "Canada LFS Unemployment Rate, SA, Monthly (%)",
     "PCT", 21, "M", "LFS"),

    ("14100017", "v2091072", "LFS_EMP", "wages_and_employment",
     "Canada LFS Employment, SA, Monthly (Thousands)",
     "THS_PERSONS", 21, "M", "LFS"),

    # -----------------------------------------------------------------------
    # Housing — New Housing Price Index (Table 18-10-0205-01)
    # -----------------------------------------------------------------------
    ("18100205", "v111955442", "NHPI_TOTAL", "Housing_Supply_and_Shelter_Inflation",
     "Canada New Housing Price Index, Total (House and Land), Monthly (2016=100)",
     "INDEX_2016_100", 45, "M", "HPI"),

    # -----------------------------------------------------------------------
    # Trade Flows — Merchandise Trade, BOP basis, SA (Table 12-10-0011-01)
    # -----------------------------------------------------------------------
    ("12100011", "v87008955", "MERCH_EXPORTS", "trade_flows",
     "Canada Merchandise Exports, BOP basis, SA, Monthly (CAD Millions)",
     "MIO_CAD", 45, "M", "MERCHANDISE_TRADE"),

    ("12100011", "v87008839", "MERCH_IMPORTS", "trade_flows",
     "Canada Merchandise Imports, BOP basis, SA, Monthly (CAD Millions)",
     "MIO_CAD", 45, "M", "MERCHANDISE_TRADE"),

    ("12100011", "v87008984", "TRADE_BAL", "trade_flows",
     "Canada Trade Balance, BOP basis, SA, Monthly (CAD Millions)",
     "MIO_CAD", 45, "M", "MERCHANDISE_TRADE"),

    # -----------------------------------------------------------------------
    # Global Macro Baseline
    # -----------------------------------------------------------------------
    ("36100104", "v62305752", "GDP_REAL_Q", "global_macro",
     "Canada GDP at Market Prices, Chained 2017$, SA at Annual Rates, Quarterly (CAD Millions)",
     "MIO_CAD_2017", 60, "Q", "NATIONAL_ACCOUNTS"),

    ("18100004", "v41690973", "CPI_ALL", "global_macro",
     "Canada CPI All Items, Monthly (2002=100)",
     "INDEX_2002_100", 21, "M", "CPI"),
]
