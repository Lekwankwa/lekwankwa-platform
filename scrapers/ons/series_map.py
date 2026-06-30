"""
ONS series catalog for all 5 vault products.

URIs are hardcoded directly (ONS search API only covers 20 cantab-style
datasets and does not index economic time series CDIDs). Data is fetched
from https://www.ons.gov.uk{uri}/data, confirmed working 2026-06-18.

pit_coverage_type: RELEASE_DATE_ONLY/accumulating
  ONS provides current-value time series only; no historical vintage API.
  Revision detection via periodic re-fetch and diff.

Confirmed observation counts (2026-06-18):
  D7BU  Food & Non-Alc Beverages CPI    461 M obs
  D7C8  Food CPI (01.1)                 461 M obs
  D7C9  Non-Alcoholic Beverages CPI     461 M obs
  KAB9  Average Weekly Earnings         316 M obs
  MGSX  Unemployment Rate               662 M obs
  MGRZ  Employment Level                662 M obs
  D7BX  Housing, Water & Fuels CPI      461 M / 153 Q obs
  IKBH  Total Trade Exports (BOP CP SA) 352 M / 285 Q obs
  IKBI  Total Trade Imports             352 M / 285 Q obs
  IKBJ  Total Trade Balance             352 M / 285 Q obs
  ABMI  GDP Quarterly                   285 Q obs
  D7G7  CPI All Items                   449 M obs
"""

from __future__ import annotations

PIT_COVERAGE  = "RELEASE_DATE_ONLY/accumulating"
SOURCE        = "ons_api"
SOURCE_AGENCY = "ONS"
ISO3          = "GBR"

VAULT_PRODUCT_MAP: dict[str, str] = {
    "food_micropricing":                    "food_pricing_data.parquet",
    "wages_and_employment":                 "wages_employment_data.parquet",
    "Housing_Supply_and_Shelter_Inflation": "housing_data.parquet",
    "trade_flows":                          "trade_flows_data.parquet",
    "global_macro":                         "global_macro_data.parquet",
}

_INF = "/economy/inflationandpriceindices/timeseries"
_LMW = "/employmentandlabourmarket/peopleinwork"
_LMU = "/employmentandlabourmarket/peoplenotinwork/unemployment"
_LME = "/employmentandlabourmarket/peopleinwork/employmentandemployeetypes"
_BOP = "/economy/nationalaccounts/balanceofpayments/timeseries"
_GDP = "/economy/grossdomesticproductgdp/timeseries"

# Each tuple:
#   (cdid, ons_uri, metric_code, vault_product,
#    macro_metric_name, unit, release_lag_days, freq, source_sub_category)
SERIES: list[tuple] = [
    # -----------------------------------------------------------------------
    # Food Pricing — CPI COICOP food sub-groups (monthly, 2015=100)
    # -----------------------------------------------------------------------
    ("D7BU", f"{_INF}/d7bu/mm23",
     "FOOD_ALL",      "food_micropricing",
     "UK CPI Food and Non-Alcoholic Beverages, Monthly Index (2015=100)",
     "INDEX_2015_100", 21, "M", "HICP_CPI"),

    ("D7C8", f"{_INF}/d7c8/mm23",
     "FOOD_ONLY",     "food_micropricing",
     "UK CPI Food (01.1), Monthly Index (2015=100)",
     "INDEX_2015_100", 21, "M", "HICP_CPI"),

    ("D7C9", f"{_INF}/d7c9/mm23",
     "FOOD_NONALC",   "food_micropricing",
     "UK CPI Non-Alcoholic Beverages (01.2), Monthly Index (2015=100)",
     "INDEX_2015_100", 21, "M", "HICP_CPI"),

    # -----------------------------------------------------------------------
    # Wages & Employment
    # -----------------------------------------------------------------------
    ("KAB9", f"{_LMW}/earningsandworkinghours/timeseries/kab9/emp",
     "AWE_TOTAL",     "wages_and_employment",
     "UK Average Weekly Earnings, Total Pay, All Employees, Monthly (GBP)",
     "GBP_AVG_WEEKLY", 45, "M", "LFS"),

    ("MGSX", f"{_LMU}/timeseries/mgsx/lms",
     "UNE_RATE",      "wages_and_employment",
     "UK Unemployment Rate, SA, Monthly (%)",
     "PCT", 45, "M", "LFS"),

    ("MGRZ", f"{_LME}/timeseries/mgrz/lms",
     "EMP_LVL",       "wages_and_employment",
     "UK Employment Level, 16+, SA, Monthly (Thousands)",
     "THS_PERSONS", 45, "M", "LFS"),

    # -----------------------------------------------------------------------
    # Housing & Credit — CPI Housing sub-index (proxy for shelter inflation)
    # -----------------------------------------------------------------------
    ("D7BX", f"{_INF}/d7bx/mm23",
     "CPI_HOUSING",   "Housing_Supply_and_Shelter_Inflation",
     "UK CPI Housing, Water and Fuels, Monthly Index (2015=100)",
     "INDEX_2015_100", 21, "M", "HICP_CPI"),

    # -----------------------------------------------------------------------
    # Trade Flows — BOP current prices SA (goods + services)
    # -----------------------------------------------------------------------
    ("IKBH", f"{_BOP}/ikbh/mret",
     "EXPORTS",       "trade_flows",
     "UK Total Trade Exports, Goods+Services, BOP, CP, SA (£m)",
     "MIO_GBP", 90, "M", "BALANCE_OF_PAYMENTS"),

    ("IKBI", f"{_BOP}/ikbi/mret",
     "IMPORTS",       "trade_flows",
     "UK Total Trade Imports, Goods+Services, BOP, CP, SA (£m)",
     "MIO_GBP", 90, "M", "BALANCE_OF_PAYMENTS"),

    ("IKBJ", f"{_BOP}/ikbj/mret",
     "TRADE_BAL",     "trade_flows",
     "UK Total Trade Balance, Goods+Services, BOP, CP, SA (£m)",
     "MIO_GBP", 90, "M", "BALANCE_OF_PAYMENTS"),

    # -----------------------------------------------------------------------
    # Global Macro Baseline
    # -----------------------------------------------------------------------
    ("ABMI", f"{_GDP}/abmi/pn2",
     "GDP_Q_SA",      "global_macro",
     "UK GDP, Chain-Linked Volumes, SA, Quarterly (£m 2019 prices)",
     "MIO_GBP_CLV", 60, "Q", "NATIONAL_ACCOUNTS"),

    ("D7G7", f"{_INF}/d7g7/mm23",
     "CPI_ALL",       "global_macro",
     "UK CPI All Items, Monthly Index (2015=100)",
     "INDEX_2015_100", 21, "M", "HICP_CPI"),
]
