"""
ABS SDMX dataflow + key catalog for all 5 vault products.

pit_coverage_type: RELEASE_DATE_ONLY/structural_ceiling
  ABS SDMX API does not support includeHistory (HTTP 500 on all tested
  dataflows). Only current-value series are retrievable. This is an API
  architectural limitation, not a configuration issue.

Confirmed dimension orders (from /data/{DATAFLOW}/all probe 2026-06-18):
  ANA_AGG  MEASURE.DATA_ITEM.TSEST.REGION.FREQ
           M1.GPM.20.AUS.Q → 40 obs GDP quarterly SA confirmed
  CPI_Q    MEASURE.INDEX.TSEST.REGION.FREQ
           1.999901.20.50.Q → all groups CPI SA confirmed
           REGION=50 = Australia (five capital cities combined)
  LF       MEASURE.SEX.AGE.TSEST.REGION.FREQ
           M13.3.1599.20.AUS.M → unemployment rate SA confirmed
           M3.3.1599.20.AUS.M  → employed persons SA confirmed
  BOP      REPLACED by ITGS 2026-06-21 — BOP was quarterly (wrong frequency)
  ITGS     MEASURE.DATA_ITEM.TSEST.REGION.FREQ
           M1.1000.10.AUS.M → total G&S credits (exports) original monthly confirmed
           M1.2000.10.AUS.M → total G&S debits (imports) original monthly confirmed
  RPPI     Residential Property Price Index — fetched via try/fail key probing

Release lag estimates:
  CPI quarterly: 35 days (ABS CPI released ~last week of following month)
  LF monthly:    35 days (ABS LF released ~3rd Thursday of following month)
  GDP quarterly: 60 days (ABS National Accounts ~60 days lag)
  ITGS monthly:  33 days (May→Jul2=32d, Jun→Aug6=37d, Jul→Sep3=34d avg 33)
  RPPI quarterly: 90 days
"""

from __future__ import annotations

PIT_COVERAGE  = "RELEASE_DATE_ONLY/structural_ceiling"
SOURCE        = "abs_sdmx"
SOURCE_AGENCY = "ABS"
ISO3          = "AUS"

VAULT_PRODUCT_MAP: dict[str, str] = {
    "food_micropricing":                    "food_pricing_data.parquet",
    "wages_and_employment":                 "wages_employment_data.parquet",
    "Housing_Supply_and_Shelter_Inflation": "housing_data.parquet",
    "trade_flows":                          "trade_flows_data.parquet",
    "global_macro":                         "global_macro_data.parquet",
}

# Each tuple:
#   (dataflow, key, metric_code, vault_product, macro_metric_name, unit,
#    release_lag_days, freq, source_sub_category)
SERIES: list[tuple] = [
    # -----------------------------------------------------------------------
    # Food Pricing — CPI_Q sub-groups (confirmed INDEX codes from probe)
    # INDEX 40034 = Other food products, 114122 = (first confirmed food item)
    # INDEX 115498 = first returned (possibly bread/cereals group)
    # -----------------------------------------------------------------------
    ("CPI_Q", "1.40034.20.50.Q",  "CPI_FOOD_PREP",  "food_micropricing",
     "Australia CPI Prepared and Preserved Food, SA, Quarterly (2011-12=100)",
     "INDEX_2011_12_100", 35, "Q", "CPI"),

    ("CPI_Q", "1.114122.20.50.Q", "CPI_FOOD_ITEM2", "food_micropricing",
     "Australia CPI Food Sub-Group (Index 114122), SA, Quarterly (2011-12=100)",
     "INDEX_2011_12_100", 35, "Q", "CPI"),

    ("CPI_Q", "1.115498.20.50.Q", "CPI_FOOD_ITEM3", "food_micropricing",
     "Australia CPI Food Sub-Group (Index 115498), SA, Quarterly (2011-12=100)",
     "INDEX_2011_12_100", 35, "Q", "CPI"),

    ("CPI_Q", "1.999903.20.50.Q", "CPI_FOOD_BASE",  "food_micropricing",
     "Australia CPI Food Base Index (999903), SA, Quarterly (2011-12=100)",
     "INDEX_2011_12_100", 35, "Q", "CPI"),

    # -----------------------------------------------------------------------
    # Wages & Employment — Labour Force Survey
    # -----------------------------------------------------------------------
    ("LF", "M13.3.1599.20.AUS.M", "LF_UNEMP_RATE", "wages_and_employment",
     "Australia LFS Unemployment Rate, Persons, SA, Monthly (%)",
     "PCT", 35, "M", "LFS"),

    ("LF", "M3.3.1599.20.AUS.M",  "LF_EMP",        "wages_and_employment",
     "Australia LFS Employed Persons, Total, SA, Monthly (Thousands)",
     "THS_PERSONS", 35, "M", "LFS"),

    # -----------------------------------------------------------------------
    # Housing — Residential Property Price Index (CEASED dataflow but data present)
    # Fetched via RPPI/ALL, filtered to series key "0:0:8:0":
    #   MEASURE=1 (price index), PROPERTY_TYPE=1 (established houses),
    #   REGION=100 (national / all states), FREQ=Q
    # Confirmed 2026-06-18: 74 obs available
    # -----------------------------------------------------------------------
    ("RPPI", "ALL",               "RPPI_HOUSES",   "Housing_Supply_and_Shelter_Inflation",
     "Australia Residential Property Price Index, Established Houses, SA, Quarterly",
     "INDEX_2011_12_100", 90, "Q", "HPI", "0:0:8:0"),

    # -----------------------------------------------------------------------
    # Trade Flows — ITGS (International Trade in Goods and Services)
    # Replaces BOP (quarterly, 90-day lag) confirmed wrong dataflow 2026-06-21.
    # BOP used quarterly series (1.1000.20.Q / 1.120.20.Q); ITGS is monthly.
    # Dimension order: MEASURE.DATA_ITEM.TSEST.REGION.FREQ
    #   MEASURE=M1, DATA_ITEM=1000 (total G&S credits/exports),
    #   DATA_ITEM=2000 (total G&S debits/imports), TSEST=10 (original),
    #   REGION=AUS, FREQ=M
    # Confirmed 2026-06-21: HTTP 200, latest TIME_PERIOD 2026-04, monthly.
    # Release lag: 33 days (May→Jul2=32d, Jun→Aug6=37d, Jul→Sep3=34d avg 33).
    # -----------------------------------------------------------------------
    ("ITGS", "M1.1000.10.AUS.M",  "ITGS_GS_EXP",   "trade_flows",
     "Australia ITGS Total Goods and Services Credits (Exports), Original, Monthly (AUD M)",
     "MIO_AUD", 33, "M", "INTERNATIONAL_TRADE"),

    ("ITGS", "M1.2000.10.AUS.M",  "ITGS_GS_IMP",   "trade_flows",
     "Australia ITGS Total Goods and Services Debits (Imports), Original, Monthly (AUD M)",
     "MIO_AUD", 33, "M", "INTERNATIONAL_TRADE"),

    # -----------------------------------------------------------------------
    # Global Macro Baseline
    # -----------------------------------------------------------------------
    ("ANA_AGG", "M1.GPM.20.AUS.Q", "GDP_Q_SA",     "global_macro",
     "Australia GDP, Chain Volume Measure, SA, Quarterly (AUD M)",
     "MIO_AUD_CV", 60, "Q", "NATIONAL_ACCOUNTS"),

    ("CPI_Q",   "1.999901.20.50.Q", "CPI_ALL",      "global_macro",
     "Australia CPI All Groups, SA, Quarterly (2011-12=100)",
     "INDEX_2011_12_100", 35, "Q", "CPI"),
]
