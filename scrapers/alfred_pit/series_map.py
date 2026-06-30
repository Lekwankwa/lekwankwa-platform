"""
Series mapping: vault sovereign_series_id → ALFRED FRED series ID.

For each dataset, maps the internal vault ID to the ALFRED-accessible
FRED series ID. Where no ALFRED series exists, notes the fallback strategy.

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

# ── Wages & Employment ────────────────────────────────────────────────────────
# Vault uses BLS CES internal IDs; ALFRED uses FRED alias IDs for the same series.
WAGES_SERIES = {
    # vault_id          : (alfred_id,  macro_metric_name,               unit)
    "CES0000000001": ("PAYEMS",   "TOTAL_NONFARM_PAYROLLS",          "THOUSANDS"),
    "CES0500000001": ("USPRIV",   "TOTAL_PRIVATE_PAYROLLS",          "THOUSANDS"),
    "CES2000000001": ("USCONS",   "CONSTRUCTION_EMPLOYMENT",         "THOUSANDS"),
    "CES3000000001": ("MANEMP",   "MANUFACTURING_EMPLOYMENT",        "THOUSANDS"),
    "CES4000000001": ("USTRADE",  "TRADE_TRANSPORT_EMPLOYMENT",      "THOUSANDS"),
    "CES5000000001": ("USINFO",   "INFORMATION_EMPLOYMENT",          "THOUSANDS"),
    "CES5500000001": ("USFIRE",   "FINANCIAL_ACTIVITIES_EMPLOYMENT", "THOUSANDS"),
    "CES6500000001": ("USEHS",    "EDUCATION_HEALTH_EMPLOYMENT",     "THOUSANDS"),
    "CES7000000001": ("USLAH",    "LEISURE_HOSPITALITY_EMPLOYMENT",  "THOUSANDS"),
    "CES9000000001": ("USGOVT",   "GOVERNMENT_EMPLOYMENT",           "THOUSANDS"),
    # CPS unemployment
    "LNS14000000":   ("UNRATE",   "UNEMPLOYMENT_RATE_U3",            "PERCENT"),
    # Avg Hourly Earnings
    "CES0500000003": ("CES0500000003", "AVG_HOURLY_EARNINGS_PRIVATE","USD"),
}

# ── Housing ───────────────────────────────────────────────────────────────────
HOUSING_SERIES = {
    "PERMIT":          ("PERMIT",         "AUTHORIZED_PERMITS_TOTAL_UNITS", "UNITS_SAAR"),
    "PERMIT1":         ("PERMIT1",        "AUTHORIZED_PERMITS_1_UNIT",      "UNITS_SAAR"),
    "PERMIT5":         ("PERMIT5",        "AUTHORIZED_PERMITS_5PLUS_UNITS", "UNITS_SAAR"),
    # CPI shelter (BLS — on ALFRED from 2011)
    "CUUR0000SEHA":    ("CUUR0000SEHA",   "CPI_RENT_PRIMARY_RESIDENCE_NSA", "INDEX"),
    "CUUR0000SEHB":    ("CUUR0000SEHB",   "CPI_LODGING_AWAY_NSA",           "INDEX"),
    "CUUR0000SAH1":    ("CUUR0000SAH1",   "CPI_HOUSING_NSA",                "INDEX"),
    "CUSR0000SEHA":    ("CUSR0000SEHA",   "CPI_RENT_PRIMARY_RESIDENCE_SA",  "INDEX"),
    "CUSR0000SEHB":    ("CUSR0000SEHB",   "CPI_LODGING_AWAY_SA",            "INDEX"),
    "CUSR0000SAH1":    ("CUSR0000SAH1",   "CPI_HOUSING_SA",                 "INDEX"),
}

# ── Global Macro ──────────────────────────────────────────────────────────────
# These are core FRED macroeconomic series with long ALFRED vintage histories.
GLOBAL_MACRO_SERIES = {
    "GDP":      ("GDP",      "NOMINAL_GDP",              "USD_BILLIONS"),
    "GDPC1":    ("GDPC1",    "REAL_GDP_CHAINED",         "USD_BILLIONS_2017"),
    "INDPRO":   ("INDPRO",   "INDUSTRIAL_PRODUCTION",    "INDEX_2017_100"),
    "CPIAUCSL": ("CPIAUCSL", "CPI_ALL_ITEMS_SA",         "INDEX_1982_84_100"),
    "CPIAUCNS": ("CPIAUCNS", "CPI_ALL_ITEMS_NSA",        "INDEX_1982_84_100"),
    "UNRATE":   ("UNRATE",   "UNEMPLOYMENT_RATE",        "PERCENT"),
    "PAYEMS":   ("PAYEMS",   "TOTAL_NONFARM_PAYROLLS",   "THOUSANDS"),
    "FEDFUNDS": ("FEDFUNDS", "FED_FUNDS_RATE",           "PERCENT"),
    "GS10":     ("GS10",     "TREASURY_10Y_YIELD",       "PERCENT"),
    "PCEPI":    ("PCEPI",    "PCE_PRICE_INDEX",          "INDEX_2017_100"),
}

# ── Food Pricing ──────────────────────────────────────────────────────────────
# BLS average price series — on ALFRED from July 2019 onward.
# Pre-2019 uses BLS release calendar fallback (2nd Tuesday after reference month).
FOOD_SERIES = {
    "APU0000701111": ("APU0000701111", "BEEF_GROUND_CHUCK_LB",          "USD_PER_LB"),
    "APU0000702111": ("APU0000702111", "BEEF_ROUND_ROAST_LB",           "USD_PER_LB"),
    "APU0000702421": ("APU0000702421", "CHICKEN_WHOLE_LB",              "USD_PER_LB"),
    "APU0000706111": ("APU0000706111", "TUNA_CANNED_6OZ",               "USD_PER_CAN"),
    "APU0000706211": ("APU0000706211", "PEANUT_BUTTER_18OZ",            "USD_PER_JAR"),
    "APU0000708111": ("APU0000708111", "WHOLE_MILK_HALF_GAL",           "USD_PER_HALF_GAL"),
    "APU0000711111": ("APU0000711111", "WHITE_BREAD_LB",                "USD_PER_LB"),
    "APU0000711412": ("APU0000711412", "SPAGHETTI_MACARONI_LB",         "USD_PER_LB"),
    "APU0000712111": ("APU0000712111", "POTATOES_LB",                   "USD_PER_LB"),
    "APU0000717111": ("APU0000717111", "COFFEE_GROUND_ROAST_LB",        "USD_PER_LB"),
    "APU0000717311": ("APU0000717311", "ORANGE_JUICE_16OZ",             "USD_PER_16OZ"),
}
# APU series ALFRED coverage starts 2019-07-11
FOOD_ALFRED_START = "2019-07-01"

# ── Trade Flows ───────────────────────────────────────────────────────────────
# HS-chapter level series NOT on ALFRED. Use aggregate ALFRED series + Census
# release calendar fallback for chapter-level official_release_date.
TRADE_ALFRED_SERIES = {
    "BOPGSTB":  ("BOPGSTB", "GOODS_TRADE_BALANCE",       "USD_MILLIONS"),
}
# Census FTD release is ~37 calendar days after month end (5th week)
TRADE_RELEASE_LAG_DAYS = 37

# ── BLS release calendar constants (used as fallback) ─────────────────────────
# BLS publishes CPI and employment data on the 2nd Tuesday / 1st Friday of
# the month following the reference month. These are used when ALFRED has
# no coverage for a series/period.
BLS_CPI_RELEASE_LAG_DAYS  = 16   # approx — 2nd Tuesday of following month
BLS_EMPL_RELEASE_LAG_DAYS = 7    # approx — 1st Friday of following month
