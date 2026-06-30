"""
Eurostat dataflow configurations for all 5 datasets.

Each config defines:
  dataflow        Eurostat dataset ID
  static_filters  Dimension filters applied as repeated query params
  metric_code     Short code used in data_vintage_id
  sovereign_series_id_template  f-string with {iso3} placeholder
  macro_metric_name
  unit_of_measure
  release_lag_days  Estimated days from obs period start to first publication
  freq            'M' monthly, 'Q' quarterly, 'A' annual
  value_col       Which raw column to read as observed_value
  source_sub_category
"""

from __future__ import annotations
from typing import Any


# ---------------------------------------------------------------------------
# Food Pricing  →  product=food_micropricing
# ---------------------------------------------------------------------------
# prc_hicp_minr  HICP monthly data (index, 2015=100) — replaces discontinued prc_hicp_midx (frozen 2025-12)
# COICOP codes: CP0111-CP0119 (food sub-categories) + CP012 (non-alc beverages)
#               + CP01 (all food and non-alcoholic beverages)
# Key dims (in order): FREQ · UNIT · COICOP · GEO · TIME
# ---------------------------------------------------------------------------

FOOD_COICOP_CODES: list[str] = [
    "CP0111",  # Bread and cereals
    "CP0112",  # Meat
    "CP0113",  # Fish and seafood
    "CP0114",  # Milk, cheese and eggs
    "CP0115",  # Oils and fats
    "CP0116",  # Fruit
    "CP0117",  # Vegetables
    "CP0118",  # Sugar, jam, honey, chocolate and confectionery
    "CP0119",  # Food products n.e.c.
    "CP012",   # Non-alcoholic beverages
    "CP01",    # Food and non-alcoholic beverages (total)
]

FOOD_COICOP_NAMES: dict[str, str] = {
    "CP0111": "Bread and Cereals",
    "CP0112": "Meat",
    "CP0113": "Fish and Seafood",
    "CP0114": "Milk, Cheese and Eggs",
    "CP0115": "Oils and Fats",
    "CP0116": "Fruit",
    "CP0117": "Vegetables",
    "CP0118": "Sugar, Jam, Honey and Confectionery",
    "CP0119": "Other Food Products",
    "CP012":  "Non-Alcoholic Beverages",
    "CP01":   "Food and Non-Alcoholic Beverages (Total)",
}

FOOD_CONFIG: dict[str, Any] = {
    "dataflow":     "prc_hicp_minr",
    "vault_product": "food_micropricing",
    "vault_file":   "food_pricing_data.parquet",
    "static_filters": {"unit": "I15", "freq": "M"},   # I15 = 2015=100 monthly index
    "coicop_dim":   "coicop18",   # prc_hicp_minr uses ECOICOP dimension (old: "coicop")
    "coicop_codes": FOOD_COICOP_CODES,
    "freq":          "M",
    "release_lag_days": 30,    # Flash HICP: ~15th of following month
    "unit_of_measure": "INDEX_2015_100",
    "source_sub_category": "HICP_CPI",
    "start_period": "2000-01",
}


# ---------------------------------------------------------------------------
# Wages & Labor  →  product=wages_and_employment
# ---------------------------------------------------------------------------
# 1. une_rt_m  Monthly unemployment rate
#    Dims: FREQ · AGE · SEX · UNIT · GEO · TIME
#    Key:  M.Y15-74.T.PC_ACT.{GEO}
# 2. lc_lci_r2  Labour Cost Index (quarterly)
#    Dims: FREQ · UNIT · S_ADJ · NACE_R2 · GEO · TIME
#    Key:  Q.LCI.NSA.B-S.{GEO}
# ---------------------------------------------------------------------------

WAGES_CONFIGS: list[dict[str, Any]] = [
    {
        # Monthly unemployment rate, seasonally adjusted, all ages (TOTAL), both sexes
        # Dim order: freq, s_adj, age, unit, sex, geo, time
        "dataflow":     "une_rt_m",
        "metric_code":  "UNE_RT_M_SA_TOTAL",
        "macro_metric_name": "Monthly Unemployment Rate, SA (Total, Both Sexes, % of Active Pop)",
        "unit_of_measure": "PCT_ACT",
        "static_filters": {
            "freq": "M", "age": "TOTAL", "sex": "T",
            "unit": "PC_ACT", "s_adj": "SA",
        },
        "freq":          "M",
        "release_lag_days": 45,
        "source_sub_category": "LFS",
        "start_period": "2000-01",
    },
    {
        # Quarterly employment level (LFS, ages 20-64, seasonally adjusted, thousands)
        # Replaces lc_lci_r2 which is not available via this API endpoint
        "dataflow":     "lfsi_emp_q",
        "metric_code":  "EMPL_Q_SA_Y2064",
        "macro_metric_name": "Quarterly Employment, SA (Ages 20-64, Both Sexes, Thousands)",
        "unit_of_measure": "THS_PER",
        "static_filters": {
            "freq": "Q", "indic_em": "EMP_LFS", "s_adj": "SA",
            "unit": "THS_PER", "age": "Y20-64", "sex": "T",
        },
        "freq":          "Q",
        "release_lag_days": 60,
        "source_sub_category": "LFS",
        "start_period": "2000-Q1",
    },
]


# ---------------------------------------------------------------------------
# Housing & Credit  →  product=Housing_Supply_and_Shelter_Inflation
# ---------------------------------------------------------------------------
# 1. prc_hpi_q  House Price Index (quarterly)
#    Dims: FREQ · PURCHASE · UNIT · GEO · TIME
#    Key:  Q.TOTAL.INX_A_AVG.{GEO}
# 2. sts_cobp_q  Building permits (quarterly)
#    Dims: FREQ · S_ADJ · INDIC_BT · UNIT · GEO · TIME
#    Key:  Q.NSA.PERMITS.NR.{GEO}
# ---------------------------------------------------------------------------

HOUSING_CONFIGS: list[dict[str, Any]] = [
    {
        # Quarterly House Price Index, 2015=100, all dwellings, total (new + existing)
        # Unit I15_Q = index 2015=100 (quarterly chain-linked)
        "dataflow":     "prc_hpi_q",
        "metric_code":  "HPI_TOTAL_Q",
        "macro_metric_name": "House Price Index, All Dwellings (2015=100 Q, Quarterly)",
        "unit_of_measure": "INDEX_2015_100",
        "static_filters": {"freq": "Q", "purchase": "TOTAL", "unit": "I15_Q"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "HPI",
        "start_period": "2005-Q1",
    },
    {
        # Building permits: residential buildings, NSA, index 2015=100 (quarterly)
        # CPA_F41001 = Residential buildings (all types)
        # indic_bt=BPRM_DW, unit=I15, s_adj=NSA
        "dataflow":     "sts_cobp_q",
        "metric_code":  "PERMITS_RESI_Q",
        "macro_metric_name": "Building Permits, Residential (Index 2015=100, NSA, Quarterly)",
        "unit_of_measure": "INDEX_2015_100",
        # No cpa2_1 filter: availability varies by country.
        # The ingestor encodes the cpa2_1 code into sovereign_series_id so each
        # building-type sub-category becomes a distinct series.
        "static_filters": {
            "freq": "Q", "s_adj": "NSA",
            "indic_bt": "BPRM_DW", "unit": "I15",
        },
        "freq":          "Q",
        "release_lag_days": 75,
        "source_sub_category": "BUILDING_PERMITS",
        "start_period": "2000-Q1",
    },
]


# ---------------------------------------------------------------------------
# Trade Flows  →  product=trade_flows
# ---------------------------------------------------------------------------
# namq_10_gdp  Quarterly National Accounts  (P6=exports, P7=imports)
# Dims: FREQ · UNIT · S_ADJ · NA_ITEM · GEO · TIME
# Key:  Q.CP_MEUR.SCA.{NA_ITEM}.{GEO}   (current prices, million EUR, SCA)
# ---------------------------------------------------------------------------

TRADE_CONFIGS: list[dict[str, Any]] = [
    {
        "dataflow":     "namq_10_gdp",
        "metric_code":  "EXPORTS_GOODS_SERVICES",
        "na_item":      "P6",
        "macro_metric_name": "Exports of Goods and Services (CP, million EUR, SCA, Quarterly)",
        "unit_of_measure": "MIO_EUR",
        "static_filters": {"freq": "Q", "unit": "CP_MEUR", "s_adj": "SCA", "na_item": "P6"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "NATIONAL_ACCOUNTS_TRADE",
        "start_period": "2000-Q1",
    },
    {
        "dataflow":     "namq_10_gdp",
        "metric_code":  "IMPORTS_GOODS_SERVICES",
        "na_item":      "P7",
        "macro_metric_name": "Imports of Goods and Services (CP, million EUR, SCA, Quarterly)",
        "unit_of_measure": "MIO_EUR",
        "static_filters": {"freq": "Q", "unit": "CP_MEUR", "s_adj": "SCA", "na_item": "P7"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "NATIONAL_ACCOUNTS_TRADE",
        "start_period": "2000-Q1",
    },
]


# ---------------------------------------------------------------------------
# Global Macro Baseline  →  product=global_macro
# ---------------------------------------------------------------------------
# 1. namq_10_gdp  Quarterly GDP and components
#    NA items: B1GQ (GDP), P3 (consumption), P51G (GFCF), B1G (GVA)
# 2. prc_hicp_minr  HICP annual rate of change (all items, coicop18=TOTAL, unit=RCH_A)
#    prc_hicp_manr discontinued 2025-12 (ECOICOP reclassification)
#    Dims: FREQ · COICOP · GEO · TIME   Key: M.CP00.{GEO}
# ---------------------------------------------------------------------------

MACRO_CONFIGS: list[dict[str, Any]] = [
    {
        "dataflow":     "namq_10_gdp",
        "metric_code":  "GDP_B1GQ_CLV",
        "na_item":      "B1GQ",
        "macro_metric_name": "GDP, Chain-Linked Volumes (CLV10, million EUR, SCA, Quarterly)",
        "unit_of_measure": "MIO_EUR_CLV",
        "static_filters": {"freq": "Q", "unit": "CLV10_MEUR", "s_adj": "SCA", "na_item": "B1GQ"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "NATIONAL_ACCOUNTS",
        "start_period": "1995-Q1",
    },
    {
        "dataflow":     "namq_10_gdp",
        "metric_code":  "GDP_B1GQ_CP",
        "na_item":      "B1GQ",
        "macro_metric_name": "GDP, Current Prices (million EUR, SCA, Quarterly)",
        "unit_of_measure": "MIO_EUR",
        "static_filters": {"freq": "Q", "unit": "CP_MEUR", "s_adj": "SCA", "na_item": "B1GQ"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "NATIONAL_ACCOUNTS",
        "start_period": "1995-Q1",
    },
    {
        "dataflow":     "namq_10_gdp",
        "metric_code":  "GFCF_P51G_CP",
        "na_item":      "P51G",
        "macro_metric_name": "Gross Fixed Capital Formation (CP, million EUR, SCA, Quarterly)",
        "unit_of_measure": "MIO_EUR",
        "static_filters": {"freq": "Q", "unit": "CP_MEUR", "s_adj": "SCA", "na_item": "P51G"},
        "freq":          "Q",
        "release_lag_days": 90,
        "source_sub_category": "NATIONAL_ACCOUNTS",
        "start_period": "1995-Q1",
    },
    {
        # prc_hicp_manr discontinued 2025-12 (ECOICOP reclassification); replaced by prc_hicp_minr.
        # All-items code changed: coicop CP00 → coicop18 TOTAL. Unit RCH_A still available.
        "dataflow":     "prc_hicp_minr",
        "metric_code":  "HICP_ALL_YOY",
        "macro_metric_name": "HICP Annual Rate of Change, All Items (%)",
        "unit_of_measure": "PCT_YOY",
        "static_filters": {"freq": "M", "coicop18": "TOTAL", "unit": "RCH_A"},
        "freq":          "M",
        "release_lag_days": 30,
        "source_sub_category": "HICP_CPI",
        "start_period": "2000-01",
    },
]


# ---------------------------------------------------------------------------
# Vault product keys — match the ALFRED vault product folder names exactly
# ---------------------------------------------------------------------------
VAULT_PRODUCT_MAP: dict[str, str] = {
    "food_pricing":       "food_micropricing",
    "wages_and_employment": "wages_and_employment",
    "housing":            "Housing_Supply_and_Shelter_Inflation",
    "trade_flows":        "trade_flows",
    "global_macro":       "global_macro",
}
