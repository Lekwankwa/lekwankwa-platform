"""
Statistics Norway (SSB) table + query catalog for all 5 vault products.

pit_coverage_type: RELEASE_DATE_ONLY/accumulating
  SSB PX-Web provides current-value series; no historical vintage retrieval.
  Revision detection via periodic re-fetch and diff.

Confirmed tables (probed 2026-06-18, updated 2026-06-21):
  14700  CPI monthly by goods and services (VareTjenesteGrp=00..12, 2025=100)
         Replaces discontinued 03013 (Konsumgrp, 2015=100, frozen 2025M12).
         Dimension renamed Konsumgrp→VareTjenesteGrp; total code TOTAL→00.
  09190  Quarterly GDP (Makrost=bnpb.nr23_9, ContentsCode=Faste, Tid=2024K1)
  07458  Quarterly LFS SA (ContentsCode: ArbeidslauseStyrken=unemp rate, Sysselsatt=employment)
         Tid format: 1997K1..2024K4 (112 quarters). Source frozen at 2024K4 as of 2026-06-21.
  12308  Monthly external trade in goods (ImpEks: 1=Imports 2=Exports, SITC='-' for total)
         PubliseringMnd is a revision dimension — parser deduplicates keeping latest.

Old table 08183 (CPI all items 2015=100, closed series through 2025M12) superseded by 14710.
Tables 09889 (wrong LFS) and 10644 (BOP stocks not trade) removed after probe 2026-06-18.
Housing starts table not yet identified — housing vault product skipped for NOR.

Release lag estimates:
  CPI monthly   : 14 days
  GDP quarterly : 90 days
  LFS quarterly : 45 days
  Trade monthly : 35 days (preliminary, released ~5 weeks after reference month)
"""

from __future__ import annotations
from typing import Any

PIT_COVERAGE  = "RELEASE_DATE_ONLY/accumulating"
SOURCE        = "ssb_statbank"
SOURCE_AGENCY = "SSB"
ISO3          = "NOR"

VAULT_PRODUCT_MAP: dict[str, str] = {
    "food_micropricing":                    "food_pricing_data.parquet",
    "wages_and_employment":                 "wages_employment_data.parquet",
    "Housing_Supply_and_Shelter_Inflation": "housing_data.parquet",
    "trade_flows":                          "trade_flows_data.parquet",
    "global_macro":                         "global_macro_data.parquet",
}

def _all_tid() -> dict:
    return {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}

def _all_dim(code: str) -> dict:
    return {"code": code, "selection": {"filter": "all", "values": ["*"]}}

def _item(code: str, values: list[str]) -> dict:
    return {"code": code, "selection": {"filter": "item", "values": values}}


# Each entry:
#   (table_id, query_body, metric_code, vault_product, macro_metric_name,
#    unit, release_lag_days, freq, source_sub_category[, dedup_dim])
#
# dedup_dim (optional 10th element): when a dimension has multiple values
# representing publication revisions, the parser keeps the latest non-null
# value per Tid period.

SERIES: list[tuple] = [
    # -----------------------------------------------------------------------
    # Food Pricing — CPI by goods and services (table 14700, replaces 03013)
    # Dimension VareTjenesteGrp: 00=total, 01=Food and non-alcoholic beverages
    # Replaces: 03013 (Konsumgrp, 2015=100, frozen 2025M12). Root cause confirmed
    # 2026-06-21: SSB rebase to 2025=100 with table rename and dimension rename.
    # -----------------------------------------------------------------------
    (
        "14700",
        {"query": [_item("VareTjenesteGrp", ["00"]), _item("ContentsCode", ["KpiIndMnd"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "CPI_FOOD_ALL", "food_micropricing",
        "Norway CPI Food and Non-Alcoholic Beverages, Monthly Index (2025=100)",
        "INDEX_2025_100", 14, "M", "CPI",
    ),
    (
        "14700",
        {"query": [_item("VareTjenesteGrp", ["01"]), _item("ContentsCode", ["KpiIndMnd"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "CPI_FOOD_01", "food_micropricing",
        "Norway CPI Food (Group 01), Monthly Index (2025=100)",
        "INDEX_2025_100", 14, "M", "CPI",
    ),
    (
        "14700",
        {"query": [_item("VareTjenesteGrp", ["11"]), _item("ContentsCode", ["KpiIndMnd"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "CPI_FOOD_REST", "food_micropricing",
        "Norway CPI Restaurants and Hotels (Group 11), Monthly Index (2025=100)",
        "INDEX_2025_100", 14, "M", "CPI",
    ),

    # -----------------------------------------------------------------------
    # Wages & Employment — LFS quarterly SA (table 07458)
    # Confirmed 2026-06-18: Alder=15-74, Tid=1997K1..2024K4 (112 quarters)
    # ContentsCode: Sysselsatt=employment, ArbeidslauseStyrken=unemployment rate
    # -----------------------------------------------------------------------
    (
        "07458",
        {"query": [_item("Alder", ["15-74"]), _item("ContentsCode", ["Sysselsatt"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "LFS_EMP", "wages_and_employment",
        "Norway LFS Employment, SA, Quarterly (1000 persons)",
        "THS_PERSONS", 45, "Q", "LFS",
    ),
    (
        "07458",
        {"query": [_item("Alder", ["15-74"]), _item("ContentsCode", ["ArbeidslauseStyrken"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "LFS_UNEMP_RATE", "wages_and_employment",
        "Norway LFS Unemployment Rate, SA, Quarterly (%)",
        "PCT", 45, "Q", "LFS",
    ),

    # -----------------------------------------------------------------------
    # Trade Flows — External trade in goods monthly (table 12308)
    # Confirmed 2026-06-18: SITC='-' (total), ImpEks: 1=Imports, 2=Exports
    # PubliseringMnd is a revision dimension (preliminary vs final monthly pub).
    # Fetched with all pub months; parser deduplicates keeping latest per Tid.
    # -----------------------------------------------------------------------
    (
        "12308",
        {"query": [_item("ImpEks", ["2"]), _item("SITC", ["-"]), _all_dim("PubliseringMnd"), _all_tid()],
         "response": {"format": "json-stat2"}},
        "TRADE_EXPORTS", "trade_flows",
        "Norway Exports of Goods, Monthly (NOK 1000)",
        "THS_NOK", 35, "M", "MERCHANDISE_TRADE",
        "PubliseringMnd",
    ),
    (
        "12308",
        {"query": [_item("ImpEks", ["1"]), _item("SITC", ["-"]), _all_dim("PubliseringMnd"), _all_tid()],
         "response": {"format": "json-stat2"}},
        "TRADE_IMPORTS", "trade_flows",
        "Norway Imports of Goods, Monthly (NOK 1000)",
        "THS_NOK", 35, "M", "MERCHANDISE_TRADE",
        "PubliseringMnd",
    ),

    # -----------------------------------------------------------------------
    # Global Macro Baseline
    # -----------------------------------------------------------------------
    (
        "09190",
        {"query": [
            _item("Makrost", ["bnpb.nr23_9"]),
            _item("ContentsCode", ["Faste"]),
            _all_tid()],
         "response": {"format": "json-stat2"}},
        "GDP_Q_CONST", "global_macro",
        "Norway GDP at Constant Prices, Quarterly (NOK Millions)",
        "MIO_NOK_CONST", 90, "Q", "NATIONAL_ACCOUNTS",
    ),
    (
        "14710",
        {"query": [_item("ContentsCode", ["KpiIndMnd"]), _all_tid()],
         "response": {"format": "json-stat2"}},
        "CPI_ALL", "global_macro",
        "Norway CPI All Items, Monthly Index (2025=100)",
        "INDEX_2025_100", 14, "M", "CPI",
    ),
]
