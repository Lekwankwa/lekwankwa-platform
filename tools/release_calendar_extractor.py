"""
Release Calendar Extractor -- Lekwankwa Corporation

Produces a SINGLE master release calendar file covering all 5 products,
all source agencies, and all country groups. Structured by product, with
per-source-group next-release entries and full product version metadata
drawn from the 2026 Product Catalog v2.

Output: release_calendar_master.json (one file, written to --vault-root)

Schema:
  {
    "generated_at": "...",
    "catalog_version": "2026_v2",
    "schema_standard": "v5.0",
    "products": {
      "food_micropricing": {
        "version": "v5.0",
        "delivery_type": "archive_and_live_feed",
        "sources": [ { "country_group": ..., "source_agency": ...,
                       "series": [...], "pit_coverage_type": ...,
                       "frequency": ..., "next_release_date": ... } ]
      },
      ...
    }
  }

CATALOG SCOPE:
  32 countries: USA + EU27 (27) + GBR + CAN + AUS + NOR
  CHE -- BLOCKED, FSO returning HTTP 503, omitted throughout
  NOR housing -- PENDING_INGESTION, no confirmed SSB residential property table

Run:
  python tools/release_calendar_extractor.py --vault-root /path/to/vault
  python tools/release_calendar_extractor.py --vault-root /path/to/vault --dry-run
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("release_calendar_extractor.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# PRODUCT CATALOG METADATA  (mirrors Lekwankwa_Product_Catalog_2026_v2)
# -------------------------------------------------------------------------

CATALOG_VERSION = "2026_v2"
SCHEMA_STANDARD = "v5.0"

# Ready status per (product_key, country_group) -- from catalog Table 1
READY_STATUS: dict[tuple[str, str], str] = {
    ("food_micropricing",                  "USA"):            "READY",
    ("food_micropricing",                  "EU27"):           "READY",
    ("food_micropricing",                  "GBR"):            "READY",
    ("food_micropricing",                  "CAN"):            "READY",
    ("food_micropricing",                  "AUS"):            "READY",
    ("food_micropricing",                  "NOR"):            "READY",
    ("wages_and_employment",               "USA"):            "READY",
    ("wages_and_employment",               "EU27"):           "READY",
    ("wages_and_employment",               "GBR"):            "READY",
    ("wages_and_employment",               "CAN"):            "READY",
    ("wages_and_employment",               "AUS"):            "READY",
    ("wages_and_employment",               "NOR"):            "READY",
    ("Housing_Supply_and_Shelter_Inflation","USA"):           "READY",
    ("Housing_Supply_and_Shelter_Inflation","EU27"):          "READY",
    ("Housing_Supply_and_Shelter_Inflation","GBR"):           "READY",
    ("Housing_Supply_and_Shelter_Inflation","CAN"):           "READY",
    ("Housing_Supply_and_Shelter_Inflation","AUS"):           "READY",
    ("Housing_Supply_and_Shelter_Inflation","NOR"):           "PENDING_INGESTION",
    ("trade_flows",                        "USA"):            "READY",
    ("trade_flows",                        "EU27"):           "READY",
    ("trade_flows",                        "GBR"):            "READY",
    ("trade_flows",                        "CAN"):            "READY",
    ("trade_flows",                        "AUS"):            "READY",
    ("trade_flows",                        "NOR"):            "READY",
    ("global_macro",                       "USA"):            "READY",
    ("global_macro",                       "EU27"):           "READY",
    ("global_macro",                       "GBR"):            "READY",
    ("global_macro",                       "CAN"):            "READY",
    ("global_macro",                       "AUS"):            "READY",
    ("global_macro",                       "NOR"):            "READY",
}

EU27 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA",
    "DEU","GRC","HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD",
    "POL","PRT","ROU","SVK","SVN","ESP","SWE",
]

PRODUCTS: dict[str, dict[str, Any]] = {
    "food_micropricing": {
        "catalog_name":    "Lekwankwa Food & Micropricing Archive",
        "version":         "v5.0",
        "delivery_type":   "archive_and_live_feed",
        "live_feed":       True,
        "frequency":       "Monthly",
        "vault_records":   "137,336+ validated records (32 countries)",
        "key_metrics": [
            "global_coicop_code (UN COICOP Level 4)",
            "observed_price_local (local currency as published)",
            "price_usd_equivalent (IMF SDR fx converted)",
            "unit_quantity_standardized (kg equivalents)",
        ],
        "sources": [
            {
                "country_group":    "USA",
                "countries":        ["USA"],
                "source_agency":    "BLS",
                "source_detail":    "BLS CPI-U (CUUR/APU series) via ALFRED",
                "pit_coverage_type":"FULL_VINTAGE",
                "series":          ["CUUR0000SAF1 (Food at home)", "APU series (item-level)"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "EU27",
                "countries":        EU27,
                "source_agency":    "EUROSTAT",
                "source_detail":    "Eurostat HICP (prc_hicp_midx, CP01 basket)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["prc_hicp_midx -- CP01 Food and non-alcoholic beverages"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "GBR",
                "countries":        ["GBR"],
                "source_agency":    "ONS",
                "source_detail":    "ONS CPI (CDID codes via api.beta.ons.gov.uk)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["CDID CPI Food series"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "CAN",
                "countries":        ["CAN"],
                "source_agency":    "STATCAN",
                "source_detail":    "StatCan CPI (NDM CSV, vector-filtered)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["StatCan CPI Food vector"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "AUS",
                "countries":        ["AUS"],
                "source_agency":    "ABS",
                "source_detail":    "ABS CPI (SDMX api.data.abs.gov.au)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["ABS CPI Food sub-index"],
                "frequency":        "Quarterly",
            },
            {
                "country_group":    "NOR",
                "countries":        ["NOR"],
                "source_agency":    "SSB",
                "source_detail":    "SSB Consumer Price Index (PX-Web Table)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["SSB CPI Food basket"],
                "frequency":        "Monthly",
            },
        ],
    },

    "wages_and_employment": {
        "catalog_name":    "Lekwankwa Wages & Labor Archive",
        "version":         "v5.0",
        "delivery_type":   "archive_and_live_feed",
        "live_feed":       True,
        "frequency":       "Monthly",
        "vault_records":   "Multi-source: CES + CPS (USA) + LFS (EU27 + non-EU)",
        "key_metrics": [
            "TOTAL_NONFARM_PAYROLLS (unit: THOUSANDS)",
            "AVG_HOURLY_EARNINGS_PRIVATE (USD/hr -- USA/GBR only)",
            "UNEMPLOYMENT_RATE_U3 (headline SA rate)",
            "UNEMPLOYMENT_RATE_U6 (broad unemployment)",
            "LABOR_FORCE_PARTICIPATION_RATE",
        ],
        "notes": "CAN/AUS/NOR use employment count as wage proxy -- no wage level series ingested",
        "sources": [
            {
                "country_group":    "USA",
                "countries":        ["USA"],
                "source_agency":    "BLS",
                "source_detail":    "BLS CES (CES* series) + BLS CPS (LNS* series) via ALFRED",
                "pit_coverage_type":"FULL_VINTAGE",
                "series":          [
                    "CES0000000001 (Total Nonfarm Payrolls)",
                    "CES0500000003 (Avg Hourly Earnings Private)",
                    "LNS14000000 (Unemployment Rate U-3)",
                    "LNS13327709 (Unemployment Rate U-6)",
                    "LNS11300000 (Labor Force Participation Rate)",
                ],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "EU27",
                "countries":        EU27,
                "source_agency":    "EUROSTAT",
                "source_detail":    "Eurostat LFS (lfsa series) + une_rt_m (unemployment)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["lfsa_ergaed (employment)", "une_rt_m (unemployment rate)"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "GBR",
                "countries":        ["GBR"],
                "source_agency":    "ONS",
                "source_detail":    "ONS LFS (AWE genuine wage data, CDID codes)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["ONS AWE (Average Weekly Earnings)", "ONS LFS unemployment"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "CAN",
                "countries":        ["CAN"],
                "source_agency":    "STATCAN",
                "source_detail":    "StatCan LFS (employment count proxy -- no wage level series)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["StatCan LFS employment count"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "AUS",
                "countries":        ["AUS"],
                "source_agency":    "ABS",
                "source_detail":    "ABS LFS (employment proxy)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["ABS Labour Force Survey employment"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "NOR",
                "countries":        ["NOR"],
                "source_agency":    "SSB",
                "source_detail":    "SSB LFS Table 07458 (quarterly 1997-2024)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["SSB Table 07458 (LFS)"],
                "frequency":        "Quarterly",
            },
        ],
    },

    "Housing_Supply_and_Shelter_Inflation": {
        "catalog_name":    "Lekwankwa Housing & Credit Archive",
        "version":         "v5.0",
        "delivery_type":   "archive_only",
        "live_feed":       False,
        "frequency":       "Monthly/Quarterly (mixed)",
        "vault_records":   "Multi-layer: permits + HPI + CPI shelter",
        "key_metrics": [
            "AUTHORIZED_PERMITS_TOTAL_UNITS (unit: UNITS_SAAR)",
            "CPI_RENT_OF_PRIMARY_RESIDENCE (INDEX -- BLS CUUR0000SEHA / Eurostat HICP CP041)",
            "HOUSE_PRICE_INDEX_PURCHASE_ONLY (INDEX_2015_100, quarterly all markets)",
        ],
        "notes": "Archive only -- mixed monthly/quarterly frequency. NOR housing PENDING (no confirmed SSB residential property table).",
        "sources": [
            {
                "country_group":    "USA",
                "countries":        ["USA"],
                "source_agency":    "CENSUS / BLS / FHFA",
                "source_detail":    "Census BPS (PERMIT series via ALFRED) + BLS CPI Shelter (CUUR/CUSR) + FHFA HPI",
                "pit_coverage_type":"FULL_VINTAGE",
                "series":          [
                    "PERMIT (New Private Housing Units Authorized)",
                    "CUUR0000SEHA (CPI Rent of Primary Residence)",
                    "FHFA HPI (Purchase Only, Quarterly)",
                ],
                "frequency":        "Monthly + Quarterly",
            },
            {
                "country_group":    "EU27",
                "countries":        EU27,
                "source_agency":    "EUROSTAT",
                "source_detail":    "Eurostat sts_cobp_m (permits) + prc_hpi_q (HPI quarterly) + HICP CP041 (rent)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          [
                    "sts_cobp_m (building permits monthly)",
                    "prc_hpi_q (House Price Index quarterly)",
                    "prc_hicp_midx CP041 (actual rentals for housing)",
                ],
                "frequency":        "Monthly + Quarterly",
            },
            {
                "country_group":    "GBR",
                "countries":        ["GBR"],
                "source_agency":    "ONS",
                "source_detail":    "ONS HPI + ONS Building Statistics",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["ONS UK HPI", "ONS Building Statistics permits"],
                "frequency":        "Monthly + Quarterly",
            },
            {
                "country_group":    "CAN",
                "countries":        ["CAN"],
                "source_agency":    "STATCAN / CMHC",
                "source_detail":    "StatCan + CMHC Housing Starts",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["CMHC Housing Starts", "StatCan New Housing Price Index"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "AUS",
                "countries":        ["AUS"],
                "source_agency":    "ABS",
                "source_detail":    "ABS RPPI (series_key_filter 0:0:8:0 -- confirmed after dataflow fix)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["ABS RPPI (Residential Property Price Index)"],
                "frequency":        "Quarterly",
            },
            {
                "country_group":    "NOR",
                "countries":        ["NOR"],
                "source_agency":    "SSB",
                "source_detail":    "PENDING -- no confirmed SSB residential property table",
                "pit_coverage_type":"PENDING_INGESTION",
                "series":          [],
                "frequency":        "N/A",
                "status":           "PENDING_INGESTION",
            },
        ],
    },

    "trade_flows": {
        "catalog_name":    "Lekwankwa Trade Flows Archive (HS-Code Level)",
        "version":         "v5.0",
        "delivery_type":   "archive_and_live_feed",
        "live_feed":       True,
        "frequency":       "Monthly",
        "vault_records":   "~43,020+ USA vault; HS2-chapter level across all 32 markets",
        "key_metrics": [
            "EXPORTS_FOB_{COMMODITY} (USD_MILLIONS, HS2-chapter)",
            "IMPORTS_CIF_{COMMODITY} (USD_MILLIONS, HS2-chapter)",
            "TRADE_BALANCE_GOODS (aggregate goods balance)",
        ],
        "notes": "GBR and AUS carry 90-day publication lag -- signals structurally 2 months behind USA/EU27",
        "sources": [
            {
                "country_group":    "USA",
                "countries":        ["USA"],
                "source_agency":    "CENSUS / BEA",
                "source_detail":    "Census FTD (HS-code level, FT-900 + full HS2 chapter series 2010+) via ALFRED",
                "pit_coverage_type":"FULL_VINTAGE",
                "series":          [
                    "FT-900 HS2 Export chapters (96 series)",
                    "FT-900 HS2 Import chapters (97 series)",
                    "BOPGSTB (Trade Balance Goods & Services, ALFRED)",
                ],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "EU27",
                "countries":        EU27,
                "source_agency":    "EUROSTAT",
                "source_detail":    "Eurostat COMEXT (full HS-code trade database)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["DS-018995 (COMEXT extra-EU trade by HS chapter)"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "GBR",
                "countries":        ["GBR"],
                "source_agency":    "HMRC",
                "source_detail":    "HMRC Overseas Trade Statistics",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["HMRC OTS HS2-chapter exports and imports"],
                "frequency":        "Monthly",
                "publication_lag":  "~90 days",
            },
            {
                "country_group":    "CAN",
                "countries":        ["CAN"],
                "source_agency":    "STATCAN",
                "source_detail":    "StatCan International Trade (NDM CSV)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["StatCan International Merchandise Trade"],
                "frequency":        "Monthly",
            },
            {
                "country_group":    "AUS",
                "countries":        ["AUS"],
                "source_agency":    "ABS",
                "source_detail":    "ABS International Trade (SDMX) -- 90-day publication lag confirmed",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["ABS International Trade in Goods and Services"],
                "frequency":        "Monthly",
                "publication_lag":  "~90 days",
            },
            {
                "country_group":    "NOR",
                "countries":        ["NOR"],
                "source_agency":    "SSB",
                "source_detail":    "SSB Table 12308 (monthly merchandise trade, stride-based PubliseringMnd dedup)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["SSB Table 12308 (merchandise trade)"],
                "frequency":        "Monthly",
            },
        ],
    },

    "global_macro": {
        "catalog_name":    "Lekwankwa Global Macro Baseline Archive",
        "version":         "v5.0",
        "delivery_type":   "archive_only",
        "live_feed":       False,
        "frequency":       "Monthly/Quarterly (mixed)",
        "vault_records":   "81,735 validated records (USA: ALFRED + IMF WEO)",
        "key_metrics": [
            "GDP_GROWTH_QOQ (Quarterly SA, PERCENTAGE)",
            "GDP_GROWTH_YOY (Annual growth rate, PERCENTAGE)",
            "CPI_HEADLINE_YOY (Headline CPI inflation, PERCENTAGE)",
            "CPI_CORE_YOY (Core ex-food-and-energy, PERCENTAGE)",
            "INDUSTRIAL_PRODUCTION_MOM (Month-on-month SA, PERCENTAGE)",
        ],
        "notes": "Archive only -- GDP is natively quarterly across all 32 markets. USA via ALFRED delivers full multi-decade revision history including advance, second, and third estimate vintages.",
        "sources": [
            {
                "country_group":    "USA",
                "countries":        ["USA"],
                "source_agency":    "BEA / FEDERAL RESERVE / BLS / IMF",
                "source_detail":    "BEA (GDP/PCE) + Federal Reserve (INDPRO) + BLS (CPI) via ALFRED back to 1913; IMF WEO bi-annual",
                "pit_coverage_type":"FULL_VINTAGE",
                "series":          [
                    "GDPC1 (Real GDP, ALFRED)",
                    "CPIAUCSL (CPI All Urban, ALFRED)",
                    "UNRATE (Unemployment Rate, ALFRED)",
                    "PAYEMS (Nonfarm Payrolls, ALFRED)",
                    "FEDFUNDS (Federal Funds Rate, ALFRED)",
                    "INDPRO (Industrial Production, ALFRED)",
                    "IMF WEO -- 8 indicators (NGDP_RPCH, LUR, PCPIPCH, etc.)",
                ],
                "frequency":        "Monthly + Quarterly + Bi-annual (IMF)",
            },
            {
                "country_group":    "EU27",
                "countries":        EU27,
                "source_agency":    "EUROSTAT",
                "source_detail":    "Eurostat National Accounts (nama_10) + Flash Estimates",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          [
                    "nama_10_gdp (GDP main aggregates)",
                    "Flash GDP estimates (t+30 days)",
                ],
                "frequency":        "Quarterly",
            },
            {
                "country_group":    "GBR",
                "countries":        ["GBR"],
                "source_agency":    "ONS",
                "source_detail":    "ONS GDP (monthly and quarterly)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["ONS GDP monthly estimate", "ONS Quarterly National Accounts"],
                "frequency":        "Monthly + Quarterly",
            },
            {
                "country_group":    "CAN",
                "countries":        ["CAN"],
                "source_agency":    "STATCAN",
                "source_detail":    "StatCan National Accounts (NDM CSV) -- back to 1914",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["StatCan GDP by industry", "StatCan National Accounts"],
                "frequency":        "Monthly + Quarterly",
            },
            {
                "country_group":    "AUS",
                "countries":        ["AUS"],
                "source_agency":    "ABS",
                "source_detail":    "ABS National Accounts (SDMX ANA_AGG)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          ["ABS ANA_AGG (National Accounts Aggregates)"],
                "frequency":        "Quarterly",
            },
            {
                "country_group":    "NOR",
                "countries":        ["NOR"],
                "source_agency":    "SSB",
                "source_detail":    "SSB National Accounts Table 09190 (quarterly back to 1978)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          ["SSB Table 09190 (National Accounts)"],
                "frequency":        "Quarterly",
            },
        ],
    },
}


# -------------------------------------------------------------------------
# SOURCE FETCHERS
# Each returns next_release_date (ISO string or None) for a given
# product_key + country_group. Failures are silenced -- live dates are
# best-effort; static metadata is always present.
# -------------------------------------------------------------------------

def _fetch_bls_next(product_key: str) -> str | None:
    try:
        import requests
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://www.bls.gov/schedule/news_release/",
            timeout=20, headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        target = {
            "food_micropricing":    "Consumer Price Index",
            "wages_and_employment": "Employment Situation",
            "global_macro":         "Consumer Price Index",
        }.get(product_key)
        if not target:
            return None
        for row in soup.select("table tr"):
            text = row.get_text(" ", strip=True)
            if target in text:
                for cell in row.find_all(["td", "th"]):
                    t = cell.get_text(strip=True)
                    if any(m in t for m in ["January","February","March","April","May","June",
                                            "July","August","September","October","November","December"]):
                        return t
        return None
    except Exception as exc:
        logger.debug(f"[BLS] {product_key}: {exc}")
        return None


def _fetch_census_next(product_key: str) -> str | None:
    try:
        import requests
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://www.census.gov/economic-indicators/",
            timeout=20, headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        target = {
            "Housing_Supply_and_Shelter_Inflation": "New Residential Construction",
            "trade_flows": "U.S. International Trade",
        }.get(product_key)
        if not target:
            return None
        for row in soup.select("table tr"):
            if target in row.get_text():
                cells = row.find_all("td")
                if len(cells) >= 2:
                    return cells[-1].get_text(strip=True) or None
        return None
    except Exception as exc:
        logger.debug(f"[Census] {product_key}: {exc}")
        return None


def _fetch_bea_next() -> str | None:
    try:
        import requests
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://www.bea.gov/news/schedule",
            timeout=20, headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.select("table tr"):
            if "Gross Domestic Product" in row.get_text():
                cells = row.find_all("td")
                if cells:
                    return cells[0].get_text(strip=True) or None
        return None
    except Exception as exc:
        logger.debug(f"[BEA]: {exc}")
        return None


def _fetch_eurostat_next(dataflow: str) -> str | None:
    try:
        import requests
        url = f"https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?dataflow={dataflow}"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return None  # Eurostat TOC doesn't expose next-release dates directly
    except Exception as exc:
        logger.debug(f"[Eurostat] {dataflow}: {exc}")
        return None


def _fetch_ons_next(product_key: str) -> str | None:
    try:
        import requests
        resp = requests.get(
            "https://api.beta.ons.gov.uk/v1/releases",
            timeout=20, params={"limit": 50},
        )
        resp.raise_for_status()
        data = resp.json()
        keyword = {
            "food_micropricing":    "Consumer Price",
            "wages_and_employment": "Labour Force",
            "Housing_Supply_and_Shelter_Inflation": "House Price",
            "trade_flows":          "Trade",
            "global_macro":         "GDP",
        }.get(product_key, "")
        for item in data.get("items", []):
            if keyword.lower() in item.get("description", {}).get("title", "").lower():
                return item.get("description", {}).get("releaseDate")
        return None
    except Exception as exc:
        logger.debug(f"[ONS] {product_key}: {exc}")
        return None


def _fetch_statcan_next(product_key: str) -> str | None:
    try:
        import requests
        resp = requests.get(
            "https://www150.statcan.gc.ca/t1/wds/rest/getChangedSeriesList",
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("object", [])
        if items:
            return items[0].get("releaseTime")
        return None
    except Exception as exc:
        logger.debug(f"[StatCan] {product_key}: {exc}")
        return None


def _fetch_abs_next(product_key: str) -> str | None:
    try:
        import requests
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://www.abs.gov.au/release-calendar",
            timeout=20, headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        keyword = {
            "food_micropricing":    "Consumer Price",
            "wages_and_employment": "Labour Force",
            "Housing_Supply_and_Shelter_Inflation": "Building Approvals",
            "trade_flows":          "International Trade",
            "global_macro":         "National Accounts",
        }.get(product_key, "")
        for item in soup.select("li"):
            if keyword.lower() in item.get_text().lower():
                return item.get_text(strip=True)[:60]
        return None
    except Exception as exc:
        logger.debug(f"[ABS] {product_key}: {exc}")
        return None


def _fetch_ssb_next(table_id: str) -> str | None:
    try:
        import requests
        resp = requests.get(
            f"https://data.ssb.no/api/v0/en/table/{table_id}",
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json().get("nextUpdate")
    except Exception as exc:
        logger.debug(f"[SSB] table {table_id}: {exc}")
        return None


_SSB_TABLES = {
    "food_micropricing":   "03013",   # CPI main table
    "wages_and_employment":"07458",
    "trade_flows":         "12308",
    "global_macro":        "09190",
}

_EUROSTAT_DATAFLOWS = {
    "food_micropricing":   "prc_hicp_midx",
    "wages_and_employment":"une_rt_m",
    "Housing_Supply_and_Shelter_Inflation": "sts_cobp_m",
    "trade_flows":         "DS-018995",
    "global_macro":        "nama_10_gdp",
}


def _enrich_source(source: dict, product_key: str, dry_run: bool) -> dict:
    """Add next_release_date to a source dict. Returns a shallow copy."""
    s = dict(source)
    if dry_run:
        s["next_release_date"] = None
        s["release_date_source"] = "dry_run"
        s["catalog_status"] = READY_STATUS.get((product_key, source["country_group"]), "UNKNOWN")
        return s

    nrd: str | None = None
    cg = source["country_group"]
    agency = source["source_agency"].split("/")[0].strip()

    if cg == "USA":
        if agency == "BLS":
            nrd = _fetch_bls_next(product_key)
        elif agency == "CENSUS":
            nrd = _fetch_census_next(product_key)
        elif agency == "BEA":
            nrd = _fetch_bea_next()
        # FHFA / FEDERAL RESERVE -- no public release calendar API
    elif cg == "EU27":
        df = _EUROSTAT_DATAFLOWS.get(product_key)
        if df:
            nrd = _fetch_eurostat_next(df)
    elif cg == "GBR":
        nrd = _fetch_ons_next(product_key)
    elif cg == "CAN":
        nrd = _fetch_statcan_next(product_key)
    elif cg == "AUS":
        nrd = _fetch_abs_next(product_key)
    elif cg == "NOR":
        tbl = _SSB_TABLES.get(product_key)
        if tbl:
            nrd = _fetch_ssb_next(tbl)

    s["next_release_date"] = nrd
    s["release_date_source"] = "live_fetch" if nrd else "unavailable"
    s["catalog_status"] = READY_STATUS.get((product_key, cg), "UNKNOWN")
    return s


# -------------------------------------------------------------------------
# BUILD MASTER
# -------------------------------------------------------------------------

def build_master(dry_run: bool = False) -> dict[str, Any]:
    products_out: dict[str, Any] = {}
    fetch_errors: list[str] = []

    for product_key, meta in PRODUCTS.items():
        logger.info(f"Processing {product_key} ...")
        enriched_sources = []
        for source in meta["sources"]:
            try:
                enriched_sources.append(_enrich_source(source, product_key, dry_run))
            except Exception as exc:
                fetch_errors.append(f"{product_key}/{source['country_group']}: {exc}")
                fallback = dict(source)
                fallback["next_release_date"] = None
                fallback["release_date_source"] = f"error: {exc}"
                fallback["catalog_status"] = READY_STATUS.get(
                    (product_key, source["country_group"]), "UNKNOWN"
                )
                enriched_sources.append(fallback)

        products_out[product_key] = {
            "catalog_name":  meta["catalog_name"],
            "version":       meta["version"],
            "delivery_type": meta["delivery_type"],
            "live_feed":     meta["live_feed"],
            "frequency":     meta["frequency"],
            "vault_records": meta["vault_records"],
            "key_metrics":   meta["key_metrics"],
            "notes":         meta.get("notes"),
            "sources":       enriched_sources,
        }
        ready  = sum(1 for s in enriched_sources if s.get("catalog_status") == "READY")
        total  = len(enriched_sources)
        logger.info(f"  {product_key}: {ready}/{total} source groups READY")

    master = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "catalog_version": CATALOG_VERSION,
        "schema_standard": SCHEMA_STANDARD,
        "scope": {
            "total_countries":  32,
            "country_groups":   ["USA", "EU27 (27 states)", "GBR", "CAN", "AUS", "NOR"],
            "excluded":         "CHE -- BLOCKED (FSO Swiss Stats Explorer returning HTTP 503)",
            "total_products":   len(PRODUCTS),
            "live_feed_products": [k for k, v in PRODUCTS.items() if v["live_feed"]],
            "archive_only_products": [k for k, v in PRODUCTS.items() if not v["live_feed"]],
        },
        "fetch_errors": fetch_errors,
        "products":     products_out,
    }
    return master


# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Lekwankwa Release Calendar Extractor")
    parser.add_argument("--vault-root", required=True,
                        help="Directory where release_calendar_master.json will be written")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip live source fetches; write static catalog metadata only")
    args = parser.parse_args()

    vault_root = Path(args.vault_root)
    vault_root.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("LEKWANKWA RELEASE CALENDAR EXTRACTOR")
    logger.info(f"Catalog: {CATALOG_VERSION} | Schema: {SCHEMA_STANDARD}")
    logger.info(f"Products: {len(PRODUCTS)} | Vault root: {vault_root}")
    if args.dry_run:
        logger.info("DRY RUN -- static metadata only, no live fetches")
    logger.info("=" * 70)

    master = build_master(dry_run=args.dry_run)

    out_path = vault_root / "release_calendar_master.json"
    out_path.write_text(json.dumps(master, indent=2, default=str), encoding="utf-8")
    logger.info(f"Written: {out_path}")

    if master["fetch_errors"]:
        logger.warning(f"Fetch errors ({len(master['fetch_errors'])}): {master['fetch_errors']}")

    total_sources = sum(len(p["sources"]) for p in master["products"].values())
    ready_sources = sum(
        sum(1 for s in p["sources"] if s.get("catalog_status") == "READY")
        for p in master["products"].values()
    )
    logger.info(f"Source groups: {ready_sources}/{total_sources} READY")
    logger.info("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
