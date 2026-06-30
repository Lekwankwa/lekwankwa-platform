"""
Release Calendar Extractor -- Lekwankwa Corporation

Event-driven.  Fires automatically after each successful vault_extractor
ingestion (same GCS OBJECT_FINALIZE trigger as quality_report_generator).
NOT a fixed monthly schedule — runs whenever new data is released.

Produces a master release calendar file plus per-product geo-split slices
(Product 1-5) aligned to the Lekwankwa_Product_Catalog_2026_v2 numbering.

Outputs (all written to metadata/release_calendar/ or --metadata-root):
  release_calendar_master.json          -- full catalog, all 5 products
  Dataset 1 - Food Micropricing/
    release_calendar_dataset_1_food_micropricing_usa_only.json
    release_calendar_dataset_1_food_micropricing_eu27_only.json
    release_calendar_dataset_1_food_micropricing_non_eu_block.json
    release_calendar_dataset_1_food_micropricing_full_32_country.json
  Dataset 2 - Wages Labor/ ... (×4 geo files)
  Dataset 3 - Housing Credit/ ... (×4)
  Dataset 4 - Trade Flows/ ... (×4)
  Dataset 5 - Global Macro/ ... (×4)

GCS Deployment:
  gcloud functions deploy release-calendar-extractor \\
    --runtime python311 --trigger-resource lekwankwa-historical-vault \\
    --trigger-event google.storage.object.finalize \\
    --entry-point cloud_function_handler \\
    --memory 512Mi --timeout 300s --region us-central1

Run:
  python tools/release_calendar_extractor.py --metadata-root metadata
  python tools/release_calendar_extractor.py --metadata-root metadata --dry-run

Legacy alias (still accepted):
  python tools/release_calendar_extractor.py --vault-root /path/to/vault
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
        logging.FileHandler("/tmp/release_calendar_extractor.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# PRODUCT CATALOG METADATA  (mirrors Lekwankwa_Product_Catalog_2026_v2)
# -------------------------------------------------------------------------

CATALOG_VERSION = "2026_v2"
SCHEMA_STANDARD = "v5.0"
SCHEMA_STANDARD_LABEL = "Golden Record Schema Standard v5.0"

# ---- License tiers (apply to every product) ----------------------------
LICENSE_TIERS: dict[str, dict[str, str]] = {
    "research": {
        "tier_label":     "Research License",
        "price_multiplier": "1x base",
        "permitted_use":  (
            "Internal analysis, market research, economic commentary. "
            "No systematic trading or client-facing signal distribution."
        ),
    },
    "backtesting": {
        "tier_label":     "Backtesting License",
        "price_multiplier": "2x base",
        "permitted_use":  (
            "Historical strategy testing and model validation against "
            "PIT-gated revision history data."
        ),
    },
    "algo_training": {
        "tier_label":     "Algorithm Training License",
        "price_multiplier": "3x base",
        "permitted_use":  (
            "ML model training including live deployment of trained models "
            "in production trading or forecasting systems."
        ),
    },
    "full_commercial": {
        "tier_label":     "Full Commercial License",
        "price_multiplier": "4x base",
        "permitted_use":  (
            "All uses including redistribution of derived signals, "
            "white-label applications, and client-facing data products."
        ),
    },
}

# ---- Enterprise bundles (multi-dataset) --------------------------------
ENTERPRISE_BUNDLES: dict[str, dict[str, Any]] = {
    "bundle_2_archive": {
        "bundle_label":   "2-Dataset Archive Bundle",
        "bundle_number":  "B1",
        "datasets":       "Any 2 of 5 datasets",
        "base_price_usd": 139_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~18% off individual purchase",
        "delivery":       "Archive (one-off)",
    },
    "bundle_3_archive": {
        "bundle_label":   "3-Dataset Archive Bundle",
        "bundle_number":  "B2",
        "datasets":       "Any 3 of 5 datasets",
        "base_price_usd": 186_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~22% off individual purchase",
        "delivery":       "Archive (one-off)",
    },
    "bundle_4_archive": {
        "bundle_label":   "4-Dataset Archive Bundle",
        "bundle_number":  "B3",
        "datasets":       "Any 4 of 5 datasets",
        "base_price_usd": 229_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~24% off individual purchase",
        "delivery":       "Archive (one-off)",
    },
    "complete_vault": {
        "bundle_label":   "Complete 32-Country Archive — The Vault",
        "bundle_number":  "B4",
        "datasets":       "All 5 datasets (32 countries)",
        "base_price_usd": 254_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~27% off ($153,000 saving vs individual)",
        "delivery":       "Archive (one-off)",
    },
    "bundle_2_live_feed": {
        "bundle_label":   "2 Live Feed Bundle",
        "bundle_number":  "B5",
        "datasets":       "Any 2 of 3 live-eligible datasets (Food, Wages, Trade)",
        "base_price_usd_per_year": 98_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~18% off individual feed prices",
        "delivery":       "Live feed (annual subscription)",
    },
    "bundle_3_live_feed": {
        "bundle_label":   "3 Live Feed Bundle — Complete",
        "bundle_number":  "B6",
        "datasets":       "Food + Wages + Trade (all 3 live-eligible datasets)",
        "base_price_usd_per_year": 164_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "~12% off individual feed prices",
        "delivery":       "Live feed (annual subscription)",
    },
    "super_bundle": {
        "bundle_label":   "Super Bundle — Archive + Year 1 Live Feed",
        "bundle_number":  "B7",
        "datasets":       "All 5 datasets (archive) + Food + Wages + Trade (12-month live feed)",
        "base_price_usd": 376_000,
        "license_tier":   "Research (1x) — multiply for higher tiers",
        "savings":        "10% off vs separate purchase",
        "delivery":       "Archive (one-off) + 12-month live feed",
    },
}

# ---- Geographic sub-bundles (all 5 datasets) ---------------------------
GEO_BUNDLES: dict[str, dict[str, Any]] = {
    "usa_only": {
        "geo_label":              "USA Only (all 5 datasets)",
        "countries":              ["USA"],
        "archive_price_usd":      115_000,
        "live_feed_price_usd_yr": 66_000,
        "pit_coverage_type":      "FULL_VINTAGE (complete ALFRED revision history)",
    },
    "eu27_only": {
        "geo_label":              "Eurostat 27 Only (all 5 datasets)",
        "countries":              "EU27 (27 member states)",
        "archive_price_usd":      185_000,
        "live_feed_price_usd_yr": 91_200,
        "pit_coverage_type":      "RELEASE_DATE_ONLY / structural_ceiling",
    },
    "non_eu_block": {
        "geo_label":              "Non-EU Block — GBR + CAN + AUS + NOR (all 5 datasets)",
        "countries":              ["GBR", "CAN", "AUS", "NOR"],
        "archive_price_usd":      68_000,
        "live_feed_price_usd_yr": 36_000,
        "pit_coverage_type":      "RELEASE_DATE_ONLY / accumulating or structural_ceiling",
    },
    "full_32_country": {
        "geo_label":              "Full 32-Country (USA + EU27 + GBR + CAN + AUS + NOR)",
        "countries":              "32 countries",
        "archive_price_usd":      254_000,
        "live_feed_price_usd_yr": 164_000,
        "pit_coverage_type":      "Mixed — see per-country PIT coverage",
    },
}

# ---- Per-product bundle eligibility ------------------------------------
_ARCHIVE_BUNDLES = ["bundle_2_archive", "bundle_3_archive", "bundle_4_archive", "complete_vault", "super_bundle"]
_LIVE_BUNDLES    = ["bundle_2_live_feed", "bundle_3_live_feed"]

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
        "product_number":    1,
        "catalog_name":      "Lekwankwa Food & Micropricing Archive",
        "product_label":     "DATASET 1 — Food & Micropricing Archive v5.0",
        "version":           "v5.0",
        "schema_standard":   SCHEMA_STANDARD_LABEL,
        "delivery_type":     "archive_and_live_feed",
        "live_feed":         True,
        "frequency":         "Monthly",
        "vault_records":     "137,336+ validated records (32 countries)",
        "license_tiers":     LICENSE_TIERS,
        "bundle_eligibility": _ARCHIVE_BUNDLES + _LIVE_BUNDLES,
        "geo_sub_bundles":   list(GEO_BUNDLES.keys()),
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
                "source_detail":    "Eurostat HICP (prc_hicp_minr, CP01 basket) — replaces discontinued prc_hicp_midx (frozen 2025-12, ECOICOP reclassification 2026)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          [
                    "prc_hicp_minr -- CP01 Food and non-alcoholic beverages (ECOICOP, 2015=100)",
                    "dimension: coicop18 (renamed from coicop in prc_hicp_midx)",
                ],
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
                "source_detail":    "SSB Consumer Price Index Table 14700 (PX-Web JSON-stat2, 2025=100) — replaces discontinued Table 03013 (2015=100, frozen 2025M12)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/accumulating",
                "series":          [
                    "14700 VareTjenesteGrp=00 (all-items CPI, KpiIndMnd, 2025=100)",
                    "14700 VareTjenesteGrp=01 (Food group, KpiIndMnd, 2025=100)",
                ],
                "frequency":        "Monthly",
            },
        ],
    },

    "wages_and_employment": {
        "product_number":    2,
        "catalog_name":      "Lekwankwa Wages & Labor Archive",
        "product_label":     "DATASET 2 — Wages & Labor Archive v5.0",
        "version":           "v5.0",
        "schema_standard":   SCHEMA_STANDARD_LABEL,
        "delivery_type":     "archive_and_live_feed",
        "live_feed":         True,
        "frequency":         "Monthly",
        "vault_records":     "Multi-source: CES + CPS (USA) + LFS (EU27 + non-EU)",
        "license_tiers":     LICENSE_TIERS,
        "bundle_eligibility": _ARCHIVE_BUNDLES + _LIVE_BUNDLES,
        "geo_sub_bundles":   list(GEO_BUNDLES.keys()),
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
        "product_number":    3,
        "catalog_name":      "Lekwankwa Housing & Credit Archive",
        "product_label":     "DATASET 3 — Housing & Credit Archive v5.0",
        "version":           "v5.0",
        "schema_standard":   SCHEMA_STANDARD_LABEL,
        "delivery_type":     "archive_only",
        "live_feed":         False,
        "frequency":         "Monthly/Quarterly (mixed)",
        "vault_records":     "Multi-layer: permits + HPI + CPI shelter",
        "license_tiers":     LICENSE_TIERS,
        "bundle_eligibility": _ARCHIVE_BUNDLES,
        "geo_sub_bundles":   list(GEO_BUNDLES.keys()),
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
                "source_detail":    "Eurostat sts_cobp_m (permits) + prc_hpi_q (HPI quarterly) + HICP CP041 rent (prc_hicp_minr, replaces discontinued prc_hicp_midx)",
                "pit_coverage_type":"RELEASE_DATE_ONLY/structural_ceiling",
                "series":          [
                    "sts_cobp_m (building permits monthly)",
                    "prc_hpi_q (House Price Index quarterly)",
                    "prc_hicp_minr CP041 (actual rentals for housing, ECOICOP, coicop18 dimension)",
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
        "product_number":    4,
        "catalog_name":      "Lekwankwa Trade Flows Archive (HS-Code Level)",
        "product_label":     "DATASET 4 — Trade Flows Archive (HS-Code Level) v5.0",
        "version":           "v5.0",
        "schema_standard":   SCHEMA_STANDARD_LABEL,
        "delivery_type":     "archive_and_live_feed",
        "live_feed":         True,
        "frequency":         "Monthly",
        "vault_records":     "~43,020+ USA vault; HS2-chapter level across all 32 markets",
        "license_tiers":     LICENSE_TIERS,
        "bundle_eligibility": _ARCHIVE_BUNDLES + _LIVE_BUNDLES,
        "geo_sub_bundles":   list(GEO_BUNDLES.keys()),
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
        "product_number":    5,
        "catalog_name":      "Lekwankwa Global Macro Baseline Archive",
        "product_label":     "DATASET 5 — Global Macro Baseline Archive v5.0",
        "version":           "v5.0",
        "schema_standard":   SCHEMA_STANDARD_LABEL,
        "delivery_type":     "archive_only",
        "live_feed":         False,
        "frequency":         "Monthly/Quarterly (mixed)",
        "vault_records":     "81,735 validated records (USA: ALFRED + IMF WEO)",
        "license_tiers":     LICENSE_TIERS,
        "bundle_eligibility": _ARCHIVE_BUNDLES,
        "geo_sub_bundles":   list(GEO_BUNDLES.keys()),
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
    """
    Parse the BLS monthly schedule page(s).
    Each <td> cell begins with the calendar day number, followed by release
    names and data months.  We find cells containing the target release,
    extract the leading day number, and combine it with the page month/year
    to build the release date.  If no future date is found in the current
    month we also try the next month's page (same session, so cookies carry).
    """
    try:
        import requests
        import re as _re
        from bs4 import BeautifulSoup
        from datetime import datetime, timezone, date
        import calendar as _cal

        target = {
            "food_micropricing":    "Consumer Price Index",
            "wages_and_employment": "Employment Situation",
            "Housing_Supply_and_Shelter_Inflation": "New Residential Construction",
            "global_macro":         "Consumer Price Index",
        }.get(product_key)
        if not target:
            return None

        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        session = requests.Session()
        session.headers["User-Agent"] = ua
        now = datetime.now(timezone.utc)

        def _scan_page(url: str) -> str | None:
            """Return ISO date string of next future release from a BLS month page."""
            # Extract YYYY and MM from URL pattern .../YYYY/MM_sched.htm
            url_m = _re.search(r"/(\d{4})/(\d{2})_sched\.htm", url)
            if not url_m:
                return None
            page_year, page_month = int(url_m.group(1)), int(url_m.group(2))

            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")

            for td in soup.find_all("td"):
                txt = td.get_text(" ", strip=True)
                if target not in txt:
                    continue
                # The cell text starts with the calendar day (a 1-2 digit number)
                day_m = _re.match(r"^(\d{1,2})\b", txt)
                if not day_m:
                    continue
                day = int(day_m.group(1))
                try:
                    rel_date = datetime(page_year, page_month, day, tzinfo=timezone.utc)
                except ValueError:
                    continue
                if rel_date > now:
                    return rel_date.strftime("%Y-%m-%d")
            return None

        # Step 1: follow the redirect from /schedule/ to discover current month URL
        r0 = session.get("https://www.bls.gov/schedule/", timeout=20)
        if r0.status_code != 200:
            return None
        current_url = r0.url   # e.g. https://www.bls.gov/schedule/2026/06_sched.htm

        result = _scan_page(current_url)
        if result:
            return result

        # Step 2: try next month
        url_m = _re.search(r"/(\d{4})/(\d{2})_sched\.htm", current_url)
        if url_m:
            y, mo = int(url_m.group(1)), int(url_m.group(2))
            if mo == 12:
                y, mo = y + 1, 1
            else:
                mo += 1
            next_url = f"https://www.bls.gov/schedule/{y}/{mo:02d}_sched.htm"
            result = _scan_page(next_url)
            if result:
                return result

        return None
    except Exception as exc:
        logger.debug(f"[BLS] {product_key}: {exc}")
        return None


def _fetch_census_next(product_key: str) -> str | None:
    """
    Census Bureau publishes release schedules on product-specific pages.
    NRC: https://www.census.gov/construction/soc/schedule.html
    Trade: https://www.census.gov/foreign-trade/schedule.html
    Return first upcoming release date after today.
    """
    try:
        import requests
        import re
        from bs4 import BeautifulSoup
        from datetime import datetime, timezone
        URL_MAP = {
            "Housing_Supply_and_Shelter_Inflation": "https://www.census.gov/construction/soc/schedule.html",
            "trade_flows": "https://www.census.gov/foreign-trade/schedule.html",
        }
        url = URL_MAP.get(product_key)
        if not url:
            return None
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        now = datetime.now(timezone.utc)
        # Collect all full dates (Month D, YYYY) and return first future one
        for m in re.finditer(
            r"(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+\d{1,2},?\s+\d{4}",
            text,
        ):
            try:
                d = datetime.strptime(m.group().replace(",", "").strip(), "%B %d %Y")
                d = d.replace(tzinfo=timezone.utc)
                if d > now:
                    return m.group()
            except ValueError:
                continue
        return None
    except Exception as exc:
        logger.debug(f"[Census] {product_key}: {exc}")
        return None


def _fetch_bea_next() -> str | None:
    """
    BEA release schedule at https://www.bea.gov/news/schedule
    Schedule entries format: 'Month Day 8:30 AM ... GDP ...'
    Returns first future date entry containing GDP.
    """
    try:
        import requests
        import re
        from bs4 import BeautifulSoup
        from datetime import datetime, timezone
        resp = requests.get(
            "https://www.bea.gov/news/schedule",
            timeout=20, headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        now = datetime.now(timezone.utc)
        year = now.year
        MONTHS = ["January","February","March","April","May","June",
                  "July","August","September","October","November","December"]
        # Find each "Month Day" entry and check for GDP in the following 200 chars
        pattern = re.compile(
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{1,2})\b"
        )
        for m in pattern.finditer(text):
            snippet = text[m.start():m.start()+200]
            if "GDP" in snippet:
                try:
                    d = datetime.strptime(f"{m.group(1)} {m.group(2)} {year}", "%B %d %Y")
                    d = d.replace(tzinfo=timezone.utc)
                    if d > now:
                        return f"{m.group(1)} {m.group(2)}, {year}"
                except ValueError:
                    continue
        return None
    except Exception as exc:
        logger.debug(f"[BEA]: {exc}")
        return None


def _fetch_eurostat_next(dataflow: str) -> str | None:
    """
    Eurostat release calendar is JS-rendered. Estimate the next release
    from the most recent TIME_PERIOD in the data + publication lag.
    Falls back to a pure frequency estimate when the API returns errors.
    All results are marked "(estimated)".

    Endpoint: Eurostat SDMX 2.1 dissemination API with format=json.
    The ?format=json parameter causes the SDMX 2.1 endpoint to return
    JSON-stat (compact) format — NOT SDMX JSON (which would be format=jsondata).
    Response structure: body["dimension"][<dim_id>]["category"]["index"]
    = {period_id: seq_num}, same as standard JSON-stat2.

    QUERIES maps each dataflow to (url_suffix, is_quarterly):
      - Path-style suffix (starts with "/") → appended to /data/{df}
      - Query-param suffix (starts with "?") → appended to /data/{df}
      - None → skip API call, use _freq_estimate() directly

    All four active queries use EU27_2020 aggregate directly (not a single-country proxy).
    Confirmed working (tested 2026-06-21):
      prc_hicp_minr/M.I15.CP01.EU27_2020?lastNObservations=1 → 200, latest 2026-05
        ↳ prc_hicp_midx discontinued 2025-12 (frozen); replaced by prc_hicp_minr
          (ECOICOP reclassification). Ingest scripts use coicop18 dimension.
          This extractor reads only TIME_PERIOD — unaffected by the dimension rename.
      une_rt_m?geo=EU27_2020&lastNObservations=1             → 200, latest 2026-05
      sts_cobp_m?geo=EU27_2020&s_adj=NSA&unit=I15&lastNObservations=1 → 200, latest 2026-05
      namq_10_gdp?geo=EU27_2020&na_item=B1GQ&s_adj=NSA&startPeriod=2024-Q1 → 200, latest 2026-Q1
        ↳ lastNObservations=1 conflicts with startPeriod on namq_10_gdp — use startPeriod only.
    DS-018995 (COMEXT trade) has no release calendar API; uses pure frequency estimate.
    """
    try:
        import requests, calendar as _cal
        from datetime import datetime, timedelta, timezone
        # is_quarterly determines fallback lag (90d vs 35d) and advance step
        QUARTERLY = {"namq_10_gdp", "prc_hpi_q"}
        # (url_suffix, is_quarterly) | None = skip API, go straight to fallback
        # lastNObservations=1 is embedded in each suffix where appropriate.
        # namq_10_gdp uses startPeriod instead (lastNObservations=1 conflicts with
        # startPeriod when multiple unit types are present, yielding stale periods).
        QUERIES: dict[str, tuple[str, bool] | None] = {
            # food: CP01 = food&non-alc beverages, I15 = index (2015=100), EU27_2020 aggregate
            # prc_hicp_midx discontinued 2025-12; replaced by prc_hicp_minr (confirmed 2026-06-21)
            # Tested 2026-06-21: HTTP 200, latest 2026-05
            "prc_hicp_minr": ("/M.I15.CP01.EU27_2020?lastNObservations=1",               False),
            # unemployment proxy for LFS release timing, EU27_2020 aggregate
            # Old key M.PC_ACT.T.EU27_2020 had wrong dimension count → HTTP 400
            "une_rt_m":      ("?geo=EU27_2020&lastNObservations=1",                       False),
            # building permits EU27_2020; s_adj=NSA + unit=I15 required to stay < 413
            # Old key M.I15.TOTAL.EU27_2020 had wrong dimension count → HTTP 400
            "sts_cobp_m":    ("?geo=EU27_2020&s_adj=NSA&unit=I15&lastNObservations=1",   False),
            # COMEXT has no release calendar API — keep pure estimate
            "DS-018995":     None,
            # Quarterly GDP, EU27_2020. startPeriod avoids 413; do NOT add lastNObservations=1
            # (it conflicts with startPeriod when multiple s_adj types are in the response).
            # Old dataflow: nama_10_gdp → annual periods "2025", wrong frequency
            # Old key Q.CLV05_MEUR.B1GQ.EU27_2020: FREQ=Q invalid dim → HTTP 400
            # unit=CLV05_MEUR is also invalid for namq_10_gdp — omit it
            # Tested 2026-06-21: HTTP 200, latest 2026-Q1
            "namq_10_gdp":   ("?geo=EU27_2020&na_item=B1GQ&s_adj=NSA&startPeriod=2024-Q1", True),
        }
        is_q = dataflow in QUARTERLY
        now = datetime.now(timezone.utc)

        def _freq_estimate() -> str:
            lag = timedelta(days=90 if is_q else 35)
            return (now + lag).strftime("%Y-%m-%d") + " (estimated)"

        query_info = QUERIES.get(dataflow)
        if query_info is None:
            return _freq_estimate()

        suffix, is_q = query_info
        base = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
        # suffix already contains lastNObservations=1 where appropriate;
        # just append format=json with the correct separator
        sep = "&" if ("?" in suffix) else "?"
        url = f"{base}/data/{dataflow}{suffix}{sep}format=json"

        try:
            resp = requests.get(
                url, timeout=30,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
        except Exception:
            return _freq_estimate()


        if resp.status_code != 200:
            return _freq_estimate()

        try:
            body = resp.json()
            # JSON-stat format: body["dimension"][*]["category"]["index"] = {period_id: seq}
            # Find the time dimension by looking for date-format keys (YYYY-MM or YYYY-QN)
            dims = body.get("dimension", {})
            latest_id = None
            for _k, _v in dims.items():
                cats = list(_v.get("category", {}).get("index", {}).keys())
                if cats and any(
                    (len(c) == 7 and c[4] == "-") or "-Q" in c
                    for c in cats[:3]
                ):
                    latest_id = cats[-1]
                    break
            if not latest_id:
                return _freq_estimate()

            # Annual period id means wrong dataflow — use fallback
            if len(latest_id) == 4 and latest_id.isdigit():
                return _freq_estimate()

            period_end: datetime | None = None
            if "-Q" in latest_id:
                year, q = latest_id.split("-Q")
                month = int(q) * 3
                _, last_day = _cal.monthrange(int(year), month)
                period_end = datetime(int(year), month, last_day, tzinfo=timezone.utc)
                advance = timedelta(days=92)
                lag = timedelta(days=90)
            else:
                dt = datetime.strptime(latest_id + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
                period_end = dt + timedelta(days=31)
                advance = timedelta(days=31)
                lag = timedelta(days=35)

            next_period_end = period_end + advance
            next_release = next_period_end + lag
            while next_release <= now:
                next_release += advance
            return next_release.strftime("%Y-%m-%d") + " (estimated)"
        except Exception:
            return _freq_estimate()
    except Exception as exc:
        logger.debug(f"[Eurostat] {dataflow}: {exc}")
        return None


def _fetch_hmrc_next(product_key: str) -> str | None:
    """
    Scrape the HMRC OTS release calendar for confirmed future release dates.
    Only relevant for trade_flows (HMRC Overseas Trade Statistics).
    Confirmed lag ~44 days: May→Jul16=46d, Jun→Aug13=44d, Jul→Sep11=42d avg 44.
    """
    if product_key != "trade_flows":
        return None
    url = "https://www.uktradeinfo.com/trade-data/release-calendar/"
    try:
        import requests, re
        from bs4 import BeautifulSoup
        from datetime import datetime, timezone
        resp = requests.get(
            url,
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        now = datetime.now(timezone.utc)
        text = soup.get_text(" ", strip=True)
        date_pat = re.compile(
            r"(\d{1,2})\s+"
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{4})"
        )
        found_any = False
        for m in date_pat.finditer(text):
            found_any = True
            try:
                dt = datetime.strptime(m.group(), "%d %B %Y").replace(tzinfo=timezone.utc)
                if dt > now:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        if not found_any:
            logger.warning(
                "[HMRC] page fetched (HTTP %d) but no date pattern found — "
                "page structure may have changed. URL: %s",
                resp.status_code, url,
            )
        else:
            logger.warning(
                "[HMRC] release calendar fetched but contained no future dates "
                "(schedule not yet published, or all dates are past). URL: %s", url,
            )
        return None
    except requests.HTTPError as exc:
        logger.warning("[HMRC] HTTP error fetching %s — %s", url, exc)
        return None
    except Exception as exc:
        logger.debug(f"[HMRC] {product_key}: {exc}")
        return None


def _fetch_ons_next(product_key: str) -> str | None:
    """
    ONS beta API (api.beta.ons.gov.uk/v1/datasets/{id}) is unreliable for release dates:
      - food/wages/housing: next_release = 'To be announced' / 'TBD' / 'TBC' (not maintained)
      - global_macro: next_release = stale past date (not updated after each release)
      - housing: dataset 'index-private-housing-rental-prices' is discontinued
      - trade: handled by _fetch_hmrc_next, routed before this function

    Fix: scrape the ONS (or MHCLG for housing) bulletin /latest page directly.
    Each bulletin page has 'Next release: DD Month YYYY' near the top, maintained
    in sync with each publication cycle.

    GBR housing (IPHRP discontinued, UK HPI moved to MHCLG): scrape
    https://www.gov.uk/government/collections/uk-house-price-index-reports
    for 'Next publication of UK HPI ... DD Month YYYY'.

    Confirmed live dates (tested 2026-06-21):
      food:       22 July 2026  (CPIH bulletin)
      wages:      21 July 2026  (UK Labour Market bulletin)
      housing:    22 July 2026  (MHCLG UK HPI collection)
      global_macro: 16 July 2026 (GDP monthly estimate bulletin)
    """
    try:
        import requests, re
        from bs4 import BeautifulSoup
        from datetime import datetime, timezone

        BULLETIN_MAP: dict[str, tuple[str, str]] = {
            # (url, next_release_pattern)
            # Pattern is a regex applied to plain text; first future date match returned.
            "food_micropricing": (
                "https://www.ons.gov.uk/economy/inflationandpriceindices/bulletins/"
                "consumerpriceinflation/latest",
                r"Next\s+release:\s*(\d{1,2}\s+\w+\s+\d{4})",
            ),
            "wages_and_employment": (
                "https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/"
                "employmentandemployeetypes/bulletins/uklabourmarket/latest",
                r"Next\s+release:\s*(\d{1,2}\s+\w+\s+\d{4})",
            ),
            "Housing_Supply_and_Shelter_Inflation": (
                # IPHRP discontinued; UK HPI now published by MHCLG (not ONS)
                # Page text: "Next publication of UK HPI ... will be published ... 22 July 2026."
                # Use a two-step match: find the section, then extract the date.
                "https://www.gov.uk/government/collections/uk-house-price-index-reports",
                r"Next\s+publication\s+of\s+UK\s+HPI\b(.{0,200})",
            ),
            "global_macro": (
                "https://www.ons.gov.uk/economy/grossdomesticproductgdp/bulletins/"
                "gdpmonthlyestimateuk/latest",
                r"Next\s+release:\s*(\d{1,2}\s+\w+\s+\d{4})",
            ),
        }

        MONTH_PAT = re.compile(
            r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+\d{4})\b",
            re.I,
        )

        entry = BULLETIN_MAP.get(product_key)
        if not entry:
            return None
        url, pattern = entry

        resp = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        m = re.search(pattern, text, re.I | re.S)
        if not m:
            logger.warning(
                "[ONS] %s: page fetched (HTTP %d) but release-date pattern not found — "
                "page structure may have changed. URL: %s",
                product_key, resp.status_code, url,
            )
            return None

        now = datetime.now(timezone.utc)
        # group(1) is either a date string (ONS) or a context window (MHCLG)
        candidate = m.group(1).strip()

        def _parse_future(s: str) -> str | None:
            for fmt in ("%d %B %Y", "%d %b %Y"):
                try:
                    dt = datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
                    return dt.strftime("%Y-%m-%d") if dt > now else None
                except ValueError:
                    continue
            return None

        result = _parse_future(candidate)
        if result:
            return result
        # candidate is a context window — find first future date in it
        for date_str in MONTH_PAT.findall(candidate):
            result = _parse_future(date_str)
            if result:
                return result
        logger.warning(
            "[ONS] %s: release-date section found but contained no future date "
            "(schedule not yet published, or all dates are past). URL: %s",
            product_key, url,
        )
        return None
    except requests.HTTPError as exc:
        logger.warning("[ONS] %s: HTTP error fetching %s — %s", product_key, url, exc)
        return None
    except Exception as exc:
        logger.debug(f"[ONS] {product_key}: {exc}")
        return None


def _fetch_statcan_next(product_key: str) -> str | None:
    """
    StatCan WDS getAllCubesListLite returns releaseTime (last release) per cube.
    Advance by frequency delta until the date is in the future.
    PIDs verified from getAllCubesListLite search on 2026-06-20.
    """
    try:
        import requests
        from datetime import datetime, timedelta, timezone
        # (PID, freq_days) -- all monthly
        KNOWN_PIDS: dict[str, tuple[int, int]] = {
            "food_micropricing":                    (18100004, 30),
            "wages_and_employment":                 (14100017, 30),
            "Housing_Supply_and_Shelter_Inflation": (34100292, 30),
            "trade_flows":                          (12100163, 30),
            "global_macro":                         (36100434, 30),
        }
        entry = KNOWN_PIDS.get(product_key)
        if not entry:
            return None
        pid, freq_days = entry
        resp = requests.get(
            "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        cube = next(
            (c for c in resp.json() if c.get("productId") == pid), None
        )
        if not cube:
            return None
        release_time = cube.get("releaseTime")
        if not release_time:
            return None
        last_dt = datetime.fromisoformat(release_time.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = timedelta(days=freq_days)
        next_dt = last_dt + delta
        while next_dt <= now:
            next_dt += delta
        return next_dt.strftime("%Y-%m-%d") + " (estimated)"
    except Exception as exc:
        logger.debug(f"[StatCan] {product_key}: {exc}")
        return None


def _fetch_abs_next(product_key: str) -> str | None:
    """
    ABS SDMX API has no release date metadata. Estimate from the most
    recent TIME_PERIOD in the data + publication lag.
    Frequency is detected from the TIME_PERIOD id format:
      "2025"     -> annual  (~90-day lag after year-end)
      "2025-Q4"  -> quarterly (~30-day lag after quarter-end)
      "2025-12"  -> monthly (~45-day lag after month-end)
    """
    try:
        import requests, urllib3
        from datetime import datetime, timedelta, timezone
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # (dataflow_id, lag_days)
        # trade_flows: ITGS = "International Trade in Goods" (monthly, Cat 5368.0)
        #   Confirmed ~33-day lag from ABS release calendar (May→Jul2=32d, Jun→Aug6=37d avg 33).
        # wages: LF confirmed ~23-day lag (May→Jun25=25d, Jun→Jul23=23d, Jul→Aug20=20d avg 23).
        # macro: ANA_AGG confirmed ~64-day lag (June Q→Sep2=64d, Sep Q→Dec2=63d).
        DATAFLOW_MAP = {
            "food_micropricing":                    ("CPI_Q",   30),
            "wages_and_employment":                 ("LF",      23),
            "Housing_Supply_and_Shelter_Inflation": ("RPPI",    30),
            "trade_flows":                          ("ITGS",    33),
            "global_macro":                         ("ANA_AGG", 64),
        }
        entry = DATAFLOW_MAP.get(product_key)
        if not entry:
            return None
        df, lag_days = entry
        resp = requests.get(
            f"https://api.data.abs.gov.au/data/{df}/all?lastNObservations=1",
            timeout=30,
            headers={
                "Accept": "application/vnd.sdmx.data+json;version=1.0",
                "User-Agent": "lekwankwa-vault/1.0",
            },
            verify=False,
        )
        body = resp.json()
        obs_dims = (
            body.get("data", {})
            .get("structure", {})
            .get("dimensions", {})
            .get("observation", [])
        )
        time_dim = next((d for d in obs_dims if d.get("id") == "TIME_PERIOD"), None)
        if not time_dim:
            return None
        vals = time_dim.get("values", [])
        if not vals:
            return None
        latest = vals[-1]
        # Detect frequency from TIME_PERIOD id format
        period_id = latest.get("id", "")
        if len(period_id) == 4 and period_id.isdigit():
            advance = timedelta(days=365)   # annual: "2025"
        elif "-Q" in period_id:
            advance = timedelta(days=92)    # quarterly: "2025-Q4"
        else:
            advance = timedelta(days=31)    # monthly: "2025-12"
        # Use "end" date of the latest period
        period_end_str = latest.get("end") or latest.get("start")
        if not period_end_str:
            return None
        period_end = datetime.fromisoformat(period_end_str.replace("Z", "+00:00"))
        if period_end.tzinfo is None:
            period_end = period_end.replace(tzinfo=timezone.utc)
        next_period_end = period_end + advance
        next_release = next_period_end + timedelta(days=lag_days)
        now = datetime.now(timezone.utc)
        while next_release <= now:
            next_release += advance
        return next_release.strftime("%Y-%m-%d") + " (estimated)"
    except Exception as exc:
        logger.debug(f"[ABS] {product_key}: {exc}")
        return None


def _fetch_ssb_next(table_id: str) -> str | None:
    """
    SSB PX-Web API metadata for these tables does not include nextUpdate.
    Fall back to scraping the SSB release calendar page and matching by topic.
    """
    try:
        import requests
        import re
        from bs4 import BeautifulSoup
        # First try: PX-Web metadata (works for some tables)
        resp = requests.get(
            f"https://data.ssb.no/api/v0/en/table/{table_id}",
            timeout=20,
        )
        resp.raise_for_status()
        nxt = resp.json().get("nextUpdate")
        if nxt:
            return nxt
        # Second try: SSB release calendar page
        TABLE_KEYWORDS = {
            "14700": "Consumer price",     # replaces 03013 (2015=100 → 2025=100 rebase)
            "07458": "Labour force survey",
            "12308": "External trade",
            "09190": "National accounts",
        }
        keyword = TABLE_KEYWORDS.get(table_id)
        if not keyword:
            return None
        cal_resp = requests.get(
            "https://www.ssb.no/en/kalender",
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        cal_resp.raise_for_status()
        soup = BeautifulSoup(cal_resp.text, "html.parser")
        # SSB calendar lists upcoming releases; find the row matching our topic
        for el in soup.find_all(["tr", "li", "article", "div"]):
            text = el.get_text(" ", strip=True)
            if keyword.lower() in text.lower():
                m = re.search(
                    r"\d{1,2}\.\s*\w+\s+\d{4}"          # Norwegian: "20. juni 2026"
                    r"|\d{4}-\d{2}-\d{2}"                # ISO: 2026-06-20
                    r"|(?:January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+\d{1,2},?\s+\d{4}",
                    text,
                )
                if m:
                    return m.group()
        return None
    except Exception as exc:
        logger.debug(f"[SSB] table {table_id}: {exc}")
        return None


_SSB_TABLES = {
    "food_micropricing":   "14700",   # replaces 03013 (2015=100 frozen 2025M12 → 2025=100 via 14700)
    "wages_and_employment":"07458",
    "trade_flows":         "12308",
    "global_macro":        "09190",
}

_EUROSTAT_DATAFLOWS = {
    "food_micropricing":   "prc_hicp_minr",
    "wages_and_employment":"une_rt_m",
    "Housing_Supply_and_Shelter_Inflation": "sts_cobp_m",
    "trade_flows":         "DS-018995",
    "global_macro":        "namq_10_gdp",   # quarterly GDP; was nama_10_gdp (annual)
}


def _parse_to_iso(raw: str | None, freq_days: int = 30) -> tuple[str | None, int | None]:
    """
    Normalise any raw date string to (ISO-date, days_until_release).
    Handles: ISO dates, 'Month YYYY', 'Month D, YYYY', '(estimated)' suffix,
    Norwegian quarter strings ('2. termin 2026'), and None.
    Returns (None, None) when the date is unparseable or no longer in the future
    (caller should keep it if it's the best available estimate).
    """
    import re
    from datetime import datetime, timedelta, timezone
    if not raw:
        return None, None
    now = datetime.now(timezone.utc)
    raw = raw.strip()

    # Strip " (estimated)" suffix before parsing
    is_estimated_suffix = raw.endswith("(estimated)")
    clean = raw.replace("(estimated)", "").strip()

    # Norwegian quarter: "2. termin 2026" → mid-quarter estimate
    q_match = re.match(r"(\d)\.\s*termin\s+(\d{4})", clean, re.IGNORECASE)
    if q_match:
        q, yr = int(q_match.group(1)), int(q_match.group(2))
        month = q * 3 - 1          # midpoint of quarter
        try:
            dt = datetime(yr, month, 15, tzinfo=timezone.utc)
            while dt <= now:
                dt = datetime(dt.year + (dt.month // 12), (dt.month % 12) + 1, 15, tzinfo=timezone.utc)
            days = (dt.date() - now.date()).days
            return dt.strftime("%Y-%m-%d"), days
        except Exception:
            return None, None

    # Try various explicit formats
    for fmt in (
        "%Y-%m-%d",            # 2026-07-15
        "%B %d, %Y",           # July 15, 2026
        "%B %d %Y",            # July 15 2026
        "%d %B %Y",            # 15 July 2026
        "%b %d, %Y",           # Jul 15, 2026
        "%B %Y",               # July 2026  → first of month
    ):
        try:
            dt = datetime.strptime(clean, fmt).replace(tzinfo=timezone.utc)
            if dt <= now:
                return None, None   # stale — caller will decide
            days = (dt.date() - now.date()).days
            return dt.strftime("%Y-%m-%d"), days
        except ValueError:
            continue

    return None, None


def _enrich_source(source: dict, product_key: str, dry_run: bool) -> dict:
    """
    Fetch next_release_date, normalise to ISO + days_until_release.
    Returns an enriched copy of the source dict.
    """
    s = dict(source)
    catalog_status = READY_STATUS.get((product_key, source["country_group"]), "UNKNOWN")
    s["catalog_status"] = catalog_status

    if dry_run:
        s["next_release_date"]    = None
        s["days_until_release"]   = None
        s["release_confirmed"]    = False
        s["release_date_source"]  = "dry_run"
        return s

    nrd_raw: str | None = None
    cg    = source["country_group"]
    agency = source["source_agency"].split("/")[0].strip()

    if cg == "USA":
        if agency == "BLS":
            nrd_raw = _fetch_bls_next(product_key)
        elif agency == "CENSUS":
            nrd_raw = _fetch_census_next(product_key)
        elif agency == "BEA":
            nrd_raw = _fetch_bea_next()
    elif cg == "EU27":
        df = _EUROSTAT_DATAFLOWS.get(product_key)
        if df:
            nrd_raw = _fetch_eurostat_next(df)
    elif cg == "GBR":
        if agency == "HMRC":
            nrd_raw = _fetch_hmrc_next(product_key)
        else:
            nrd_raw = _fetch_ons_next(product_key)
    elif cg == "CAN":
        nrd_raw = _fetch_statcan_next(product_key)
    elif cg == "AUS":
        nrd_raw = _fetch_abs_next(product_key)
    elif cg == "NOR":
        tbl = _SSB_TABLES.get(product_key)
        if tbl:
            nrd_raw = _fetch_ssb_next(tbl)

    # Normalise raw string → ISO + delta
    iso_date, days = _parse_to_iso(nrd_raw)
    is_estimated = nrd_raw is not None and "(estimated)" in nrd_raw

    s["next_release_date"]    = iso_date
    s["days_until_release"]   = days
    s["release_confirmed"]    = bool(iso_date) and not is_estimated
    s["release_date_source"]  = (
        "live_schedule" if iso_date and not is_estimated
        else ("estimated" if iso_date else "unavailable")
    )
    return s


# -------------------------------------------------------------------------
# RELEASE-DUE CHECK  (imported by all scraper run.py entry points)
# -------------------------------------------------------------------------

def is_release_due(
    product: str,
    country: str,
    source: str,
    as_of: str | None = None,
    calendar_root: str | None = None,
    window_days: int = 2,
) -> bool:
    """
    Return True if a new release is expected for product/country/source on
    or within `window_days` of `as_of` (default: today).

    Looks for a cached release calendar JSON in:
        metadata/release_calendar/release_calendar_master.json

    Falls back to True (always scrape) if the calendar is unavailable,
    so a missing calendar never silently blocks data ingestion.

    Parameters
    ----------
    product      : product name (e.g. "food_micropricing")
    country      : ISO alpha-3 or "EU27"
    source       : source key (e.g. "bls_cpi", "census_ft900")
    as_of        : ISO date string "YYYY-MM-DD" (default: today UTC)
    calendar_root: override path to metadata/release_calendar/
    window_days  : number of days around the release date to consider "due"
    """
    from datetime import date, timedelta

    if as_of is None:
        as_of = date.today().isoformat()

    try:
        as_of_dt = date.fromisoformat(as_of)
    except ValueError:
        logger.warning("is_release_due: invalid as_of=%r — defaulting to True", as_of)
        return True

    # Locate the master release calendar
    root = Path(calendar_root) if calendar_root else \
           Path(__file__).resolve().parents[1] / "metadata" / "release_calendar"
    master_path = root / "release_calendar_master.json"

    if not master_path.exists():
        logger.debug(
            "is_release_due: calendar not found at %s — defaulting to True",
            master_path,
        )
        return True

    try:
        import json as _json
        with open(master_path, encoding="utf-8") as fh:
            master = _json.load(fh)
    except Exception as exc:
        logger.warning("is_release_due: failed to load calendar: %s — defaulting to True", exc)
        return True

    # Navigate: master → products → [product] → sources → [source] → next_release_date
    products = master.get("products", master)   # handle both wrapping styles
    prod_data = products.get(product)
    if prod_data is None:
        logger.debug("is_release_due: product %r not in calendar — defaulting to True", product)
        return True

    # Look for country-specific entry or fall back to global
    sources = (
        prod_data.get("countries", {}).get(country, {}).get("sources", {})
        or prod_data.get("sources", {})
    )
    src_data = sources.get(source)
    if src_data is None:
        logger.debug(
            "is_release_due: source %r/%r/%r not in calendar — defaulting to True",
            product, country, source,
        )
        return True

    next_release = src_data.get("next_release_date") or src_data.get("next_release")
    if not next_release:
        logger.debug("is_release_due: no next_release_date for %r/%r/%r — defaulting to True",
                     product, country, source)
        return True

    try:
        release_dt = date.fromisoformat(str(next_release)[:10])
    except ValueError:
        logger.warning("is_release_due: unparseable next_release_date %r — defaulting to True",
                       next_release)
        return True

    # Due if the release date is within ±window_days of today
    delta = abs((release_dt - as_of_dt).days)
    due   = delta <= window_days
    logger.debug(
        "is_release_due: %s/%s/%s next=%s as_of=%s delta=%dd → %s",
        product, country, source, release_dt, as_of, delta,
        "DUE" if due else "skip",
    )
    return due


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
            "product_number":  meta["product_number"],
            "product_label":   meta["product_label"],
            "version":         meta["version"],
            "schema_standard": meta["schema_standard"],
            "delivery_type":   meta["delivery_type"],
            "live_feed":       meta["live_feed"],
            "frequency":       meta["frequency"],
            "vault_records":   meta["vault_records"],
            "key_metrics":     meta["key_metrics"],
            "notes":           meta.get("notes"),
            "sources":         enriched_sources,
        }
        ready  = sum(1 for s in enriched_sources if s.get("catalog_status") == "READY")
        total  = len(enriched_sources)
        logger.info(f"  {product_key}: {ready}/{total} source groups READY")

    master = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "schema_standard": SCHEMA_STANDARD,
        "scope": {
            "total_countries":     32,
            "country_groups":      ["USA", "EU27 (27 states)", "GBR", "CAN", "AUS", "NOR"],
            "total_products":      len(PRODUCTS),
            "live_feed_products":  [k for k, v in PRODUCTS.items() if v["live_feed"]],
            "archive_only_products": [k for k, v in PRODUCTS.items() if not v["live_feed"]],
            "validation_pipeline": "9-Stage (PIT, Schema, Sanity, Temporal, Coverage, "
                                   "Referential Integrity, Outlier, Changelog, GX)",
            "delivery_format":     "Compressed Parquet via Google Cloud Storage",
            "live_feed_sla":       "Monthly delta files within 24 hours of source publication",
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
    parser.add_argument("--metadata-root", default=None,
                        help="Root of metadata/ directory (output goes to <metadata-root>/release_calendar/). "
                             "Default: 'metadata' relative to repo root.")
    parser.add_argument("--vault-root", default=None,
                        help="Legacy alias for --metadata-root (still accepted for backward compatibility).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip live source fetches; write static catalog metadata only")
    parser.add_argument("--gcs-bucket", default=None, metavar="BUCKET",
                        help="GCS bucket to upload output, e.g. gs://lekwankwa-vault")
    parser.add_argument("--gcs-prefix", default="metadata/release_calendar", metavar="PREFIX",
                        help="GCS object prefix (default: metadata/release_calendar)")
    args = parser.parse_args()

    # Resolve root: --metadata-root takes priority over legacy --vault-root
    raw_root = args.metadata_root or args.vault_root
    if raw_root is None:
        raw_root = str(Path(__file__).resolve().parent.parent / "metadata")

    metadata_root = Path(raw_root)
    metadata_root.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("LEKWANKWA RELEASE CALENDAR EXTRACTOR")
    logger.info(f"Catalog: {CATALOG_VERSION} | Schema: {SCHEMA_STANDARD}")
    logger.info(f"Products: {len(PRODUCTS)} | Metadata root: {metadata_root}")
    if args.dry_run:
        logger.info("DRY RUN -- static metadata only, no live fetches")
    logger.info("Event-driven: fires on every new data release (not a fixed schedule)")
    logger.info("=" * 70)

    master = build_master(dry_run=args.dry_run)

    # -----------------------------------------------------------------------
    # Output tree:
    #   <metadata_root>/release_calendar/
    #       release_calendar_master.json
    #       Dataset 1 - Food Micropricing/
    #           release_calendar_dataset_1_food_micropricing_usa_only.json
    #           ...
    #       Dataset 2 - Wages Labor/  ...
    #       Dataset 3 - Housing Credit/  ...
    #       Dataset 4 - Trade Flows/  ...
    #       Dataset 5 - Global Macro/  ...
    # -----------------------------------------------------------------------
    rc_root = metadata_root / "release_calendar"
    rc_root.mkdir(parents=True, exist_ok=True)

    # Remove ALL stale release calendar files from legacy locations
    stale_globs = [
        metadata_root.glob("release_calendar_product_*.json"),
        metadata_root.glob("release_calendar_dataset_*.json"),
        metadata_root.glob("release_calendar_master.json"),
        iter([Path(__file__).parent.parent / "release_calendar_master.json"]),
    ]
    for gen in stale_globs:
        for stale in gen:
            if stale.exists():
                stale.unlink()
                logger.info(f"Removed stale file: {stale}")

    # Master goes in the Release Calendar root
    master_path = rc_root / "release_calendar_master.json"
    master_size_before = master_path.stat().st_size if master_path.exists() else 0
    master_path.write_text(
        json.dumps(master, indent=2, default=str, ensure_ascii=False), encoding="utf-8"
    )
    master_size_after = master_path.stat().st_size
    logger.info(f"Written: {master_path}  ({master_size_after:,} bytes)")

    as_of_date   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    delivery_sla = "Delta files delivered within 24 hours of source publication via GCS (Parquet)."

    DATASET_STEM = {
        "food_micropricing":                    ("dataset_1_food_micropricing",  "Dataset 1 - Food Micropricing"),
        "wages_and_employment":                 ("dataset_2_wages_labor",         "Dataset 2 - Wages Labor"),
        "Housing_Supply_and_Shelter_Inflation": ("dataset_3_housing_credit",      "Dataset 3 - Housing Credit"),
        "trade_flows":                          ("dataset_4_trade_flows",          "Dataset 4 - Trade Flows"),
        "global_macro":                         ("dataset_5_global_macro",         "Dataset 5 - Global Macro"),
    }

    GEO_BUNDLES_SLICES = [
        ("usa_only",        "USA Only",              lambda cg: cg == "USA"),
        ("eu27_only",       "Eurostat 27 Only",       lambda cg: cg == "EU27"),
        ("non_eu_block",    "GBR + CAN + AUS + NOR", lambda cg: cg in ("GBR", "CAN", "AUS", "NOR")),
        ("full_32_country", "Full 32-Country",        lambda cg: True),
    ]

    def _build_schedule(sources: list, geo_filter) -> list:
        entries = [
            {
                "country_group":       s.get("country_group"),
                "countries":           s.get("countries"),
                "source_agency":       s.get("source_agency"),
                "series_description":  s.get("source_detail"),
                "frequency":           s.get("frequency"),
                "pit_coverage_type":   s.get("pit_coverage_type"),
                "next_release_date":   s.get("next_release_date"),
                "days_until_release":  s.get("days_until_release"),
                "release_confirmed":   s.get("release_confirmed", False),
                "release_date_source": s.get("release_date_source", "unavailable"),
                "catalog_status":      s.get("catalog_status"),
            }
            for s in sources if geo_filter(s.get("country_group", ""))
        ]
        entries.sort(key=lambda e: (e["days_until_release"] is None, e["days_until_release"] or 0))
        return [{"rank": i, **e} for i, e in enumerate(entries, 1)]

    def _make_summary(schedule: list) -> dict:
        with_date = [e for e in schedule if e["days_until_release"] is not None]
        return {
            "total_source_groups":        len(schedule),
            "source_groups_ready":        sum(1 for e in schedule if e["catalog_status"] == "READY"),
            "next_release_confirmed":     sum(1 for e in schedule if e["release_date_source"] == "live_schedule"),
            "next_release_estimated":     sum(1 for e in schedule if e["release_date_source"] == "estimated"),
            "next_release_unavailable":   sum(1 for e in schedule if e["release_date_source"] == "unavailable"),
            "earliest_next_release_date": with_date[0]["next_release_date"] if with_date else None,
            "earliest_next_release_days": with_date[0]["days_until_release"] if with_date else None,
        }

    for product_key, product_data in master["products"].items():
        num     = product_data["product_number"]
        sources = product_data["sources"]
        stem, folder_name = DATASET_STEM.get(product_key, (f"dataset_{num}", f"Dataset {num}"))

        dataset_dir = rc_root / folder_name
        dataset_dir.mkdir(exist_ok=True)

        for geo_key, geo_label, geo_filter in GEO_BUNDLES_SLICES:
            schedule = _build_schedule(sources, geo_filter)
            if not schedule:
                continue

            product_label = (
                f"{product_data['product_label']} — {geo_label}"
                if geo_key != "full_32_country"
                else product_data["product_label"]
            )

            doc = {
                "document_type":    "Release Calendar — Next Publication Dates",
                "product_number":   num,
                "geo_bundle":       geo_label,
                "product_label":    product_label,
                "schema_standard":  master["schema_standard"],
                "generated_at":     master["generated_at"],
                "as_of_date":       as_of_date,
                "delivery_sla":     delivery_sla,
                "release_schedule": schedule,
                "summary":          _make_summary(schedule),
            }

            fname = f"release_calendar_{stem}_{geo_key}.json"
            out   = dataset_dir / fname
            out.write_text(json.dumps(doc, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
            logger.info(f"Written: {out}")

    if master["fetch_errors"]:
        logger.warning(f"Fetch errors ({len(master['fetch_errors'])}): {master['fetch_errors']}")

    total_sources = sum(len(p["sources"]) for p in master["products"].values())
    ready_sources = sum(
        sum(1 for s in p["sources"] if s.get("catalog_status") == "READY")
        for p in master["products"].values()
    )
    logger.info(f"Source groups: {ready_sources}/{total_sources} READY")
    logger.info("=" * 70)

    # GCS upload (runs after every new data release)
    if args.gcs_bucket:
        _upload_release_calendar_to_gcs(rc_root, args.gcs_bucket, args.gcs_prefix)

    return 0


def _upload_release_calendar_to_gcs(
    local_dir: Path, gcs_bucket: str, gcs_prefix: str
) -> None:
    """Upload all release_calendar files to GCS. Triggered on every data release."""
    try:
        from google.cloud import storage  # type: ignore
    except ImportError:
        logger.error("google-cloud-storage not installed. Run: pip install google-cloud-storage")
        return

    bucket_name = gcs_bucket.lstrip("gs://")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for local_file in sorted(local_dir.rglob("*.json")):
        rel = local_file.relative_to(local_dir)
        blob_name = f"{gcs_prefix.rstrip('/')}/{rel}".replace("\\", "/")
        bucket.blob(blob_name).upload_from_filename(
            str(local_file), content_type="application/json"
        )
        logger.info(f"Uploaded: gs://{bucket_name}/{blob_name}")


def cloud_function_handler(event: dict, context) -> None:
    """
    GCS OBJECT_FINALIZE Cloud Function entry point.
    Fires whenever any file lands in lekwankwa-historical-vault.
    Rebuilds the release calendar and uploads to GCS.
    """
    bucket = event.get("bucket", "lekwankwa-historical-vault")
    name   = event.get("name", "")
    logger.info("cloud_function_handler triggered by gs://%s/%s", bucket, name)

    gcs_bucket = f"gs://{bucket}"
    tmp_dir    = Path("/tmp/release_calendar")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    master = build_master(dry_run=False)

    # Write master JSON to /tmp then upload
    master_path = tmp_dir / "release_calendar_master.json"
    import json as _json
    master_path.write_text(_json.dumps(master, indent=2, default=str), encoding="utf-8")

    _upload_release_calendar_to_gcs(tmp_dir, gcs_bucket, "metadata/release_calendar")
    logger.info("Release calendar updated in GCS.")


if __name__ == "__main__":
    sys.exit(main())
