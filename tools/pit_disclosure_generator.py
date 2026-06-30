"""
tools/pit_disclosure_generator.py — Lekwankwa Corporation
=========================================================
Point-in-Time (PIT) Coverage Disclosure Generator.

Produces a master disclosure file plus per-product geo-split slices:
  quality_reports/pit_disclosure/
    pit_disclosure_master.json                          -- all 32 countries × 5 products
    Dataset 1 - Food Micropricing/
      pit_disclosure_dataset_1_food_micropricing_usa_only.json
      pit_disclosure_dataset_1_food_micropricing_eu27_only.json
      pit_disclosure_dataset_1_food_micropricing_non_eu_block.json
      pit_disclosure_dataset_1_food_micropricing_full_32_country.json
    Dataset 2 - Wages Labor/ ...
    Dataset 3 - Housing Credit/ ...
    Dataset 4 - Trade Flows/ ...
    Dataset 5 - Global Macro/ ...

Triggered:
  - Cloud Function: fires when any scraper writes a run_markers/ completion file to GCS
  - Cloud Scheduler: daily 22:00 UTC (after last scraper run at 21:00 UTC)
  - CLI: python tools/pit_disclosure_generator.py [--gcs-bucket gs://lekwankwa-metadata]

Deploy Cloud Function:
  gcloud functions deploy pit-disclosure-generator \\
    --gen2 --runtime python311 --region africa-south1 \\
    --source deploy/pit_disclosure_function \\
    --entry-point cloud_function_handler \\
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \\
    --trigger-event-filters="bucket=lekwankwa-vault" \\
    --set-env-vars VAULT_ROOT=gs://lekwankwa-vault,METADATA_BUCKET=gs://lekwankwa-metadata \\
    --service-account lekwankwa-pipeline@fluted-alloy-498317-u0.iam.gserviceaccount.com
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ---------------------------------------------------------------------------
# Schema metadata
# ---------------------------------------------------------------------------

PIT_SPEC_VERSION = "5.0"
SCHEMA_STANDARD = "Lekwankwa PIT Schema v3.0-MARKET-READY"
INGESTION_START_DATE = "2026-07-01"  # First scraper production run

# ---------------------------------------------------------------------------
# Country definitions
# ---------------------------------------------------------------------------

EU27_MEMBERS: list[tuple[str, str]] = [
    ("AUT", "Austria"), ("BEL", "Belgium"), ("BGR", "Bulgaria"),
    ("CYP", "Cyprus"), ("CZE", "Czechia"), ("DEU", "Germany"),
    ("DNK", "Denmark"), ("ESP", "Spain"), ("EST", "Estonia"),
    ("FIN", "Finland"), ("FRA", "France"), ("GRC", "Greece"),
    ("HRV", "Croatia"), ("HUN", "Hungary"), ("IRL", "Ireland"),
    ("ITA", "Italy"), ("LTU", "Lithuania"), ("LVA", "Latvia"),
    ("MLT", "Malta"), ("NLD", "Netherlands"), ("POL", "Poland"),
    ("PRT", "Portugal"), ("ROU", "Romania"), ("SVK", "Slovakia"),
    ("SVN", "Slovenia"), ("SWE", "Sweden"),
    ("EU27", "European Union (27-country aggregate)"),
]

NON_EU_SOURCES: dict[str, tuple[str, str]] = {
    "GBR": ("United Kingdom", "ONS (Office for National Statistics)"),
    "CAN": ("Canada", "Statistics Canada"),
    "AUS": ("Australia", "Australian Bureau of Statistics (ABS)"),
    "NOR": ("Norway", "Statistics Norway (SSB)"),
}

# ---------------------------------------------------------------------------
# Product definitions
# ---------------------------------------------------------------------------

DATASET_STEM: dict[str, tuple[str, str]] = {
    "food_micropricing":                    ("dataset_1_food_micropricing", "Dataset 1 - Food Micropricing"),
    "wages_and_employment":                 ("dataset_2_wages_labor",       "Dataset 2 - Wages Labor"),
    "Housing_Supply_and_Shelter_Inflation": ("dataset_3_housing_credit",    "Dataset 3 - Housing Credit"),
    "trade_flows":                          ("dataset_4_trade_flows",        "Dataset 4 - Trade Flows"),
    "global_macro":                         ("dataset_5_global_macro",       "Dataset 5 - Global Macro"),
}

PRODUCTS: dict[str, dict[str, Any]] = {
    "food_micropricing": {
        "product_number": 1,
        "product_label": "Food Micropricing",
        # USA — Tier 1
        "usa_sources": ["BLS CPI-U food sub-categories (via ALFRED)"],
        "usa_coverage_start": "2019-07-01",
        "usa_series_examples": ["CUUR0000SAF1", "CUUR0000SF11001"],
        "usa_known_gaps": ["BLS October 2025 government funding lapse: CPI not published"],
        # EU27 — Tier 3
        "eu27_eurostat_dataset": "prc_hicp_midx / prc_hicp_manr",
        "eu27_indicator": "HICP food price index (monthly)",
        "eu27_frequency": "monthly",
        "eu27_release_lag_days": 30,
        "eu27_release_lag_note": "Flash HICP published ~15th of following month",
        "eu27_precision_days": 3,
        "eu27_coverage_start": "2000-01-01",
        # Non-EU — Tier 3
        "non_eu_indicator": "CPI food sub-index (monthly)",
        "non_eu_frequency": "monthly",
        "non_eu_release_lag_days": 30,
        "non_eu_precision_days": 5,
        "non_eu_coverage_start": "2005-01-01",
    },
    "wages_and_employment": {
        "product_number": 2,
        "product_label": "Wages & Employment",
        "usa_sources": ["BLS CES (average hourly earnings)", "BLS CPS (unemployment rate) — via ALFRED"],
        "usa_coverage_start": "1947-01-01",
        "usa_series_examples": ["CES0500000003", "LNS14000000"],
        "usa_known_gaps": ["BLS October 2025 government funding lapse: some CPS releases delayed"],
        "eu27_eurostat_dataset": "lc_lci_lev",
        "eu27_indicator": "Labour Cost Index (quarterly)",
        "eu27_frequency": "quarterly",
        "eu27_release_lag_days": 75,
        "eu27_release_lag_note": "Eurostat quarterly labour cost data release schedule",
        "eu27_precision_days": 7,
        "eu27_coverage_start": "2000-Q1",
        "non_eu_indicator": "Average earnings / labour cost index (mixed frequency)",
        "non_eu_frequency": "monthly/quarterly",
        "non_eu_release_lag_days": 60,
        "non_eu_precision_days": 7,
        "non_eu_coverage_start": "2005-01-01",
    },
    "Housing_Supply_and_Shelter_Inflation": {
        "product_number": 3,
        "product_label": "Housing Supply & Shelter Inflation",
        "usa_sources": [
            "BLS CPI Shelter series (via ALFRED, from 2011)",
            "Census Building Permits Survey (via ALFRED, from 1999)",
        ],
        "usa_coverage_start": "1999-01-01",
        "usa_series_examples": ["CUSR0000SAH1", "PERMIT", "PERMIT1"],
        "usa_known_gaps": [],
        "eu27_eurostat_dataset": "prc_hpi_q (House Price Index) + sts_cobp_q (Building Permits)",
        "eu27_indicator": "House Price Index + Building Permits (quarterly)",
        "eu27_frequency": "quarterly",
        "eu27_release_lag_days": 90,
        "eu27_release_lag_note": "Eurostat quarterly national accounts release schedule",
        "eu27_precision_days": 7,
        "eu27_coverage_start": "2005-Q1",
        "non_eu_indicator": "House price index + dwelling approvals (quarterly)",
        "non_eu_frequency": "quarterly",
        "non_eu_release_lag_days": 90,
        "non_eu_precision_days": 7,
        "non_eu_coverage_start": "2005-Q1",
    },
    "trade_flows": {
        "product_number": 4,
        "product_label": "Trade Flows",
        "usa_sources": ["Census FT-900 monthly trade data (via ALFRED)"],
        "usa_coverage_start": "1992-01-01",
        "usa_series_examples": ["BOPGSTB", "BOPGEXP", "BOPGIMP"],
        "usa_known_gaps": [],
        "eu27_eurostat_dataset": "ext_lt_introeu2",
        "eu27_indicator": "Intra-EU and extra-EU trade flows (monthly)",
        "eu27_frequency": "monthly",
        "eu27_release_lag_days": 90,
        "eu27_release_lag_note": "Eurostat Balance of Payments publication schedule",
        "eu27_precision_days": 7,
        "eu27_coverage_start": "2000-01-01",
        "non_eu_indicator": "Merchandise trade (monthly)",
        "non_eu_frequency": "monthly",
        "non_eu_release_lag_days": 60,
        "non_eu_precision_days": 7,
        "non_eu_coverage_start": "2000-01-01",
    },
    "global_macro": {
        "product_number": 5,
        "product_label": "Global Macro",
        # USA — Tier 1 (ALFRED macro series) + Tier 2 (IMF)
        "usa_sources": ["FRED macro series via ALFRED (GDP, PCE, unemployment, CPI)"],
        "usa_coverage_start": "1947-01-01",
        "usa_series_examples": ["GDPC1", "UNRATE", "CPIAUCSL", "FEDFUNDS"],
        "usa_known_gaps": [],
        # EU27 — Tier 3 (Eurostat HICP YOY) + Tier 2 (IMF)
        "eu27_eurostat_dataset": "prc_hicp_minr (HICP rate of change)",
        "eu27_indicator": "HICP inflation rate year-on-year (monthly)",
        "eu27_frequency": "monthly",
        "eu27_release_lag_days": 30,
        "eu27_release_lag_note": "Flash HICP rate of change published ~15th of following month",
        "eu27_precision_days": 3,
        "eu27_coverage_start": "1997-01-01",
        "non_eu_indicator": "CPI inflation / GDP growth (quarterly)",
        "non_eu_frequency": "quarterly",
        "non_eu_release_lag_days": 75,
        "non_eu_precision_days": 7,
        "non_eu_coverage_start": "2000-01-01",
        # IMF — Tier 2 (applies to ALL countries in global_macro)
        "imf_indicators": ["NGDPD", "PCPIPCH", "LUR", "BCA", "GGXWDN", "GGXCNL"],
        "imf_indicator_labels": [
            "GDP (current USD billions)",
            "Inflation (avg consumer prices, % change)",
            "Unemployment rate",
            "Current account balance (% of GDP)",
            "Gross government debt (% of GDP)",
            "Net lending/borrowing (fiscal balance, % of GDP)",
        ],
        "imf_coverage_start": "1980-01-01",
        "imf_vintages": ["April WEO", "July Update", "October WEO", "January Update"],
    },
}

# ---------------------------------------------------------------------------
# Geo bundle definitions (same keys as release_calendar_extractor)
# ---------------------------------------------------------------------------

GEO_BUNDLES: list[tuple[str, str, Any]] = [
    ("usa_only",        "USA Only",               lambda cg: cg == "USA"),
    ("eu27_only",       "Eurostat 27 Only",        lambda cg: cg == "EU27"),
    ("non_eu_block",    "GBR + CAN + AUS + NOR",  lambda cg: cg in ("GBR", "CAN", "AUS", "NOR")),
    ("full_32_country", "Full 32-Country",         lambda cg: True),
]

# ---------------------------------------------------------------------------
# PIT field documentation (same for all products/tiers)
# ---------------------------------------------------------------------------

PIT_FIELDS_TIER1 = {
    "official_release_date": "Exact date from ALFRED realtime_start — the true FRED publication date",
    "as_of_date": "Equals official_release_date on initial publication; UTC detection timestamp on subsequent revisions",
    "revision_number": "0 = first published; N = Nth revision (as tracked in ALFRED)",
    "is_revised_figure": "False for original row; True for all revision rows",
    "data_vintage_id": "Unique identifier encoding source + country + metric + period + revision",
}

PIT_FIELDS_TIER2 = {
    "official_release_date": "Named WEO vintage publication date (e.g., 2024-04-01 for April 2024 WEO)",
    "as_of_date": "Set to official_release_date (vintage publication date)",
    "revision_number": "1 = first captured vintage; increments with each subsequent WEO vintage",
    "is_revised_figure": "False for first captured vintage; True for updates within the same observation year",
    "data_vintage_id": "Format: IMF-WEO-{INDICATOR}-{OBS_YEAR}-{Apr|Jul|Oct|Jan}",
}

PIT_FIELDS_TIER3 = {
    "official_release_date": "Estimated from source agency publication schedule: obs_date + release_lag_days",
    "as_of_date": "Set to official_release_date on initial ingestion; UTC detection timestamp on detected revisions",
    "revision_number": "1 = initial ingestion; increments when revision detected on next scraper run",
    "is_revised_figure": "False for initial ingestion row; True when changed value detected by scraper",
    "data_vintage_id": "Unique identifier encoding source + country + metric + period + ingestion sequence",
}

TIER_LIMITATIONS = {
    1: None,
    2: (
        "IMF does not revise WEO data between the four named publications. "
        "Intra-vintage changes are not captured. Exact intraday publication time not available."
    ),
    3: (
        "Historical pre-ingestion vintages are irrecoverable: the Eurostat SDMX API "
        "(includeHistory=true) and equivalent ONS/StatCan/ABS/SSB APIs do not expose prior publication vintages. "
        "Revision history is available forward from first ingestion in July 2026 only. "
        "Release dates are estimated to ±3–7 days — not extracted from API timestamps."
    ),
}

# ---------------------------------------------------------------------------
# Entry builder
# ---------------------------------------------------------------------------


def _build_usa_entry(product_key: str, product: dict) -> dict:
    return {
        "country_iso3": "USA",
        "country_name": "United States",
        "country_group": "USA",
        "pit_tier": 1,
        "pit_tier_label": "Tier 1 — ALFRED Full Bitemporal",
        "source_agency": "Bureau of Labor Statistics / U.S. Census Bureau / Federal Reserve (St. Louis Fed)",
        "source_api": "ALFRED (Archival FRED) — realtime vintage API",
        "catalog_status": "READY",
        "official_release_date_type": "exact",
        "official_release_date_precision_days": 0,
        "official_release_date_source": "ALFRED realtime_start field (true FRED publication date)",
        "revision_history_available": True,
        "revision_history_scope": "complete_since_first_publication",
        "revision_history_note": "Every revision published to FRED is captured with its exact publication date",
        "ingestion_mode": "ALFRED_bitemporal",
        "coverage_start_date": product["usa_coverage_start"],
        "source_series": product["usa_sources"],
        "vault_location": f"product={product_key}/country=USA",
        "known_gaps": product.get("usa_known_gaps", []),
        "tier_limitations": TIER_LIMITATIONS[1],
        "pit_fields": PIT_FIELDS_TIER1,
    }


def _build_eu27_entry(iso3: str, country_name: str, product_key: str, product: dict) -> dict:
    cg = "EU27"
    freq = product.get("eu27_frequency", "monthly")
    lag = product.get("eu27_release_lag_days", 45)
    precision = product.get("eu27_precision_days", 5)
    precision_note = f"±{precision} days"
    if freq == "quarterly":
        precision_note = f"±{precision} days (quarterly series)"
    return {
        "country_iso3": iso3,
        "country_name": country_name,
        "country_group": cg,
        "pit_tier": 3,
        "pit_tier_label": "Tier 3 — Release-Date Stamped Snapshot",
        "source_agency": "Eurostat",
        "source_api": "Eurostat SDMX 2.1 Dissemination API",
        "catalog_status": "READY",
        "official_release_date_type": "estimated",
        "official_release_date_precision_days": precision,
        "official_release_date_precision_note": precision_note,
        "official_release_date_source": (
            f"Computed: obs_date + {lag} days (Eurostat {freq} release lag schedule)"
        ),
        "release_lag_days": lag,
        "release_lag_note": product.get("eu27_release_lag_note", ""),
        "revision_history_available": True,
        "revision_history_scope": f"forward_from_{INGESTION_START_DATE}",
        "revision_history_note": (
            f"Pre-{INGESTION_START_DATE} vintages irrecoverable from Eurostat SDMX API. "
            "Revisions detected going forward on each monthly scraper run."
        ),
        "ingestion_mode": "eurostat_sdmx_snapshot",
        "coverage_start_date": product.get("eu27_coverage_start", "2000-01-01"),
        "eurostat_dataset": product.get("eu27_eurostat_dataset", ""),
        "indicator": product.get("eu27_indicator", ""),
        "vault_location": f"product={product_key}/country={iso3}",
        "known_gaps": [],
        "tier_limitations": TIER_LIMITATIONS[3],
        "pit_fields": PIT_FIELDS_TIER3,
    }


def _build_non_eu_entry(iso3: str, country_name: str, source_agency: str,
                        product_key: str, product: dict) -> dict:
    lag = product.get("non_eu_release_lag_days", 45)
    precision = product.get("non_eu_precision_days", 7)
    freq = product.get("non_eu_frequency", "monthly")
    return {
        "country_iso3": iso3,
        "country_name": country_name,
        "country_group": iso3,
        "pit_tier": 3,
        "pit_tier_label": "Tier 3 — Release-Date Stamped Snapshot",
        "source_agency": source_agency,
        "source_api": f"{source_agency} public data API",
        "catalog_status": "READY",
        "official_release_date_type": "estimated",
        "official_release_date_precision_days": precision,
        "official_release_date_precision_note": f"±{precision} days ({freq} series)",
        "official_release_date_source": (
            f"Computed: obs_date + {lag} days (agency publication schedule)"
        ),
        "release_lag_days": lag,
        "revision_history_available": True,
        "revision_history_scope": f"forward_from_{INGESTION_START_DATE}",
        "revision_history_note": (
            f"Pre-{INGESTION_START_DATE} vintages irrecoverable from public API. "
            "Revisions detected going forward on each monthly scraper run."
        ),
        "ingestion_mode": "national_agency_snapshot",
        "coverage_start_date": product.get("non_eu_coverage_start", "2005-01-01"),
        "indicator": product.get("non_eu_indicator", ""),
        "vault_location": f"product={product_key}/country={iso3}",
        "known_gaps": [],
        "tier_limitations": TIER_LIMITATIONS[3],
        "pit_fields": PIT_FIELDS_TIER3,
    }


def _build_imf_entry(iso3: str, country_name: str, country_group: str,
                     product: dict) -> dict:
    """For global_macro only — all countries have IMF QUAD_VINTAGE coverage."""
    return {
        "country_iso3": iso3,
        "country_name": country_name,
        "country_group": country_group,
        "pit_tier": 2,
        "pit_tier_label": "Tier 2 — IMF QUAD_VINTAGE",
        "source_agency": "International Monetary Fund",
        "source_api": "IMF World Economic Outlook (WEO) DataMapper API",
        "catalog_status": "READY",
        "official_release_date_type": "named_vintage",
        "official_release_date_precision_days": None,
        "official_release_date_precision_note": (
            "Publication month known; exact day is the scheduled WEO release date"
        ),
        "official_release_date_source": (
            "Named WEO publication cycle: April WEO (Apr-01), July Update (Jul-01), "
            "October WEO (Oct-01), January Update (Jan-01 next year)"
        ),
        "vintages_per_year": 4,
        "vintages": product.get("imf_vintages", ["April WEO", "July Update", "October WEO", "January Update"]),
        "vintage_id_format": "IMF-WEO-{INDICATOR}-{OBS_YEAR}-{Apr|Jul|Oct|Jan}",
        "revision_history_available": True,
        "revision_history_scope": "4_vintages_per_observation_year",
        "revision_history_note": (
            "Each of 4 annual WEO publications is stored as a separate vault row. "
            "IMF does not revise WEO between publications — only at the next vintage."
        ),
        "ingestion_mode": "imf_quad_vintage",
        "coverage_start_date": product.get("imf_coverage_start", "1980-01-01"),
        "indicators": product.get("imf_indicators", []),
        "indicator_labels": product.get("imf_indicator_labels", []),
        "vault_location": f"product=global_macro/country={iso3}/source=imf_weo",
        "known_gaps": [],
        "tier_limitations": TIER_LIMITATIONS[2],
        "pit_fields": PIT_FIELDS_TIER2,
    }


# ---------------------------------------------------------------------------
# Master builder
# ---------------------------------------------------------------------------


def build_entries(product_key: str, product: dict) -> list[dict]:
    """Return all 32 country entries for a product, ranked Tier 1 → 2 → 3."""
    entries: list[dict] = []

    is_macro = (product_key == "global_macro")

    # USA
    usa = _build_usa_entry(product_key, product)
    if is_macro:
        usa["also_has_tier_2_imf"] = True
        usa["imf_supplements"] = "IMF QUAD_VINTAGE also captured in parallel for all USA macro indicators"
    entries.append(usa)

    # EU27 members + aggregate
    for iso3, country_name in EU27_MEMBERS:
        if is_macro:
            # Global macro: best tier for EU27 is Tier 2 (IMF) — Tier 3 (Eurostat) also present
            imf_entry = _build_imf_entry(iso3, country_name, "EU27", product)
            eu_entry = _build_eu27_entry(iso3, country_name, product_key, product)
            imf_entry["also_has_tier_3_national"] = True
            imf_entry["tier_3_national_source"] = eu_entry["source_agency"]
            imf_entry["tier_3_national_dataset"] = eu_entry.get("eurostat_dataset", "")
            entries.append(imf_entry)
        else:
            entries.append(_build_eu27_entry(iso3, country_name, product_key, product))

    # Non-EU block
    for iso3, (country_name, agency) in NON_EU_SOURCES.items():
        if is_macro:
            imf_entry = _build_imf_entry(iso3, country_name, iso3, product)
            non_eu_entry = _build_non_eu_entry(iso3, country_name, agency, product_key, product)
            imf_entry["also_has_tier_3_national"] = True
            imf_entry["tier_3_national_source"] = non_eu_entry["source_agency"]
            entries.append(imf_entry)
        else:
            entries.append(_build_non_eu_entry(iso3, country_name, agency, product_key, product))

    # Rank by tier (Tier 1 first)
    return [{"rank": i + 1, **e} for i, e in enumerate(entries)]


def _make_summary(entries: list[dict]) -> dict:
    tiers = [e["pit_tier"] for e in entries]
    date_types = [e["official_release_date_type"] for e in entries]
    with_gaps = [e for e in entries if e.get("known_gaps")]
    return {
        "total_entries": len(entries),
        "tier_1_alfred_count": tiers.count(1),
        "tier_2_imf_count": tiers.count(2),
        "tier_3_estimated_count": tiers.count(3),
        "exact_release_date_count": date_types.count("exact"),
        "named_vintage_count": date_types.count("named_vintage"),
        "estimated_release_date_count": date_types.count("estimated"),
        "entries_with_known_gaps": len(with_gaps),
    }


def build_master(run_date: str | None = None) -> dict:
    """Build the full master PIT disclosure object."""
    now = datetime.now(timezone.utc)
    generated_at = now.isoformat(timespec="seconds")
    as_of_date = run_date or now.strftime("%Y-%m-%d")

    products_out: dict[str, Any] = {}
    for product_key, product in PRODUCTS.items():
        entries = build_entries(product_key, product)
        products_out[product_key] = {
            "product_number": product["product_number"],
            "product_label": product["product_label"],
            "entries": entries,
            "summary": _make_summary(entries),
        }

    all_entries = [e for p in products_out.values() for e in p["entries"]]

    return {
        "document_type": "Point-in-Time Coverage Disclosure",
        "schema_version": "1.0",
        "pit_specification_version": PIT_SPEC_VERSION,
        "schema_standard": SCHEMA_STANDARD,
        "generated_at": generated_at,
        "as_of_date": as_of_date,
        "ingestion_live_from": INGESTION_START_DATE,
        "total_countries": 32,
        "total_products": len(PRODUCTS),
        "tiers": {
            "tier_1_alfred": {
                "name": "USA Full Bitemporal (ALFRED)",
                "description": (
                    "All 5 USA products covered by ALFRED (Archival FRED). "
                    "Every official BLS/Census/FRED revision captured with exact publication date."
                ),
                "official_release_date_precision": "exact (day-level, 0 days error)",
                "revision_history": "Complete since first FRED publication",
                "countries": ["USA"],
            },
            "tier_2_imf_quad_vintage": {
                "name": "IMF QUAD_VINTAGE (Global Macro)",
                "description": (
                    "Global macro product for all 32 countries uses IMF WEO "
                    "QUAD_VINTAGE: 4 named vintages per observation year."
                ),
                "official_release_date_precision": "named vintage publication month",
                "revision_history": "4 snapshots per year per observation",
                "vintages": ["April WEO", "July Update", "October WEO", "January Update"],
                "countries": "all_32_countries (global_macro product only)",
            },
            "tier_3_release_date_stamped": {
                "name": "31-Country Release-Date Stamped Snapshots",
                "description": (
                    "EU27 (Eurostat SDMX), GBR (ONS), CAN (StatCan), AUS (ABS), NOR (SSB). "
                    "Official release dates estimated from agency publication schedules."
                ),
                "official_release_date_precision": "estimated ±3 days (monthly), ±7 days (quarterly)",
                "revision_history": f"Forward from {INGESTION_START_DATE}; pre-ingestion not recoverable",
                "countries": [iso3 for iso3, _ in EU27_MEMBERS] + list(NON_EU_SOURCES.keys()),
            },
        },
        "global_summary": _make_summary(all_entries),
        "disclosure_statements": {
            "usa_tier_1": (
                "USA data products carry full point-in-time vintage and revision history via ALFRED. "
                "Every official revision since original BLS/Census/FRED publication is captured "
                "with its exact publication date. Backtests can reconstruct the exact data "
                "available on any historical date with day-level precision."
            ),
            "imf_tier_2": (
                "IMF World Economic Outlook global macro data is captured in QUAD_VINTAGE format: "
                "four named vintages per observation year (April, July, October, January). "
                "Each vintage carries its exact scheduled publication date, enabling backtests "
                "to use only the WEO vintage that was available at decision time."
            ),
            "tier_3_non_usa": (
                "All 31 non-USA country datasets carry a release-date estimate on every "
                "observation, computed from each source agency's known publication schedule. "
                "Release dates are accurate to ±3 days (monthly series) and ±7 days (quarterly). "
                "Revision detection is active from July 2026: when a source agency revises a "
                "previously published value, a new versioned record is appended with "
                "is_revised_figure=True. Historical pre-ingestion revisions are not available "
                "for non-ALFRED sources due to API limitations. "
                "USA food micropricing has exact ALFRED PIT vintage and revision history from "
                "July 2019; earlier dates use estimated release dates."
            ),
        },
        "products": products_out,
    }


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def _write_json(path: Path, doc: dict) -> None:
    path.write_text(json.dumps(doc, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Written: {path}  ({path.stat().st_size:,} bytes)")


def write_outputs(
    out_dir: Path,
    master: dict,
    gcs_bucket: str | None = None,
    gcs_prefix: str = "quality_reports/pit_disclosure",
) -> None:
    pit_root = out_dir / "pit_disclosure"
    pit_root.mkdir(parents=True, exist_ok=True)

    # Master file
    _write_json(pit_root / "pit_disclosure_master.json", master)

    generated_at = master["generated_at"]
    as_of_date = master["as_of_date"]

    for product_key, product_data in master["products"].items():
        stem, folder_name = DATASET_STEM.get(
            product_key,
            (f"dataset_{product_data['product_number']}", f"Dataset {product_data['product_number']}"),
        )
        dataset_dir = pit_root / folder_name
        dataset_dir.mkdir(exist_ok=True)

        all_entries = product_data["entries"]

        for geo_key, geo_label, geo_filter in GEO_BUNDLES:
            filtered = [e for e in all_entries if geo_filter(e.get("country_group", ""))]
            if not filtered:
                continue

            product_label_geo = (
                f"{product_data['product_label']} — {geo_label}"
                if geo_key != "full_32_country"
                else product_data["product_label"]
            )

            doc = {
                "document_type": "Point-in-Time Coverage Disclosure",
                "product_number": product_data["product_number"],
                "product_label": product_label_geo,
                "geo_bundle": geo_label,
                "pit_specification_version": master["pit_specification_version"],
                "schema_standard": master["schema_standard"],
                "generated_at": generated_at,
                "as_of_date": as_of_date,
                "ingestion_live_from": master["ingestion_live_from"],
                "summary": _make_summary(filtered),
                "disclosure_statements": master["disclosure_statements"],
                "entries": [{"rank": i + 1, **{k: v for k, v in e.items() if k != "rank"}}
                            for i, e in enumerate(filtered)],
            }

            fname = f"pit_disclosure_{stem}_{geo_key}.json"
            _write_json(dataset_dir / fname, doc)

    logger.info(f"PIT disclosure: {len(PRODUCTS)} products × 4 geo splits written to {pit_root}")

    if gcs_bucket:
        _upload_to_gcs(pit_root, gcs_bucket, gcs_prefix)


def _upload_to_gcs(local_dir: Path, gcs_bucket: str, gcs_prefix: str) -> None:
    try:
        from google.cloud import storage  # type: ignore
    except ImportError:
        logger.error("google-cloud-storage not installed; skipping GCS upload")
        return

    bucket_name = gcs_bucket.lstrip("gs://").split("/")[0]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for local_file in sorted(local_dir.rglob("*.json")):
        rel = local_file.relative_to(local_dir)
        blob_name = f"{gcs_prefix.rstrip('/')}/{rel}".replace("\\", "/")
        bucket.blob(blob_name).upload_from_filename(str(local_file), content_type="application/json")
        logger.info(f"Uploaded: gs://{bucket_name}/{blob_name}")


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------


def cloud_function_handler(event: dict, context: Any) -> None:  # type: ignore
    """
    GCS OBJECT_FINALIZE trigger.
    Fires when a scraper writes a run_markers/extractor_*.complete file to the vault.
    Regenerates PIT disclosure and uploads to the metadata bucket.
    """
    name = event.get("name", "")
    import re as _re
    if not _re.match(r"run_markers/extractor_.+\.complete$", name):
        return  # not a scraper completion marker — ignore

    metadata_bucket = os.environ.get("METADATA_BUCKET", "gs://lekwankwa-metadata")
    tmp_dir = Path("/tmp/pit_disclosure_out")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    master = build_master()
    write_outputs(tmp_dir, master, gcs_bucket=metadata_bucket, gcs_prefix="quality_reports/pit_disclosure")
    logger.info("PIT disclosure updated in GCS: %s/quality_reports/pit_disclosure/", metadata_bucket)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lekwankwa PIT Disclosure Generator")
    p.add_argument(
        "--out-dir", default="/tmp/pit_disclosure_out",
        help="Local output directory (default: /tmp/pit_disclosure_out)",
    )
    p.add_argument(
        "--gcs-bucket", default=None, metavar="BUCKET",
        help="GCS bucket for upload, e.g. gs://lekwankwa-metadata (omit to skip upload)",
    )
    p.add_argument(
        "--gcs-prefix", default="quality_reports/pit_disclosure", metavar="PREFIX",
        help="GCS object prefix (default: quality_reports/pit_disclosure)",
    )
    p.add_argument(
        "--run-date", default=None,
        help="Override as_of_date (YYYY-MM-DD). Default: today UTC.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Build master and log structure without writing or uploading",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    master = build_master(run_date=args.run_date)

    if args.dry_run:
        logger.info("DRY RUN — master built, no files written")
        for pkey, pdata in master["products"].items():
            logger.info(
                "  %-40s → %d entries  (Tier1=%d T2=%d T3=%d)",
                pkey,
                pdata["summary"]["total_entries"],
                pdata["summary"]["tier_1_alfred_count"],
                pdata["summary"]["tier_2_imf_count"],
                pdata["summary"]["tier_3_estimated_count"],
            )
        sys.exit(0)

    out_dir = Path(args.out_dir)
    write_outputs(
        out_dir, master,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix,
    )
    logger.info("Done.")
    sys.exit(0)
