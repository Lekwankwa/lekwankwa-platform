"""
Lekwankwa Corporation — Institutional Data Licensing Platform
Premium multi-page Streamlit application for quantitative data product showcase.
"""

import os
import io
import json
import pathlib
import streamlit as st
import pandas as pd

# ──────────────────────────────────────────────────────────────
#  PATH RESOLUTION
# ──────────────────────────────────────────────────────────────
BASE_DIR    = pathlib.Path(__file__).parent.resolve()
NEUDATA_DIR = BASE_DIR / "neudata_submission"
LOGO_PATH   = next(
    (p for p in [
        BASE_DIR / "streamlit" / "logo.jpeg",
        BASE_DIR / "logo.jpeg",
        pathlib.Path.home() / "Downloads" / "Company Logo.jpeg",
    ] if p.exists()),
    BASE_DIR / "streamlit" / "logo.jpeg",
)

# ──────────────────────────────────────────────────────────────
#  DATASET CATALOG
#  Maps dropdown label → (parquet path, markdown dictionary path)
# ──────────────────────────────────────────────────────────────
DATASETS: dict[str, dict] = {
    "Food Micropricing — USA": {
        "parquet":  NEUDATA_DIR / "sample_parquet_food_pricing" / "food_prices_v4.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_FOOD_PRICING_DATA_DICTIONARY.md",
        "filename": "food_prices_v4.0_sample.parquet",
        "product":  "Food Micropricing",
        "schema_version": "v5.0",
        "records":  "29,825 records | 1980–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "USDA ERS / BLS CPI / ALFRED",
    },
    "Wages & Labour — CES Payroll (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_wages_and_employment" / "wages_and_employment_ces_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_WAGES_AND_EMPLOYMENT_DATA_DICTIONARY.md",
        "filename": "wages_and_employment_ces_v1.0_sample.parquet",
        "product":  "Wages & Labour",
        "schema_version": "v5.0",
        "records":  "CES schema sample | 1939–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "BLS CES",
    },
    "Wages & Labour — CPS Unemployment (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_wages_and_employment" / "wages_and_employment_cps_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_WAGES_AND_EMPLOYMENT_DATA_DICTIONARY.md",
        "filename": "wages_and_employment_cps_v1.0_sample.parquet",
        "product":  "Wages & Labour",
        "schema_version": "v5.0",
        "records":  "8,802 records (11 unemployment series) | 1948–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "BLS CPS",
    },
    "Housing — Shelter Inflation CPI (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_housing" / "housing_shelter_inflation_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_HOUSING_DATA_DICTIONARY.md",
        "filename": "housing_shelter_inflation_v1.0_sample.parquet",
        "product":  "Housing Supply & Shelter",
        "schema_version": "v5.0",
        "records":  "10,923 records | 1914–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "BLS CPI Shelter Series",
    },
    "Housing — Building Permits (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_housing" / "housing_permits_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_HOUSING_DATA_DICTIONARY.md",
        "filename": "housing_permits_v1.0_sample.parquet",
        "product":  "Housing Supply & Shelter",
        "schema_version": "v5.0",
        "records":  "8,564 records | 1960–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "US Census Bureau BPS via FRED",
    },
    "Trade Flows — HS-Code Level (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_trade_flows" / "trade_flows_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_TRADE_FLOWS_DATA_DICTIONARY.md",
        "filename": "trade_flows_v1.0_sample.parquet",
        "product":  "Trade Flows (HS-Code Level)",
        "schema_version": "v5.0",
        "records":  "43,020 records | 1992–2026 | Monthly · 2015–2017 · 3-Year Sample",
        "sources":  "US Census Bureau FT-900 / ALFRED",
    },
    "Global Macro Baseline — IMF WEO (USA)": {
        "parquet":  NEUDATA_DIR / "sample_parquet_global_macro" / "global_macro_imf_weo_v1.0_sample.parquet",
        "dict":     NEUDATA_DIR / "USA_GLOBAL_MACRO_DATA_DICTIONARY.md",
        "filename": "global_macro_imf_weo_v1.0_sample.parquet",
        "product":  "Global Macro Baseline",
        "schema_version": "v5.0",
        "records":  "81,735 records (ALFRED + IMF WEO) | 1913–2031 · 2015–2017 · 3-Year Sample",
        "sources":  "ALFRED (St. Louis Fed) / IMF WEO",
    },
}

# ──────────────────────────────────────────────────────────────
#  PRICING CATALOG  (Research License 1x · Full 32-Country)
# ──────────────────────────────────────────────────────────────
PRICING = [
    {
        "product":      "Food Micropricing",
        "coverage":     "1980–2026 · 40+ Items · 32 Countries",
        "source":       "BLS / USDA ERS / Eurostat / NSOs",
        "archive_usd":  89_000,
        "live_usd":     62_000,
        "live":         True,
    },
    {
        "product":      "Wages & Labour",
        "coverage":     "1939–2026 · CPS + CES · 32 Countries",
        "source":       "BLS CES + CPS / Eurostat / NSOs",
        "archive_usd":  78_000,
        "live_usd":     52_000,
        "live":         True,
    },
    {
        "product":      "Housing Supply & Shelter",
        "coverage":     "1914–2026 · CPI + Permits · 32 Countries",
        "source":       "BLS / US Census / Eurostat / NSOs",
        "archive_usd":  82_000,
        "live_usd":     None,
        "live":         False,
    },
    {
        "product":      "Trade Flows (HS-Code Level)",
        "coverage":     "1992–2026 · 99 HS-2 Chapters · 32 Countries",
        "source":       "US Census FT-900 / Eurostat / HMRC / StatCan",
        "archive_usd":  98_000,
        "live_usd":     72_000,
        "live":         True,
    },
    {
        "product":      "Global Macro Baseline",
        "coverage":     "1913–2031 · 18 Series · 32 Countries",
        "source":       "ALFRED (St. Louis Fed) / IMF WEO",
        "archive_usd":  58_000,
        "live_usd":     None,
        "live":         False,
    },
]

LICENSE_TIERS = [
    ("Research",           "1×",  "Academic and non-commercial research; no model deployment"),
    ("Backtesting",        "2×",  "Signal research and strategy backtesting; internal use only"),
    ("Algorithm Training", "3×",  "Training ML/quantitative models for live deployment"),
    ("Full Commercial",    "4×",  "Unlimited internal + client-facing commercial use"),
]

# ──────────────────────────────────────────────────────────────
#  32-COUNTRY COVERAGE TABLE
# ──────────────────────────────────────────────────────────────
COVERAGE_TABLE = [
    {
        "region":    "United States",
        "countries": "1",
        "products":  "5 / 5",
        "pit_model": "FULL VINTAGE (ALFRED avg 8.80 revisions) + RELEASE_DATE_ONLY",
        "live_feed": "Food · Wages · Trade",
        "status":    "READY",
    },
    {
        "region":    "European Union (EU27)",
        "countries": "27",
        "products":  "5 / 5 each",
        "pit_model": "RELEASE_DATE_ONLY (Eurostat SDMX structural ceiling)",
        "live_feed": "Food · Wages · Trade",
        "status":    "READY",
    },
    {
        "region":    "GBR · CAN · AUS",
        "countries": "3",
        "products":  "5 / 5 each",
        "pit_model": "RELEASE_DATE_ONLY (accumulating; ONS / StatCan / ABS)",
        "live_feed": "Food · Wages · Trade",
        "status":    "READY",
    },
    {
        "region":    "Norway (NOR)",
        "countries": "1",
        "products":  "4 / 5",
        "pit_model": "RELEASE_DATE_ONLY (SSB Statbank)",
        "live_feed": "Food · Wages · Trade",
        "status":    "4 READY · Housing unavailable",
    },
]

# ──────────────────────────────────────────────────────────────
#  VALIDATION MANIFEST (9-Stage Engine · 32-Country Scope)
# ──────────────────────────────────────────────────────────────
VALIDATION_MANIFEST = {
    "manifest_id":             "LKW-VAULT-MANIFEST-2026-06-20",
    "generated_at":            "2026-06-20T00:00:00Z",
    "vault_version":           "5.0",
    "schema_standard":         "SDMX 2.1 + ISO 8601 + ISO 3166-1",
    "overall_status":          "PASS",
    "products_certified":      5,
    "countries_certified":     31,
    "country_dataset_ready":   "159 / 165",
    "total_records_certified": 336_004,
    "validation_engine":       "Lekwankwa 9-Stage Automated Engine v2.0 + Live Feed Post-Delta Audit v1.0",
    "live_feed_audit": {
        "status":        "ACTIVE",
        "script":        "live_feed_audit.py",
        "trigger":       "Runs automatically after every --mode live vault_extractor run and 9-stage PASS",
        "gate":          "Exit 0 = GCS write cleared; Exit 1 = GCS write halted",
        "checks": {
            "C1_NON_NULL":               "12 non-nullable schema fields · 0 nulls permitted in any delivery file",
            "C2_SCRAPER_PLACEHOLDER_DQC":"PRIMARY/SECONDARY rows with dqc=False flagged before delivery",
            "C3_CROSS_PIPELINE_DUPLICATE":"Same (series, date) conflicting value across different source pipelines",
            "C4_TIMESTAMP_CONTAMINATION": "as_of_date and conversion_timestamp integrity vs pipeline run date",
            "C5_FILENAME_CONTENT_MATCH":  "macro_metric_name vocabulary consistent with declared product",
        },
        "audit_log": "audit_logs/live_feed_audit_log_{product}_{timestamp}.json",
    },
    "stages": {
        "stage_1_pit_validation": {
            "status": "PASS",
            "checks_run": 10,
            "description": "Point-in-Time integrity — published_date >= data_timestamp for all records",
            "products_checked": ["food_micropricing", "wages_employment", "housing", "trade_flows", "global_macro"],
            "pit_violations": 0,
        },
        "stage_2_sanity_checks": {
            "status": "PASS",
            "checks_run": 50,
            "description": "Row counts, column presence, null ratios, and domain-specific range checks across 32 countries",
            "anomalies_detected": 0,
        },
        "stage_3_schema_compliance": {
            "status": "PASS",
            "checks_run": 15,
            "standard": "SDMX 2.1",
            "description": "Column types, naming conventions, unit vocabularies, and ISO code alignment",
            "fields_validated": 312,
        },
        "stage_4_temporal_consistency": {
            "status": "PASS",
            "checks_run": 17,
            "description": "No temporal gaps in monthly series; observation_period monotonicity verified",
            "gap_records_found": 0,
        },
        "stage_5_referential_integrity": {
            "status": "PASS",
            "description": "All foreign key relationships (sovereign_series_id, data_vintage_id) validated",
            "orphan_records": 0,
        },
        "stage_6_lineage": {
            "status": "PASS",
            "description": "Full audit trail from source API endpoint to Hive partition confirmed",
            "lineage_entries": 336_004,
        },
        "stage_7_gx_universal_validation": {
            "status": "PASS",
            "framework": "Great Expectations v0.18",
            "expectations_evaluated": 135,
            "expectations_passed": 135,
            "success_rate": "100.00%",
        },
        "stage_8_outlier_extraction": {
            "status": "PASS",
            "description": "Statistical outlier detection using IQR + z-score; all flagged records documented",
            "outliers_documented": 41,
            "outliers_suppressed": 0,
        },
        "stage_9_changelog_generation": {
            "status": "PASS",
            "description": "Machine-readable JSON changelog produced for every schema version transition",
            "changelog_entries": 312,
        },
    },
    "compliance_statement": (
        "Sourcing strictly restricted to open-government APIs and bulk downloads. "
        "Zero web-scraping dependencies. 100% Flat Parquet schemas."
    ),
}

# ──────────────────────────────────────────────────────────────
#  GLOBAL CSS INJECTION
# ──────────────────────────────────────────────────────────────
def inject_css() -> None:
    st.markdown(
        """
        <style>
        /* ── Global resets ── */
        html, body, [class*="css"] {
            font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
        }

        /* ── Sidebar ── */
        section[data-testid="stSidebar"] {
            background-color: #000000 !important;
            border-right: 1px solid #2a2a2a;
        }
        section[data-testid="stSidebar"] .stRadio label {
            color: #ffffff !important;
            font-size: 0.95rem;
            letter-spacing: 0.03em;
        }
        section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] {
            gap: 6px;
        }

        /* ── Main area ── */
        .main .block-container {
            padding-top: 2.5rem;
            padding-bottom: 4rem;
            padding-left: 1.5rem;
            padding-right: 1.5rem;
            max-width: 1100px;
        }

        /* ── Divider ── */
        hr { border-color: #2a2a2a !important; }

        /* ── Metric cards ── */
        div[data-testid="metric-container"] {
            background-color: #111111;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 1rem 1.2rem;
        }
        div[data-testid="metric-container"] label {
            color: #888888 !important;
            font-size: 0.75rem !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
            color: #ffffff !important;
            font-size: 1.4rem !important;
            font-weight: 700;
        }

        /* ── Dataframe ── */
        .stDataFrame { border: 1px solid #2a2a2a; border-radius: 6px; }

        /* ── Download button ── */
        div[data-testid="stDownloadButton"] button {
            background-color: #ffffff !important;
            color: #000000 !important;
            font-weight: 600;
            border: none;
            border-radius: 5px;
            padding: 0.55rem 1.4rem;
            letter-spacing: 0.04em;
            width: 100%;
            transition: opacity 0.15s ease;
        }
        div[data-testid="stDownloadButton"] button:hover { opacity: 0.85; }

        /* ── Select box ── */
        div[data-baseweb="select"] {
            border: 1px solid #333333 !important;
            border-radius: 6px;
        }

        /* ── Price table — desktop ── */
        .price-table-wrap {
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 1.2rem;
        }
        .price-table-header {
            display: flex;
            align-items: center;
            padding: 0.65rem 1.4rem;
            background-color: #111111;
            border-bottom: 1px solid #2a2a2a;
        }
        .price-table-row {
            display: flex;
            align-items: center;
            padding: 1rem 1.4rem;
            border-bottom: 1px solid #1a1a1a;
            background-color: #0a0a0a;
            transition: background 0.15s;
            gap: 0.5rem;
        }
        .price-table-row:hover { background-color: #131313; }
        .price-table-row:last-child { border-bottom: none; }
        .col-product  { flex: 2.4; font-weight: 600; font-size: 0.93rem; color: #ffffff; }
        .col-coverage { flex: 2.2; font-size: 0.8rem; color: #888888; }
        .col-source   { flex: 1.6; font-size: 0.8rem; color: #888888; }
        .col-archive  { flex: 1.3; font-size: 1.05rem; font-weight: 700; color: #ffffff; text-align: right; }
        .col-live     { flex: 1.2; font-size: 0.88rem; font-weight: 600; color: #555555; text-align: right; }
        .col-header   { flex: 1;   font-size: 0.68rem; color: #555555; text-transform: uppercase; letter-spacing: 0.1em; }
        .col-header.right { text-align: right; }
        .live-badge   { display:inline-block; background:#0a2a0a; border:1px solid #1a5a1a;
                        border-radius:3px; padding:0.1rem 0.4rem; font-size:0.7rem;
                        color:#39d353; font-weight:600; margin-left:0.3rem; }
        .archive-only { color: #444444; font-size: 0.75rem; }

        /* ── License tier table ── */
        .tier-table {
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 2rem;
        }
        .tier-row {
            display: flex;
            align-items: center;
            padding: 0.75rem 1.4rem;
            border-bottom: 1px solid #1a1a1a;
            background-color: #0a0a0a;
            gap: 1rem;
        }
        .tier-row:last-child { border-bottom: none; }
        .tier-name  { flex: 1.5; font-weight: 600; font-size: 0.88rem; color: #ffffff; }
        .tier-mult  { flex: 0.6; font-size: 0.88rem; font-weight: 700; color: #ffffff;
                      text-align: center; background:#111; border:1px solid #333;
                      border-radius:4px; padding:0.2rem 0; }
        .tier-desc  { flex: 3; font-size: 0.8rem; color: #666; }

        /* ── Coverage grid ── */
        .coverage-table-wrap {
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 2rem;
        }
        .coverage-row {
            display: flex;
            align-items: flex-start;
            padding: 0.85rem 1.2rem;
            border-bottom: 1px solid #1a1a1a;
            background: #0a0a0a;
            gap: 0.5rem;
        }
        .coverage-row:first-child { background: #111111; }
        .coverage-row:last-child  { border-bottom: none; }
        .cov-region   { flex: 2;   font-weight: 600; font-size: 0.85rem; color: #ffffff; }
        .cov-n        { flex: 0.6; font-size: 0.8rem; color: #888; text-align: center; }
        .cov-products { flex: 0.8; font-size: 0.8rem; color: #aaa; text-align: center; }
        .cov-pit      { flex: 2.8; font-size: 0.76rem; color: #666; }
        .cov-feed     { flex: 1.4; font-size: 0.76rem; color: #888; }
        .cov-status   { flex: 1.2; font-size: 0.76rem; font-weight: 600; text-align: right; }
        .cov-header   { font-size: 0.65rem; color: #444; text-transform: uppercase; letter-spacing: 0.1em; }
        .status-ready   { color: #39d353; }
        .status-pending { color: #f5a623; }
        .status-blocked { color: #555555; }

        /* ── Price cards — mobile ── */
        .price-card-grid {
            display: none;
            flex-direction: column;
            gap: 0.6rem;
            margin-bottom: 2rem;
        }
        .price-card {
            background-color: #0a0a0a;
            border: 1px solid #1e1e1e;
            border-radius: 8px;
            padding: 1rem 1.1rem;
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 1rem;
        }
        .price-card-left { flex: 1; }
        .price-card-name { font-size: 0.92rem; font-weight: 600; color: #ffffff; margin-bottom: 0.25rem; }
        .price-card-meta { font-size: 0.75rem; color: #666666; line-height: 1.5; }
        .price-card-right { text-align: right; flex-shrink: 0; }
        .price-card-archive { font-size: 1.05rem; font-weight: 700; color: #ffffff; }
        .price-card-live { font-size: 0.78rem; color: #39d353; margin-top: 0.15rem; }
        .price-card-archive-only { font-size: 0.72rem; color: #444; margin-top: 0.15rem; }

        /* ── Vault bundle ── */
        .vault-box {
            border: 1.5px solid #ffffff;
            border-radius: 10px;
            padding: 2rem 2.2rem;
            background: linear-gradient(135deg, #0d0d0d 0%, #111111 100%);
            margin-top: 1.5rem;
        }
        .super-box {
            border: 1.5px solid #333333;
            border-radius: 10px;
            padding: 2rem 2.2rem;
            background: linear-gradient(135deg, #080808 0%, #0d0d0d 100%);
            margin-top: 1rem;
        }
        .vault-headline {
            font-size: 1.55rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.01em;
            margin-bottom: 0.25rem;
        }
        .vault-sub {
            font-size: 0.88rem;
            color: #777777;
            margin-bottom: 1.4rem;
        }
        .vault-price {
            font-size: 2.8rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.02em;
        }
        .vault-price-label {
            font-size: 0.78rem;
            color: #555555;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-top: 0.15rem;
        }
        .vault-body-flex {
            display: flex;
            align-items: flex-end;
            gap: 2rem;
            flex-wrap: wrap;
        }

        /* ── Compliance banner ── */
        .compliance-banner {
            border: 1px solid #2a2a2a;
            border-left: 3px solid #ffffff;
            border-radius: 5px;
            padding: 0.9rem 1.3rem;
            background-color: #0a0a0a;
            font-size: 0.84rem;
            color: #aaaaaa;
            line-height: 1.6;
            margin-bottom: 2rem;
        }
        .compliance-banner strong { color: #ffffff; }

        /* ── Section header ── */
        .section-eyebrow {
            font-size: 0.72rem;
            font-weight: 600;
            color: #555555;
            text-transform: uppercase;
            letter-spacing: 0.15em;
            margin-bottom: 0.5rem;
        }
        .section-title {
            font-size: 1.9rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.02em;
            margin-bottom: 0.3rem;
            line-height: 1.15;
        }
        .section-sub {
            font-size: 0.9rem;
            color: #666666;
            margin-bottom: 2rem;
            line-height: 1.5;
        }

        /* ── Feature pillars grid ── */
        .pillars-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin-top: 0.5rem;
        }
        .pillar-card {
            padding: 1.2rem;
            border: 1px solid #1e1e1e;
            border-radius: 8px;
            background: #080808;
        }
        .pillar-eyebrow {
            font-size: 0.7rem;
            color: #555;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            margin-bottom: 0.5rem;
        }
        .pillar-title {
            font-size: 1rem;
            font-weight: 700;
            color: #fff;
            margin-bottom: 0.5rem;
        }
        .pillar-body {
            font-size: 0.82rem;
            color: #666;
            line-height: 1.55;
        }

        /* ── Stage badges ── */
        .stages-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 0.5rem;
            margin-bottom: 1.5rem;
        }
        .stage-pass {
            background-color: #0f0f0f;
            border: 1px solid #2a2a2a;
            border-radius: 5px;
            padding: 0.75rem 1rem;
        }
        .stage-pass .badge-num  { font-size: 0.65rem; color: #555555; text-transform: uppercase; letter-spacing: 0.1em; }
        .stage-pass .badge-name { font-size: 0.9rem; font-weight: 600; color: #ffffff; margin: 0.2rem 0; }
        .stage-pass .badge-stat { font-size: 0.75rem; color: #39d353; font-weight: 600; }

        /* ── Live feed audit check badges ── */
        .audit-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 0.5rem;
            margin-bottom: 1.5rem;
        }
        .audit-check {
            background-color: #0a0f0a;
            border: 1px solid #1a3a1a;
            border-radius: 5px;
            padding: 0.75rem 1rem;
        }
        .audit-check .badge-num  { font-size: 0.65rem; color: #2a5a2a; text-transform: uppercase; letter-spacing: 0.1em; }
        .audit-check .badge-name { font-size: 0.9rem; font-weight: 600; color: #ffffff; margin: 0.2rem 0; }
        .audit-check .badge-stat { font-size: 0.75rem; color: #39d353; font-weight: 600; }
        .audit-check .badge-desc { font-size: 0.72rem; color: #444444; margin-top: 0.3rem; line-height: 1.4; }

        /* ── Dataset meta pill ── */
        .meta-pill {
            display: inline-block;
            background-color: #111111;
            border: 1px solid #2a2a2a;
            border-radius: 4px;
            padding: 0.25rem 0.65rem;
            font-size: 0.76rem;
            color: #888888;
            margin: 0 0.2rem 0.4rem 0;
        }

        /* ═══════════════════════════════════════
           MOBILE RESPONSIVE  (max-width: 768px)
        ═══════════════════════════════════════ */
        @media (max-width: 768px) {

            /* Main padding */
            .main .block-container {
                padding-left: 0.8rem;
                padding-right: 0.8rem;
                padding-top: 1.2rem;
            }

            /* Section titles */
            .section-title { font-size: 1.4rem; }
            .section-sub   { font-size: 0.85rem; }

            /* Hide desktop table, show mobile cards */
            .price-table-wrap   { display: none !important; }
            .price-card-grid    { display: flex !important; }
            .coverage-table-wrap { display: none !important; }

            /* Vault box */
            .vault-box, .super-box {
                padding: 1.2rem 1rem;
            }
            .vault-headline { font-size: 1.1rem; }
            .vault-price    { font-size: 2rem; }
            .vault-body-flex {
                flex-direction: column;
                align-items: flex-start;
                gap: 1rem;
            }

            /* Pillars: single column */
            .pillars-grid { grid-template-columns: 1fr; }

            /* Stage badges: 1 column */
            .stages-grid { grid-template-columns: 1fr; }

            /* Compliance banner */
            .compliance-banner {
                font-size: 0.8rem;
                padding: 0.75rem 1rem;
            }

            /* Meta pills wrap */
            .meta-pill {
                font-size: 0.7rem;
                padding: 0.2rem 0.5rem;
            }

            /* Metric value smaller */
            div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
                font-size: 1.1rem !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────
#  SIDEBAR
# ──────────────────────────────────────────────────────────────
def render_sidebar() -> str:
    with st.sidebar:
        try:
            if LOGO_PATH.exists():
                st.image(str(LOGO_PATH), use_container_width=True)
            else:
                st.markdown(
                    "<div style='text-align:center; padding:1rem 0; font-size:1.1rem;"
                    " font-weight:800; color:#ffffff; letter-spacing:0.05em;'>"
                    "LEKWANKWA<br><span style='font-size:0.6rem; color:#555555;"
                    " font-weight:400; letter-spacing:0.2em;'>CORPORATION</span></div>",
                    unsafe_allow_html=True,
                )
        except Exception:
            st.markdown(
                "<div style='text-align:center; padding:1rem 0;'>"
                "<span style='font-size:1rem; font-weight:800; color:#ffffff;'>LEKWANKWA</span></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<hr style='border-color:#1a1a1a; margin:0.8rem 0 1rem;'>", unsafe_allow_html=True)

        st.markdown(
            "<div style='font-size:0.65rem; color:#444444; text-transform:uppercase;"
            " letter-spacing:0.15em; margin-bottom:0.6rem;'>Navigation</div>",
            unsafe_allow_html=True,
        )

        page = st.radio(
            label="Navigate",
            options=[
                "Corporate Showroom",
                "Data Sandbox",
                "Data Quality Hub",
            ],
            label_visibility="collapsed",
        )

        st.markdown("<hr style='border-color:#1a1a1a; margin:1.5rem 0 1rem;'>", unsafe_allow_html=True)

        st.markdown(
            "<div style='font-size:0.72rem; color:#ffffff; font-weight:600;"
            " margin-bottom:0.4rem;'>Compliance Guarantee</div>"
            "<div style='font-size:0.71rem; color:#555555; line-height:1.55;'>"
            "Sourcing strictly restricted to open-government APIs and bulk downloads. "
            "Zero web-scraping dependencies. 100% Flat Parquet schemas."
            "</div>",
            unsafe_allow_html=True,
        )

        st.markdown(
            "<div style='border-top:1px solid #1a1a1a; margin-top:1.5rem;"
            " padding-top:1rem;'>"
            "<div style='font-size:0.65rem; color:#444444; text-transform:uppercase;"
            " letter-spacing:0.12em; margin-bottom:0.35rem;'>Company Registration</div>"
            "<div style='font-size:0.78rem; font-weight:700; color:#888888;"
            " letter-spacing:0.02em;'>LEKWANKWA CORPORATION</div>"
            "<div style='font-size:0.72rem; color:#444444; margin-top:0.2rem;"
            " font-family:monospace; letter-spacing:0.04em;'>2025/617516/07</div>"
            "<div style='margin-top:1rem;'>"
            "<div style='font-size:0.65rem; color:#444444; text-transform:uppercase;"
            " letter-spacing:0.12em; margin-bottom:0.35rem;'>Contact</div>"
            "<a href='mailto:info@lekwankwa.com' style='font-size:0.76rem; color:#888888;"
            " text-decoration:none; letter-spacing:0.02em;'>info@lekwankwa.com</a>"
            "</div>"
            "<div style='font-size:0.6rem; color:#2a2a2a; margin-top:0.9rem;'>"
            "© 2026 Lekwankwa Corporation. All rights reserved.</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    return page


# ──────────────────────────────────────────────────────────────
#  PAGE 1 — CORPORATE SHOWROOM
# ──────────────────────────────────────────────────────────────
def page_showroom() -> None:
    # ── Hero ──
    st.markdown(
        "<div class='section-eyebrow'>Lekwankwa Corporation — Data Licensing</div>"
        "<div class='section-title'>Institutional-Grade Historical<br>Data Archives</div>"
        "<div class='section-sub'>"
        "High-fidelity, flat-schema quantitative data sourced exclusively from official "
        "government APIs across 32 sovereign jurisdictions. Point-in-Time enabled. "
        "Audit-ready. One-off CAPEX acquisition."
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Compliance banner ──
    st.markdown(
        "<div class='compliance-banner'>"
        "<strong>Compliance Guarantee</strong> — "
        "Sourcing strictly restricted to open-government APIs and bulk downloads "
        "from 32 sovereign jurisdictions (USA · EU27 · GBR · CAN · AUS · NOR). "
        "<strong>Zero web-scraping dependencies.</strong> "
        "<strong>100% Flat Parquet schemas.</strong> "
        "9-stage automated vault validation + 5-check live feed post-delta audit. "
        "SDMX 2.1 aligned. Full PIT revision history."
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Key metrics ──
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Products", "5")
    col2.metric("Countries", "31 Active")
    col3.metric("Records Certified", "336K+")
    col4.metric("Validation", "9 / 9 PASS")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 32-Country Coverage ──
    st.markdown(
        "<div class='section-eyebrow'>32-Country Sovereign Coverage</div>",
        unsafe_allow_html=True,
    )

    cov_header = (
        "<div class='coverage-table-wrap'>"
        "  <div class='coverage-row'>"
        "    <span class='cov-region cov-header'>Region / Country</span>"
        "    <span class='cov-n cov-header' style='text-align:center'>Countries</span>"
        "    <span class='cov-products cov-header' style='text-align:center'>Products</span>"
        "    <span class='cov-pit cov-header'>PIT Model</span>"
        "    <span class='cov-feed cov-header'>Live Feed</span>"
        "    <span class='cov-status cov-header' style='text-align:right'>Status</span>"
        "  </div>"
    )
    cov_rows = ""
    for row in COVERAGE_TABLE:
        st_lower = row["status"].lower()
        if "ready" in st_lower and "pending" not in st_lower:
            sc = "status-ready"
        elif "pending" in st_lower:
            sc = "status-pending"
        else:
            sc = "status-blocked"
        cov_rows += (
            f"<div class='coverage-row'>"
            f"  <span class='cov-region'>{row['region']}</span>"
            f"  <span class='cov-n'>{row['countries']}</span>"
            f"  <span class='cov-products'>{row['products']}</span>"
            f"  <span class='cov-pit'>{row['pit_model']}</span>"
            f"  <span class='cov-feed'>{row['live_feed']}</span>"
            f"  <span class='cov-status {sc}'>{row['status']}</span>"
            f"</div>"
        )
    cov_rows += "</div>"
    st.markdown(cov_header + cov_rows, unsafe_allow_html=True)

    # ── Pricing ──
    st.markdown(
        "<div class='section-eyebrow'>Historical Archive Pricing — Research License (1×) · Full 32-Country</div>",
        unsafe_allow_html=True,
    )

    # Desktop table
    header_html = (
        "<div class='price-table-wrap'>"
        "  <div class='price-table-header'>"
        "    <span class='col-header col-product'>Data Product</span>"
        "    <span class='col-header col-coverage'>Coverage</span>"
        "    <span class='col-header col-source'>Primary Sources</span>"
        "    <span class='col-header col-archive right'>Archive (CAPEX)</span>"
        "    <span class='col-header col-live right'>Live Feed / yr</span>"
        "  </div>"
    )
    rows_html = ""
    for item in PRICING:
        arch_fmt = f"${item['archive_usd']:,.0f}"
        if item["live"]:
            live_fmt = "<span class='live-badge'>Coming Soon</span>"
        else:
            live_fmt = "<span class='archive-only'>Archive only</span>"
        rows_html += (
            f"<div class='price-table-row'>"
            f"  <span class='col-product'>{item['product']}</span>"
            f"  <span class='col-coverage'>{item['coverage']}</span>"
            f"  <span class='col-source'>{item['source']}</span>"
            f"  <span class='col-archive'>{arch_fmt}</span>"
            f"  <span class='col-live'>{live_fmt}</span>"
            f"</div>"
        )
    rows_html += "</div>"

    # Mobile cards
    cards_html = "<div class='price-card-grid'>"
    for item in PRICING:
        arch_fmt = f"${item['archive_usd']:,.0f}"
        if item["live"]:
            live_html = "<div class='price-card-live'>Live feed: Coming Soon</div>"
        else:
            live_html = "<div class='price-card-archive-only'>Archive only</div>"
        cards_html += (
            f"<div class='price-card'>"
            f"  <div class='price-card-left'>"
            f"    <div class='price-card-name'>{item['product']}</div>"
            f"    <div class='price-card-meta'>{item['coverage']}<br>{item['source']}</div>"
            f"  </div>"
            f"  <div class='price-card-right'>"
            f"    <div class='price-card-archive'>{arch_fmt}</div>"
            f"    {live_html}"
            f"  </div>"
            f"</div>"
        )
    cards_html += "</div>"

    st.markdown(header_html + rows_html + cards_html, unsafe_allow_html=True)

    # ── License Tier Multipliers ──
    st.markdown(
        "<div style='font-size:0.72rem; color:#555; text-transform:uppercase;"
        " letter-spacing:0.12em; margin-bottom:0.6rem;'>License Tier Multipliers</div>",
        unsafe_allow_html=True,
    )
    tier_html = "<div class='tier-table'>"
    for name, mult, desc in LICENSE_TIERS:
        tier_html += (
            f"<div class='tier-row'>"
            f"  <span class='tier-name'>{name}</span>"
            f"  <span class='tier-mult'>{mult}</span>"
            f"  <span class='tier-desc'>{desc}</span>"
            f"</div>"
        )
    tier_html += "</div>"
    st.markdown(tier_html, unsafe_allow_html=True)

    # ── Complete Archive Vault ──
    st.markdown(
        "<div class='vault-box'>"
        "  <div style='font-size:0.65rem; color:#555555; text-transform:uppercase;"
        "    letter-spacing:0.2em; margin-bottom:0.5rem;'>Enterprise Bundle — All 5 Products</div>"
        "  <div class='vault-headline'>The Lekwankwa Complete Archive</div>"
        "  <div class='vault-headline' style='color:#888888; font-size:1.1rem;"
        "    font-weight:600; letter-spacing:0.02em;'>(The \"Vault\")</div>"
        "  <div class='vault-sub' style='margin-top:0.6rem;'>"
        "    Permanent corporate access to the entire historical library across all five data products "
        "    · 32 sovereign jurisdictions · Research License 1×."
        "  </div>"
        "  <div class='vault-body-flex'>"
        "    <div>"
        "      <div class='vault-price'>$254,000</div>"
        "      <div class='vault-price-label'>One-off CAPEX · Historical Archive License</div>"
        "    </div>"
        "    <div style='flex:1; min-width:200px;'>"
        "      <div style='font-size:0.78rem; color:#555555; margin-bottom:0.6rem;'>"
        "        Includes all five products (32 countries each):</div>"
        "      <div style='font-size:0.82rem; color:#aaaaaa; line-height:1.9;'>"
        "        Food Micropricing<br>"
        "        Wages &amp; Labour<br>"
        "        Housing Supply &amp; Shelter<br>"
        "        Trade Flows (HS-Code Level)<br>"
        "        Global Macro Baseline"
        "      </div>"
        "    </div>"
        "  </div>"
        "  <div style='margin-top:1.5rem; padding-top:1.2rem; border-top:1px solid #2a2a2a;'>"
        "    <div style='font-size:0.72rem; color:#555555; text-transform:uppercase;"
        "      letter-spacing:0.1em; margin-bottom:0.4rem;'>Annual Refresh Option</div>"
        "    <div style='font-size:0.83rem; color:#777777; line-height:1.65;'>"
        "      Subsequent annual dataset refreshes available under an optional maintenance "
        "      agreement at <span style='color:#aaaaaa; font-weight:600;'>15% of asset value "
        "      annually</span> ($38,100 / year)."
        "    </div>"
        "  </div>"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Super Bundle (Archive + Year 1 Live Feed) ──
    st.markdown(
        "<div class='super-box'>"
        "  <div style='font-size:0.65rem; color:#555555; text-transform:uppercase;"
        "    letter-spacing:0.2em; margin-bottom:0.5rem;'>Enterprise Super Bundle</div>"
        "  <div class='vault-headline' style='font-size:1.25rem;'>Archive + Year 1 Live Feed</div>"
        "  <div class='vault-sub' style='margin-top:0.4rem;'>"
        "    Complete Archive (all 5 products) plus a 12-month live feed subscription for the "
        "    three feedable products: Food, Wages, and Trade."
        "  </div>"
        "  <div class='vault-body-flex'>"
        "    <div>"
        "      <div class='vault-price' style='font-size:2.2rem;'>$376,000</div>"
        "      <div class='vault-price-label'>CAPEX + Year 1 Feed · Research License 1×</div>"
        "    </div>"
        "    <div style='flex:1; min-width:180px; font-size:0.8rem; color:#555; line-height:1.8;'>"
        "      $254,000 Archive + $62,000 Food Feed<br>"
        "      + $52,000 Wages Feed + $72,000 Trade Feed<br>"
        "      <span style='color:#666;'>= $440,000 &rarr; bundled at $376,000</span>"
        "    </div>"
        "  </div>"
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br><br>", unsafe_allow_html=True)

    # ── Feature pillars ──
    st.markdown(
        "<div class='section-eyebrow'>Why Lekwankwa</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='pillars-grid'>"
        "  <div class='pillar-card'>"
        "    <div class='pillar-eyebrow'>PIT Guarantee</div>"
        "    <div class='pillar-title'>Zero Look-Ahead Bias</div>"
        "    <div class='pillar-body'>Every record carries actual publication timestamps "
        "    and full revision history. FULL VINTAGE bitemporal model for USA (ALFRED avg "
        "    8.80 revisions); RELEASE_DATE_ONLY for EU27 and Non-EU jurisdictions. "
        "    Backtesting simulations are provably clean.</div>"
        "  </div>"
        "  <div class='pillar-card'>"
        "    <div class='pillar-eyebrow'>Schema Standard</div>"
        "    <div class='pillar-title'>Flat Parquet · SDMX 2.1</div>"
        "    <div class='pillar-body'>100% flat-schema Parquet files. No nested JSON. "
        "    No proprietary formats. SDMX-aligned column naming, ISO 8601 timestamps, "
        "    ISO 3166-1 geo codes. Golden Record Schema v5.0 consistent across all "
        "    32 countries and 5 products.</div>"
        "  </div>"
        "  <div class='pillar-card'>"
        "    <div class='pillar-eyebrow'>Source Integrity</div>"
        "    <div class='pillar-title'>32-Country Gov-API Pipeline</div>"
        "    <div class='pillar-body'>Data sourced exclusively from sovereign government "
        "    APIs: BLS, US Census, ALFRED, USDA (USA) · Eurostat SDMX (EU27) · "
        "    ONS, StatCan, ABS, SSB (Non-EU). Zero web-scraping. "
        "    Full lineage logged per record.</div>"
        "  </div>"
        "</div>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────
#  PAGE 2 — DATA SANDBOX
# ──────────────────────────────────────────────────────────────
def page_sandbox() -> None:
    st.markdown(
        "<div class='section-eyebrow'>Institutional Data Access — Free Sample</div>"
        "<div class='section-title'>Data Sandbox &amp; Schema Dictionaries</div>"
        "<div class='section-sub'>"
        "Free 36-month sample — point-in-time compliant with full vintage history, "
        "including all revision events captured. Run your own backtest before purchasing "
        "the complete historical archive."
        "<br><br>"
        "Select a USA dataset below to inspect the sample (2015–2017), download the "
        "Parquet file, and review the full schema dictionary inline. All sample data uses "
        "the Golden Record Schema v5.0 — the same schema applied across all 32 countries "
        "in the production vault."
        "</div>",
        unsafe_allow_html=True,
    )

    dataset_name = st.selectbox(
        "Select Dataset",
        options=list(DATASETS.keys()),
        help="Each entry corresponds to a validated USA sample Parquet file (2015–2017).",
    )

    meta = DATASETS[dataset_name]
    parquet_path: pathlib.Path = meta["parquet"]
    dict_path: pathlib.Path    = meta["dict"]

    # Meta pills
    st.markdown(
        f"<span class='meta-pill'>Product: {meta['product']}</span>"
        f"<span class='meta-pill'>Schema: {meta['schema_version']}</span>"
        f"<span class='meta-pill'>Source: {meta['sources']}</span>"
        f"<span class='meta-pill'>{meta['records']}</span>",
        unsafe_allow_html=True,
    )

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Load Parquet ──
    try:
        df = pd.read_parquet(parquet_path)
        row_count, col_count = df.shape

        st.markdown(
            f"<div style='font-size:0.72rem; color:#555555; text-transform:uppercase;"
            f" letter-spacing:0.12em; margin-bottom:0.5rem;'>"
            f"Sample Data — 2015–2017 · {row_count:,} rows · {col_count} columns</div>",
            unsafe_allow_html=True,
        )

        st.dataframe(
            df,
            use_container_width=True,
            height=380,
        )

        # Download button
        parquet_bytes = io.BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)

        st.download_button(
            label=f"Download Sample Parquet — {meta['filename']}",
            data=parquet_bytes,
            file_name=meta["filename"],
            mime="application/octet-stream",
        )

    except FileNotFoundError:
        st.warning(
            f"Sample file not found at: `{parquet_path}`. "
            "Verify the `neudata_submission` directory is present alongside `app.py`."
        )
        mock_df = pd.DataFrame({
            "data_timestamp": pd.to_datetime(["2015-01-01", "2016-01-01", "2017-01-01"]),
            "observed_value": [100.0, 102.3, 104.1],
            "confidence_tier": ["PRIMARY"] * 3,
            "data_quality_certified": [True] * 3,
        })
        st.dataframe(mock_df, use_container_width=True)

    except Exception as exc:
        st.error(f"Error loading Parquet file: {exc}")

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Data Dictionary ──
    st.markdown(
        "<div style='font-size:0.72rem; color:#555555; text-transform:uppercase;"
        " letter-spacing:0.12em; margin-bottom:1rem;'>Schema Dictionary</div>",
        unsafe_allow_html=True,
    )

    try:
        dict_text = dict_path.read_text(encoding="utf-8")
        st.markdown(dict_text, unsafe_allow_html=False)
    except FileNotFoundError:
        st.warning(
            f"Data dictionary not found at: `{dict_path}`. "
            "Verify markdown dictionary files are present in the `neudata_submission` folder."
        )
    except Exception as exc:
        st.error(f"Error loading data dictionary: {exc}")


# ──────────────────────────────────────────────────────────────
#  PAGE 3 — DATA QUALITY HUB
# ──────────────────────────────────────────────────────────────
def page_quality_hub() -> None:
    st.markdown(
        "<div class='section-eyebrow'>Audit-Ready Infrastructure</div>"
        "<div class='section-title'>Data Quality Hub</div>"
        "<div class='section-sub'>"
        "Every Lekwankwa data product passes a 9-stage automated validation engine "
        "before delivery. The manifest below is machine-readable and shipped with each archive. "
        "Coverage spans 32 countries across 5 data products."
        "</div>",
        unsafe_allow_html=True,
    )

    # Compliance banner
    st.markdown(
        "<div class='compliance-banner'>"
        "<strong>Sourcing strictly restricted to open-government APIs and bulk downloads. "
        "Zero web-scraping dependencies. 100% Flat Parquet schemas.</strong> "
        "32 sovereign jurisdictions. 159 / 165 country-dataset combinations READY. "
        "Every live feed delivery passes a 9-stage vault validation suite "
        "<em>and</em> a 5-check post-delta audit gate before any GCS write."
        "</div>",
        unsafe_allow_html=True,
    )

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Overall Status", "PASS")
    col2.metric("Products Certified", "5 / 5")
    col3.metric("Records Certified", "336,004")
    col4.metric("GX Success Rate", "100.00%")

    st.markdown("<br>", unsafe_allow_html=True)

    # Stage cards
    st.markdown(
        "<div class='section-eyebrow'>9-Stage Vault Validation Engine — All Systems Green</div>",
        unsafe_allow_html=True,
    )

    stage_defs = [
        ("01", "PIT Validation",           "10 checks · 0 look-ahead violations"),
        ("02", "Sanity Checks",             "50 checks · Zero anomalies · 32 countries"),
        ("03", "Schema Compliance",         "15 SDMX checks · 312 fields validated"),
        ("04", "Temporal Consistency",      "17 checks · No gaps detected"),
        ("05", "Referential Integrity",     "0 orphan records"),
        ("06", "Lineage",                   "336,004 entries audited"),
        ("07", "GX Universal Validation",  "135 / 135 expectations passed"),
        ("08", "Outlier Extraction",        "41 documented · 0 suppressed"),
        ("09", "Changelog Generation",      "312 entries across all versions"),
    ]

    badges_html = "<div class='stages-grid'>"
    for num, name, detail in stage_defs:
        badges_html += (
            f"<div class='stage-pass'>"
            f"  <div class='badge-num'>Stage {num}</div>"
            f"  <div class='badge-name'>{name}</div>"
            f"  <div class='badge-stat'>PASS &nbsp;·&nbsp; {detail}</div>"
            f"</div>"
        )
    badges_html += "</div>"
    st.markdown(badges_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Live Feed Post-Delta Audit ──
    st.markdown(
        "<div class='section-eyebrow'>Live Feed Post-Delta Audit — 5-Check Gate (Runs After 9-Stage Suite)</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div style='font-size:0.82rem; color:#555555; margin-bottom:1rem; line-height:1.55;'>"
        "Every delta file produced by the live feed extractor must pass this 5-check audit "
        "before the GCS write is permitted. The audit runs automatically as a separate process "
        "after the 9-stage suite returns PASS. Exit code 1 halts the write; a permanent "
        "per-run JSON log is written to <code>audit_logs/</code>."
        "</div>",
        unsafe_allow_html=True,
    )

    audit_checks = [
        ("C1", "Non-Null Gate",
         "ACTIVE",
         "12 non-nullable fields per SCHEMA_STANDARD v5.0 · 0 nulls permitted · "
         "is_interpolated checked only when column is present (field-presence rule)"),
        ("C2", "Scraper Placeholder",
         "ACTIVE",
         "PRIMARY / SECONDARY rows with data_quality_certified=False flagged before delivery · "
         "Pattern confirmed in food/wages/housing USA — 4th occurrence caught here"),
        ("C3", "Cross-Pipeline Duplicate",
         "ACTIVE",
         "Same (series, date) conflicting observed_value across different source pipelines · "
         "Within-delta check + full vault scan · Sweden permits pattern"),
        ("C4", "Timestamp Integrity",
         "ACTIVE",
         "as_of_date and conversion_timestamp checked against pipeline run date · "
         "Flags scrape-timestamp contamination where as_of_date != official_release_date"),
        ("C5", "Content Fingerprint",
         "ACTIVE",
         "macro_metric_name keyword vocabulary consistent with declared product · "
         "Catches permits file containing HPI data and equivalent mis-routing"),
    ]

    audit_html = "<div class='audit-grid'>"
    for code, name, status, desc in audit_checks:
        audit_html += (
            f"<div class='audit-check'>"
            f"  <div class='badge-num'>{code}</div>"
            f"  <div class='badge-name'>{name}</div>"
            f"  <div class='badge-stat'>{status}</div>"
            f"  <div class='badge-desc'>{desc}</div>"
            f"</div>"
        )
    audit_html += "</div>"
    st.markdown(audit_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Full JSON manifest
    st.markdown(
        "<div class='section-eyebrow'>Machine-Readable Certification Manifest</div>",
        unsafe_allow_html=True,
    )
    st.json(VALIDATION_MANIFEST, expanded=False)

    st.markdown("<br>", unsafe_allow_html=True)

    # Per-product status table
    st.markdown(
        "<div class='section-eyebrow'>Per-Product Certification Summary (USA Component)</div>",
        unsafe_allow_html=True,
    )

    product_status = pd.DataFrame([
        {
            "Product": "Food Micropricing",
            "Schema": "v5.0",
            "USA Records": "29,825",
            "Sources": "USDA ERS / BLS / ALFRED",
            "PIT": "PASS", "Sanity": "PASS", "Schema": "PASS",
            "Temporal": "PASS", "RI": "PASS", "Lineage": "PASS",
            "GX": "PASS", "Outlier": "PASS", "Changelog": "PASS",
            "Overall": "9/9",
        },
        {
            "Product": "Wages & Labour",
            "Schema": "v2.0",
            "USA Records": "8,802 (CPS confirmed)",
            "Sources": "BLS CPS / BLS CES",
            "PIT": "PASS", "Sanity": "PASS", "Schema": "PASS",
            "Temporal": "PASS", "RI": "PASS", "Lineage": "PASS",
            "GX": "PASS", "Outlier": "PASS", "Changelog": "PASS",
            "Overall": "9/9*",
        },
        {
            "Product": "Housing Supply & Shelter",
            "Schema": "v2.0",
            "USA Records": "19,487",
            "Sources": "BLS CPI / US Census BPS",
            "PIT": "PASS", "Sanity": "PASS", "Schema": "PASS",
            "Temporal": "PASS", "RI": "PASS", "Lineage": "PASS",
            "GX": "PASS", "Outlier": "PASS", "Changelog": "PASS",
            "Overall": "9/9",
        },
        {
            "Product": "Trade Flows (HS-Code Level)",
            "Schema": "v2.0",
            "USA Records": "43,020",
            "Sources": "US Census FT-900 / ALFRED",
            "PIT": "PASS", "Sanity": "PASS", "Schema": "PASS",
            "Temporal": "PASS", "RI": "PASS", "Lineage": "PASS",
            "GX": "PASS", "Outlier": "PASS", "Changelog": "PASS",
            "Overall": "9/9",
        },
        {
            "Product": "Global Macro Baseline",
            "Schema": "v2.0",
            "USA Records": "81,735",
            "Sources": "ALFRED / IMF WEO",
            "PIT": "PASS", "Sanity": "PASS", "Schema": "PASS",
            "Temporal": "PASS", "RI": "PASS", "Lineage": "PASS",
            "GX": "PASS", "Outlier": "PASS", "Changelog": "PASS",
            "Overall": "9/9",
        },
    ])

    st.dataframe(product_status, use_container_width=True, hide_index=True)

    st.markdown(
        "<div style='font-size:0.75rem; color:#444444; margin-top:0.6rem;'>"
        "* Wages CPS: 8,802 rows confirmed (11 unemployment series). "
        "All USA wages, housing, and food sample records carry data_quality_certified = True "
        "following June 2026 vault backfill. Schema v5.0 across all products."
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # Certification statement
    st.markdown(
        "<div style='border:1px solid #1e1e1e; border-radius:8px; padding:1.5rem 1.8rem;"
        " background:#080808; margin-top:1rem;'>"
        "<div style='font-size:0.65rem; color:#444; text-transform:uppercase;"
        " letter-spacing:0.15em; margin-bottom:0.5rem;'>Certification Statement</div>"
        "<div style='font-size:0.95rem; font-weight:700; color:#fff;"
        " margin-bottom:0.7rem;'>Lekwankwa Corporation — Audit Certification v2026.06</div>"
        "<div style='font-size:0.85rem; color:#666; line-height:1.65;'>"
        "This data vault has been independently processed through the Lekwankwa 9-Stage "
        "Automated Validation Engine across 32 sovereign jurisdictions (USA · EU27 · "
        "GBR · CAN · AUS · NOR). All records carry sovereign series identifiers, "
        "data vintage IDs, and full PIT metadata. Source provenance is traceable to "
        "official government API endpoints. No web-scraped content is present in any product."
        "</div>"
        "<div style='font-size:0.75rem; color:#333; margin-top:1rem;'>"
        "Certification Date: 2026-06-20 &nbsp;·&nbsp; "
        "Manifest ID: LKW-VAULT-MANIFEST-2026-06-20 &nbsp;·&nbsp; "
        "Engine Version: 2.0 + Audit v1.0 &nbsp;·&nbsp; "
        "Country-Dataset READY: 159 / 165"
        "</div>"
        "</div>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────
#  ENTRYPOINT
# ──────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="Lekwankwa Corporation — Institutional Data",
        page_icon="◼",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_css()

    page = render_sidebar()

    if page == "Corporate Showroom":
        page_showroom()
    elif page == "Data Sandbox":
        page_sandbox()
    elif page == "Data Quality Hub":
        page_quality_hub()


if __name__ == "__main__":
    main()
