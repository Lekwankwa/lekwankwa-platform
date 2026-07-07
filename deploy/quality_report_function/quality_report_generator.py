"""
quality_report_generator.py — Lekwankwa Corporation
=====================================================

Event-driven quality monitor.  Runs automatically after each vault_extractor
data-release ingestion (NOT on a fixed calendar schedule).  Produces geo-split
granular reports, live and archive masters, an ALERT JSON on CRITICAL/HIGH
findings, and an append-only run-history index.

TRIGGER CHAIN (GCS event-driven — fires on every new data release)
-------------------------------------------------------------------
1.  vault_extractor --mode live writes a completion marker on success:
      {vault_root}/run_markers/extractor_{product}_{YYYY-MM-DD}.complete
2.  A Cloud Storage OBJECT_FINALIZE trigger fires this Cloud Function when any
    extractor_{product}_*.complete object lands in the bucket.
3.  This function checks markers for all required live products on that run date.
    If any are missing → exits cleanly; re-triggered when the next extractor
    writes its marker.
4.  Once all required live-product markers are present, the full quality report
    (geo-split) is generated and uploaded to GCS.
5.  Archive products (housing, global_macro) trigger separately via
    extractor_archive_*.complete markers.

DEPLOYMENT
----------
  gcloud functions deploy quality-report-generator \\
    --runtime python311 --trigger-resource lekwankwa-historical-vault \\
    --trigger-event google.storage.object.finalize \\
    --entry-point cloud_function_handler \\
    --set-env-vars VAULT_ROOT=gs://lekwankwa-historical-vault \\
    --set-secrets ALERT_EMAIL_PASS=quality-report-alert-pass:latest \\
    --memory 1Gi --timeout 540s --region us-central1

OUTPUTS (metadata/quality_reports/)
------------------------------------
  live/YYYY-MM/
    quality_report_master_YYYY-MM-DD.json         ← all live products
    quality_report_dataset_1_food_micropricing_usa_only_YYYY-MM-DD.json
    quality_report_dataset_1_food_micropricing_eu27_only_YYYY-MM-DD.json
    quality_report_dataset_1_food_micropricing_non_eu_block_YYYY-MM-DD.json
    quality_report_dataset_1_food_micropricing_full_32_country_YYYY-MM-DD.json
    quality_report_dataset_2_wages_labor_<geo>_YYYY-MM-DD.json    (×4)
    quality_report_dataset_4_trade_flows_<geo>_YYYY-MM-DD.json    (×4)
    ALERT_YYYY-MM.json                            ← only when CRITICAL/HIGH found
  archive/YYYY-MM/
    quality_report_master_YYYY-MM-DD.json         ← all archive products
    quality_report_dataset_3_housing_credit_<geo>_YYYY-MM-DD.json (×4)
    quality_report_dataset_5_global_macro_<geo>_YYYY-MM-DD.json   (×4)
  quality_report_history.json                     ← append-only run index

LIVE-FEED EXCLUSIONS (applied to live/ only, never archive/)
-------------------------------------------------------------
  food_micropricing    — AUS excluded (quarterly, not monthly)
  wages_and_employment — NOR excluded (SSB Table 07458 frozen at 2024-Q4)

LOCAL TESTING
-------------
  python tools/quality_report_generator.py --vault-root ./lekwankwa-historical-vault \\
      --out-dir ./metadata/quality_reports --dry-run-alerts
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime
import glob as _glob_module
import hashlib
import hmac
import json
import os
import re as _re_module
import re
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALERT_EMAIL_TO = "info@lekwankwa.com"
ALERT_EMAIL_FROM = "info@lekwankwa.com"
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
APPROVAL_SERVICE_URL = os.environ.get("APPROVAL_SERVICE_URL", "https://approval.lekwankwa.com")
APPROVAL_TOKEN_EXPIRY_DAYS = 7

EU27 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK",
    "SVN","ESP","SWE",
]

NON_EU_COUNTRIES = ["GBR", "CAN", "AUS", "NOR"]

ALL_COUNTRIES: dict[str, list[str]] = {
    "USA": ["USA"],
    "EU27": EU27,
    "GBR": ["GBR"],
    "CAN": ["CAN"],
    "AUS": ["AUS"],
    "NOR": ["NOR"],
}

PRODUCTS = [
    "food_micropricing",
    "wages_and_employment",
    "Housing_Supply_and_Shelter_Inflation",
    "trade_flows",
    "global_macro",
]

LIVE_FEED_PRODUCTS = frozenset(["food_micropricing", "wages_and_employment", "trade_flows"])
ARCHIVE_ONLY_PRODUCTS = frozenset(["Housing_Supply_and_Shelter_Inflation", "global_macro"])

# ---------------------------------------------------------------------------
# Geo-split constants (mirrors release_calendar structure)
# ---------------------------------------------------------------------------

# product → file stem used in geo-split filenames
_PRODUCT_STEMS: dict[str, str] = {
    "food_micropricing":                      "dataset_1_food_micropricing",
    "wages_and_employment":                   "dataset_2_wages_labor",
    "Housing_Supply_and_Shelter_Inflation":   "dataset_3_housing_credit",
    "trade_flows":                            "dataset_4_trade_flows",
    "global_macro":                           "dataset_5_global_macro",
}

# product → dataset subfolder name (matches release_calendar folder names)
_DATASET_FOLDER: dict[str, str] = {
    "food_micropricing":                    "Dataset 1 - Food Micropricing",
    "wages_and_employment":                 "Dataset 2 - Wages Labor",
    "Housing_Supply_and_Shelter_Inflation": "Dataset 3 - Housing Credit",
    "trade_flows":                          "Dataset 4 - Trade Flows",
    "global_macro":                         "Dataset 5 - Global Macro",
}

# Geo bundles: (key, human-readable label)
GEO_BUNDLES: list[tuple[str, str]] = [
    ("usa_only",        "USA Only"),
    ("eu27_only",       "EU27 Only"),
    ("non_eu_block",    "Non-EU Block (GBR / CAN / AUS / NOR)"),
    ("full_32_country", "Full 32-Country Coverage"),
]

EU27_SET: frozenset[str] = frozenset(EU27)
NON_EU_SET: frozenset[str] = frozenset(["GBR", "CAN", "AUS", "NOR"])

# Countries/products excluded from live feed — frozen, pending, or non-monthly cadence
LIVE_FEED_EXCLUDED: frozenset[tuple[str, str]] = frozenset({
    ("food_micropricing", "AUS"),         # ABS CPI is quarterly — excluded from monthly live feed
    ("wages_and_employment", "NOR"),      # SSB Table 07458 FROZEN at 2024-Q4
    ("Housing_Supply_and_Shelter_Inflation", "NOR"),  # PENDING_INGESTION
})

# Sources confirmed discontinued — no further releases expected from the source agency
KNOWN_DISCONTINUED: frozenset[tuple[str, str]] = frozenset({
    ("wages_and_employment", "NOR"),            # SSB Table 07458: last release 2024-Q4, confirmed stopped
    ("Housing_Supply_and_Shelter_Inflation", "AUS"),  # ABS last data 2021, no new releases observed
})

# Structural lag: days from end of reference period to agency publication
# Source: confirmed via live API probes across all 6 country groups, June 2026
# (product, country_group) → lag_days
STRUCTURAL_LAG_DAYS: dict[tuple[str, str], int] = {
    # food_micropricing
    ("food_micropricing", "USA"): 14,   # BLS CPI, release ~14th of following month
    ("food_micropricing", "EU27"): 30,  # Eurostat HICP flash ~14d; final ~30d
    ("food_micropricing", "GBR"): 21,   # ONS CPI, ~third Wed of following month
    ("food_micropricing", "CAN"): 21,   # StatCan CPI, ~third Wed of following month
    ("food_micropricing", "AUS"): 35,   # ABS CPI quarterly, ~5 weeks after quarter end
    ("food_micropricing", "NOR"): 14,   # SSB CPI, ~14d after month end

    # wages_and_employment
    ("wages_and_employment", "USA"): 7,   # BLS CES, first Friday of following month
    ("wages_and_employment", "EU27"): 45, # Eurostat unemployment monthly
    ("wages_and_employment", "GBR"): 45,  # ONS LFS, ~6 weeks after reference month
    ("wages_and_employment", "CAN"): 21,  # StatCan LFS, ~3 weeks after reference month
    ("wages_and_employment", "AUS"): 21,  # ABS LFS, ~3 weeks after reference month
    ("wages_and_employment", "NOR"): 45,  # SSB LFS quarterly (FROZEN at 2024K4)

    # Housing_Supply_and_Shelter_Inflation (archive only)
    ("Housing_Supply_and_Shelter_Inflation", "USA"): 30,  # Census permits, ~4 weeks
    ("Housing_Supply_and_Shelter_Inflation", "EU27"): 90, # Eurostat permits, ~3 months
    ("Housing_Supply_and_Shelter_Inflation", "GBR"): 45,  # ONS
    ("Housing_Supply_and_Shelter_Inflation", "CAN"): 21,  # CMHC monthly
    ("Housing_Supply_and_Shelter_Inflation", "AUS"): 90,  # ABS quarterly
    ("Housing_Supply_and_Shelter_Inflation", "NOR"): 30,  # PENDING (placeholder)

    # trade_flows
    ("trade_flows", "USA"): 45,   # BFT monthly
    ("trade_flows", "EU27"): 90,  # Eurostat trade, long lag
    ("trade_flows", "GBR"): 90,   # ONS trade
    ("trade_flows", "CAN"): 45,   # StatCan trade
    ("trade_flows", "AUS"): 33,   # ABS ITGS monthly
    ("trade_flows", "NOR"): 35,   # SSB external trade monthly

    # global_macro
    ("global_macro", "USA"): 14,  # ALFRED CPI monthly (dominant series frequency)
    ("global_macro", "EU27"): 30, # Eurostat GDP flash ~30d; use monthly frequency
    ("global_macro", "GBR"): 30,  # ONS monthly GDP estimate
    ("global_macro", "CAN"): 60,  # StatCan GDP-by-expenditure quarterly
    ("global_macro", "AUS"): 60,  # ABS national accounts quarterly
    ("global_macro", "NOR"): 14,  # SSB CPI monthly (monthly series dominates vault count)
}

# Frequency for expected-period calculation: 'M' monthly, 'Q' quarterly
FREQUENCY: dict[tuple[str, str], str] = {
    # food
    ("food_micropricing", "USA"): "M",
    ("food_micropricing", "EU27"): "M",
    ("food_micropricing", "GBR"): "M",
    ("food_micropricing", "CAN"): "M",
    ("food_micropricing", "AUS"): "Q",  # ABS CPI is quarterly
    ("food_micropricing", "NOR"): "M",
    # wages
    ("wages_and_employment", "USA"): "M",
    ("wages_and_employment", "EU27"): "M",
    ("wages_and_employment", "GBR"): "M",
    ("wages_and_employment", "CAN"): "M",
    ("wages_and_employment", "AUS"): "M",
    ("wages_and_employment", "NOR"): "Q",  # SSB LFS quarterly (frozen)
    # housing
    ("Housing_Supply_and_Shelter_Inflation", "USA"): "M",
    ("Housing_Supply_and_Shelter_Inflation", "EU27"): "Q",
    ("Housing_Supply_and_Shelter_Inflation", "GBR"): "M",
    ("Housing_Supply_and_Shelter_Inflation", "CAN"): "M",
    ("Housing_Supply_and_Shelter_Inflation", "AUS"): "Q",
    ("Housing_Supply_and_Shelter_Inflation", "NOR"): "M",
    # trade
    ("trade_flows", "USA"): "M",
    ("trade_flows", "EU27"): "M",
    ("trade_flows", "GBR"): "M",
    ("trade_flows", "CAN"): "M",
    ("trade_flows", "AUS"): "M",
    ("trade_flows", "NOR"): "M",
    # global_macro
    ("global_macro", "USA"): "M",
    ("global_macro", "EU27"): "M",
    ("global_macro", "GBR"): "M",
    ("global_macro", "CAN"): "Q",
    ("global_macro", "AUS"): "Q",
    ("global_macro", "NOR"): "M",
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Finding:
    finding_id: str           # deterministic: {product}_{country}_{code}
    product: str
    country_group: str
    check_type: str           # FRESHNESS | VALIDATION_STAGE | CONSISTENCY | MONITOR
    code: str                 # e.g. FRESHNESS_FROZEN, STAGE_FAIL, NULL_FIELD
    severity: str             # CRITICAL | HIGH | MEDIUM | LOW
    status: str               # OPEN | ACKNOWLEDGED | RESOLVED | WONT_FIX
    message: str
    detail: dict              # arbitrary context
    first_seen: str           # ISO date YYYY-MM-DD
    last_seen: str
    consecutive_months: int   # months this finding has been OPEN without status change


@dataclasses.dataclass
class SeriesGapResult:
    product: str
    country: str
    series_id: Optional[str]       # None = partition-level check (any series)
    source: Optional[str]
    months_found: int
    min_months: int
    severity: str                  # CRITICAL (0 months) or HIGH (< min_months)
    required_by: list              # strategy numbers
    notes: str
    is_gap: bool                   # True when months_found < min_months


@dataclasses.dataclass
class CountryResult:
    country: str              # ISO3
    country_group: str
    vault_latest: Optional[str]        # ISO date string or None
    expected_latest: Optional[str]
    days_behind: Optional[int]
    freshness_status: str     # FRESH | STALE | FROZEN | PENDING | NO_DATA
    live_feed_eligible: bool
    validation_overall: str   # PASS | FAIL | NO_SUMMARY
    validation_stages_passed: int
    validation_stages_failed: int
    consistency_issues: list[str]
    findings: list[Finding]


@dataclasses.dataclass
class ProductReport:
    product: str
    run_date: str
    country_results: list[CountryResult]
    severity_counts: dict[str, int]
    findings: list[Finding]
    generation_errors: list[str]


@dataclasses.dataclass
class MasterReport:
    run_date: str
    products_included: list[str]
    severity_counts: dict[str, int]
    critical_findings: list[Finding]
    high_findings: list[Finding]
    all_findings: list[Finding]
    cross_check_passed: bool
    cross_check_notes: list[str]
    generation_errors: list[str]


# ---------------------------------------------------------------------------
# Freshness helpers
# ---------------------------------------------------------------------------

def _month_end(d: datetime.date) -> datetime.date:
    """Last day of d's month."""
    next_month = (d.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
    return next_month - datetime.timedelta(days=1)


def _quarter_start(d: datetime.date) -> datetime.date:
    """First day of the quarter containing d."""
    q_month = ((d.month - 1) // 3) * 3 + 1
    return d.replace(month=q_month, day=1)


def _quarter_end(d: datetime.date) -> datetime.date:
    """Last day of the quarter containing d."""
    q_start = _quarter_start(d)
    # Quarter spans 3 months; find the end of the 3rd month
    end_month = q_start.month + 2
    if end_month == 3: return q_start.replace(month=3, day=31)
    if end_month == 6: return q_start.replace(month=6, day=30)
    if end_month == 9: return q_start.replace(month=9, day=30)
    return q_start.replace(month=12, day=31)


def _prev_month_start(d: datetime.date) -> datetime.date:
    return (d.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)


def _prev_quarter_start(d: datetime.date) -> datetime.date:
    qs = _quarter_start(d)
    return _quarter_start(qs - datetime.timedelta(days=1))


def compute_expected_latest(
    today: datetime.date,
    freq: str,
    lag_days: int,
) -> datetime.date:
    """
    Latest period start that should be in the vault by `today` given the
    structural publication lag.  Walks backward until period_end + lag <= today.
    """
    if freq == "M":
        candidate = today.replace(day=1)
        for _ in range(36):  # safety stop
            period_end = _month_end(candidate)
            if period_end + datetime.timedelta(days=lag_days) <= today:
                return candidate
            candidate = _prev_month_start(candidate)
        return candidate  # fallback
    elif freq == "Q":
        candidate = _quarter_start(today)
        for _ in range(12):
            period_end = _quarter_end(candidate)
            if period_end + datetime.timedelta(days=lag_days) <= today:
                return candidate
            candidate = _prev_quarter_start(candidate)
        return candidate
    else:
        raise ValueError(f"Unsupported frequency: {freq}")


def classify_freshness(
    vault_latest: Optional[datetime.date],
    expected_latest: datetime.date,
    lag_days: int,
    freq: str,
    consecutive_frozen_runs: int,
) -> tuple[str, int]:
    """
    Returns (freshness_status, days_behind).

    FRESH   — vault matches expected period
    STALE   — vault is behind expected by more than 2× structural lag
    FROZEN  — behind by 365+ days (multi-year gap = definitive freeze),
              OR behind 180+ days with no movement across 3 consecutive runs
    NO_DATA — vault has no data at all
    """
    if vault_latest is None:
        return "NO_DATA", 0

    days_behind = (expected_latest - vault_latest).days

    if days_behind < 0:
        # Vault is ahead (archive products may have future IMF projections)
        days_behind = 0

    if days_behind > 365 or (days_behind > 180 and consecutive_frozen_runs >= 3):
        return "FROZEN", days_behind
    if days_behind > 2 * lag_days:
        return "STALE", days_behind
    return "FRESH", days_behind


# ---------------------------------------------------------------------------
# Vault reader
# ---------------------------------------------------------------------------

def read_vault_latest(
    vault_root: Path,
    product: str,
    country_iso3: str,
    date_col: str = "data_timestamp",
) -> Optional[datetime.date]:
    """Return the latest obs date in the vault for this product/country."""
    try:
        import pandas as pd
    except ImportError:
        return None

    partitions = list(
        (vault_root / f"product={product}" / f"country={country_iso3}").rglob("*.parquet")
    )
    if not partitions:
        return None
    frames = []
    for p in partitions:
        try:
            df_part = pd.read_parquet(p, columns=[date_col])
            # Normalize to UTC before concat to avoid mixed tz-aware/tz-naive concat errors
            df_part[date_col] = pd.to_datetime(df_part[date_col], utc=True, errors="coerce")
            frames.append(df_part)
        except Exception:
            pass
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    ts = df[date_col].max()
    if pd.isnull(ts):
        return None
    return ts.date()


def read_vault_row_count(
    vault_root: Path,
    product: str,
    country_iso3: str,
) -> int:
    try:
        import pandas as pd
    except ImportError:
        return 0
    partitions = list(
        (vault_root / f"product={product}" / f"country={country_iso3}").rglob("*.parquet")
    )
    total = 0
    for p in partitions:
        try:
            total += len(pd.read_parquet(p, columns=["product"]))
        except Exception:
            pass
    return total


# ---------------------------------------------------------------------------
# Series manifest check (DATA_GAP detection)
# ---------------------------------------------------------------------------

def load_expected_series_manifest(manifest_path: Path) -> list[dict]:
    """
    Load catalog_expected_series.yaml.  Returns the list under 'expected_series'.
    Each entry is a dict with keys: product, country, series_id, source,
    value_col, min_months, backtest_start, required_by, notes.
    """
    try:
        import yaml  # type: ignore
    except ImportError:
        raise RuntimeError("PyYAML required for series manifest check: pip install pyyaml")
    with open(manifest_path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("expected_series", [])


_ANCILLARY_FILE_PATTERNS = frozenset(["outlier", "changelog", "change_log", "audit"])
_YEAR_MONTH_RE = _re_module.compile(r"year=(\d{4}).month=(\d{1,2}).")


def _is_ancillary_file(fpath: str) -> bool:
    name = Path(fpath).name.lower()
    return any(pat in name for pat in _ANCILLARY_FILE_PATTERNS)


def _count_months_in_vault(
    vault_root: Path,
    product: str,
    country: str,
    source: Optional[str],
    series_id: Optional[str],
    max_scan_files: int = 500,
) -> int:
    """
    Count distinct observation months for a product/country/source/series_id.

    For partition-level checks (series_id=None): counts distinct year=Y/month=M
    path segments across all data files — fast, no file reads.

    For series-level checks (series_id specified): scans the most-recent
    max_scan_files data files and counts distinct path months where that series
    appears in sovereign_series_id.  This correctly handles partitions that mix
    multiple series with different coverage depths (e.g. PERMIT spans 1960-2026
    while CUSR0000SAH1 has only 1 file) — only months where the target series
    is actually present are counted.

    Returns 0 if the partition has no data files, or if series_id is specified
    but not found in any sampled file.
    """
    try:
        import pandas as pd
    except ImportError:
        return 0

    if source:
        pattern = str(
            vault_root / f"product={product}" / f"country={country}"
            / f"source={source}" / "**" / "*.parquet"
        )
    else:
        pattern = str(
            vault_root / f"product={product}" / f"country={country}"
            / "**" / "*.parquet"
        )

    all_files = sorted(_glob_module.glob(pattern, recursive=True))
    if not all_files:
        return 0

    # Exclude ancillary files (outliers, changelogs) which lack data columns.
    data_files = [f for f in all_files if not _is_ancillary_file(f)]
    if not data_files:
        return 0

    # Partition-level check (no series_id): count distinct (year, month) from paths.
    if series_id is None:
        months: set[tuple[int, int]] = set()
        for fpath in data_files:
            m = _YEAR_MONTH_RE.search(fpath)
            if m:
                months.add((int(m.group(1)), int(m.group(2))))
        return len(months)

    # Series-level check: count distinct months where THIS series is actually present.
    # Sample both the oldest and newest data files so that series with early-only
    # coverage (discontinued BLS items) are detected alongside recent-only series.
    # Strategy: first 20% of files + last 80%, deduplicated, capped at max_scan_files.
    n_first = min(max_scan_files // 5, len(data_files))
    n_last  = min(max_scan_files - n_first, len(data_files))
    sample_set = set(data_files[:n_first]) | set(data_files[-n_last:])
    sample = sorted(sample_set)
    found_months: set[tuple[int, int]] = set()
    for fpath in sample:
        try:
            try:
                df = pd.read_parquet(fpath, columns=["sovereign_series_id"])
            except Exception:
                df = pd.read_parquet(fpath)
            if "sovereign_series_id" in df.columns and series_id in df["sovereign_series_id"].values:
                m = _YEAR_MONTH_RE.search(fpath)
                if m:
                    found_months.add((int(m.group(1)), int(m.group(2))))
        except Exception:
            continue

    return len(found_months)


def check_series_vault_presence(
    vault_root: Path,
    expected_series: list[dict],
    max_scan_files: int = 500,
) -> list[SeriesGapResult]:
    """
    Diff the expected-series manifest against the live vault.

    For each manifest entry:
      - months_found == 0            → CRITICAL (series completely absent)
      - 0 < months_found < min_months → HIGH    (series present but sparse)
      - months_found >= min_months   → OK       (no finding emitted; is_gap=False)

    Returns one SeriesGapResult per manifest entry.
    """
    results: list[SeriesGapResult] = []

    for entry in expected_series:
        product    = entry.get("product", "")
        country    = entry.get("country", "")
        series_id  = entry.get("series_id")       # None = partition-level check
        source     = entry.get("source")
        min_months = int(entry.get("min_months", 1))
        required   = entry.get("required_by", [])
        notes      = entry.get("notes", "")

        if not product or not country:
            continue

        months_found = _count_months_in_vault(
            vault_root, product, country, source, series_id,
            max_scan_files=max_scan_files,
        )

        if months_found == 0:
            severity = "CRITICAL"
            is_gap   = True
        elif months_found < min_months:
            severity = "HIGH"
            is_gap   = True
        else:
            severity = "LOW"
            is_gap   = False

        results.append(SeriesGapResult(
            product=product,
            country=country,
            series_id=series_id,
            source=source,
            months_found=months_found,
            min_months=min_months,
            severity=severity,
            required_by=required,
            notes=notes,
            is_gap=is_gap,
        ))

    return results


# ---------------------------------------------------------------------------
# Validation summary reader
# ---------------------------------------------------------------------------

def _scope_covers(scope: str, country_group: str) -> bool:
    """Return True if the validation summary scope string covers this country group."""
    s = scope.lower().replace("-", "_")
    if country_group == "USA":
        return "usa" in s or "non_eu" not in s and "eu27" not in s
    if country_group == "EU27":
        return "eu27" in s
    return any(x in s for x in [country_group.lower(), "non_eu"])


def find_latest_validation_summary(
    search_root: Path,
    product: str,
    country_group: str,
) -> Optional[dict]:
    """
    Locate the most recent validation_summary_*.json that covers this
    product and country_group.  Reads only the metadata fields (not the
    full stage detail) to stay light.
    """
    p_lower = product.lower().replace("housing_supply_and_shelter_inflation", "housing")
    candidates = []
    for f in sorted(search_root.glob(f"validation_summary_*.json")):
        name = f.stem.lower()
        if p_lower not in name:
            continue
        candidates.append(f)

    # Most recent last after sort
    for f in reversed(candidates):
        try:
            with open(f, encoding="utf-8") as fh:
                d = json.load(fh)
            scope = d.get("scope", "")
            if _scope_covers(scope, country_group):
                return d
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Consistency audit (lightweight, no full parquet scan for large vaults)
# ---------------------------------------------------------------------------

def audit_consistency(
    vault_root: Path,
    product: str,
    country_iso3: str,
    sample_rows: int = 5000,
) -> list[str]:
    """
    Run consistency checks on a random sample of vault rows.
    Returns a list of issue descriptions (empty list = clean).
    """
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        return ["pandas not available — consistency audit skipped"]

    _META_FILES = {"outliers.parquet", "changelog.parquet"}
    partitions = sorted(
        f for f in (vault_root / f"product={product}" / f"country={country_iso3}").rglob("*.parquet")
        if f.name not in _META_FILES
    )
    if not partitions:
        return []

    frames = []
    remaining = sample_rows
    for p in partitions[-10:]:  # read most recent 10 partitions
        try:
            df = pd.read_parquet(p)
            # Normalize any tz-aware datetime columns to UTC to prevent concat errors
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_convert(None)
            frames.append(df.head(remaining))
            remaining -= len(df)
            if remaining <= 0:
                break
        except Exception:
            pass

    if not frames:
        return []

    df = pd.concat(frames, ignore_index=True)
    issues: list[str] = []

    # C1: data_quality_certified must be present and True on PRIMARY/SECONDARY rows.
    # DERIVED rows carry dqc=False by design (gap-fills) — flag separately as INFO.
    if "data_quality_certified" not in df.columns:
        issues.append("MISSING_FIELD:data_quality_certified")
    elif df["data_quality_certified"].isna().any():
        null_pct = df["data_quality_certified"].isna().mean() * 100
        issues.append(f"NULL_FIELD:data_quality_certified ({null_pct:.1f}% null)")
    elif (df["data_quality_certified"] == False).any():
        dqc_false = df["data_quality_certified"] == False
        if "confidence_tier" in df.columns:
            derived_false   = dqc_false & (df["confidence_tier"] == "DERIVED")
            genuine_false   = dqc_false & ~(df["confidence_tier"] == "DERIVED")
            if genuine_false.any():
                false_pct = genuine_false.mean() * 100
                issues.append(f"DQC_FALSE:data_quality_certified ({false_pct:.1f}% False on PRIMARY/SECONDARY rows)")
            if derived_false.any():
                derived_pct = derived_false.mean() * 100
                issues.append(f"EXPECTED_FILL:{derived_pct:.1f}% of rows are DERIVED gap-fills (dqc=False by design)")
        else:
            false_pct = dqc_false.mean() * 100
            issues.append(f"DQC_FALSE:data_quality_certified ({false_pct:.1f}% False)")

    # C2: product field matches expected
    if "product" in df.columns:
        wrong = df["product"] != product
        if wrong.any():
            issues.append(f"PRODUCT_MISMATCH:{wrong.sum()} rows have wrong product field")

    # C3: confidence_tier — flag non-PRIMARY rows that are NOT DERIVED.
    # DERIVED rows are synthetic gap-fills included by design; they are not anomalies.
    if "confidence_tier" in df.columns:
        non_primary_non_derived = (df["confidence_tier"] != "PRIMARY") & (df["confidence_tier"] != "DERIVED")
        if non_primary_non_derived.any():
            pct = non_primary_non_derived.mean() * 100
            issues.append(f"NON_PRIMARY_TIER:{pct:.1f}% of rows not confidence_tier=PRIMARY (excluding DERIVED)")

    # C4: published_date / data_timestamp ordering (PIT integrity)
    ts_col = "data_timestamp"
    pub_col = (
        "published_date" if "published_date" in df.columns
        else "official_release_date" if "official_release_date" in df.columns
        else None
    )
    if ts_col in df.columns and pub_col:
        # Force utc=True on both to handle mixed tz-aware / tz-naive / mixed-offset columns
        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        pub = pd.to_datetime(df[pub_col], utc=True, errors="coerce")
        violations = (pub < ts).sum()
        if violations:
            issues.append(f"PIT_VIOLATION:{violations} rows have published_date < data_timestamp")

    # C5: duplicate record_ids (if field exists)
    if "record_id" in df.columns:
        dupes = df["record_id"].duplicated().sum()
        if dupes:
            issues.append(f"DUPLICATE_RECORD_ID:{dupes} duplicate record_id values in sample")

    return issues


# ---------------------------------------------------------------------------
# Vault integrity audit (structural / cross-partition checks)
# ---------------------------------------------------------------------------

def audit_vault_integrity(
    vault_root: Path,
    product: str,
    country_iso3: str,
) -> list[str]:
    """
    Structural and cross-partition integrity checks that audit_consistency()
    misses because it only samples the most-recent 10 partitions.

    VI1 — Cross-partition data_vintage_id dedup.
          Loads only the data_vintage_id column from ALL parquet files.
          Ratio total_rows / unique_vids > 1.5x → DEDUP_VIOLATION (HIGH).
          Catches scrapers that write the same vintage to multiple month= folders.

    VI2 — Non-zero-padded month= folder names.
          Path-only check, no parquet reads.
          month=1..month=9 instead of month=01..month=09 → PATH_CONVENTION (MEDIUM).

    VI3 — Future-year partition guard.
          Path-only check, no parquet reads.
          year= folders beyond current_year + 6 → FUTURE_PARTITION (INFO).
          current_year + 1 to current_year + 6 logged as INFO so forecast
          partitions remain visible without triggering alert noise.

    Returns a list of issue strings in the same format as audit_consistency().
    """
    try:
        import pandas as pd
    except ImportError:
        return []

    _META_FILES = {"outliers.parquet", "changelog.parquet"}
    source_root = vault_root / f"product={product}" / f"country={country_iso3}"
    if not source_root.exists():
        return []

    issues: list[str] = []
    current_year = datetime.date.today().year

    # ── VI2: Non-zero-padded month= names (path only) ────────────────────────
    bad_month_dirs: list[str] = []
    for month_dir in source_root.rglob("month=*"):
        if not month_dir.is_dir():
            continue
        month_val = month_dir.name.replace("month=", "")
        try:
            m = int(month_val)
            if 1 <= m <= 9 and len(month_val) == 1:
                bad_month_dirs.append(str(month_dir.relative_to(source_root)))
        except ValueError:
            pass
    if bad_month_dirs:
        example = bad_month_dirs[0].split("\\")[-1] if "\\" in bad_month_dirs[0] else bad_month_dirs[0]
        issues.append(
            f"PATH_CONVENTION:{len(bad_month_dirs)} month= folders use non-zero-padded "
            f"names (e.g. {example}). Expected month=01..month=09."
        )

    # ── VI3: Future-year partition guard (path only) ─────────────────────────
    far_future: list[str] = []
    near_future: list[str] = []
    for year_dir in source_root.rglob("year=*"):
        if not year_dir.is_dir():
            continue
        year_val = year_dir.name.replace("year=", "")
        try:
            y = int(year_val)
            if y > current_year + 6:
                far_future.append(year_val)
            elif y > current_year:
                near_future.append(year_val)
        except ValueError:
            pass
    if far_future:
        issues.append(
            f"FUTURE_PARTITION:{len(far_future)} year= folders exceed "
            f"current_year+6 ({current_year + 6}): {sorted(far_future)[:5]}"
        )
    if near_future:
        issues.append(
            f"FORECAST_PARTITION:{len(near_future)} year= folders are future "
            f"(current_year+1 to current_year+6): {sorted(near_future)[:8]}. "
            f"Verify rows are marked is_forecast=True."
        )

    # ── VI1: Cross-partition data_vintage_id dedup ────────────────────────────
    all_files = [
        f for f in source_root.rglob("*.parquet")
        if f.name not in _META_FILES
    ]
    if not all_files:
        return issues

    vid_frames: list = []
    for fpath in all_files:
        try:
            try:
                df = pd.read_parquet(fpath, columns=["data_vintage_id"])
            except Exception:
                df = pd.read_parquet(fpath)
                if "data_vintage_id" not in df.columns:
                    continue
                df = df[["data_vintage_id"]]
            if df["data_vintage_id"].notna().any():
                vid_frames.append(df[["data_vintage_id"]])
        except Exception:
            continue

    if vid_frames:
        all_vids = pd.concat(vid_frames, ignore_index=True)
        total  = len(all_vids)
        unique = all_vids["data_vintage_id"].nunique()
        if unique > 0:
            ratio = total / unique
            if ratio > 1.5:
                issues.append(
                    f"DEDUP_VIOLATION:{total:,} total rows but only {unique:,} unique "
                    f"data_vintage_ids ({ratio:.1f}x duplication). "
                    f"Same vintage written to multiple partitions."
                )

    return issues


# ---------------------------------------------------------------------------
# Status tracking (carry forward from prior run history)
# ---------------------------------------------------------------------------

def load_finding_status(history_file: Path) -> dict[str, dict]:
    """
    Return a map of finding_id → {status, first_seen, consecutive_months}
    from the most recent run in the history file.
    """
    if not history_file.exists():
        return {}
    try:
        with open(history_file, encoding="utf-8") as f:
            history = json.load(f)
    except Exception:
        return {}
    runs = history.get("runs", [])
    if not runs:
        return {}
    # Build from all runs: carry forward unresolved findings
    status_map: dict[str, dict] = {}
    for run in runs:
        for finding in run.get("findings", []):
            fid = finding["finding_id"]
            current_status = finding.get("status", "OPEN")
            if current_status in ("RESOLVED", "WONT_FIX"):
                # Remove from active tracking
                status_map.pop(fid, None)
            else:
                prev = status_map.get(fid, {})
                status_map[fid] = {
                    "status": current_status,
                    "first_seen": finding.get("first_seen", run.get("run_date", "")),
                    "consecutive_months": prev.get("consecutive_months", 0) + 1,
                }
    return status_map


def make_finding(
    product: str,
    country_group: str,
    check_type: str,
    code: str,
    severity: str,
    message: str,
    detail: dict,
    run_date: str,
    prior_status_map: dict[str, dict],
) -> Finding:
    finding_id = f"{product}_{country_group}_{code}".lower().replace(" ", "_")
    prior = prior_status_map.get(finding_id, {})
    return Finding(
        finding_id=finding_id,
        product=product,
        country_group=country_group,
        check_type=check_type,
        code=code,
        severity=severity,
        status=prior.get("status", "OPEN"),
        message=message,
        detail=detail,
        first_seen=prior.get("first_seen", run_date),
        last_seen=run_date,
        consecutive_months=prior.get("consecutive_months", 1),
    )


# ---------------------------------------------------------------------------
# Severity rules
# ---------------------------------------------------------------------------

def freshness_severity(
    status: str,
    product: str,
    country_group: str,
    days_behind: int,
) -> str:
    """
    FROZEN + live-feed product → CRITICAL
    FROZEN + live-feed product but explicitly excluded (NOR wages) → MEDIUM
    FROZEN + archive-only → MEDIUM
    STALE  + live-feed → HIGH
    STALE  + archive  → MEDIUM
    DISCONTINUED → always MEDIUM (source confirmed stopped)
    """
    key = (product, country_group)
    is_live_feed = product in LIVE_FEED_PRODUCTS
    is_excluded = key in LIVE_FEED_EXCLUDED

    if status == "DISCONTINUED":
        return "MEDIUM"
    if status == "FROZEN":
        if is_live_feed and not is_excluded:
            return "CRITICAL"
        return "MEDIUM"
    if status == "STALE":
        if is_live_feed and not is_excluded:
            return "HIGH"
        return "MEDIUM"
    if status == "NO_DATA":
        if is_live_feed:
            return "CRITICAL"
        return "HIGH"
    return "LOW"


def stage_fail_severity(product: str, stage_name: str) -> str:
    """
    Any validation stage failure on a live-feed product → CRITICAL.
    Archive-only → HIGH.
    """
    if product in LIVE_FEED_PRODUCTS:
        return "CRITICAL"
    return "HIGH"


def consistency_severity(code: str, product: str) -> str:
    critical_codes = {"MISSING_FIELD", "PIT_VIOLATION", "DUPLICATE_RECORD_ID"}
    high_codes     = {"DEDUP_VIOLATION"}
    medium_codes   = {"PATH_CONVENTION"}
    info_codes     = {"EXPECTED_FILL", "FUTURE_PARTITION", "FORECAST_PARTITION"}
    code_prefix = code.split(":")[0]
    if code_prefix in critical_codes:
        return "CRITICAL" if product in LIVE_FEED_PRODUCTS else "HIGH"
    if code_prefix in high_codes:
        return "HIGH"
    if code_prefix in medium_codes:
        return "MEDIUM"
    if code_prefix in info_codes:
        return "INFO"
    return "MEDIUM"


# ---------------------------------------------------------------------------
# GCS marker check
# ---------------------------------------------------------------------------

def check_extractor_markers(
    vault_root: str,
    run_date: str,
    required_products: list[str],
    local_mode: bool = False,
) -> tuple[bool, list[str], list[str]]:
    """
    Check whether all required extractor completion markers exist.
    Returns (all_present, present_list, missing_list).
    """
    present, missing = [], []
    for prod in required_products:
        marker_name = f"extractor_{prod}_{run_date}.complete"
        if local_mode:
            marker_path = Path(vault_root) / "run_markers" / marker_name
            if marker_path.exists():
                present.append(prod)
            else:
                missing.append(prod)
        else:
            try:
                from google.cloud import storage  # type: ignore
                client = storage.Client()
                bucket_name = vault_root.replace("gs://", "").split("/")[0]
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(f"run_markers/{marker_name}")
                if blob.exists():
                    present.append(prod)
                else:
                    missing.append(prod)
            except Exception as e:
                missing.append(prod)
    return len(missing) == 0, present, missing


def write_extractor_marker(vault_root: Path, product: str, run_date: str) -> None:
    """Write a local extractor completion marker (used in --mode live after success)."""
    marker_dir = vault_root / "run_markers"
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / f"extractor_{product}_{run_date}.complete").touch()


# ---------------------------------------------------------------------------
# Granular report builder
# ---------------------------------------------------------------------------

def generate_granular_report(
    product: str,
    vault_root: Path,
    search_root: Path,
    run_date: str,
    today: datetime.date,
    prior_status_map: dict[str, dict],
    history_file: Optional[Path],
    min_consecutive_for_frozen: int = 3,
) -> ProductReport:
    """Build one granular product report across all countries."""
    country_results: list[CountryResult] = []
    all_findings: list[Finding] = []
    generation_errors: list[str] = []

    # Load prior vault_latest history for FROZEN detection
    prior_vault_latest: dict[str, list[str]] = {}  # country → [dates from last N runs]
    if history_file and history_file.exists():
        try:
            with open(history_file, encoding="utf-8") as f:
                hist = json.load(f)
            for run in hist.get("runs", []):
                prod_data = run.get("products", {}).get(product, {})
                for ciso3, cdata in prod_data.get("countries", {}).items():
                    prior_vault_latest.setdefault(ciso3, []).append(
                        cdata.get("vault_latest", "")
                    )
        except Exception:
            pass

    for country_group, iso3_list in ALL_COUNTRIES.items():
        for country_iso3 in iso3_list:
            # Check PENDING skip
            is_pending = (product, country_group) in {
                ("Housing_Supply_and_Shelter_Inflation", "NOR"),
            }

            # Read vault
            try:
                vault_latest = read_vault_latest(vault_root, product, country_iso3)
            except Exception as e:
                generation_errors.append(f"{product}/{country_iso3}: vault read error: {e}")
                vault_latest = None

            lag = STRUCTURAL_LAG_DAYS.get((product, country_group), 30)
            freq = FREQUENCY.get((product, country_group), "M")
            expected_latest = compute_expected_latest(today, freq, lag)

            # Consecutive frozen check
            prior_dates = prior_vault_latest.get(country_iso3, [])
            if vault_latest is not None:
                all_same = all(d == str(vault_latest) for d in prior_dates[-min_consecutive_for_frozen:]) and len(prior_dates) >= min_consecutive_for_frozen - 1
                consecutive_frozen_runs = min_consecutive_for_frozen if all_same else 0
            else:
                consecutive_frozen_runs = 0

            if is_pending:
                freshness_status = "PENDING"
                days_behind = None
            else:
                freshness_status, days_behind = classify_freshness(
                    vault_latest, expected_latest, lag, freq, consecutive_frozen_runs
                )
                if freshness_status == "FROZEN" and (product, country_group) in KNOWN_DISCONTINUED:
                    freshness_status = "DISCONTINUED"

            live_feed_eligible = (
                product in LIVE_FEED_PRODUCTS
                and (product, country_group) not in LIVE_FEED_EXCLUDED
            )

            # Validation summary
            val_summary = find_latest_validation_summary(search_root, product, country_group)
            if val_summary:
                val_overall = val_summary.get("overall", "NO_SUMMARY")
                val_passed = val_summary.get("stages_passed", 0)
                val_failed = val_summary.get("stages_failed", 0)
            else:
                val_overall = "NO_SUMMARY"
                val_passed = val_failed = 0

            # Consistency audit (only for representative ISO3 per group to keep runtime sane)
            representative = iso3_list[0]  # audit first country per group
            if country_iso3 == representative and not is_pending:
                consistency_issues = audit_consistency(vault_root, product, country_iso3)
                consistency_issues += audit_vault_integrity(vault_root, product, country_iso3)
            else:
                consistency_issues = []

            # Build findings for this country
            findings: list[Finding] = []

            # Freshness findings
            if freshness_status in ("STALE", "FROZEN", "NO_DATA", "DISCONTINUED"):
                sev = freshness_severity(freshness_status, product, country_group, days_behind or 0)
                extra_msg = ""
                if (product, country_group) in LIVE_FEED_EXCLUDED:
                    extra_msg = " (excluded from live feed tier)"
                if freshness_status == "DISCONTINUED":
                    fmsg = (
                        f"{product} / {country_group}: DISCONTINUED — data collection has stopped. "
                        f"Last available: {vault_latest} ({days_behind} days behind expected)."
                    )
                else:
                    fmsg = (
                        f"{product} / {country_group}: {freshness_status}. "
                        f"Vault latest = {vault_latest}, expected = {expected_latest}, "
                        f"days behind = {days_behind}{extra_msg}."
                    )
                f = make_finding(
                    product=product,
                    country_group=country_group,
                    check_type="FRESHNESS",
                    code=f"FRESHNESS_{freshness_status}",
                    severity=sev,
                    message=fmsg,
                    detail={
                        "vault_latest": str(vault_latest),
                        "expected_latest": str(expected_latest),
                        "days_behind": days_behind,
                        "lag_days": lag,
                        "frequency": freq,
                        "live_feed_eligible": live_feed_eligible,
                        "consecutive_frozen_runs": consecutive_frozen_runs,
                    },
                    run_date=run_date,
                    prior_status_map=prior_status_map,
                )
                findings.append(f)

            # Validation stage failures
            if val_overall == "FAIL" and val_summary:
                for stage in val_summary.get("stage_results", []):
                    if stage.get("status") == "FAIL":
                        sev = stage_fail_severity(product, stage.get("name", ""))
                        f = make_finding(
                            product=product,
                            country_group=country_group,
                            check_type="VALIDATION_STAGE",
                            code=f"STAGE_FAIL_S{stage.get('stage', '?')}",
                            severity=sev,
                            message=(
                                f"{product} / {country_group}: Validation stage "
                                f"'{stage.get('name')}' FAILED."
                            ),
                            detail={"stage": stage.get("stage"), "name": stage.get("name")},
                            run_date=run_date,
                            prior_status_map=prior_status_map,
                        )
                        findings.append(f)

            # Consistency findings
            for issue in consistency_issues:
                sev = consistency_severity(issue, product)
                f = make_finding(
                    product=product,
                    country_group=country_group,
                    check_type="CONSISTENCY",
                    code=issue.split(":")[0],
                    severity=sev,
                    message=f"{product} / {country_group}: Consistency issue: {issue}.",
                    detail={"issue": issue},
                    run_date=run_date,
                    prior_status_map=prior_status_map,
                )
                findings.append(f)

            # Escalation: CRITICAL/HIGH open for 2+ consecutive months
            for f in findings:
                if f.severity in ("CRITICAL", "HIGH") and f.status == "OPEN" and f.consecutive_months >= 2:
                    f.detail["ESCALATION"] = (
                        f"Finding has been OPEN for {f.consecutive_months} consecutive months."
                    )

            all_findings.extend(findings)
            country_results.append(CountryResult(
                country=country_iso3,
                country_group=country_group,
                vault_latest=str(vault_latest) if vault_latest else None,
                expected_latest=str(expected_latest) if not is_pending else None,
                days_behind=days_behind,
                freshness_status=freshness_status,
                live_feed_eligible=live_feed_eligible,
                validation_overall=val_overall,
                validation_stages_passed=val_passed,
                validation_stages_failed=val_failed,
                consistency_issues=consistency_issues,
                findings=findings,
            ))

    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    for f in all_findings:
        severity_counts[f.severity] = severity_counts.get(f.severity, 0) + 1

    return ProductReport(
        product=product,
        run_date=run_date,
        country_results=country_results,
        severity_counts=severity_counts,
        findings=all_findings,
        generation_errors=generation_errors,
    )


# ---------------------------------------------------------------------------
# Master report builder
# ---------------------------------------------------------------------------

def generate_master_report(
    granular_reports: list[ProductReport],
    run_date: str,
) -> MasterReport:
    """
    Aggregate from the 5 granular reports.  Never re-reads the vault —
    all values MUST trace to a granular report.
    """
    all_findings: list[Finding] = []
    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    cross_check_notes: list[str] = []
    generation_errors: list[str] = []

    for gr in granular_reports:
        if gr.generation_errors:
            generation_errors.extend(gr.generation_errors)
        for f in gr.findings:
            all_findings.append(f)
            severity_counts[f.severity] = severity_counts.get(f.severity, 0) + 1

    # Cross-check: verify master severity_counts == sum of granular counts
    granular_total = sum(
        s
        for gr in granular_reports
        for s in gr.severity_counts.values()
    )
    master_total = sum(severity_counts.values())
    if granular_total != master_total:
        cross_check_notes.append(
            f"CROSS_CHECK_MISMATCH: master total findings ({master_total}) != "
            f"sum of granular findings ({granular_total})"
        )
        cross_check_passed = False
    else:
        cross_check_passed = True

    products_included = [gr.product for gr in granular_reports]

    return MasterReport(
        run_date=run_date,
        products_included=products_included,
        severity_counts=severity_counts,
        critical_findings=[f for f in all_findings if f.severity == "CRITICAL"],
        high_findings=[f for f in all_findings if f.severity == "HIGH"],
        all_findings=all_findings,
        cross_check_passed=cross_check_passed,
        cross_check_notes=cross_check_notes,
        generation_errors=generation_errors,
    )


# ---------------------------------------------------------------------------
# Report serializers
# ---------------------------------------------------------------------------

def _finding_to_dict(f: Finding) -> dict:
    return dataclasses.asdict(f)


def granular_to_json(report: ProductReport) -> dict:
    report_mode = (
        "ARCHIVE_DELIVERY"
        if report.product in ARCHIVE_ONLY_PRODUCTS
        else "LIVE_FEED_MONTHLY"
    )
    return {
        "report_type": "GRANULAR",
        "report_mode": report_mode,
        "product": report.product,
        "run_date": report.run_date,
        "severity_counts": report.severity_counts,
        "total_findings": sum(report.severity_counts.values()),
        "countries": [
            {
                "country": cr.country,
                "country_group": cr.country_group,
                "vault_latest": cr.vault_latest,
                "expected_latest": cr.expected_latest,
                "days_behind": cr.days_behind,
                "freshness_status": cr.freshness_status,
                "live_feed_eligible": cr.live_feed_eligible,
                "validation_overall": cr.validation_overall,
                "validation_stages_passed": cr.validation_stages_passed,
                "validation_stages_failed": cr.validation_stages_failed,
                "consistency_issues": cr.consistency_issues,
                "findings": [_finding_to_dict(f) for f in cr.findings],
            }
            for cr in report.country_results
        ],
        "all_findings": [_finding_to_dict(f) for f in report.findings],
        "generation_errors": report.generation_errors,
    }


def master_to_json(report: MasterReport) -> dict:
    return {
        "report_type": "MASTER",
        "run_date": report.run_date,
        "products_included": report.products_included,
        "severity_counts": report.severity_counts,
        "total_findings": sum(report.severity_counts.values()),
        "cross_check_passed": report.cross_check_passed,
        "cross_check_notes": report.cross_check_notes,
        "critical_findings": [_finding_to_dict(f) for f in report.critical_findings],
        "high_findings": [_finding_to_dict(f) for f in report.high_findings],
        "all_findings": [_finding_to_dict(f) for f in report.all_findings],
        "generation_errors": report.generation_errors,
    }


def granular_to_md(report: ProductReport) -> str:
    lines = [
        f"# Quality Report — {report.product}",
        f"**Run date**: {report.run_date}",
        "",
        "## Severity Summary",
        "| Severity | Count |",
        "|----------|-------|",
    ]
    for sev, n in report.severity_counts.items():
        lines.append(f"| {sev} | {n} |")
    lines += ["", "## Country Results", ""]
    for cr in report.country_results:
        flag = "⚠️" if cr.findings else "✓"
        lines.append(
            f"### {cr.country} ({cr.country_group}) {flag}"
        )
        lines.append(f"- Vault latest: `{cr.vault_latest}`")
        lines.append(f"- Expected latest: `{cr.expected_latest}`")
        lines.append(f"- Freshness: **{cr.freshness_status}** (days behind: {cr.days_behind})")
        lines.append(f"- Live-feed eligible: {cr.live_feed_eligible}")
        lines.append(f"- Validation: {cr.validation_overall} ({cr.validation_stages_passed} passed, {cr.validation_stages_failed} failed)")
        if cr.consistency_issues:
            lines.append(f"- Consistency issues: {', '.join(cr.consistency_issues)}")
        if cr.findings:
            lines.append("- **Findings:**")
            for f in cr.findings:
                lines.append(f"  - [{f.severity}] `{f.code}`: {f.message}")
        lines.append("")
    if report.generation_errors:
        lines += ["## Generation Errors", ""]
        for e in report.generation_errors:
            lines.append(f"- {e}")
    return "\n".join(lines)


def master_to_md(report: MasterReport) -> str:
    lines = [
        "# Quality Report — MASTER",
        f"**Run date**: {report.run_date}",
        f"**Products**: {', '.join(report.products_included)}",
        f"**Cross-check**: {'PASSED' if report.cross_check_passed else 'FAILED'}",
        "",
        "## Severity Summary",
        "| Severity | Count |",
        "|----------|-------|",
    ]
    for sev, n in report.severity_counts.items():
        lines.append(f"| {sev} | {n} |")
    lines += ["", "## CRITICAL Findings", ""]
    for f in report.critical_findings:
        lines.append(f"- [{f.severity}|{f.status}] `{f.code}` ({f.product}/{f.country_group}): {f.message}")
    lines += ["", "## HIGH Findings", ""]
    for f in report.high_findings:
        lines.append(f"- [{f.severity}|{f.status}] `{f.code}` ({f.product}/{f.country_group}): {f.message}")
    if report.cross_check_notes:
        lines += ["", "## Cross-check Notes", ""]
        for n in report.cross_check_notes:
            lines.append(f"- {n}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def build_alert_payload(
    master: MasterReport,
    run_date: str,
    run_month: str,
) -> dict:
    return {
        "alert_type": "DATA_QUALITY_ALERT",
        "run_date": run_date,
        "run_month": run_month,
        "critical_count": len(master.critical_findings),
        "high_count": len(master.high_findings),
        "critical_findings": [_finding_to_dict(f) for f in master.critical_findings],
        "high_findings": [_finding_to_dict(f) for f in master.high_findings],
    }


def _make_approval_token(fix_id: str, finding: Finding, run_date: str) -> str:
    """HMAC-SHA256 signed token: base64url(payload).hex_sig"""
    secret = os.environ.get("APPROVAL_SECRET", "dev-insecure-secret").encode()
    exp = int(
        (datetime.datetime.utcnow() + datetime.timedelta(days=APPROVAL_TOKEN_EXPIRY_DAYS)).timestamp()
    )
    payload = json.dumps({
        "fix_id": fix_id,
        "finding_id": finding.finding_id,
        "product": finding.product,
        "country_group": finding.country_group,
        "code": finding.code,
        "severity": finding.severity,
        "run_date": run_date,
        "exp": exp,
    }, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
    sig = hmac.new(secret, b64.encode(), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def get_proposed_fix(finding: Finding) -> str:
    code = finding.code
    if code.startswith("FRESHNESS"):
        return f"Re-run {finding.product} scraper for {finding.country_group} to refresh stale data."
    if code.startswith("STAGE_FAIL"):
        return f"Re-run validation pipeline for {finding.product} / {finding.country_group}."
    if code.startswith("EXPECTED_FILL"):
        return "No action required. DERIVED gap-fill rows carry dqc=False by design. Clients filter on confidence_tier."
    if code.startswith("DQC_FALSE"):
        return f"Run DQC repair script on {finding.product} / {finding.country_group} vault partition."
    if code.startswith("PIT_VIOLATION"):
        return f"Rollback last vault partition for {finding.product} / {finding.country_group} and re-ingest."
    if code.startswith("MISSING_FIELD"):
        return f"Re-run field normalization for {finding.product} / {finding.country_group}."
    if code.startswith("NULL_FIELD"):
        return f"Run null-field repair for {finding.product} / {finding.country_group}."
    if code.startswith("DUPLICATE_RECORD_ID"):
        return f"Run deduplication repair for {finding.product} / {finding.country_group}."
    if code.startswith("DEDUP_VIOLATION"):
        return (
            f"Collapse duplicate partitions in {finding.product}/{finding.country_group}: "
            f"for each observation year keep exactly one canonical month= folder and remove all copies. "
            f"Run vault-wide dedup audit: total_rows / unique_data_vintage_id should equal 1.0x per source."
        )
    if code.startswith("PATH_CONVENTION"):
        return (
            f"Rename non-zero-padded month= folders in {finding.product}/{finding.country_group} "
            f"to zero-padded format (month=01..month=09). "
            f"Re-write parquet files into the renamed paths and delete the old folders."
        )
    if code.startswith("FUTURE_PARTITION"):
        return (
            f"Review year= folders beyond current_year+6 in {finding.product}/{finding.country_group}. "
            f"If rows are IMF/agency forecasts, verify is_forecast=True on all rows. "
            f"If ingestion error, delete the future partition and re-ingest from correct period."
        )
    if code.startswith("FORECAST_PARTITION"):
        return (
            f"Verify that all rows in future year= folders for {finding.product}/{finding.country_group} "
            f"carry is_forecast=True. No action needed if confirmed; flag scraper if is_forecast=False on any future row."
        )
    if code.startswith("DATA_GAP_SERIES"):
        series = finding.detail.get("series_id", "unknown")
        strats = finding.detail.get("required_by_strategies", [])
        strat_str = f" (required by strategies {strats})" if strats else ""
        return (
            f"Ingest {series} into {finding.product}/{finding.country_group} vault{strat_str}. "
            f"See catalog_expected_series.yaml for source details."
        )
    if code.startswith("DATA_GAP_PARTITION"):
        strats = finding.detail.get("required_by_strategies", [])
        strat_str = f" (required by strategies {strats})" if strats else ""
        return (
            f"Ingest {finding.product}/{finding.country_group} partition{strat_str}. "
            f"Partition has fewer months than required by catalog_expected_series.yaml."
        )
    return f"Investigate and remediate {code} in {finding.product} / {finding.country_group}."


_EMAIL_HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
body{{font-family:Arial,sans-serif;font-size:14px;color:#202124;max-width:700px;margin:0 auto;padding:20px;}}
h1{{color:#d93025;font-size:20px;margin-bottom:4px;}}
.sub{{color:#5f6368;font-size:13px;margin-bottom:24px;}}
table{{width:100%;border-collapse:collapse;margin-bottom:24px;}}
th{{background:#f1f3f4;text-align:left;padding:8px 12px;font-size:12px;color:#5f6368;border-bottom:2px solid #dadce0;}}
td{{padding:10px 12px;border-bottom:1px solid #e8eaed;vertical-align:top;}}
.sev-CRITICAL{{color:#d93025;font-weight:bold;}}
.sev-HIGH{{color:#e37400;font-weight:bold;}}
.fix{{font-size:12px;color:#5f6368;margin-top:4px;}}
.btn-approve{{display:inline-block;background:#137333;color:#fff!important;padding:6px 16px;border-radius:4px;text-decoration:none;font-size:12px;font-weight:bold;margin-right:8px;}}
.btn-reject{{display:inline-block;background:#c5221f;color:#fff!important;padding:6px 16px;border-radius:4px;text-decoration:none;font-size:12px;font-weight:bold;}}
.footer{{font-size:11px;color:#9aa0a6;border-top:1px solid #e8eaed;padding-top:16px;margin-top:24px;}}
</style>
</head>
<body>
<h1>Data Quality Alert</h1>
<p class="sub">Run date: {run_date} &nbsp;|&nbsp; {critical_count} CRITICAL &nbsp;|&nbsp; {high_count} HIGH</p>
<table>
<tr>
  <th>Severity</th><th>Product / Country</th><th>Finding</th><th>Approve fix</th>
</tr>
{rows}
</table>
<p class="footer">
  Lekwankwa Corporation &mdash; Data Quality Operations<br>
  Approval links expire in {expiry_days} days. Reply to this email to escalate.
</p>
</body>
</html>"""

_EMAIL_ROW_TEMPLATE = """<tr>
  <td class="sev-{severity}">{severity}</td>
  <td>{product}<br><span style="font-size:12px;color:#5f6368">{country_group}</span></td>
  <td>{code}<br><span class="fix">{proposed_fix}</span></td>
  <td>
    <a class="btn-approve" href="{approve_url}">APPROVE</a>
    <a class="btn-reject" href="{reject_url}">REJECT</a>
  </td>
</tr>"""


def build_html_alert_email(
    findings: list[Finding],
    run_date: str,
    approval_service_url: str,
) -> tuple[str, str]:
    """Return (html_body, plain_body) for the alert email."""
    rows = []
    plain_lines = [f"Quality Alert — {run_date}", ""]
    for f in findings:
        fix_id = f"{f.finding_id}_{run_date}"
        token = _make_approval_token(fix_id, f, run_date)
        approve_url = f"{approval_service_url}/approve?token={token}"
        reject_url = f"{approval_service_url}/reject?token={token}"
        proposed_fix = get_proposed_fix(f)
        rows.append(_EMAIL_ROW_TEMPLATE.format(
            severity=f.severity,
            product=f.product,
            country_group=f.country_group,
            code=f.code,
            proposed_fix=proposed_fix,
            approve_url=approve_url,
            reject_url=reject_url,
        ))
        plain_lines.append(
            f"[{f.severity}] {f.product}/{f.country_group} — {f.code}\n"
            f"  Fix: {proposed_fix}\n"
            f"  APPROVE: {approve_url}\n"
            f"  REJECT:  {reject_url}"
        )
    critical_count = sum(1 for f in findings if f.severity == "CRITICAL")
    high_count = sum(1 for f in findings if f.severity == "HIGH")
    html = _EMAIL_HTML_TEMPLATE.format(
        run_date=run_date,
        critical_count=critical_count,
        high_count=high_count,
        rows="\n".join(rows),
        expiry_days=APPROVAL_TOKEN_EXPIRY_DAYS,
    )
    return html, "\n\n".join(plain_lines)


def send_alert_email(
    subject: str,
    html_body: str,
    plain_body: str,
    dry_run: bool = False,
) -> bool:
    if dry_run:
        print(f"[DRY-RUN ALERT] Subject: {subject}")
        print(f"[DRY-RUN ALERT] Plain body preview:\n{plain_body[:600]}")
        return True

    password = os.environ.get("ALERT_EMAIL_PASS", "")
    if not password:
        print("WARNING: ALERT_EMAIL_PASS not set — alert email not sent", file=sys.stderr)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL_FROM
        msg["To"] = ALERT_EMAIL_TO
        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(ALERT_EMAIL_FROM, password)
            server.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO, msg.as_string())
        return True
    except Exception as e:
        print(f"ERROR: Alert email failed: {e}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def write_md(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def append_history(
    history_file: Path,
    run_date: str,
    granular_reports: list[ProductReport],
    master: MasterReport,
) -> None:
    """Never overwrite — append this run to the history index."""
    if history_file.exists():
        try:
            with open(history_file, encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = {"runs": []}
    else:
        history = {"runs": []}

    # Build per-product/country vault_latest snapshot for future FROZEN detection
    products_snapshot: dict[str, dict] = {}
    for gr in granular_reports:
        countries_snapshot: dict[str, dict] = {}
        for cr in gr.country_results:
            countries_snapshot[cr.country] = {
                "vault_latest": cr.vault_latest,
                "freshness_status": cr.freshness_status,
                "days_behind": cr.days_behind,
            }
        products_snapshot[gr.product] = {"countries": countries_snapshot}

    run_entry = {
        "run_date": run_date,
        "severity_counts": master.severity_counts,
        "total_findings": sum(master.severity_counts.values()),
        "findings": [_finding_to_dict(f) for f in master.all_findings],
        "products": products_snapshot,
    }
    history["runs"].append(run_entry)

    history_file.parent.mkdir(parents=True, exist_ok=True)
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(
    vault_root: Path,
    out_dir: Path,
    search_root: Path,
    dry_run_alerts: bool = False,
    check_markers: bool = False,
    run_date: Optional[str] = None,
    skip_monitor_failure_on_missing_markers: bool = False,
    mode: str = "all",
    series_manifest_path: Optional[Path] = None,
) -> int:
    """
    Returns 0 on success, 1 on DATA_QUALITY_FAILURE (CRITICAL/HIGH found),
    2 on MONITOR_FAILURE (extractor markers missing or report generation error).

    mode: 'all' (default), 'live' (food/wages/trade only), 'archive' (housing/macro only).
    series_manifest_path: path to catalog_expected_series.yaml; enables DATA_GAP check.
    """
    today = datetime.date.today()
    run_date = run_date or str(today)
    run_month = run_date[:7]  # YYYY-MM

    if mode == "live":
        products_to_run = [p for p in PRODUCTS if p in LIVE_FEED_PRODUCTS]
    elif mode == "archive":
        products_to_run = [p for p in PRODUCTS if p in ARCHIVE_ONLY_PRODUCTS]
    else:
        products_to_run = list(PRODUCTS)

    live_out_dir = out_dir / "live" / run_month
    archive_out_dir = out_dir / "archive" / run_month
    live_out_dir.mkdir(parents=True, exist_ok=True)
    archive_out_dir.mkdir(parents=True, exist_ok=True)
    history_file = out_dir / "quality_report_history.json"

    print(f"[quality_report_generator] run_date={run_date}  vault={vault_root}")

    # --- Step 0: Extractor marker check ---
    if check_markers and mode != "archive":
        live_products = list(LIVE_FEED_PRODUCTS)
        all_present, present, missing = check_extractor_markers(
            str(vault_root), run_date, live_products, local_mode=True
        )
        if not all_present and not skip_monitor_failure_on_missing_markers:
            print(
                f"MONITOR_FAILURE: Extractor markers missing for: {missing}. "
                "Exiting cleanly — will retry when remaining markers arrive."
            )
            return 2
        if missing:
            print(f"WARNING: Skipping marker check for missing: {missing}")

    # --- Step 1: Load prior status ---
    prior_status_map = load_finding_status(history_file)

    # --- Step 1.5: Series manifest check (DATA_GAP detection) ---
    # Compares catalog_expected_series.yaml against the live vault.
    # Surfaces any series with zero vault partitions as CRITICAL DATA_GAP,
    # and under-threshold series as HIGH DATA_GAP.  These findings are
    # orthogonal to FRESHNESS: FRESHNESS cannot detect a series that was
    # never ingested; DATA_GAP catches that complementary failure mode.
    gap_findings: list[Finding] = []
    if series_manifest_path and series_manifest_path.exists():
        print(f"  → series manifest check: {series_manifest_path.name}")
        try:
            expected_series = load_expected_series_manifest(series_manifest_path)
            gap_results = check_series_vault_presence(vault_root, expected_series)

            for result in gap_results:
                if not result.is_gap:
                    continue
                sid  = result.series_id or "PARTITION"
                code = (
                    f"DATA_GAP_SERIES_{sid}"
                    if result.series_id
                    else f"DATA_GAP_PARTITION_{result.product}_{result.country}"
                ).upper().replace("-", "_")

                strats_str = (
                    f" Required by strategies: {result.required_by}."
                    if result.required_by else ""
                )
                if result.months_found == 0:
                    msg = (
                        f"DATA_GAP CRITICAL: {result.product}/{result.country} "
                        f"series '{sid}' has ZERO months in vault."
                        f"{strats_str} {result.notes}"
                    )
                else:
                    msg = (
                        f"DATA_GAP HIGH: {result.product}/{result.country} "
                        f"series '{sid}' has only {result.months_found} months "
                        f"(min required: {result.min_months})."
                        f"{strats_str} {result.notes}"
                    )

                f = make_finding(
                    product=result.product,
                    country_group=result.country,
                    check_type="DATA_GAP",
                    code=code,
                    severity=result.severity,
                    message=msg,
                    detail={
                        "series_id":             result.series_id,
                        "source":                result.source,
                        "months_found":          result.months_found,
                        "min_months_required":   result.min_months,
                        "required_by_strategies": result.required_by,
                        "notes":                 result.notes,
                    },
                    run_date=run_date,
                    prior_status_map=prior_status_map,
                )
                gap_findings.append(f)

            critical_gaps = sum(1 for f in gap_findings if f.severity == "CRITICAL")
            high_gaps     = sum(1 for f in gap_findings if f.severity == "HIGH")
            ok_count      = sum(1 for r in gap_results if not r.is_gap)
            print(
                f"     manifest: {len(expected_series)} entries checked — "
                f"{ok_count} OK, {high_gaps} HIGH, {critical_gaps} CRITICAL"
            )

            # Write standalone DATA_GAP report for operational visibility
            if gap_findings:
                gap_payload = {
                    "report_type":      "DATA_GAP",
                    "run_date":         run_date,
                    "manifest_path":    str(series_manifest_path),
                    "entries_checked":  len(expected_series),
                    "gaps_found":       len(gap_findings),
                    "critical_count":   critical_gaps,
                    "high_count":       high_gaps,
                    "gap_findings":     [_finding_to_dict(f) for f in gap_findings],
                    "ok_series":        [
                        {
                            "product":      r.product,
                            "country":      r.country,
                            "series_id":    r.series_id,
                            "months_found": r.months_found,
                        }
                        for r in gap_results if not r.is_gap
                    ],
                }
                gap_file = live_out_dir / f"DATA_GAP_{run_month}.json"
                write_json(gap_file, gap_payload)
                print(f"     DATA_GAP report → {gap_file.name}")

        except Exception as exc:
            print(
                f"  WARNING: series manifest check failed: {exc}",
                file=sys.stderr,
            )
    elif series_manifest_path:
        print(
            f"  WARNING: series manifest not found: {series_manifest_path}",
            file=sys.stderr,
        )

    # --- Step 2: Granular reports ---
    granular_reports: list[ProductReport] = []
    for product in products_to_run:
        print(f"  → {product} ...")
        try:
            gr = generate_granular_report(
                product=product,
                vault_root=vault_root,
                search_root=search_root,
                run_date=run_date,
                today=today,
                prior_status_map=prior_status_map,
                history_file=history_file,
            )
            granular_reports.append(gr)

            # Write granular outputs
            is_live_product = product in LIVE_FEED_PRODUCTS
            granular_json = granular_to_json(gr)
            print(f"     findings: {gr.severity_counts}")

            # Write geo-split to live/ for live-feed products (with live exclusions)
            if is_live_product:
                _write_geo_split_reports(
                    product=product,
                    granular_json=granular_json,
                    out_dir=live_out_dir,
                    run_date=run_date,
                    is_live=True,
                )

            # Write geo-split to archive/ for all 5 products (no live exclusions)
            _write_geo_split_reports(
                product=product,
                granular_json=granular_json,
                out_dir=archive_out_dir,
                run_date=run_date,
                is_live=False,
            )
        except Exception as e:
            print(f"  ERROR generating granular report for {product}: {e}", file=sys.stderr)
            granular_reports.append(ProductReport(
                product=product,
                run_date=run_date,
                country_results=[],
                severity_counts={"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0},
                findings=[],
                generation_errors=[f"MONITOR_FAILURE: {e}"],
            ))

    # --- Step 3: Master reports (live master + archive master, written separately) ---
    live_reports    = [gr for gr in granular_reports if gr.product in LIVE_FEED_PRODUCTS]
    archive_reports = list(granular_reports)  # all 5 datasets in archive

    for report_set, target_dir, label in (
        (live_reports,    live_out_dir,    "live"),
        (archive_reports, archive_out_dir, "archive"),
    ):
        if not report_set:
            continue
        master = generate_master_report(report_set, run_date)
        master_json_path = target_dir / f"quality_report_master_{run_date}.json"
        write_json(master_json_path, master_to_json(master))
        print(f"  → {label} master: {master.severity_counts}  cross_check_passed={master.cross_check_passed}")

    # Use live master (or first available) for alerting
    master = generate_master_report(granular_reports, run_date)

    # Inject DATA_GAP findings from Step 1.5 into the master report.
    # These are not in any granular ProductReport (they are cross-product),
    # so they must be merged here before alerting and history append.
    for gf in gap_findings:
        master.all_findings.append(gf)
        if gf.severity == "CRITICAL":
            master.critical_findings.append(gf)
        elif gf.severity == "HIGH":
            master.high_findings.append(gf)
        master.severity_counts[gf.severity] = (
            master.severity_counts.get(gf.severity, 0) + 1
        )

    # --- Step 4: Alerting ---
    needs_alert = master.severity_counts.get("CRITICAL", 0) + master.severity_counts.get("HIGH", 0) > 0
    if needs_alert:
        alert_payload = build_alert_payload(master, run_date, run_month)
        alert_file = live_out_dir / f"ALERT_{run_month}.json"
        write_json(alert_file, alert_payload)
        print(f"  → alert payload written: {alert_file.name}")

        subject = (
            f"[Lekwankwa QC] {master.severity_counts.get('CRITICAL',0)} CRITICAL, "
            f"{master.severity_counts.get('HIGH',0)} HIGH findings — {run_date}"
        )
        alert_findings = master.critical_findings + master.high_findings
        html_body, plain_body = build_html_alert_email(
            alert_findings, run_date, APPROVAL_SERVICE_URL
        )
        send_alert_email(subject, html_body, plain_body, dry_run=dry_run_alerts)

        # Self-healing escalation for CRITICAL/HIGH findings
        try:
            from tools.self_healing.handler import handle_quality_finding
            heal_context = {
                "product":  "multi",
                "country":  "multi",
                "source":   "quality_report_generator",
                "run_date": run_date,
                "layer":    "QUALITY_REPORT",
            }
            findings_as_dicts = [
                dataclasses.asdict(f)
                for f in (master.critical_findings + master.high_findings)
            ]
            handle_quality_finding(__file__, heal_context, findings_as_dicts)
        except Exception as _sh_exc:
            print(f"[SELF-HEAL] Failed to trigger self-healing: {_sh_exc}",
                  file=sys.stderr)

    # --- Step 5: Append-only history ---
    append_history(history_file, run_date, granular_reports, master)
    print(f"  → history appended: {history_file}")

    # --- Step 6: Monitor failure check ---
    has_generation_errors = any(gr.generation_errors for gr in granular_reports)
    if has_generation_errors:
        print("MONITOR_FAILURE: One or more products had generation errors.", file=sys.stderr)
        return 2
    if needs_alert:
        return 1  # DATA_QUALITY_FAILURE
    return 0


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------

def cloud_function_handler(event: dict, context: Any) -> None:  # type: ignore
    """
    GCS OBJECT_FINALIZE trigger.
    Fires when any file is written to the vault bucket.
    Checks for run_markers/extractor_*.complete patterns.
    """
    name = event.get("name", "")
    if not re.match(r"run_markers/extractor_.+\.complete$", name):
        return  # Not a marker file — ignore

    vault_root_env = os.environ.get("VAULT_ROOT", "gs://lekwankwa-pipeline-ops")
    run_date = datetime.date.today().isoformat()

    # Determine out_dir (GCS path handled by mounting or gsutil within the function)
    # For Cloud Functions, write to /tmp first then upload
    out_dir = Path("/tmp/quality_reports")
    search_root = Path("/tmp")

    rc = run(
        vault_root=Path(vault_root_env.replace("gs://", "/gcs/")),
        out_dir=out_dir,
        search_root=search_root,
        dry_run_alerts=False,
        check_markers=True,
        run_date=run_date,
        skip_monitor_failure_on_missing_markers=False,
        mode="live",
    )
    if rc == 2:
        # Not all markers present yet — normal exit, wait for next extractor to complete
        return
    # Upload /tmp outputs to GCS
    _upload_outputs_to_gcs(out_dir, vault_root_env, run_date[:7])


# ---------------------------------------------------------------------------
# Geo-split helpers
# ---------------------------------------------------------------------------

def _filter_by_geo(countries: list[dict], geo_key: str) -> list[dict]:
    """Return the subset of country result dicts belonging to this geo bundle."""
    if geo_key == "usa_only":
        return [c for c in countries if c.get("country_group") == "USA"]
    if geo_key == "eu27_only":
        return [c for c in countries if c.get("country_group") == "EU27"
                or c.get("country") in EU27_SET]
    if geo_key == "non_eu_block":
        return [c for c in countries if c.get("country_group") in NON_EU_SET
                or c.get("country") in NON_EU_SET]
    # full_32_country — all
    return list(countries)


def _apply_live_exclusions(countries: list[dict], product: str) -> list[dict]:
    """Strip excluded country groups from live reports (never applied to archive)."""
    excl = {cg for (p, cg) in LIVE_FEED_EXCLUDED if p == product}
    if not excl:
        return countries
    return [c for c in countries
            if c.get("country_group", c.get("country", "")) not in excl]


def _recount_severity(countries: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for c in countries:
        for f in c.get("findings", []):
            sev = f.get("severity", "MEDIUM")
            counts[sev] = counts.get(sev, 0) + 1
    return counts


def _write_geo_split_reports(
    product: str,
    granular_json: dict,
    out_dir: Path,
    run_date: str,
    is_live: bool,
) -> None:
    """
    Read the granular product-level JSON and write four geo-split files:
      quality_report_{stem}_{geo_key}_{run_date}.json
    Named to mirror the release_calendar structure exactly.
    Triggered by new data release, not a fixed calendar.
    """
    stem = _PRODUCT_STEMS.get(product, f"product_{product}")
    all_countries: list[dict] = granular_json.get("countries", [])
    all_findings: list[dict]  = granular_json.get("all_findings", [])
    report_mode = granular_json.get("report_mode", "LIVE_FEED_MONTHLY" if is_live else "ARCHIVE_DELIVERY")

    excl_note: list[str] = []
    if is_live:
        if ("food_micropricing", "AUS") in LIVE_FEED_EXCLUDED and product == "food_micropricing":
            excl_note.append("AUS excluded — quarterly releases only")
        if ("wages_and_employment", "NOR") in LIVE_FEED_EXCLUDED and product == "wages_and_employment":
            excl_note.append("NOR excluded — SSB Table 07458 frozen at 2024-Q4")

    for geo_key, geo_label in GEO_BUNDLES:
        geo_countries = _filter_by_geo(all_countries, geo_key)
        if is_live:
            geo_countries = _apply_live_exclusions(geo_countries, product)

        if not geo_countries:
            continue

        included_cgs = {c.get("country_group", c.get("country", "")) for c in geo_countries}
        geo_findings = [
            f for f in all_findings
            if f.get("country_group", f.get("iso_alpha3", "")) in included_cgs
        ]
        sev = _recount_severity(geo_countries)

        doc = {
            "document_type":   "Quality Report — Data Integrity & Freshness",
            "report_type":     "GEO_GRANULAR",
            "report_mode":     report_mode,
            "product":         product,
            "geo_bundle":      geo_label,
            "geo_key":         geo_key,
            "schema_standard": "v5.0",
            "generated_at":    run_date,
            "run_date":        run_date,
            "delivery_mode":   "live" if is_live else "archive",
            "live_exclusions": excl_note if is_live else [],
            "gap_fill_note": (
                "Rows where confidence_tier = \"DERIVED\" are synthetic gap-fills, not sovereign observations. "
                "They carry data_quality_certified = False by design. "
                "Filter on confidence_tier = \"PRIMARY\" for analysis requiring only confirmed sovereign data."
            ),
            "severity_counts": sev,
            "total_findings":  sum(sev.values()),
            "total_countries": len(geo_countries),
            "countries":       geo_countries,
            "all_findings":    geo_findings,
            "summary": {
                "total_countries":    len(geo_countries),
                "countries_fresh":    sum(1 for c in geo_countries if c.get("freshness_status") == "FRESH"),
                "countries_stale":    sum(1 for c in geo_countries if c.get("freshness_status") != "FRESH"),
                "total_findings":     sum(sev.values()),
                "severity_counts":    sev,
                "cross_check_status": "PASS" if sev.get("CRITICAL", 0) == 0 else "FAIL",
            },
        }

        fname = f"quality_report_{stem}_{geo_key}_{run_date}.json"
        ds_folder = _DATASET_FOLDER.get(product, stem)
        target_dir = out_dir / ds_folder
        target_dir.mkdir(parents=True, exist_ok=True)
        write_json(target_dir / fname, doc)
        print(f"     geo-split → {ds_folder}/{fname}")


def _upload_outputs_to_gcs(local_dir: Path, gcs_root: str, run_month: str) -> None:
    """Upload metadata/quality_reports/live/{YYYY-MM}/ and .../archive/{YYYY-MM}/ to GCS."""
    try:
        from google.cloud import storage  # type: ignore
        client = storage.Client()
        bucket_name = gcs_root.replace("gs://", "").split("/")[0]
        bucket = client.bucket(bucket_name)
        for subfolder in ("live", "archive"):
            source_dir = local_dir / subfolder / run_month
            if not source_dir.exists():
                continue
            for fpath in source_dir.rglob("*"):
                if fpath.is_file():
                    rel = fpath.relative_to(source_dir)
                    blob_name = f"metadata/quality_reports/{subfolder}/{run_month}/{rel.as_posix()}"
                    bucket.blob(blob_name).upload_from_filename(str(fpath))
        history_path = local_dir / "quality_report_history.json"
        if history_path.exists():
            bucket.blob("metadata/quality_reports/quality_report_history.json").upload_from_filename(
                str(history_path)
            )
    except Exception as e:
        print(f"ERROR: GCS upload failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lekwankwa quality report generator")
    p.add_argument("--vault-root", required=True, help="Local path to vault root")
    p.add_argument("--out-dir", default="./metadata/quality_reports", help="Output directory (default: ./metadata/quality_reports)")
    p.add_argument("--search-root", default=".", help="Directory to search for validation_summary_*.json")
    p.add_argument("--dry-run-alerts", action="store_true", help="Print alerts, do not send email")
    p.add_argument("--check-markers", action="store_true", help="Enforce extractor marker check")
    p.add_argument("--run-date", default=None, help="Override run date (YYYY-MM-DD)")
    p.add_argument(
        "--skip-marker-gate",
        action="store_true",
        help="Skip marker check even if some are missing (for manual testing)",
    )
    p.add_argument(
        "--mode",
        choices=["all", "live", "archive"],
        default="all",
        help=(
            "all: run all 5 products (default). "
            "live: run only live-feed products (food, wages, trade) — "
            "used by Cloud Scheduler monthly trigger. "
            "archive: run only archive products (housing, global_macro) — "
            "used at client delivery time."
        ),
    )
    p.add_argument(
        "--series-manifest",
        default=None,
        metavar="YAML",
        help=(
            "Path to catalog_expected_series.yaml.  When provided, the generator "
            "diffs every listed series against the live vault and emits CRITICAL "
            "DATA_GAP findings for series with zero vault partitions and HIGH "
            "findings for series below their min_months threshold.  "
            "Default: auto-detect at backtesting/backtest_engine/config/catalog_expected_series.yaml "
            "relative to the vault root."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.stdout = __import__("io").TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    args = _parse_args()

    # Auto-detect series manifest if not explicitly given
    _manifest_path: Optional[Path] = None
    if args.series_manifest:
        _manifest_path = Path(args.series_manifest)
    else:
        # Look for catalog_expected_series.yaml alongside the vault root
        _auto = (
            Path(args.vault_root).parent
            / "backtesting" / "backtest_engine" / "config"
            / "catalog_expected_series.yaml"
        )
        if _auto.exists():
            _manifest_path = _auto

    rc = run(
        vault_root=Path(args.vault_root),
        out_dir=Path(args.out_dir),
        search_root=Path(args.search_root),
        dry_run_alerts=args.dry_run_alerts,
        check_markers=args.check_markers,
        run_date=args.run_date,
        skip_monitor_failure_on_missing_markers=args.skip_marker_gate,
        mode=args.mode,
        series_manifest_path=_manifest_path,
    )
    sys.exit(rc)
