"""
PIT-compliant vintage row builder for Eurostat data.

Since Eurostat has no ALFRED-equivalent full-vintage API, all initial rows
are version 1 (is_revised_figure=False).  The official_release_date is
estimated from Eurostat's known publication schedules:

  HICP (monthly)         obs_date + 30 d   (flash ~15th of following month)
  Unemployment (monthly) obs_date + 45 d   (LFS flash ~6 weeks after month-end)
  Labour Cost (quarterly) obs_date + 75 d
  HPI (quarterly)        obs_date + 90 d
  Building permits (qtrly) obs_date + 75 d
  Trade BOP (quarterly)  obs_date + 90 d
  GDP/National accounts  obs_date + 90 d   (flash t+30 d, but use final estimate timing)
  HICP annual rate (M)   obs_date + 30 d

Going forward, revision tracking is handled by revision_detector.py via temporal
accumulation: re-fetch each series periodically, diff against stored values, and
write revision_number=N+1 rows when values change.

NOTE: The Eurostat SDMX dissemination API has NO mechanism for vintage retrieval:
  - includeHistory=true  silently ignored on v2.1 (HTTP 400 on v1.0)
  - updatedAfter         works on v2.1 but returns only the CURRENT revised value
                         (not the superseded one) — so it detects that revisions
                         occurred but cannot recover what the old value was
  - namq_10_revise       HTTP 404 on the dissemination API
Historical vintages for data already in the vault are permanently unrecoverable
via the public API.  The revision_detector can only track NEW revisions from the
date of first execution onward.

Public API:
  build_vault_rows(obs_df, iso3, metric_code, config, extra_fields)
      → pd.DataFrame   (schema-compliant vault rows)

  write_partition(df, vault_root, year, month, filename)
      → None   (Hive-partitioned write, deduplicates on data_vintage_id)
"""

from __future__ import annotations

import logging
from datetime import timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .country_map import ISO3_TO_NAME, ISO3_TO_TIER

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Release-date estimation
# ---------------------------------------------------------------------------

def _estimate_release_date(obs_date: pd.Timestamp, release_lag_days: int) -> str:
    """
    Return ISO date string for the estimated first-publication date.

    obs_date        First calendar date of the observation period.
    release_lag_days Days from obs_date to expected publication.
    """
    est = obs_date + pd.Timedelta(days=release_lag_days)
    return est.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Vintage-ID builder
# ---------------------------------------------------------------------------

def build_vintage_id(iso3: str, metric_code: str, obs_date: pd.Timestamp,
                     version: int = 1) -> str:
    """
    Generate a data_vintage_id conforming to SCHEMA_STANDARD.yaml v5.

    Format: EUROSTAT-{ISO3}-{METRIC_CODE}-{YYYY-MM}-v{N}
    Example: EUROSTAT-DEU-HICP_CP0111-2024-01-v1
    """
    period = obs_date.strftime("%Y-%m")
    return f"EUROSTAT-{iso3}-{metric_code}-{period}-v{version}"


# ---------------------------------------------------------------------------
# Core row builder
# ---------------------------------------------------------------------------

def build_vault_rows(
    obs_df:           pd.DataFrame,
    iso3:             str,
    metric_code:      str,
    sovereign_series_id_fn,     # callable(row) -> str
    macro_metric_name: str,
    unit_of_measure:   str,
    release_lag_days:  int,
    freq:             str,       # "M", "Q", "A"
    value_col:        str = "value",
    source_sub_category: str = "EUROSTAT",
    extra_fields:     Optional[dict[str, Any]] = None,
    *,
    obs_date_col:     str = "_obs_date",   # internal column added by caller
) -> pd.DataFrame:
    """
    Convert raw Eurostat observations to schema-compliant vault rows.

    The caller must have already:
      1. Added an "_obs_date" column (pd.Timestamp) representing the
         first calendar date of each observation period.
      2. Filtered to a single ISO3 country's rows (or pass all and let
         this function handle them — but obs_date_col must be set).

    Returns a DataFrame with all SCHEMA_STANDARD v5 fields populated.
    """
    if obs_df.empty:
        return pd.DataFrame()

    rows = []
    now_utc = pd.Timestamp.utcnow().isoformat() + "Z"
    country_name = ISO3_TO_NAME.get(iso3, iso3)
    market_tier  = ISO3_TO_TIER.get(iso3, "Developed")

    for _, row in obs_df.iterrows():
        obs_date = row[obs_date_col]
        if pd.isna(obs_date):
            continue
        val = row.get(value_col)
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        sid  = sovereign_series_id_fn(row)
        vid  = build_vintage_id(iso3, metric_code, obs_date, version=1)
        rdate = _estimate_release_date(obs_date, release_lag_days)
        period_str = obs_date.strftime("%Y-%m")

        vault_row: dict[str, Any] = {
            # --- PIT mandatory fields ---
            "data_vintage_id":       vid,
            "sovereign_series_id":   sid,
            "data_timestamp":        obs_date.isoformat() + "Z",
            "official_release_date": rdate,
            "revision_number":       1,
            "is_revised_figure":     False,

            # --- Gold-standard schema fields ---
            "confidence_tier":       "PRIMARY",
            "macro_metric_name":     macro_metric_name,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       unit_of_measure,

            # --- Provenance ---
            "iso_alpha3":            iso3,
            "country_name":          country_name,
            "country_code":          iso3,
            "market_tier":           market_tier,
            "source":                "eurostat_sdmx",
            "source_agency":         "EUROSTAT",
            "source_sub_category":   source_sub_category,
            "portal_url":            "https://ec.europa.eu/eurostat/",
            "extraction_method":     "EUROSTAT_SDMX_API",
            "sdmx_frequency":        freq,
            "is_forecast":           False,
            "data_quality_certified": True,
            "observation_period":    period_str,
            "published_date":        rdate,
        }

        # Status code from API (e=estimated, p=provisional, etc.)
        if "status" in row and row["status"]:
            vault_row["data_status"] = row["status"]

        if extra_fields:
            vault_row.update(extra_fields)

        rows.append(vault_row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Hive-partitioned write helper
# ---------------------------------------------------------------------------

def write_partition(
    df:        pd.DataFrame,
    vault_root: Path,
    year:      int,
    month:     int,
    filename:  str,
) -> None:
    """
    Write df to vault_root/year=YYYY/month=MM/filename.parquet.

    Deduplicates on data_vintage_id (keep='first') before writing.
    Appends to existing file if present.
    """
    if df.empty:
        return

    part = vault_root / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / filename

    if out.exists():
        try:
            existing = pd.read_parquet(out)
        except Exception as read_exc:
            # Existing partition file is unreadable (corrupt footer from an
            # interrupted write, or an incompatible schema from an older code
            # version). Rebuild it from the incoming data rather than crashing
            # the whole scrape — this self-heals the partition going forward.
            # Previously uncaught here, this crashed every EU27 dataset scrape
            # the moment it hit one stale partition (scrapers/utilities/incremental.py
            # already handles this exact case for the USA scrapers).
            log.warning("Could not read existing partition %s (%s) — rewriting from incoming data.",
                        out, read_exc)
            combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")
            combined.to_parquet(out, index=False, engine="pyarrow")
            log.debug("Written %d rows → %s", len(combined), out)
            return
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")

    combined.to_parquet(out, index=False, engine="pyarrow")
    log.debug("Written %d rows → %s", len(combined), out)
