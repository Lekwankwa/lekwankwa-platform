"""
Shared PIT row builder for non-EU national statistics scrapers.
(ONS/GBR, StatCan/CAN — all RELEASE_DATE_ONLY)

Provides the same interface as eurostat/revision_tracker.py but
source-agnostic (no EU-27 country_map dependency).

Public API:
    build_vintage_id(source_prefix, iso3, metric_code, obs_date, version)
    estimate_release_date(obs_date, release_lag_days) -> str
    build_vault_row(**kwargs) -> dict
    write_partition(df, vault_root, year, month, filename)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

_COUNTRY_NAMES: dict[str, str] = {
    "GBR": "United Kingdom",
    "CAN": "Canada",
    "CHE": "Switzerland",
}


def build_vintage_id(
    source_prefix: str,
    iso3: str,
    metric_code: str,
    obs_date: pd.Timestamp,
    version: int = 1,
) -> str:
    period = obs_date.strftime("%Y-%m")
    return f"{source_prefix}-{iso3}-{metric_code}-{period}-v{version}"


def estimate_release_date(obs_date: pd.Timestamp, release_lag_days: int) -> str:
    est = obs_date + pd.Timedelta(days=release_lag_days)
    return est.strftime("%Y-%m-%d")


def build_vault_row(
    *,
    source_prefix: str,
    iso3: str,
    metric_code: str,
    sovereign_series_id: str,
    macro_metric_name: str,
    obs_date: pd.Timestamp,
    observed_value: float,
    unit_of_measure: str,
    release_lag_days: int,
    freq: str,
    source: str,
    source_agency: str,
    source_sub_category: str,
    pit_coverage_type: str,
    extra_fields: dict | None = None,
    version: int = 1,
) -> dict:
    vid   = build_vintage_id(source_prefix, iso3, metric_code, obs_date, version)
    rdate = estimate_release_date(obs_date, release_lag_days)
    return {
        "data_vintage_id":        vid,
        "confidence_tier":        "PRIMARY",
        "sovereign_series_id":    sovereign_series_id,
        "macro_metric_name":      macro_metric_name,
        "reporting_date":         obs_date.strftime("%Y-%m-%d"),
        "official_release_date":  rdate,
        "as_of_date":             rdate + "T00:00:00Z",
        "observed_value":         float(observed_value),
        "unit_of_measure":        unit_of_measure,
        "is_revised_figure":      False,
        "data_timestamp":         obs_date.isoformat() + "Z",
        "revision_number":        version,
        "iso_alpha3":             iso3,
        "country_name":           _COUNTRY_NAMES.get(iso3, iso3),
        "source":                 source,
        "source_agency":          source_agency,
        "source_sub_category":    source_sub_category,
        "sdmx_frequency":         freq,
        "published_date":         rdate,
        "data_quality_certified": True,
        "is_forecast":            False,
        "pit_coverage_type":      pit_coverage_type,
        **(extra_fields or {}),
    }


def write_partition(
    df: pd.DataFrame,
    vault_root: Path,
    year: int,
    month: int,
    filename: str,
) -> None:
    if df.empty:
        return
    part = vault_root / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / filename
    if out.exists():
        try:
            existing = pd.read_parquet(out)
        except Exception as read_exc:
            # Existing partition file is unreadable (corrupt footer, or
            # pyarrow's dataset-schema-unification quirk across sibling Hive
            # partitions with differing column encodings). Rebuild from the
            # incoming data rather than crashing the whole scrape — this
            # self-heals the partition going forward. Same fix already
            # applied to scrapers/eurostat/revision_tracker.py and already
            # used by scrapers/utilities/incremental.py for USA scrapers.
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
