"""
EurostatRevisionDetector
========================
Tracks data revisions by comparing a freshly-fetched SDMX snapshot
against the values already stored in the Hive vault.

Architecture
------------
The Eurostat SDMX dissemination API (both v1.0 JSON and v2.1 XML) does NOT
support historical vintage retrieval:
  - includeHistory=true  silently ignored on v2.1 (v1.0 returns HTTP 400)
  - updatedAfter         works on v2.1 but returns only the CURRENT revised
                         value, not the superseded one
  - namq_10_revise       404 -- no dedicated revision dataset

The correct architecture is TEMPORAL ACCUMULATION:
  1. First run (ingestion sprint): stores current values as revision_number=1.
  2. Subsequent runs (this detector): re-fetches each series, compares against
     the most recently stored revision, and writes a new revision_number=N+1
     row only when the value has materially changed.

This cannot backfill the revision history for data already in the vault —
those historical vintages are permanently unrecoverable via the public API.
The detector only tracks revisions going forward from its first execution.

When to run
-----------
Run monthly (or quarterly for Q series) immediately after Eurostat's known
publication calendar dates.  Revisions show up when Eurostat publishes a
benchmark update, seasonal-adjustment revision, or preliminary→final upgrade.

Usage
-----
    from scrapers.eurostat.revision_detector import EurostatRevisionDetector
    from scrapers.eurostat.series_map import MACRO_CONFIGS

    detector = EurostatRevisionDetector(vault_root=Path("lekwankwa-historical-vault"))
    report = detector.run_for_config(
        cfg=MACRO_CONFIGS[0],          # GDP_B1GQ_CLV
        iso3="DEU",
        vault_product="global_macro",
        vault_file="global_macro_data.parquet",
    )
    print(report)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .eurostat_client import fetch_dataset, period_to_date
from .revision_tracker import build_vintage_id, _estimate_release_date, write_partition
from .country_map import ALL_GEO2, GEO2_TO_ISO3

log = logging.getLogger(__name__)

# Relative tolerance for deciding whether a value has "changed".
# 0.01% avoids flagging floating-point rounding as a revision.
_CHANGE_THRESHOLD = 0.0001


@dataclass
class RevisionReport:
    iso3:          str
    metric_code:   str
    n_fetched:     int = 0
    n_unchanged:   int = 0
    n_revised:     int = 0
    n_new_periods: int = 0
    revised_periods: list = field(default_factory=list)   # [(obs_date, old_val, new_val)]
    warnings:      list = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"RevisionReport {self.iso3}/{self.metric_code}",
            f"  Fetched:      {self.n_fetched}",
            f"  Unchanged:    {self.n_unchanged}",
            f"  New periods:  {self.n_new_periods}",
            f"  Revised:      {self.n_revised}",
        ]
        for obs_date, old, new in self.revised_periods[:10]:
            pct = (new - old) / old * 100 if old else float("nan")
            lines.append(f"    {obs_date}: {old:.2f} → {new:.2f}  ({pct:+.3f}%)")
        if self.warnings:
            for w in self.warnings[:5]:
                lines.append(f"  WARN: {w}")
        return "\n".join(lines)


class EurostatRevisionDetector:
    """
    Compares fresh SDMX values against the vault and writes revision rows.

    Parameters
    ----------
    vault_root
        Root of the Hive-partitioned vault, e.g.
        Path("lekwankwa-historical-vault").
    detection_date
        The date to use as official_release_date for newly detected revisions.
        Defaults to today's UTC date.  Override in tests.
    """

    def __init__(
        self,
        vault_root: Path,
        detection_date: Optional[pd.Timestamp] = None,
    ) -> None:
        self._vault_root = vault_root
        self._detection_date = (
            detection_date
            if detection_date is not None
            else pd.Timestamp.utcnow().normalize().tz_localize(None)
        )

    def run_for_config(
        self,
        cfg:            dict,
        iso3:           str,
        vault_product:  str,
        vault_file:     str,
        geo2:           Optional[str] = None,
    ) -> RevisionReport:
        """
        Fetch fresh data for one (config, country) pair, diff against vault,
        write new revision rows for changed values.
        """
        metric_code   = cfg["metric_code"]
        release_lag   = cfg["release_lag_days"]
        static_filters = cfg.get("static_filters", {})
        start_period  = cfg.get("start_period", "2000-01")
        geo2_code     = geo2 or _iso3_to_geo2(iso3)
        if not geo2_code:
            rep = RevisionReport(iso3=iso3, metric_code=metric_code)
            rep.warnings.append(f"No Eurostat geo2 code for {iso3}")
            return rep

        report = RevisionReport(iso3=iso3, metric_code=metric_code)

        # 1. Load fresh values from Eurostat API
        df_fresh = fetch_dataset(
            dataset_id   = cfg["dataflow"],
            filters      = static_filters,
            geo_list     = [geo2_code],
            start_period = start_period,
        )
        if df_fresh.empty:
            report.warnings.append("Empty API response")
            return report

        # Map geo → iso3, parse obs_date
        geo_col = next((c for c in df_fresh.columns if c.lower().startswith("geo")), "geo")
        df_fresh["_iso3"] = df_fresh[geo_col].map(GEO2_TO_ISO3)
        df_fresh = df_fresh[df_fresh["_iso3"] == iso3].copy()
        df_fresh["_obs_date"] = df_fresh["time"].apply(period_to_date)
        df_fresh = df_fresh.dropna(subset=["_obs_date", "value"])
        report.n_fetched = len(df_fresh)

        # 2. Load existing vault rows for this (iso3, metric_code)
        vault_dir = (
            self._vault_root
            / f"product={vault_product}"
            / f"country={iso3}"
            / "source=eurostat_sdmx"
        )
        existing = _load_vault_latest(vault_dir, metric_code)
        # existing: {obs_date -> (observed_value, max_revision_number)}

        # 3. Diff
        detection_date_str = self._detection_date.strftime("%Y-%m-%d")
        new_rows: list[dict] = []

        for _, row in df_fresh.iterrows():
            obs_date = row["_obs_date"]
            new_val  = float(row["value"])

            if obs_date in existing:
                old_val, max_rev = existing[obs_date]
                if _materially_changed(old_val, new_val):
                    # Genuine revision detected
                    next_rev = int(max_rev) + 1
                    vid = build_vintage_id(iso3, metric_code, obs_date, version=next_rev)
                    new_rows.append(_make_revision_row(
                        iso3, metric_code, obs_date, new_val, next_rev,
                        detection_date_str, vid, cfg,
                    ))
                    report.n_revised += 1
                    report.revised_periods.append((obs_date.date(), old_val, new_val))
                    log.info(
                        "Revision detected %s/%s %s: %.4f → %.4f (v%d)",
                        iso3, metric_code, obs_date.date(), old_val, new_val, next_rev,
                    )
                else:
                    report.n_unchanged += 1
            else:
                # New observation period not yet in vault — not a revision, just new data
                report.n_new_periods += 1
                # Don't write here — let the main ingestor handle new periods

        # 4. Write revision rows to vault
        if new_rows:
            df_new = pd.DataFrame(new_rows)
            df_new["_obs_dt"] = pd.to_datetime(
                df_new["data_timestamp"], errors="coerce", utc=True
            )
            for (year, month), grp in df_new.groupby([
                df_new["_obs_dt"].dt.year,
                df_new["_obs_dt"].dt.month,
            ]):
                write_partition(
                    grp.drop(columns=["_obs_dt"]),
                    vault_dir, int(year), int(month), vault_file,
                )
            log.info(
                "Wrote %d revision rows for %s/%s", len(new_rows), iso3, metric_code
            )

        return report


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_vault_latest(vault_dir: Path, metric_code: str) -> dict:
    """
    Load all vault parquet files under vault_dir, filter to series matching
    metric_code, and return a dict: {obs_date -> (observed_value, max_revision_number)}.
    Only keeps the latest revision per obs_date.
    """
    if not vault_dir.exists():
        return {}

    files = sorted(vault_dir.rglob("*.parquet"))
    if not files:
        return {}

    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            frames.append(df)
        except Exception as exc:
            log.warning("Could not read %s: %s", f, exc)

    if not frames:
        return {}

    df = pd.concat(frames, ignore_index=True)

    # Filter to this metric_code (series IDs have format METRIC_CODE_ISO3)
    if "sovereign_series_id" in df.columns:
        df = df[df["sovereign_series_id"].str.startswith(metric_code, na=False)].copy()

    if df.empty:
        return {}

    if "data_timestamp" not in df.columns or "observed_value" not in df.columns:
        return {}

    df["_obs_date"] = pd.to_datetime(
        df["data_timestamp"], errors="coerce", utc=True
    ).dt.tz_localize(None).dt.normalize()

    rev_col = df["revision_number"] if "revision_number" in df.columns else pd.Series(1, index=df.index)

    result = {}
    for _, row in df.iterrows():
        obs_d = row["_obs_date"]
        if pd.isna(obs_d):
            continue
        val = pd.to_numeric(row.get("observed_value"), errors="coerce")
        rev = pd.to_numeric(row.get("revision_number", 1), errors="coerce")
        if pd.isna(val):
            continue
        if obs_d not in result or rev > result[obs_d][1]:
            result[obs_d] = (float(val), int(rev) if not pd.isna(rev) else 1)

    return result


def _materially_changed(old: float, new: float) -> bool:
    """Return True when abs relative change exceeds _CHANGE_THRESHOLD."""
    if old == 0:
        return abs(new) > 1e-9
    return abs(new - old) / abs(old) > _CHANGE_THRESHOLD


def _make_revision_row(
    iso3:       str,
    metric_code: str,
    obs_date:   pd.Timestamp,
    new_val:    float,
    rev_number: int,
    release_date_str: str,
    vid:        str,
    cfg:        dict,
) -> dict[str, Any]:
    from .country_map import ISO3_TO_NAME, ISO3_TO_TIER

    return {
        "data_vintage_id":       vid,
        "sovereign_series_id":   f"{metric_code}_{iso3}",
        "data_timestamp":        obs_date.isoformat() + "Z",
        "official_release_date": release_date_str,
        "revision_number":       rev_number,
        "is_revised_figure":     True,
        "confidence_tier":       "PRIMARY",
        "macro_metric_name":     cfg.get("macro_metric_name", ""),
        "reporting_date":        obs_date.strftime("%Y-%m-%d"),
        "as_of_date":            release_date_str + "Z",
        "observed_value":        new_val,
        "unit_of_measure":       cfg.get("unit_of_measure", ""),
        "iso_alpha3":            iso3,
        "country_name":          ISO3_TO_NAME.get(iso3, iso3),
        "source":                "eurostat_sdmx",
        "source_agency":         "EUROSTAT",
        "source_sub_category":   cfg.get("source_sub_category", ""),
        "sdmx_frequency":        cfg.get("freq", ""),
        "is_forecast":           False,
        "data_quality_certified": True,
        "published_date":        release_date_str,
        "extraction_method":     "EUROSTAT_REVISION_DETECTOR",
    }


def _iso3_to_geo2(iso3: str) -> Optional[str]:
    """Reverse lookup ISO3 → Eurostat 2-letter geo code."""
    from .country_map import GEO2_TO_ISO3
    for geo2, i3 in GEO2_TO_ISO3.items():
        if i3 == iso3:
            return geo2
    return None
