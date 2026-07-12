"""
Live Revision Detector — Forward-going revision capture for all 5 datasets.

On every scheduled pull, for each series/reporting_date:
  1. Fetch latest value from the source API
  2. Compare against the most recent stored value for that (series, date)
  3. If unchanged   → do nothing
  4. If changed     → write a new row with incremented version number,
                      is_revised_figure=True, as_of_date=UTC now

This implements true bitemporal tracking: once a value is written to the
vault, it is NEVER modified. Revisions create new rows.

Usage:
    python live_revision_detector.py [--dataset wages|housing|macro|food|trade|all]

Designed to run monthly (e.g. via cron/task scheduler) immediately after
each dataset's primary scraper has fetched the latest release.

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from scrapers.utilities.vault_io import get_vault_root
import requests
import urllib3

urllib3.disable_warnings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("live_revision_detector.log")],
)
logger = logging.getLogger(__name__)

FRED_KEY    = os.getenv("FRED_API_KEY", "136178f657b4aba7ad9e55938a1473bd")
FRED_BASE   = "https://api.stlouisfed.org/fred"
NOW_UTC     = datetime.now(timezone.utc)
_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
# ── Dataset configurations ────────────────────────────────────────────────────

DATASET_CONFIGS = {
    "wages": {
        "vault_path": _VAULT_BASE / "product=wages_and_employment/country=USA",
        "sources":    ["alfred_vintage"],
        "file":       "wages_data.parquet",
        "value_col":  "observed_value",
        "series_map": {
            "CES0000000001": "PAYEMS",
            "CES0500000001": "USPRIV",
            "CES2000000001": "USCONS",
            "CES3000000001": "MANEMP",
            "LNS14000000":   "UNRATE",
            "CES0500000003": "CES0500000003",
        },
    },
    "housing": {
        "vault_path": _VAULT_BASE / "product=Housing_Supply_and_Shelter_Inflation/country=USA",
        "sources":    ["alfred_vintage"],
        "file_map":   {"PERMIT": "building_permits_data.parquet",
                       "PERMIT1":"building_permits_data.parquet",
                       "PERMIT5":"building_permits_data.parquet",
                       "CUUR0000SEHA":"shelter_data.parquet"},
        "value_col":  "observed_value",
        "series_map": {
            "PERMIT":       "PERMIT",
            "PERMIT1":      "PERMIT1",
            "PERMIT5":      "PERMIT5",
            "CUUR0000SEHA": "CUUR0000SEHA",
        },
    },
    "macro": {
        "vault_path": _VAULT_BASE / "product=global_macro/country=USA",
        "sources":    ["alfred_vintage"],
        "file":       "global_macro_data.parquet",
        "value_col":  "observed_value",
        "series_map": {
            "GDP":      "GDP",
            "GDPC1":    "GDPC1",
            "INDPRO":   "INDPRO",
            "CPIAUCSL": "CPIAUCSL",
            "UNRATE":   "UNRATE",
            "PAYEMS":   "PAYEMS",
        },
    },
    "food": {
        "vault_path": _VAULT_BASE / "product=food_micropricing/country=USA",
        "sources":    ["alfred_vintage"],
        "file":       "food_pricing_data.parquet",
        "value_col":  "observed_price_local",
        "series_map": {
            "APU0000701111": "APU0000701111",
            "APU0000702111": "APU0000702111",
            "APU0000706111": "APU0000706111",
            "APU0000711111": "APU0000711111",
        },
    },
    "trade": {
        "vault_path": _VAULT_BASE / "product=trade_flows/country=USA",
        "sources":    ["alfred_vintage"],
        "file":       "trade_data.parquet",
        "value_col":  "observed_value",
        "series_map": {
            "BOPGSTB": "BOPGSTB",
        },
    },
}


def _fetch_current_value(fred_series_id: str) -> Optional[dict]:
    """Fetch the single latest observation for a series from FRED (current vintage)."""
    try:
        r = requests.get(
            f"{FRED_BASE}/series/observations",
            params={
                "series_id":      fred_series_id,
                "api_key":        FRED_KEY,
                "file_type":      "json",
                "sort_order":     "desc",
                "limit":          5,
                "observation_start": "2020-01-01",
            },
            verify=False,
            timeout=20,
        )
        if r.status_code != 200:
            return None
        obs = r.json().get("observations", [])
        if not obs:
            return None
        # Return all recent observations for comparison
        return [o for o in obs if o.get("value") not in (".", None, "")]
    except Exception as exc:
        logger.warning(f"FRED fetch error for {fred_series_id}: {exc}")
        return None


def _get_latest_stored(vault_path: Path, sources: list, fname: str,
                       series_id: str, value_col: str) -> pd.DataFrame:
    """Return stored rows for this series, most recent vintage per data date."""
    rows = []
    for src in sources:
        src_path = vault_path / f"source={src}"
        if not src_path.exists():
            continue
        for f in src_path.rglob(f"*{fname}"):
            try:
                df = pd.read_parquet(f)
                if "sovereign_series_id" not in df.columns:
                    continue
                subset = df[df["sovereign_series_id"] == series_id]
                if not subset.empty:
                    rows.append(subset)
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    # Keep most recent vintage per data date
    if "revision_number" in combined.columns:
        combined = combined.sort_values("revision_number").groupby(
            ["sovereign_series_id",
             combined.get("reporting_date", combined.get("data_timestamp"))],
            dropna=False
        ).last().reset_index(drop=True)
    return combined


def _next_version(existing_df: pd.DataFrame, data_date: str, series_id: str) -> int:
    """Return next version number (max existing + 1)."""
    if existing_df.empty or "data_vintage_id" not in existing_df.columns:
        return 1
    mask = (
        (existing_df.get("reporting_date", existing_df.get("data_timestamp", pd.Series(dtype=str)))
         .astype(str).str.startswith(data_date[:7]))
        & (existing_df["sovereign_series_id"] == series_id)
    )
    matching = existing_df[mask]["data_vintage_id"].dropna()
    if matching.empty:
        return 1
    versions = matching.str.extract(r"-v(\d+)$")[0].dropna().astype(int)
    return (versions.max() + 1) if not versions.empty else 1


def _write_revision(vault_path: Path, src: str, fname: str,
                    new_row: pd.Series) -> None:
    """Append a revision row to the appropriate partition file."""
    ts = pd.to_datetime(new_row.get("reporting_date") or new_row.get("data_timestamp"),
                        errors="coerce")
    if pd.isna(ts):
        return
    part = vault_path / f"source={src}" / f"year={ts.year}" / f"month={ts.month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / fname

    row_df = pd.DataFrame([new_row])
    if out.exists():
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, row_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = row_df

    combined.to_parquet(out, index=False, engine="pyarrow")
    logger.info(f"  Revision written -> {out} | {new_row['data_vintage_id']}")


def detect_and_write_revisions(dataset_name: str, cfg: dict) -> int:
    """
    For each series in the dataset, check if the latest FRED value differs
    from the latest stored value. Write a new revision row if it does.
    """
    logger.info(f"\n--- Checking revisions: {dataset_name} ---")
    revisions = 0
    vault_path = cfg["vault_path"]
    sources    = cfg["sources"]
    value_col  = cfg["value_col"]

    for vault_sid, fred_sid in cfg["series_map"].items():
        fname = cfg.get("file") or cfg.get("file_map", {}).get(vault_sid, "data.parquet")

        live_obs = _fetch_current_value(fred_sid)
        if not live_obs:
            logger.debug(f"  No live data for {fred_sid}")
            continue

        stored = _get_latest_stored(vault_path, sources, fname, vault_sid, value_col)

        for obs in live_obs:
            data_date = obs.get("date", "")
            new_val   = obs.get("value")
            if not data_date or new_val in (None, ".", ""):
                continue
            try:
                new_val = float(new_val)
            except (ValueError, TypeError):
                continue

            # Find stored value for this data date
            if stored.empty:
                stored_val = None
            else:
                ts_col = "reporting_date" if "reporting_date" in stored.columns else "data_timestamp"
                date_mask = stored[ts_col].astype(str).str.startswith(data_date[:7])
                match = stored[date_mask]
                stored_val = float(match[value_col].iloc[-1]) if not match.empty and value_col in match.columns else None

            if stored_val is not None and abs(stored_val - new_val) < 1e-6:
                continue  # unchanged — do nothing

            # Value changed (or new) — write revision row
            next_v = _next_version(stored, data_date, vault_sid)
            vid    = f"ALFRED-{vault_sid}-{data_date[:7]}-v{next_v}"
            prefix = "BLS" if "BLS" in cfg.get("file", "") or "APU" in vault_sid else "FRED"
            vid    = f"{prefix}-{vault_sid}-{data_date[:7]}-v{next_v}"

            new_row = {
                "sovereign_series_id":  vault_sid,
                "data_vintage_id":      vid,
                "reporting_date":       data_date,
                "data_timestamp":       data_date,
                "observed_value":       new_val,
                value_col:              new_val,
                "official_release_date": NOW_UTC.strftime("%Y-%m-%d"),
                "published_date":        NOW_UTC.strftime("%Y-%m-%d"),
                "as_of_date":           NOW_UTC.isoformat(),
                "is_revised_figure":    (next_v > 1),
                "revision_number":      next_v,
                "confidence_tier":      "PRIMARY",
                "source":               sources[0],
                "source_system":        "LIVE_REVISION_DETECTOR",
                "iso_alpha3":           "USA",
                "extraction_method":    "ALFRED_API",
                "data_quality_certified": True,
            }

            action = "REVISION" if next_v > 1 else "NEW"
            logger.info(f"  [{action}] {vault_sid} {data_date[:7]}: "
                        f"stored={stored_val} -> new={new_val} ({vid})")
            _write_revision(vault_path, sources[0], fname, pd.Series(new_row))
            revisions += 1

    return revisions


def run(datasets: list) -> None:
    logger.info("=" * 70)
    logger.info("LIVE REVISION DETECTOR")
    logger.info(f"Run: {NOW_UTC.isoformat()}")
    logger.info("=" * 70)

    total = 0
    for name in datasets:
        cfg = DATASET_CONFIGS.get(name)
        if not cfg:
            logger.warning(f"Unknown dataset: {name}")
            continue
        total += detect_and_write_revisions(name, cfg)

    logger.info(f"\nTotal revision rows written: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", default="all",
        help="Dataset to check: wages|housing|macro|food|trade|all"
    )
    args = parser.parse_args()

    if args.dataset == "all":
        targets = list(DATASET_CONFIGS.keys())
    else:
        targets = [d.strip() for d in args.dataset.split(",")]

    run(targets)
