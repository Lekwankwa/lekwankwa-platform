"""
Provides:
  - get_vault_latest_month()       scan Hive partitions for latest (year, month)
  - compute_scrape_range()         year-granular start/end for BLS-style scrapers
  - compute_scrape_range_monthly() month-granular start/end for month-loop scrapers
  - revision_upsert()              smart vault write: new rows added, revised rows versioned
  - BLS_KNOWN_GAPS                 months where BLS published no data (funding lapses etc.)

Usage in each scraper:
    from scrapers.utilities.incremental import (
        get_vault_latest_month, compute_scrape_range,
        compute_scrape_range_monthly, revision_upsert, BLS_KNOWN_GAPS,
    )
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known BLS data gaps — months where BLS published NO data.
# Scrapers skip these months in incremental mode rather than treating them
# as errors. Add new entries here as they occur.
# ---------------------------------------------------------------------------
BLS_KNOWN_GAPS: frozenset[tuple[int, int]] = frozenset({
    (2025, 10),   # U.S. government funding lapse — BLS did not publish Oct 2025
})


# ---------------------------------------------------------------------------
# Vault partition scanner
# ---------------------------------------------------------------------------

_YEAR_MONTH_RE = re.compile(r"year=(\d{4})/month=(\d{2})")


def _latest_year_month_from_paths(paths: list[str]) -> tuple[int, int] | None:
    """Extract the max (year, month) from a list of parquet file paths."""
    latest: tuple[int, int] | None = None
    for p in paths:
        m = _YEAR_MONTH_RE.search(p.replace("\\", "/"))
        if not m:
            continue
        year, month = int(m.group(1)), int(m.group(2))
        if latest is None or (year, month) > latest:
            latest = (year, month)
    return latest


def get_vault_latest_month(scan_root) -> tuple[int, int] | None:
    """
    Scan a Hive-partitioned vault tree for the most recent year/month partition.

    Handles both path layouts used in the vault:
        scan_root/year=YYYY/month=MM/...           (food, trade, IMF)

    Works for both local paths and gs:// VaultPath objects — GCS has no real
    directories, so listing is done via gcsfs rather than pathlib.rglob().
    """
    root_str = str(scan_root)

    if root_str.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(root_str):
            return None
        parquet_paths = [p for p in fs.find(root_str) if p.endswith(".parquet")]
        latest = _latest_year_month_from_paths(parquet_paths)
    else:
        local_root = Path(root_str)
        if not local_root.exists():
            return None
        parquet_paths = [str(p) for p in local_root.rglob("*.parquet")]
        latest = _latest_year_month_from_paths(parquet_paths)

    if latest:
        log.info("Vault latest partition: year=%d  month=%02d", *latest)
    else:
        log.info("Vault appears empty — will use default start year.")
    return latest


# ---------------------------------------------------------------------------
# Scrape range helpers
# ---------------------------------------------------------------------------

def compute_scrape_range(
    scan_root: Path,
    default_start_year: int,
    since: str | None = None,
    revision_lookback_years: int = 2,
) -> tuple[int, int]:
    """
    Return (start_year, end_year) for year-granular incremental scrapers
    (BLS API, IMF DataMapper).

    Decision order:
      1. --since YYYY or YYYY-MM supplied → use that year.
      2. Vault has data → start = latest_year - revision_lookback_years
         (re-fetches recent years so BLS benchmark revisions are captured).
      3. Vault empty → fall back to default_start_year.

    end_year is always the current UTC year.
    """
    end_year = datetime.now(timezone.utc).year

    if since:
        try:
            start_year = int(since.split("-")[0])
            log.info("--since override → start_year=%d", start_year)
            return max(default_start_year, start_year), end_year
        except (ValueError, IndexError):
            log.warning("Invalid --since value %r — ignored.", since)

    latest = get_vault_latest_month(scan_root)
    if latest:
        start_year = max(default_start_year, latest[0] - revision_lookback_years)
        log.info("Incremental range: %d – %d  (revision lookback %d yr)",
                 start_year, end_year, revision_lookback_years)
    else:
        start_year = default_start_year
        log.info("Empty vault → full range %d – %d", start_year, end_year)

    return start_year, end_year


def compute_scrape_range_monthly(
    scan_root: Path,
    default_start_year: int,
    since: str | None = None,
    revision_lookback_months: int = 3,
) -> tuple[int, int, int, int]:
    """
    Return (start_year, start_month, end_year, end_month) for month-granular
    scrapers (Census FT-900, Census BPS permits).

    Decision order:
      1. --since YYYY-MM supplied → use that month exactly.
      2. Vault has data → step back revision_lookback_months from latest month
         so recently-revised months are re-fetched.
      3. Vault empty → default_start_year, month=1.

    end is always today's UTC year/month.
    """
    now = datetime.now(timezone.utc)
    end_year, end_month = now.year, now.month

    if since:
        try:
            parts = since.split("-")
            sy, sm = int(parts[0]), int(parts[1])
            log.info("--since override → %d-%02d", sy, sm)
            return sy, sm, end_year, end_month
        except (ValueError, IndexError):
            log.warning("Invalid --since value %r — ignored.", since)

    latest = get_vault_latest_month(scan_root)
    if latest:
        ly, lm = latest
        # Step back revision_lookback_months
        for _ in range(revision_lookback_months):
            lm -= 1
            if lm == 0:
                lm, ly = 12, ly - 1
        start_year  = max(default_start_year, ly)
        start_month = lm if ly > default_start_year else 1
        log.info("Incremental monthly range: %d-%02d → %d-%02d",
                 start_year, start_month, end_year, end_month)
    else:
        start_year, start_month = default_start_year, 1
        log.info("Empty vault → full range %d-01 → %d-%02d",
                 start_year, end_year, end_month)

    return start_year, start_month, end_year, end_month


# ---------------------------------------------------------------------------
# Revision-aware vault writer
# ---------------------------------------------------------------------------

def revision_upsert(
    path: Path,
    incoming: pd.DataFrame,
    key_cols: list[str],
    value_col: str,
    tolerance: float = 1e-6,
) -> tuple[int, int]:
    """
    Smart Parquet write that preserves point-in-time revision history.

    For each row in `incoming`:
      - No matching row in vault            → written as-is (revision_number unchanged)
      - Matching row found, same value      → skipped (no write)
      - Matching row found, value differs   → old row gets superseded_by=new_record_id;
                                              new row written with revision_number+1,
                                              is_revised_figure=True

    key_cols: columns that uniquely identify one observation
              e.g. ["sovereign_series_id", "data_timestamp"]
    value_col: the numeric column to compare for value changes
              e.g. "observed_price_local" or "observed_value"

    Returns (rows_added, revisions_detected).
    """
    if incoming.empty:
        return 0, 0

    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists():
        incoming.to_parquet(path, engine="pyarrow", index=False)
        return len(incoming), 0

    try:
        existing = pd.read_parquet(path)
    except Exception as read_exc:
        # Existing partition file is unreadable (corrupt footer from an
        # interrupted write, or an incompatible schema from an older code
        # version). Rebuild it from the incoming data rather than crashing
        # the whole scrape — this self-heals the partition going forward.
        log.warning("Could not read existing partition %s (%s) — rewriting from incoming data.",
                    path, read_exc)
        incoming.to_parquet(path, engine="pyarrow", index=False)
        return len(incoming), 0

    if existing.empty:
        incoming.to_parquet(path, engine="pyarrow", index=False)
        return len(incoming), 0

    existing = existing.copy()

    # Build string composite key for fast lookup
    avail_keys = [c for c in key_cols if c in existing.columns]
    incoming_avail = [c for c in key_cols if c in incoming.columns]

    if not avail_keys or not incoming_avail:
        # Can't match — fall back to simple append-dedup
        merged = pd.concat([existing, incoming], ignore_index=True)
        merged.to_parquet(path, engine="pyarrow", index=False)
        return len(incoming), 0

    existing["_key"] = existing[avail_keys].astype(str).agg("|".join, axis=1)

    rows_added = 0
    revisions  = 0
    new_rows: list[dict] = []

    for _, new_row in incoming.iterrows():
        row_key = "|".join(str(new_row[c]) for c in incoming_avail)
        matches = existing[existing["_key"] == row_key]

        if matches.empty:
            # Brand new observation
            new_rows.append(new_row.to_dict())
            rows_added += 1
        else:
            # Pick the highest-revision existing row
            if "revision_number" in matches.columns:
                latest_match = matches.sort_values("revision_number").iloc[-1]
            else:
                latest_match = matches.iloc[-1]

            old_val = latest_match.get(value_col)
            new_val = new_row.get(value_col)

            try:
                value_changed = abs(float(old_val) - float(new_val)) > tolerance
            except (TypeError, ValueError):
                value_changed = str(old_val) != str(new_val)

            if value_changed:
                new_id = str(new_row.get("record_id", ""))
                # Mark superseded
                existing.loc[latest_match.name, "superseded_by"] = new_id
                # Bump revision
                new_dict = new_row.to_dict()
                old_rev = int(latest_match.get("revision_number", 0))
                new_dict["revision_number"] = old_rev + 1
                new_dict["is_revised_figure"] = True
                new_rows.append(new_dict)
                rows_added += 1
                revisions  += 1
            # else: same value — no write needed

    existing = existing.drop(columns=["_key"], errors="ignore")

    if new_rows:
        result = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    else:
        result = existing

    result.to_parquet(path, engine="pyarrow", index=False)
    return rows_added, revisions
