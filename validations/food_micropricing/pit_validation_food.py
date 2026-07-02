"""Bitemporal PIT validation — Food Micropricing (BLS CPI + USDA ERS Food Price Outlook)."""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bitemporal_core import (  # noqa: E402
    check_unique_record_ids, check_knowledge_completeness,
    check_valid_to_knowledge_ordering, check_knowledge_ordering,
    check_as_of_published_cohesion, check_knowledge_horizon,
    check_anti_retroactive_ingestion, check_conversion_horizon,
    check_publication_lag, check_knowledge_monotonicity,
    check_bitemporal_uniqueness, check_supersession_integrity,
    write_report,
)
from _vault_root import VAULT_ROOT, vault_exists, vault_glob  # noqa: E402

# ── Config ────────────────────────────────────────────────────────────────────
VAULT_BASE  = VAULT_ROOT
PRODUCT     = "food_micropricing"
COUNTRY     = "USA"
SOURCES     = ["bls", "usda_ers"]
REPORT_JSON = Path("food_pricing_bitemporal_pit_report.json")
REPORT_TXT  = Path("food_pricing_bitemporal_pit_report.txt")

# Publication lag by source (months between data_timestamp and published_date):
#   bls        : CPI Average Retail Prices (APU series) — published ~14th of following month → 1–2 months
#   usda_ers   : ERS Food Price Outlook (CU* CPI series) — published ~70 days after month-end → 2–3 months
#
# CRITICAL: usda_ers lag is 2–3 months, NOT 1–2 like BLS.
# If both sources used the same lag bounds, PIT validation would NOT catch
# incorrectly-labelled ERS rows that were given the BLS release schedule.
PUB_LAG_BOUNDS = {
    "bls":      (1, 2),   # ~14 days → first or second month after reference month
    "usda_ers": (2, 3),   # ~70 days → second or third month after reference month
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("food_pricing_bitemporal_pit.log", encoding="utf-8"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── Loader ────────────────────────────────────────────────────────────────────

def _load():
    frames = {}
    # Source-specific filename patterns — never glob "*.parquet" broadly because
    # vault directories also contain changelog.parquet files with a different schema.
    _FILENAMES = {
        "bls":      "food_pricing_data.parquet",        # BLS historical pipeline
        "usda_ers": "food_pricing_data.parquet", # ERS Food Price Outlook pipeline
    }
    for src in SOURCES:
        src_path = f"{VAULT_BASE}/product={PRODUCT}/country={COUNTRY}/source={src}"
        if not vault_exists(src_path):
            logger.warning(f"  source={src}: vault path not found — skipping")
            continue
        fname = _FILENAMES.get(src, "*.parquet")
        files = vault_glob(src_path, fname)
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception as exc:
                logger.warning(f"Skipping {f}: {exc}")
        if dfs:
            frames[src] = pd.concat(dfs, ignore_index=True)
            logger.info(f"  source={src}: {len(frames[src]):,} records ({len(files)} files)")
    if not frames:
        raise FileNotFoundError("No food data found in vault")
    df = pd.concat(frames.values(), ignore_index=True)

    # Normalise v2 gold-standard column names → canonical bitemporal names
    # so bitemporal_core checks (which use 'published_date' / 'source_series_id') work unchanged.
    _NORM = {
        "official_release_date": "published_date",
        "sovereign_series_id":   "source_series_id",
    }
    df     = df.rename(columns={k: v for k, v in _NORM.items() if k in df.columns and v not in df.columns})
    frames = {k: v.rename(columns={k2: v2 for k2, v2 in _NORM.items() if k2 in v.columns and v2 not in v.columns})
              for k, v in frames.items()}
    return df, frames

# ── Runner ────────────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING — BITEMPORAL PIT VALIDATION")
    logger.info("=" * 70)
    df, frames = _load()
    results = [
        check_unique_record_ids(df),
        check_knowledge_completeness(df),
        check_valid_to_knowledge_ordering(df),
        check_knowledge_ordering(df),
        check_as_of_published_cohesion(df),
        check_knowledge_horizon(df),
        check_anti_retroactive_ingestion(df),
        check_conversion_horizon(df),
        check_publication_lag(frames, PUB_LAG_BOUNDS),
        check_knowledge_monotonicity(df, sample=50, min_len=4),
        check_bitemporal_uniqueness(df),
        check_supersession_integrity(df),
    ]
    return write_report(REPORT_JSON, REPORT_TXT, PRODUCT, COUNTRY, results)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
