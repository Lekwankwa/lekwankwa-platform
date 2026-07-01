"""
migrate_food_schema_v2.py
Lekwankwa Corporation Pty Ltd

One-shot migration: food_micropricing vault parquet files → gold standard schema v2.

RENAMES (old → new):
  item_name          → standard_name
  item_description   → local_name
  item_code          → global_coicop_code
  item_value         → observed_price_local
  usd_equivalent     → price_usd_equivalent
  unit               → unit_measure_standardized
  source_url         → portal_url
  published_date     → official_release_date
  source_series_id   → sovereign_series_id

NEW COLUMNS (computed, not requiring API calls):
  iso_alpha3, market_tier, source_agency, source_sub_category, dataset_id,
  release_frequency, internal_item_id, data_vintage_id, confidence_tier,
  observation_period, is_revised_figure, fx_rate_applied, fx_rate_date,
  unit_quantity_standardized

Idempotent: files already at v2 schema (sovereign_series_id present) are skipped.

Run from repo root:
    python3.10 validations/food_micropricing/migrate_food_schema_v2.py
"""

import calendar
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

VAULT_ROOT = Path("lekwankwa-historical-vault/product=food_micropricing")
MAX_WORKERS = 8

# ── Column renames ─────────────────────────────────────────────────────────────
RENAMES = {
    "item_name":        "standard_name",
    "item_description": "local_name",
    "item_code":        "global_coicop_code",
    "item_value":       "observed_price_local",
    "usd_equivalent":   "price_usd_equivalent",
    "unit":             "unit_measure_standardized",
    "source_url":       "portal_url",
    "published_date":   "official_release_date",
    "source_series_id": "sovereign_series_id",
}

# ── BLS series → internal item ID ─────────────────────────────────────────────
BLS_ITEM_IDS = {
    "APU0000701111": "GRAIN-01",   # Rice, white long grain
    "APU0000702111": "GRAIN-02",   # Wheat flour, all purpose
    "APU0000702421": "GRAIN-03",   # Bread, white sliced
    "APU0000703112": "MEAT-01",    # Beef, ground
    "APU0000706111": "MEAT-02",    # Chicken, whole
    "APU0000706211": "MEAT-03",    # Chicken breast, boneless
    "APU0000708111": "DAIRY-01",   # Eggs, hen
    "APU0000709112": "DAIRY-02",   # Milk, whole
    "APU0000712111": "DAIRY-03",   # Butter, unsalted
    "APU0000711111": "DAIRY-04",   # Cheese, cheddar
    "APU0000714229": "OILS-01",    # Vegetable oil, corn/blended
    "APU0000720111": "VEG-01",     # Tomatoes, fresh round
    "APU0000720211": "VEG-02",     # Potatoes, white loose
    "APU0000720311": "VEG-03",     # Lettuce, iceberg
    "APU0000711412": "SUGAR-01",   # Sugar, white granulated
    "APU0000717311": "BEV-01",     # Coffee, ground roasted
    "APU0000717111": "BEV-02",     # Tea, black loose leaf
    # Additional BLS series that appear in historical data
    "APU0000711211": "FRUIT-01",   # Apples, Red Delicious
    "APU0000711411": "SUGAR-02",   # Cookies, chocolate chip
}

# ── BLS series → unit conversion factor (to standardised kg / litre) ──────────
BLS_UNIT_FACTORS = {
    "APU0000701111": 0.453592,   # lb → kg
    "APU0000702111": 2.268000,   # 5 lb bag → kg
    "APU0000702421": 0.453592,   # lb → kg
    "APU0000703112": 0.453592,   # lb → kg
    "APU0000706111": 0.453592,   # lb → kg
    "APU0000706211": 0.453592,   # lb → kg
    "APU0000708111": 0.720000,   # dozen → kg (approx)
    "APU0000709112": 3.785410,   # gallon → litre
    "APU0000712111": 0.453592,   # lb → kg
    "APU0000711111": 0.453592,   # lb → kg
    "APU0000714229": 0.946353,   # quart → litre
    "APU0000720111": 0.453592,   # lb → kg
    "APU0000720211": 0.453592,   # lb → kg
    "APU0000720311": 0.453592,   # lb → kg
    "APU0000711412": 0.453592,   # lb → kg
    "APU0000717311": 0.453592,   # lb → kg
    "APU0000717111": 0.453592,   # lb → kg
    "APU0000711211": 0.453592,   # lb → kg  (apples)
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _last_day(ts: pd.Timestamp) -> str:
    last = calendar.monthrange(ts.year, ts.month)[1]
    return f"{ts.year:04d}-{ts.month:02d}-{last:02d}"


def _usda_item_id(series_id: str) -> str:
    """Derive internal item ID for USDA series from series prefix (e.g. GRAPES → USDA-GRAPES)."""
    prefix = str(series_id).split("_")[0] if "_" in str(series_id) else str(series_id)
    return f"USDA-{prefix[:10].upper()}"


def _migrate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all schema v2 transforms to a single partition DataFrame."""
    # ── 1. Rename columns ──────────────────────────────────────────────────────
    df = df.rename(columns={k: v for k, v in RENAMES.items() if k in df.columns})

    sid_col = "sovereign_series_id"
    ts = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    src = df["source"].str.lower() if "source" in df.columns else pd.Series([""] * len(df), index=df.index)
    is_bls = src == "bls"

    # ── 2. Constant / derived-from-existing columns ────────────────────────────
    df["iso_alpha3"]         = "USA"
    df["market_tier"]        = "Developed"
    df["release_frequency"]  = "Monthly"
    df["fx_rate_applied"]    = 1.0
    df["confidence_tier"]    = "PRIMARY"
    df["source_agency"]      = src.str.upper()
    df["source_sub_category"] = src.map({"bls": "CPI", "usda": "NASS"}).fillna("Unknown")
    df["dataset_id"]         = df[sid_col]

    # ── 3. Item IDs and unit conversion factors (series-specific) ─────────────
    df["internal_item_id"]         = None
    df["unit_quantity_standardized"] = float("nan")

    df.loc[is_bls, "internal_item_id"]          = df.loc[is_bls, sid_col].map(BLS_ITEM_IDS).fillna("BLS-UNKNOWN")
    df.loc[is_bls, "unit_quantity_standardized"] = df.loc[is_bls, sid_col].map(BLS_UNIT_FACTORS)
    df.loc[~is_bls, "internal_item_id"]          = df.loc[~is_bls, sid_col].apply(_usda_item_id)
    # USDA unit_quantity_standardized stays NaN — units vary by commodity

    # ── 4. Timestamp-derived columns ──────────────────────────────────────────
    df["observation_period"] = ts.dt.strftime("%Y-%m").where(ts.notna(), None)
    df["fx_rate_date"]       = ts.apply(lambda t: _last_day(t) if pd.notna(t) else None)

    # ── 5. Revision-derived columns ───────────────────────────────────────────
    rev = df.get("revision_number", pd.Series([0] * len(df), index=df.index)).fillna(0).astype(int)
    df["is_revised_figure"] = rev > 0

    # ── 6. Composite vintage ID ────────────────────────────────────────────────
    agency  = df["source_agency"].fillna("UNKNOWN")
    period  = ts.dt.strftime("%Y-%m").where(ts.notna(), "9999-99")
    df["data_vintage_id"] = (
        agency + "-" + df[sid_col].astype(str) + "-" + period + "-v" + (rev + 1).astype(str)
    )

    return df


def _migrate_file(fpath: Path) -> tuple:
    try:
        df = pd.read_parquet(fpath)
        if df.empty:
            return True, "skipped-empty"
        if "sovereign_series_id" in df.columns:
            return True, "already-v2"
        df_new = _migrate_df(df)
        df_new.to_parquet(fpath, engine="pyarrow", index=False)
        return True, "migrated"
    except Exception as exc:
        return False, f"ERROR: {exc}"


def main():
    files = sorted(VAULT_ROOT.rglob("base_data.parquet"))
    if not files:
        logger.error(f"No base_data.parquet files found under {VAULT_ROOT}")
        return False

    logger.info(f"Found {len(files)} base_data.parquet files under {VAULT_ROOT}")
    logger.info(f"Using {MAX_WORKERS} worker threads")
    logger.info("")

    counts = {"migrated": 0, "already-v2": 0, "skipped-empty": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_migrate_file, f): f for f in files}
        for i, fut in enumerate(as_completed(futures), 1):
            success, outcome = fut.result()
            if success:
                counts[outcome] = counts.get(outcome, 0) + 1
            else:
                counts["error"] += 1
                logger.error(f"  {futures[fut]}: {outcome}")
            if i % 100 == 0 or i == len(files):
                logger.info(
                    f"  [{i}/{len(files)}] migrated={counts['migrated']}  "
                    f"already-v2={counts['already-v2']}  errors={counts['error']}"
                )

    logger.info("")
    logger.info("Migration complete:")
    for k, v in counts.items():
        logger.info(f"  {k}: {v}")

    return counts["error"] == 0


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
