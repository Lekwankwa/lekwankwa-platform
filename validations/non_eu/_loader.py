"""
Shared loader for non-EU country validation (GBR, CAN).
All stage scripts import from here.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd

_VAULT_ROOT = os.environ.get("VAULT_ROOT", "").strip() or "lekwankwa-historical-vault"
# NOTE: unlike load()/_find_files() above, changelog_generator_non_eu.py,
# lineage_non_eu.py, outlier_extractor_non_eu.py and schema_compliance_non_eu.py
# use VAULT with real pathlib.Path methods (.exists(), .rglob(), .relative_to())
# and have no gs:// branch — they only work against a local path. This fixes the
# ImportError (VAULT never existed); it does not add cloud-storage support to
# those four scripts.
VAULT = Path(_VAULT_ROOT)

# ISO3 → (country_name, vault_source, source_agency)
COUNTRIES: dict[str, tuple[str, str, str]] = {
    "GBR": ("United Kingdom", "ons_api",       "ONS"),
    "CAN": ("Canada",         "statcan_csv",   "StatCan"),
}

PRODUCT_FILENAMES: dict[str, str] = {
    "food_micropricing":                   "food_pricing_data.parquet",
    "wages_and_employment":                "wages_employment_data.parquet",
    "Housing_Supply_and_Shelter_Inflation": "housing_data.parquet",
    "trade_flows":                         "trade_flows_data.parquet",
    "global_macro":                        "global_macro_data.parquet",
}

ALL_PRODUCTS = list(PRODUCT_FILENAMES.keys())


def active_countries(product: str) -> dict[str, tuple[str, str, str]]:
    """Return COUNTRIES filtered by product-level exclusions."""
    return dict(COUNTRIES)


def _find_files(src_dir: str, filename: str) -> list[str]:
    """List matching parquet file paths under src_dir — works for gs:// and local paths."""
    if _VAULT_ROOT.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(src_dir):
            return []
        return sorted(p for p in fs.find(src_dir) if p.endswith(filename))
    else:
        local_dir = Path(src_dir)
        if not local_dir.exists():
            return []
        return sorted(str(p) for p in local_dir.rglob(filename))


def load(product: str, exclude_outliers: bool = True) -> pd.DataFrame:
    """Load all non-EU vault data for one product (all active countries, all years)."""
    filename = PRODUCT_FILENAMES.get(product, "*.parquet")
    frames: list[pd.DataFrame] = []
    for iso, (_, source, _) in active_countries(product).items():
        src_dir = f"{_VAULT_ROOT.rstrip('/')}/product={product}/country={iso}/source={source}"
        for f in _find_files(src_dir, filename):
            fname = f.rsplit("/", 1)[-1]
            if exclude_outliers and ("outlier" in fname or "changelog" in fname):
                continue
            try:
                frames.append(pd.read_parquet(f))
            except Exception:
                pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
