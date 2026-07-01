"""
Shared loader for non-EU country validation (GBR, CAN, AUS, NOR).
All stage scripts import from here.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

VAULT = Path("lekwankwa-historical-vault")

# ISO3 → (country_name, vault_source, source_agency)
COUNTRIES: dict[str, tuple[str, str, str]] = {
    "GBR": ("United Kingdom", "ons_api",       "ONS"),
    "CAN": ("Canada",         "statcan_csv",   "StatCan"),
    "AUS": ("Australia",      "abs_sdmx",      "ABS"),
    "NOR": ("Norway",         "ssb_statbank",  "SSB"),
}

# Products where NOR has no data (no housing table found)
SKIP_NOR = {"Housing_Supply_and_Shelter_Inflation"}

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
    skip = SKIP_NOR if product in SKIP_NOR else set()
    return {iso: v for iso, v in COUNTRIES.items() if iso not in skip}


def load(product: str, exclude_outliers: bool = True) -> pd.DataFrame:
    """Load all non-EU vault data for one product (all active countries, all years)."""
    filename = PRODUCT_FILENAMES.get(product, "*.parquet")
    frames: list[pd.DataFrame] = []
    for iso, (_, source, _) in active_countries(product).items():
        src_dir = VAULT / f"product={product}" / f"country={iso}" / f"source={source}"
        if not src_dir.exists():
            continue
        for f in sorted(src_dir.rglob(filename)):
            if exclude_outliers and ("outlier" in f.name or "changelog" in f.name):
                continue
            try:
                frames.append(pd.read_parquet(f))
            except Exception:
                pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
