"""
data_sets_config.py
Lekwankwa Corporation Pty Ltd

Local configuration for the USA food-pricing historical backfill scripts
(usa_historical_ingestion.py, usa_historical_ingestion_v2.py,
bls_api_backfill_1980_1999.py).

Provides:
  - COICOP_ITEMS   : canonical list of tracked food items (COICOP-coded)
  - BLS_SERIES_IDS : mapping of item_code -> BLS Average Price API metadata

Source of truth: this is the same 17-item BLS series ↔ COICOP mapping used
in production by scrapers/food_pricing/usa_food_scraper.py (BLS_SERIES) and
validated against configs/catalog_expected_series.yaml. Kept in sync with
that module — update both if the tracked item list changes.
"""
from __future__ import annotations

# ── BLS series → COICOP mapping (17 core items) ───────────────────────────────
# Format: (series_id, coicop_code, item_name, item_description, category,
#          bls_unit, factor_to_std_unit, std_unit)
_BLS_SERIES = [
    # Cereals & Grains
    ("APU0000701111", "01.1.1.1", "Rice",           "white, long grain",    "Cereals & Grains", "lb",  0.453592, "kg"),
    ("APU0000702111", "01.1.1.2", "Wheat Flour",    "all purpose",          "Cereals & Grains", "5lb", 2.268,    "kg"),
    ("APU0000702421", "01.1.1.3", "Bread",          "white, sliced",        "Cereals & Grains", "lb",  0.453592, "kg"),

    # Meat & Poultry
    ("APU0000703112", "01.1.2.2", "Beef",           "minced/ground",        "Meat & Poultry",   "lb",  0.453592, "kg"),
    ("APU0000706111", "01.1.2.3", "Chicken",        "whole, fresh",         "Meat & Poultry",   "lb",  0.453592, "kg"),
    ("APU0000706211", "01.1.2.4", "Chicken Breast", "boneless",             "Meat & Poultry",   "lb",  0.453592, "kg"),

    # Dairy & Eggs
    # Eggs: 0.72 kg/doz assumes ~60g per egg (large grade); matches the
    # factor used in scrapers/food_pricing/usa_food_scraper.py BLS_SERIES.
    ("APU0000708111", "01.1.4.3", "Eggs",           "hen, medium/large",    "Dairy & Eggs",     "doz", 0.72,     "kg"),
    ("APU0000709112", "01.1.4.1", "Milk",           "whole, pasteurised",   "Dairy & Eggs",     "gal", 3.78541,  "litre"),
    ("APU0000712111", "01.1.4.5", "Butter",         "unsalted",             "Dairy & Eggs",     "lb",  0.453592, "kg"),
    ("APU0000711111", "01.1.4.4", "Cheese",         "cheddar",              "Dairy & Eggs",     "lb",  0.453592, "kg"),

    # Oils & Fats
    ("APU0000714229", "01.1.5.1", "Vegetable Oil",  "corn/blended",         "Oils & Fats",      "qt",  0.946353, "litre"),

    # Vegetables
    ("APU0000720111", "01.1.7.1", "Tomatoes",       "fresh, round",         "Vegetables",       "lb",  0.453592, "kg"),
    ("APU0000720211", "01.1.7.3", "Potatoes",       "white, loose",         "Vegetables",       "lb",  0.453592, "kg"),
    ("APU0000720311", "01.1.7.4", "Lettuce",        "iceberg",              "Vegetables",       "lb",  0.453592, "kg"),

    # Sugar & Spices
    ("APU0000711412", "01.1.8.1", "Sugar",          "white, granulated",    "Sugar & Spices",   "lb",  0.453592, "kg"),

    # Beverages
    ("APU0000717311", "01.2.1.2", "Coffee",         "ground, roasted",      "Beverages",        "lb",  0.453592, "kg"),
    ("APU0000717111", "01.2.1.1", "Tea",            "black, loose leaf",    "Beverages",        "lb",  0.453592, "kg"),
]

# ── COICOP_ITEMS: canonical item catalogue ────────────────────────────────────
# Each entry uses the "Name (description)" format expected by the backfill
# scripts' parse_item_name() helper.
COICOP_ITEMS = [
    {
        "item_code": coicop_code,
        "item_name": f"{name} ({description})" if description else name,
        "category": category,
        "unit_of_measurement": std_unit,
    }
    for _series_id, coicop_code, name, description, category, _bls_unit, _factor, std_unit
    in _BLS_SERIES
]

# ── BLS_SERIES_IDS: item_code -> BLS series metadata ──────────────────────────
BLS_SERIES_IDS = {
    coicop_code: {
        "series_id": series_id,
        "bls_unit": bls_unit,
        **({"factor_to_litre": factor} if std_unit == "litre" else {"factor_to_kg": factor}),
    }
    for series_id, coicop_code, _name, _description, _category, bls_unit, factor, std_unit
    in _BLS_SERIES
}
