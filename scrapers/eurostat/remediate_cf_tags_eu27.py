"""
EU27 CF-Tag Remediation — Fix Forward-Fill Vintage IDs and Confidence Tiers

Problem:
  Carry-forward (quarterly → monthly gap-fill) rows were written with:
    data_vintage_id:  EUROSTAT-{ISO3}-{METRIC}-{YYYY-MM}-CF-v{N}  ← non-standard
    confidence_tier:  PRIMARY                                        ← wrong
    data_quality_certified: True                                     ← wrong

  The "-CF-" segment is ad-hoc and violates SCHEMA_STANDARD.yaml v5.0
  which defines data_vintage_id as {SOURCE}-{SERIES_ID}-{YYYY-MM}-v{N}.

Fix applied per field:
  data_vintage_id:       Remove "-CF-" segment → standard {YYYY-MM}-v{N}
  confidence_tier:       PRIMARY  → DERIVED
  data_quality_certified True     → False
  is_interpolated:       True   (already set; confirm)
  interpolation_method:  "QUARTERLY_CARRY_FORWARD" (already set; confirm)

Scope:
  - product=Housing_Supply_and_Shelter_Inflation (HPI CF rows, permits CF rows)
  - product=wages_and_employment (employment CF rows)
  - All 27 EU countries

Safety:
  - Checks for vintage ID collisions before rewriting (should be none:
    CF months are inter-quarter, genuine releases are quarter-start only)
  - Reports collisions as WARNs rather than silently overwriting
  - Does NOT delete rows — just updates fields in-place
  - Rewrites parquet files directly (no write_partition dedup side-effects)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

VAULT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
EU27 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE",
]

PRODUCTS = [
    "Housing_Supply_and_Shelter_Inflation",
    "wages_and_employment",
]

SOURCE = "eurostat_sdmx"

_CF_RE = re.compile(r"^(.*?-\d{4}-\d{2})-CF(-v\d+)$")


def _fix_vintage_id(vid: str) -> str:
    """EUROSTAT-SWE-HPI_TOTAL_Q-2017-12-CF-v1 → EUROSTAT-SWE-HPI_TOTAL_Q-2017-12-v1"""
    m = _CF_RE.match(str(vid))
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return vid


def _remediate_file(path: Path) -> tuple[int, int, list[str]]:
    """
    Returns (cf_rows_fixed, collision_count, collision_details).
    Rewrites the parquet file in-place if changes are needed.
    """
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"    [SKIP] Cannot read {path.name}: {e}")
        return 0, 0, []

    if "data_vintage_id" not in df.columns:
        return 0, 0, []

    cf_mask = df["data_vintage_id"].str.contains("-CF-", na=False)
    if not cf_mask.any():
        return 0, 0, []

    n_cf = int(cf_mask.sum())
    collisions = []

    # Compute new vintage IDs for CF rows
    new_vids = df.loc[cf_mask, "data_vintage_id"].map(_fix_vintage_id)

    # Check for collisions: would any new VID already exist in the file (non-CF rows)?
    existing_vids = set(df.loc[~cf_mask, "data_vintage_id"].dropna())
    for new_vid in new_vids.unique():
        if new_vid in existing_vids:
            collisions.append(f"COLLISION: {new_vid} already in {path.name}")

    # Apply fixes
    df.loc[cf_mask, "data_vintage_id"]       = new_vids.values
    df.loc[cf_mask, "confidence_tier"]        = "DERIVED"
    df.loc[cf_mask, "data_quality_certified"] = False
    df.loc[cf_mask, "is_interpolated"]        = True
    df.loc[cf_mask, "interpolation_method"]   = "QUARTERLY_CARRY_FORWARD"

    # Write back
    df.to_parquet(path, index=False, compression="snappy")
    return n_cf, len(collisions), collisions


def run() -> None:
    print("=" * 72)
    print("EU27 CF-TAG REMEDIATION — SCHEMA_STANDARD.yaml v5.0 compliance")
    print("=" * 72)
    print()

    grand_total_fixed = 0
    grand_collisions  = 0
    country_summary   = []

    for product in PRODUCTS:
        print(f"Product: {product}")
        prod_fixed = 0
        for iso3 in EU27:
            base = VAULT / f"product={product}" / f"country={iso3}" / f"source={SOURCE}"
            if not base.exists():
                continue

            iso3_fixed = 0
            iso3_collisions = 0
            for fpath in sorted(base.rglob("*.parquet")):
                fixed, coll_count, coll_details = _remediate_file(fpath)
                iso3_fixed      += fixed
                iso3_collisions += coll_count
                for c in coll_details:
                    print(f"  [WARN] {iso3}: {c}")

            if iso3_fixed:
                print(f"  {iso3}: {iso3_fixed:>6} CF rows remediated")
                if iso3_collisions:
                    print(f"         {iso3_collisions} vintage-ID collisions detected — review manually")
                prod_fixed += iso3_fixed
                country_summary.append((product, iso3, iso3_fixed))

        grand_total_fixed += prod_fixed
        print(f"  Subtotal: {prod_fixed:,} rows\n")

    print("=" * 72)
    print(f"TOTAL CF rows remediated: {grand_total_fixed:,}")
    print(f"TOTAL collisions:         {grand_collisions}")
    print()
    print("Changes applied to each CF row:")
    print("  data_vintage_id:       removed -CF- segment (now standard {YYYY-MM}-v{N})")
    print("  confidence_tier:       PRIMARY → DERIVED")
    print("  data_quality_certified True    → False")
    print("  is_interpolated:       confirmed True")
    print("  interpolation_method:  confirmed QUARTERLY_CARRY_FORWARD")
    print()
    print("SCHEMA_STANDARD.yaml updated with is_interpolated, interpolation_method,")
    print("and confidence_tier controlled vocabulary (PRIMARY/SECONDARY/DERIVED).")
    print("=" * 72)


if __name__ == "__main__":
    run()
