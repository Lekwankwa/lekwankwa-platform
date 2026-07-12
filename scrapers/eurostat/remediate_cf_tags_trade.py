"""
Trade Flows CF-Tag Remediation — same fix as remediate_cf_tags_eu27.py
but targeting the trade_flows vault only.

Run after remediate_cf_tags_eu27.py for complete coverage.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from scrapers.utilities.vault_io import get_vault_root

VAULT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
PRODUCT = "trade_flows"

_CF_RE = re.compile(r"^(.*?-\d{4}-\d{2})-CF(-v\d+)$")


def _fix_vintage_id(vid: str) -> str:
    m = _CF_RE.match(str(vid))
    return f"{m.group(1)}{m.group(2)}" if m else vid


def run() -> None:
    print("=" * 72)
    print("TRADE FLOWS CF-TAG REMEDIATION")
    print("=" * 72)

    total_fixed = 0
    base = VAULT / f"product={PRODUCT}"
    if not base.exists():
        print(f"Product vault not found: {base}")
        return

    for fpath in sorted(base.rglob("*.parquet")):
        try:
            df = pd.read_parquet(fpath)
        except Exception:
            continue

        if "data_vintage_id" not in df.columns:
            continue

        cf_mask = df["data_vintage_id"].str.contains("-CF-", na=False)
        if not cf_mask.any():
            continue

        n_cf = int(cf_mask.sum())
        new_vids = df.loc[cf_mask, "data_vintage_id"].map(_fix_vintage_id)
        existing_vids = set(df.loc[~cf_mask, "data_vintage_id"].dropna())
        for new_vid in new_vids.unique():
            if new_vid in existing_vids:
                print(f"  [WARN] collision: {new_vid} in {fpath.name}")

        df.loc[cf_mask, "data_vintage_id"]       = new_vids.values
        df.loc[cf_mask, "confidence_tier"]        = "DERIVED"
        df.loc[cf_mask, "data_quality_certified"] = False
        df.loc[cf_mask, "is_interpolated"]        = True
        df.loc[cf_mask, "interpolation_method"]   = "QUARTERLY_CARRY_FORWARD"
        df.to_parquet(fpath, index=False, compression="snappy")
        total_fixed += n_cf

    # Summary by country
    print(f"Total trade CF rows remediated: {total_fixed:,}")
    print("=" * 72)


if __name__ == "__main__":
    run()
