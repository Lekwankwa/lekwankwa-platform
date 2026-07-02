#!/usr/bin/env python3
"""
tools/vault_schema_repair_2026_07_02.py
----------------------------------------
One-time repair for food_micropricing/USA vault files where the 'source'
column was written with an Arrow DictionaryType instead of a plain string
type. pandas.read_parquet() over gcsfs can auto-discover a Hive-partitioned
dataset from a single file's path (year=YYYY/month=MM/...) and try to unify
schemas across ALL sibling partitions in the tree — when even one file has
a dictionary-encoded 'source' column, this fails the WHOLE READ with:

    Unable to merge: Field source has incompatible types:
    string vs dictionary<values=string, indices=int32, ordered=0>

This has been worked around defensively at every read site (scrapers and
validation scripts), but the defensive fallback (opening an explicit file
handle per corrupted file) is too slow to run against 500+ files within a
600s validation timeout. This script fixes the files themselves, once, so
normal fast reads work everywhere going forward.

Run from project root, with VAULT_ROOT set to the target vault:
    VAULT_ROOT=gs://lekwankwa-vault python tools/vault_schema_repair_2026_07_02.py --dry-run
    VAULT_ROOT=gs://lekwankwa-vault python tools/vault_schema_repair_2026_07_02.py
    (unset VAULT_ROOT to repair the local lekwankwa-historical-vault instead)
"""
from __future__ import annotations

import argparse
import os
import sys
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "validations"))
from _vault_root import VAULT_ROOT, IS_GCS, vault_exists, vault_glob, vault_read_parquet  # noqa: E402

PRODUCT = "food_micropricing"
COUNTRY = "USA"
SOURCES = ["bls", "usda_ers"]
FILENAME = "food_pricing_data.parquet"


def _write(path: str, df) -> None:
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False,
                   use_dictionary=False)


_PRINTED_FULL_TRACEBACK = False


def repair_source(source: str, dry_run: bool) -> tuple[int, int, int]:
    """Returns (scanned, repaired, failed) counts for one source."""
    global _PRINTED_FULL_TRACEBACK
    base = f"{VAULT_ROOT}/product={PRODUCT}/country={COUNTRY}/source={source}"
    if not vault_exists(base):
        print(f"  source={source}: vault path not found, skipping")
        return 0, 0, 0

    files = [f for f in vault_glob(base, FILENAME)]
    scanned = repaired = failed = 0

    for f in files:
        scanned += 1
        try:
            import pandas as pd
            # Fast path first — if this already works, the file is fine.
            try:
                pd.read_parquet(f)
                continue
            except Exception as fast_exc:
                if "Unable to merge" not in str(fast_exc):
                    raise

            df = vault_read_parquet(f)  # slow-but-robust fallback read
            changed = False
            for col in df.columns:
                if str(df[col].dtype) == "category":
                    df[col] = df[col].astype(str)
                    changed = True
            if changed:
                if dry_run:
                    print(f"  would repair: {f}")
                else:
                    _write(f, df)
                    print(f"  repaired: {f}")
                repaired += 1
        except Exception as exc:
            failed += 1
            print(f"  FAILED: {f} -> {exc}")
            if not _PRINTED_FULL_TRACEBACK:
                _PRINTED_FULL_TRACEBACK = True
                print("  --- FULL TRACEBACK (first failure only) ---")
                traceback.print_exc()
                print("  --- END TRACEBACK ---")
                return scanned, repaired, failed

    return scanned, repaired, failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="Scan and report what would be repaired, without writing anything.")
    args = parser.parse_args()

    print("=" * 70)
    print(f"VAULT SCHEMA REPAIR — food_micropricing/USA {'[DRY RUN]' if args.dry_run else ''}")
    print(f"Vault root: {VAULT_ROOT} ({'GCS' if IS_GCS else 'local'})")
    print("=" * 70)

    total_scanned = total_repaired = total_failed = 0
    for source in SOURCES:
        print(f"\nsource={source}")
        scanned, repaired, failed = repair_source(source, args.dry_run)
        total_scanned += scanned
        total_repaired += repaired
        total_failed += failed

    print("\n" + "=" * 70)
    print(f"Scanned:  {total_scanned}")
    print(f"{'Would repair' if args.dry_run else 'Repaired'}: {total_repaired}")
    print(f"Failed:   {total_failed}")
    print("=" * 70)
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
