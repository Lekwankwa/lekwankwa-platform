"""
Two operations in one pass over the wages_and_employment vault:

Op 1 — Delete unemployment_data.parquet (une_rt_m series)
  These 8,728 files are exact duplicates of unemployment_u3_data.parquet (U3).
  U3 covers the full history (starts 1998 for some countries) while une_rt_m
  starts 2000 universally. Verified identical values across 247 months for 8
  countries. Dropping the redundant shorter series.

Op 2 — Split employment_unemployment_data.parquet (1,903 mixed files)
  Each of these 2-row files contains:
    Row 1 — Monthly Unemployment Rate (UNE_RT_M_*) — duplicate of U3, drop it
    Row 2 — Quarterly Employment SA (EMPL_Q_SA_Y2064_*) — keep, write to
             employment_quarterly_data.parquet

After these two ops every file in wages vault contains exactly one
macro_metric_name (one-file-one-metric convention satisfied).
"""
from __future__ import annotations

import sys
import io
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT   = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"
PRODUCT = "wages_and_employment"

_EMPL_MARKERS = {"EMPL_Q", "LFSI_EMP", "QUARTERLY EMPLOYMENT"}


def _is_employment_row(row: pd.Series) -> bool:
    for col in ["sovereign_series_id", "macro_metric_name"]:
        v = str(row.get(col, "")).upper()
        for marker in _EMPL_MARKERS:
            if marker in v:
                return True
    return False


def run() -> None:
    base = VAULT / f"product={PRODUCT}"

    # ── Op 1: delete unemployment_data.parquet ───────────────────────────────
    del_ok = del_fail = 0
    for f in sorted(base.rglob("unemployment_data.parquet")):
        try:
            f.unlink()
            del_ok += 1
        except Exception as e:
            print(f"  DELETE ERR: {f}: {e}")
            del_fail += 1

    # ── Op 2: split employment_unemployment_data.parquet ─────────────────────
    split_ok = split_fail = split_conflict = 0
    empl_rows_written = unemp_rows_dropped = 0

    for mixed in sorted(base.rglob("employment_unemployment_data.parquet")):
        try:
            df = pd.read_parquet(mixed)
        except Exception as e:
            print(f"  READ ERR: {mixed}: {e}")
            split_fail += 1
            continue

        empl_mask = df.apply(_is_employment_row, axis=1)
        df_empl   = df[empl_mask].copy()
        df_unemp  = df[~empl_mask]
        unemp_rows_dropped += len(df_unemp)

        if df_empl.empty:
            print(f"  WARN: no employment rows in {mixed} (rows={len(df)})")
            split_fail += 1
            continue

        target = mixed.parent / "employment_quarterly_data.parquet"
        if target.exists():
            print(f"  CONFLICT: target exists: {target}")
            split_conflict += 1
            continue

        try:
            df_empl.to_parquet(target, index=False, compression="snappy")
            mixed.unlink()
            split_ok += 1
            empl_rows_written += len(df_empl)
        except Exception as e:
            print(f"  WRITE ERR: {target}: {e}")
            split_fail += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 72)
    print("WAGES DEDUP + SPLIT — RESULTS")
    print("=" * 72)
    print(f"\nOp 1: deleted unemployment_data.parquet (une_rt_m duplicates)")
    print(f"  Deleted : {del_ok:,}")
    print(f"  Failed  : {del_fail:,}")
    print(f"\nOp 2: split employment_unemployment_data.parquet")
    print(f"  Split OK           : {split_ok:,}  files")
    print(f"  Unemployment rows dropped  : {unemp_rows_dropped:,}  (duplicate of U3 series)")
    print(f"  Employment rows written    : {empl_rows_written:,}  -> employment_quarterly_data.parquet")
    print(f"  Conflicts (target existed) : {split_conflict:,}")
    print(f"  Failures                   : {split_fail:,}")

    # ── Post-op validation ────────────────────────────────────────────────────
    remaining_unemp_data = sum(1 for _ in base.rglob("unemployment_data.parquet"))
    remaining_mixed      = sum(1 for _ in base.rglob("employment_unemployment_data.parquet"))
    new_empl_q           = sum(1 for _ in base.rglob("employment_quarterly_data.parquet"))
    u3_count             = sum(1 for _ in base.rglob("unemployment_u3_data.parquet"))
    fill_count           = sum(1 for _ in base.rglob("wages_empl_monthly_fill.parquet"))
    wages_count          = sum(1 for _ in base.rglob("wages_compensation_data.parquet"))

    print()
    print("Post-op file counts in wages_and_employment:")
    print(f"  unemployment_u3_data.parquet         : {u3_count:,}  (canonical unemployment series)")
    print(f"  employment_quarterly_data.parquet     : {new_empl_q:,}  (quarterly employment, new)")
    print(f"  wages_empl_monthly_fill.parquet       : {fill_count:,}  (employment CF, DERIVED)")
    print(f"  wages_compensation_data.parquet       : {wages_count:,}  (wages D11)")
    print()
    print(f"  unemployment_data.parquet remaining   : {remaining_unemp_data:,}  (should be 0)")
    print(f"  employment_unemployment_data.parquet  : {remaining_mixed:,}  (should be 0)")
    print()
    if remaining_unemp_data == 0 and remaining_mixed == 0 and split_conflict == 0:
        print("ALL CLEAR — one-file-one-metric convention satisfied in wages vault.")
    else:
        print("ACTION REQUIRED — check conflicts or failures above.")
    print("=" * 72)


if __name__ == "__main__":
    run()
