"""
Targeted wages_and_employment filename remediation.

Problem caused by audit script's substring classification bug:
  "UNEMPLOY" contains "EMPLOY" as a substring → unemployment files were
  misclassified as {EMPLOYMENT, UNEMPLOYMENT} instead of {UNEMPLOYMENT}.

Two bad outcomes:
  A) 9,995 `unemployment_data.parquet` files (UNEMPLOYMENT_RATE_U3 series)
     were wrongly renamed to `employment_unemployment_data.parquet`.

  B) 10,631 `wages_data.parquet` files (Monthly Unemployment Rate series,
     and on quarterly months also Quarterly Employment) could not be renamed
     because the target `employment_unemployment_data.parquet` already existed
     in each directory (created by mistake A above).

Fix applied per directory (order matters within each dir):
  Step 1: `employment_unemployment_data.parquet` (U3 content)
          → `unemployment_u3_data.parquet`
          (gives U3 series a proper specific name; frees up the name space)

  Step 2a: `wages_data.parquet` with Quarterly Employment (mixed content)
           → `employment_unemployment_data.parquet`
           (freed by step 1; correct name for mixed unemployment+employment)

  Step 2b: `wages_data.parquet` with only Monthly Unemployment Rate
           → `unemployment_data.parquet`
           (now free since U3 moved to unemployment_u3_data.parquet)

Post-fix naming convention:
  unemployment_data.parquet         — Monthly Unemployment Rate (une_rt_m)
  unemployment_u3_data.parquet      — U3 Unemployment Rate (UNEMPLOYMENT_RATE_U3)
  employment_unemployment_data.parquet — Mixed: employment + unemployment (quarterly months)
  wages_empl_monthly_fill.parquet   — Quarterly employment CF carry-forward (DERIVED)
  wages_compensation_data.parquet   — Wages and Salaries (D11)
"""
from __future__ import annotations

import sys
import io
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT   = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"
PRODUCT = "wages_and_employment"

_EMPL_Q_MARKERS = {"EMPL_Q", "LFSI_EMP", "QUARTERLY EMPLOYMENT"}


def _has_employment(df: pd.DataFrame) -> bool:
    """Return True if df contains any quarterly employment series."""
    for col in ["sovereign_series_id", "macro_metric_name"]:
        if col not in df.columns:
            continue
        for v in df[col].dropna().unique():
            v_up = str(v).upper()
            for marker in _EMPL_Q_MARKERS:
                if marker in v_up:
                    return True
    return False


def _fix_directory(dirpath: Path) -> dict[str, int]:
    """
    Apply the two-step rename within one partition directory.
    Returns count of each rename type performed.
    """
    counts = {"step1": 0, "step2a": 0, "step2b": 0, "skip": 0}

    eu_file   = dirpath / "employment_unemployment_data.parquet"
    wages_file = dirpath / "wages_data.parquet"

    # ── Step 1: undo bad rename of U3 unemployment file ────────────────────
    if eu_file.exists():
        try:
            df = pd.read_parquet(eu_file)
        except Exception:
            counts["skip"] += 1
            return counts

        # Verify it is unemployment-only (U3 / EUROSTAT_UNE_* series)
        # If it already has EMPL_Q it was legitimately mixed — leave it alone
        if _has_employment(df):
            # This is a correctly named mixed file; do not rename
            pass
        else:
            target = dirpath / "unemployment_u3_data.parquet"
            if not target.exists():
                eu_file.rename(target)
                counts["step1"] += 1
            else:
                counts["skip"] += 1

    # ── Step 2: rename wages_data.parquet to correct name ──────────────────
    if wages_file.exists():
        try:
            df = pd.read_parquet(wages_file)
        except Exception:
            counts["skip"] += 1
            return counts

        if _has_employment(df):
            # Mixed: monthly unemployment + quarterly employment
            target = dirpath / "employment_unemployment_data.parquet"
            if not target.exists():
                wages_file.rename(target)
                counts["step2a"] += 1
            else:
                counts["skip"] += 1
        else:
            # Unemployment only (Monthly Unemployment Rate series)
            target = dirpath / "unemployment_data.parquet"
            if not target.exists():
                wages_file.rename(target)
                counts["step2b"] += 1
            else:
                counts["skip"] += 1

    return counts


def run() -> None:
    base = VAULT / f"product={PRODUCT}"
    if not base.exists():
        print(f"Vault not found: {base}")
        return

    total = {"step1": 0, "step2a": 0, "step2b": 0, "skip": 0}
    dirs_processed = 0

    # Collect all leaf directories (partition folders containing .parquet files)
    dirs_with_target = set()
    for f in base.rglob("employment_unemployment_data.parquet"):
        dirs_with_target.add(f.parent)
    for f in base.rglob("wages_data.parquet"):
        dirs_with_target.add(f.parent)

    for dirpath in sorted(dirs_with_target):
        counts = _fix_directory(dirpath)
        for k, v in counts.items():
            total[k] += v
        dirs_processed += 1

    # ── Validation ──────────────────────────────────────────────────────────
    remaining_eu     = sum(1 for _ in base.rglob("employment_unemployment_data.parquet")
                          if not _has_employment(pd.read_parquet(_)))
    remaining_wages  = sum(1 for _ in base.rglob("wages_data.parquet"))
    new_unemp        = sum(1 for _ in base.rglob("unemployment_data.parquet"))
    new_unemp_u3     = sum(1 for _ in base.rglob("unemployment_u3_data.parquet"))
    eu_mixed         = sum(1 for _ in base.rglob("employment_unemployment_data.parquet"))
    fill_ok          = sum(1 for _ in base.rglob("wages_empl_monthly_fill.parquet"))
    wages_ok         = sum(1 for _ in base.rglob("wages_compensation_data.parquet"))

    print("=" * 72)
    print("WAGES FILENAME REMEDIATION — RESULTS")
    print("=" * 72)
    print(f"Directories processed: {dirs_processed}")
    print()
    print(f"Step 1  (employment_unemployment → unemployment_u3): {total['step1']:,}")
    print(f"Step 2a (wages_data mixed → employment_unemployment): {total['step2a']:,}")
    print(f"Step 2b (wages_data unemp-only → unemployment_data):  {total['step2b']:,}")
    print(f"Skipped (target existed or read error):               {total['skip']:,}")
    print()
    print("Post-fix file counts:")
    print(f"  unemployment_data.parquet        : {new_unemp:,}  (Monthly Unemployment Rate)")
    print(f"  unemployment_u3_data.parquet     : {new_unemp_u3:,}  (U3 Unemployment Rate)")
    print(f"  employment_unemployment_data.parq: {eu_mixed:,}  (Mixed employment+unemployment)")
    print(f"  wages_empl_monthly_fill.parquet  : {fill_ok:,}  (Employment CF — correct)")
    print(f"  wages_compensation_data.parquet  : {wages_ok:,}  (Wages — correct)")
    print()
    print(f"Remaining wages_data.parquet       : {remaining_wages:,}  (should be 0)")
    print(f"Remaining EU-only empl_unemp files : {remaining_eu:,}  (should be 0)")
    if remaining_wages == 0 and remaining_eu == 0 and total["skip"] == 0:
        print("\nALL CLEAR — zero filename/content mismatches in wages vault.")
    else:
        print("\nACTION REQUIRED — some files could not be fixed.")
    print("=" * 72)


if __name__ == "__main__":
    run()
