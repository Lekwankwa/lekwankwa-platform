"""
Split EU27 trade_flows vault files from two-metric to one-file-one-metric.

Each existing file in the trade vault contains exactly 2 rows:
  Row 1 — Exports of Goods and Services (EXPORTS_GOODS_SERVICES_{ISO3})
  Row 2 — Imports of Goods and Services (IMPORTS_GOODS_SERVICES_{ISO3})

Split to:
  trade_exports_data.parquet    (from trade_data.parquet)
  trade_imports_data.parquet    (from trade_data.parquet)
  trade_exports_fill.parquet    (from trade_monthly_fill.parquet)
  trade_imports_fill.parquet    (from trade_monthly_fill.parquet)

SCOPE: EU27 eurostat_sdmx only. USA trade files are not touched.
"""
from __future__ import annotations

import sys
import io
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
PRODUCT = "trade_flows"
SOURCE  = "eurostat_sdmx"

# Maps old filename → (exports target, imports target)
_SPLIT_MAP = {
    "trade_data.parquet":          ("trade_exports_data.parquet",  "trade_imports_data.parquet"),
    "trade_monthly_fill.parquet":  ("trade_exports_fill.parquet",  "trade_imports_fill.parquet"),
}


def _is_exports(row: pd.Series) -> bool:
    for col in ["sovereign_series_id", "macro_metric_name"]:
        v = str(row.get(col, "")).upper()
        if "EXPORT" in v:
            return True
    return False


def _split_file(src: Path, dst_exp: Path, dst_imp: Path) -> tuple[bool, str | None]:
    """
    Split src into exports and imports files.
    Returns (success, error_message).
    """
    if dst_exp.exists() or dst_imp.exists():
        return False, f"SKIP: target(s) already exist in {src.parent.name}"
    try:
        df = pd.read_parquet(src)
    except Exception as e:
        return False, f"READ ERR: {e}"

    exp_mask = df.apply(_is_exports, axis=1)
    df_exp = df[exp_mask].copy()
    df_imp = df[~exp_mask].copy()

    if df_exp.empty or df_imp.empty:
        return False, f"WARN: could not classify rows (exp={len(df_exp)} imp={len(df_imp)})"

    try:
        df_exp.to_parquet(dst_exp, index=False, compression="snappy")
        df_imp.to_parquet(dst_imp, index=False, compression="snappy")
        src.unlink()
        return True, None
    except Exception as e:
        # Clean up partial writes
        if dst_exp.exists(): dst_exp.unlink(missing_ok=True)
        if dst_imp.exists(): dst_imp.unlink(missing_ok=True)
        return False, f"WRITE ERR: {e}"


def run() -> None:
    base = VAULT / f"product={PRODUCT}"

    ok = fail = skip = 0
    fail_msgs: list[str] = []

    for old_name, (exp_name, imp_name) in _SPLIT_MAP.items():
        for src in sorted(base.rglob(old_name)):
            # EU27 eurostat_sdmx only
            if SOURCE not in str(src):
                continue

            dst_exp = src.parent / exp_name
            dst_imp = src.parent / imp_name

            success, err = _split_file(src, dst_exp, dst_imp)
            if success:
                ok += 1
            elif err and err.startswith("SKIP"):
                skip += 1
            else:
                fail += 1
                if err:
                    fail_msgs.append(f"{src.relative_to(base)}: {err}")

    # Validation
    remaining_old  = sum(1 for n in _SPLIT_MAP for _ in base.rglob(n) if SOURCE in str(_))
    new_exp_data   = sum(1 for _ in base.rglob("trade_exports_data.parquet") if SOURCE in str(_))
    new_imp_data   = sum(1 for _ in base.rglob("trade_imports_data.parquet") if SOURCE in str(_))
    new_exp_fill   = sum(1 for _ in base.rglob("trade_exports_fill.parquet") if SOURCE in str(_))
    new_imp_fill   = sum(1 for _ in base.rglob("trade_imports_fill.parquet") if SOURCE in str(_))

    print("=" * 72)
    print("TRADE VAULT SPLIT — RESULTS (EU27 eurostat_sdmx only)")
    print("=" * 72)
    print(f"Split OK  : {ok:,}")
    print(f"Skipped   : {skip:,}")
    print(f"Failed    : {fail:,}")
    if fail_msgs:
        for m in fail_msgs[:20]:
            print(f"  {m}")
    print()
    print("Post-split file counts (EU27):")
    print(f"  trade_exports_data.parquet : {new_exp_data:,}")
    print(f"  trade_imports_data.parquet : {new_imp_data:,}")
    print(f"  trade_exports_fill.parquet : {new_exp_fill:,}")
    print(f"  trade_imports_fill.parquet : {new_imp_fill:,}")
    print()
    print(f"Original files remaining (should be 0): {remaining_old:,}")
    print()
    if remaining_old == 0 and skip == 0 and fail == 0:
        print("ALL CLEAR — one-file-one-metric satisfied in trade_flows (EU27).")
    else:
        print("ACTION REQUIRED — check failures above.")
    print("=" * 72)


if __name__ == "__main__":
    run()
