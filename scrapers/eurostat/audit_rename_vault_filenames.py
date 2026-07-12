"""
STEP 1 — Audit filename-to-content consistency across housing, wages, trade vaults.
STEP 2 — Rename every mismatched file (data untouched).
STEP 3 — Post-rename verification.

Naming conventions used:
  Housing:
    permits_monthly_fill.parquet   → AUTHORIZED_PERMITS CF rows only
    hpi_monthly_fill.parquet       → HPI CF rows only
    housing_monthly_fill.parquet   → mixed HPI + permits CF rows in same file
    permits_eu27_data.parquet      → AUTHORIZED_PERMITS primary quarterly data
    hpi_purchase_data.parquet      → HPI primary quarterly data
    housing_data.parquet           → mixed/other housing primary data
    housing_hicp_rent_data.parquet → HICP rent data

  Wages:
    wages_empl_monthly_fill.parquet     → employment CF rows only
    wages_compensation_data.parquet     → wages/compensation primary data
    unemployment_data.parquet           → unemployment primary data

  Trade:
    trade_monthly_fill.parquet  → trade CF rows only
    trade_data.parquet          → trade primary quarterly data
"""
from __future__ import annotations

import sys
import io
from pathlib import Path

import pandas as pd
from scrapers.utilities.vault_io import get_vault_root

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
PRODUCTS = [
    "Housing_Supply_and_Shelter_Inflation",
    "wages_and_employment",
    "trade_flows",
]

# ── Content-type detection ────────────────────────────────────────────────────

def _content_types(df: pd.DataFrame) -> frozenset[str]:
    """Return frozenset of content type tags from a dataframe."""
    types: set[str] = set()

    mmn_col  = df.get("macro_metric_name") if "macro_metric_name" in df.columns else None
    ssid_col = df.get("sovereign_series_id") if "sovereign_series_id" in df.columns else None
    ssc_col  = df.get("source_sub_category") if "source_sub_category" in df.columns else None

    for col in [mmn_col, ssid_col, ssc_col]:
        if col is None:
            continue
        for raw in col.dropna().unique():
            v = str(raw).upper()
            if "HOUSE PRICE" in v or "HPI" in v:
                types.add("HPI")
            if "PERMIT" in v or "AUTHORIZED_PERMIT" in v or "COBP" in v:
                types.add("PERMITS")
            if "HICP" in v or "RENT" in v:
                types.add("HICP_RENT")
            if "EMPLOY" in v or "EMPL_Q" in v or "LFSI" in v:
                types.add("EMPLOYMENT")
            if "UNEMPLOY" in v or "UNEMP" in v:
                types.add("UNEMPLOYMENT")
            if "WAGE" in v or "COMPENSATION" in v or "D11" in v or "EARN" in v:
                types.add("WAGES")
            if "EXPORT" in v or "IMPORT" in v or "TRADE" in v or "NAMQ" in v:
                types.add("TRADE")

    return frozenset(types)


# ── Filename expectations per product ─────────────────────────────────────────
# Maps a frozenset of content types → expected filename stem.
# The file will be flagged as mismatched if its current stem does NOT appear
# in the set of acceptable stems for that content fingerprint.

_HOUSING_EXPECTED: dict[frozenset, set[str]] = {
    frozenset({"HPI"}):                     {"hpi_monthly_fill", "hpi_purchase_data", "housing_data"},
    frozenset({"PERMITS"}):                 {"permits_monthly_fill", "permits_eu27_data", "housing_data"},
    frozenset({"HPI", "PERMITS"}):          {"housing_monthly_fill", "housing_data"},
    frozenset({"HICP_RENT"}):               {"housing_hicp_rent_data", "housing_data"},
    frozenset({"HPI", "HICP_RENT"}):        {"housing_data", "housing_hicp_rent_data"},
    frozenset({"PERMITS", "HICP_RENT"}):    {"housing_data"},
    frozenset({"HPI", "PERMITS", "HICP_RENT"}): {"housing_data"},
}

_WAGES_EXPECTED: dict[frozenset, set[str]] = {
    frozenset({"EMPLOYMENT"}):              {"wages_empl_monthly_fill", "employment_data"},
    frozenset({"UNEMPLOYMENT"}):            {"unemployment_data"},
    frozenset({"WAGES"}):                   {"wages_compensation_data"},
    frozenset({"EMPLOYMENT", "UNEMPLOYMENT"}): {"wages_employment_unemployment_data", "employment_unemployment_data"},
    frozenset({"WAGES", "EMPLOYMENT"}):     {"wages_employment_data"},
    frozenset({"WAGES", "UNEMPLOYMENT"}):   {"wages_unemployment_data"},
    frozenset({"WAGES", "EMPLOYMENT", "UNEMPLOYMENT"}): {"wages_all_data"},
}

_TRADE_EXPECTED: dict[frozenset, set[str]] = {
    frozenset({"TRADE"}):                   {"trade_monthly_fill", "trade_data"},
}

_PRODUCT_EXPECTED = {
    "Housing_Supply_and_Shelter_Inflation": _HOUSING_EXPECTED,
    "wages_and_employment":                 _WAGES_EXPECTED,
    "trade_flows":                          _TRADE_EXPECTED,
}

# This is a Eurostat/EU27 naming tool. Non-EU countries (USA, GBR, CAN) have
# their own scrapers with their own filename conventions (e.g. USA housing uses
# shelter_inflation_data.parquet / housing_permits_data.parquet). Applying this
# tool's EU-flavored, content-based stems (permits_eu27_data,
# housing_hicp_rent_data) to those files is a mislabeling bug — USA CPI shelter
# was renamed to housing_hicp_rent_data and USA Census permits to
# permits_eu27_data. Skip them entirely.
_NON_EU_COUNTRIES = frozenset({"USA", "GBR", "CAN"})


def _expected_stems(product: str, ctypes: frozenset) -> set[str]:
    """Return set of acceptable filename stems for this content fingerprint."""
    mapping = _PRODUCT_EXPECTED.get(product, {})
    return mapping.get(ctypes, set())


# ── Correct filename derivation ────────────────────────────────────────────────
# For a mismatched file, derive the authoritative correct filename stem.

def _correct_stem(product: str, ctypes: frozenset, current_stem: str, df: pd.DataFrame) -> str:
    """Derive the authoritative filename stem for this content."""

    is_fill = (
        "is_interpolated" in df.columns and
        bool(df["is_interpolated"].any())
    )

    if product == "Housing_Supply_and_Shelter_Inflation":
        if ctypes == frozenset({"HPI"}):
            return "hpi_monthly_fill" if is_fill else "hpi_purchase_data"
        if ctypes == frozenset({"PERMITS"}):
            return "permits_monthly_fill" if is_fill else "permits_eu27_data"
        if ctypes >= frozenset({"HPI", "PERMITS"}):
            return "housing_monthly_fill" if is_fill else "housing_data"
        if "HICP_RENT" in ctypes:
            return "housing_hicp_rent_data"
        return "housing_data"

    if product == "wages_and_employment":
        if ctypes == frozenset({"EMPLOYMENT"}):
            return "wages_empl_monthly_fill" if is_fill else "employment_data"
        if ctypes == frozenset({"UNEMPLOYMENT"}):
            return "unemployment_data"
        if ctypes == frozenset({"WAGES"}):
            return "wages_compensation_data"
        if "EMPLOYMENT" in ctypes and "UNEMPLOYMENT" in ctypes:
            return "employment_unemployment_data"
        if "WAGES" in ctypes:
            return "wages_data"
        return "wages_data"

    if product == "trade_flows":
        if "TRADE" in ctypes:
            return "trade_monthly_fill" if is_fill else "trade_data"
        return "trade_data"

    return current_stem


# ── Main ──────────────────────────────────────────────────────────────────────

def run() -> None:
    mismatches: list[dict] = []
    total_files = 0
    skipped = 0

    # ── STEP 1: Diagnose ──────────────────────────────────────────────────────
    print("=" * 72)
    print("STEP 1 — Filename / Content Audit")
    print("=" * 72)

    for product in PRODUCTS:
        base = VAULT / f"product={product}"
        if not base.exists():
            print(f"\n[SKIP] {product} — vault directory not found")
            continue

        product_mismatches = []

        for fpath in sorted(base.rglob("*.parquet")):
            # Skip non-data helper files
            if fpath.stem in ("changelog", "outliers"):
                continue
            # Never relabel non-EU countries — they own their filename conventions.
            country = next(
                (p.split("=", 1)[1] for p in fpath.parts if p.startswith("country=")),
                None,
            )
            if country in _NON_EU_COUNTRIES:
                continue
            total_files += 1

            try:
                df = pd.read_parquet(fpath)
            except Exception as e:
                skipped += 1
                continue

            if "data_vintage_id" not in df.columns and "macro_metric_name" not in df.columns:
                skipped += 1
                continue

            ctypes = _content_types(df)
            if not ctypes:
                skipped += 1
                continue

            current_stem = fpath.stem
            ok_stems = _expected_stems(product, ctypes)

            if ok_stems and current_stem not in ok_stems:
                correct = _correct_stem(product, ctypes, current_stem, df)
                new_path = fpath.with_name(correct + ".parquet")
                rel = str(fpath.relative_to(base))
                product_mismatches.append({
                    "product": product,
                    "fpath": fpath,
                    "new_path": new_path,
                    "rel": rel,
                    "current_stem": current_stem,
                    "correct_stem": correct,
                    "ctypes": ctypes,
                    "rows": len(df),
                })

        if product_mismatches:
            print(f"\n[MISMATCHES] {product}:")
            for m in product_mismatches:
                print(f"  {m['rel']}")
                print(f"    Current name : {m['current_stem']}.parquet")
                print(f"    Correct name : {m['correct_stem']}.parquet")
                print(f"    Content types: {', '.join(sorted(m['ctypes']))}")
                print(f"    Rows affected: {m['rows']:,}")
            mismatches.extend(product_mismatches)
        else:
            print(f"\n[CLEAN] {product}: no filename/content mismatches found")

    if not mismatches:
        print("\nNo mismatches anywhere. Nothing to rename.")
        print(f"\nFiles scanned: {total_files}  |  Skipped (no VID/metric col): {skipped}")
        return

    print(f"\nTotal mismatches found: {len(mismatches)}")

    # ── STEP 2: Rename ────────────────────────────────────────────────────────
    # (no progress output between step 1 and step 3 per spec)
    rename_ok = 0
    rename_fail = 0
    rename_errors: list[str] = []

    for m in mismatches:
        src  = m["fpath"]
        dst  = m["new_path"]
        if dst.exists():
            rename_errors.append(f"SKIP: target already exists: {dst}")
            rename_fail += 1
            continue
        try:
            src.rename(dst)
            rename_ok += 1
        except Exception as e:
            rename_errors.append(f"ERR: {src} -> {dst}: {e}")
            rename_fail += 1

    # ── STEP 3: Validation ────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("STEP 3 — Post-Rename Validation")
    print("=" * 72)

    remaining = 0
    post_files = 0
    for product in PRODUCTS:
        base = VAULT / f"product={product}"
        if not base.exists():
            continue
        for fpath in sorted(base.rglob("*.parquet")):
            if fpath.stem in ("changelog", "outliers"):
                continue
            country = next(
                (p.split("=", 1)[1] for p in fpath.parts if p.startswith("country=")),
                None,
            )
            if country in _NON_EU_COUNTRIES:
                continue
            post_files += 1
            try:
                df = pd.read_parquet(fpath)
            except Exception:
                continue
            if "data_vintage_id" not in df.columns and "macro_metric_name" not in df.columns:
                continue
            ctypes = _content_types(df)
            if not ctypes:
                continue
            ok_stems = _expected_stems(product, ctypes)
            if ok_stems and fpath.stem not in ok_stems:
                remaining += 1
                print(f"  [STILL WRONG] {fpath.relative_to(base)} — content={ctypes}")

    print()
    print(f"Files scanned (pre-rename):  {total_files}")
    print(f"Files skipped (no metadata): {skipped}")
    print(f"Mismatches found:            {len(mismatches)}")
    print(f"Successfully renamed:        {rename_ok}")
    print(f"Rename failures:             {rename_fail}")
    if rename_errors:
        for e in rename_errors:
            print(f"  {e}")
    print(f"Mismatches remaining:        {remaining}")
    print()
    if remaining == 0 and rename_fail == 0:
        print("ALL CLEAR — zero filename/content mismatches remain.")
    else:
        print("ACTION REQUIRED — mismatches remain or renames failed.")
    print("=" * 72)


if __name__ == "__main__":
    run()
