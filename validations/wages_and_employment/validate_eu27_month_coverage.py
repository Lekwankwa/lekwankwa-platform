"""
EU27 Wages & Employment — Month/Year Coverage Validation

Checks every calendar month is present for all 27 EU countries across
the three series in the wages_and_employment vault:

  Series A — Monthly Unemployment Rate (une_rt_m / macro_employment):
    Expected: 12 months/year, 2000-2026

  Series B — Quarterly Employment (lfsi_emp_q):
    Expected: 12 months/year after carry-forward, 2009-2026
    (Before fill: only Jan/Apr/Jul/Oct; after fill: all 12)

  Series C — Annual Wages D11 (nama_10_a10):
    Expected: January only per year (annual series — by design)
    Not flagged as missing; annotated as ANNUAL.

Usage:
  python validations/wages_and_employment/validate_eu27_month_coverage.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

_VAULT_ROOT = os.environ.get("VAULT_ROOT", "").strip() or "lekwankwa-historical-vault"
SOURCE = "eurostat_sdmx"
PRODUCT = "wages_and_employment"

EU27_ISO3 = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE",
]

UNEMP_KEYWORDS  = ["Unemployment Rate", "UNEMPLOYMENT_RATE"]
EMPL_KEYWORDS   = ["Employment", "EMPLOYMENT"]
WAGES_KEYWORDS  = ["WAGES_AND_SALARIES", "Wages", "Compensation"]
FILL_KEYWORDS   = ["carry_forward", "monthly_fill", "wages_empl_monthly_fill"]

# Series A should have 12 months/year from 2000; Series B from 2009 (after carry-forward)
SERIES_A_START = 2000
SERIES_B_START = 2009


def _load_country(iso3: str) -> pd.DataFrame:
    base = f"{_VAULT_ROOT.rstrip('/')}/product={PRODUCT}/country={iso3}/source={SOURCE}"
    frames = []

    if _VAULT_ROOT.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(base):
            return pd.DataFrame()
        paths = sorted(p for p in fs.find(base) if p.endswith(".parquet"))
    else:
        base_path = Path(base)
        if not base_path.exists():
            return pd.DataFrame()
        paths = sorted(str(p) for p in base_path.rglob("*.parquet"))

    for f in paths:
        try:
            df = pd.read_parquet(f)
            df["_file"] = f.rsplit("/", 1)[-1]
            frames.append(df)
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _matches(name: str, keywords: list[str]) -> bool:
    return any(kw.lower() in name.lower() for kw in keywords)


def _check_month_coverage(df: pd.DataFrame, year_start: int, year_end: int = 2025) -> dict:
    """Return {year: [missing months]}."""
    df = df.copy()
    df["_rd"] = pd.to_datetime(df["reporting_date"], errors="coerce")
    gaps: dict[int, list[int]] = {}
    for yr in range(year_start, year_end + 1):
        subset = df[df["_rd"].dt.year == yr]
        months_present = set(subset["_rd"].dt.month.dropna().astype(int))
        missing = sorted(set(range(1, 13)) - months_present)
        if missing:
            gaps[yr] = missing
    return gaps


def run() -> None:
    print("=" * 75)
    print("EU27 WAGES & EMPLOYMENT — MONTH/YEAR COVERAGE VALIDATION")
    print("=" * 75)
    print()

    header = f"{'ISO3':<6} {'UNEMP_MONTHLY':<16} {'EMPL_QUARTERLY':<18} {'WAGES_D11':<12} {'STATUS'}"
    print(header)
    print("-" * 75)

    overall_issues: list[str] = []

    for iso3 in EU27_ISO3:
        df_all = _load_country(iso3)
        if df_all.empty:
            print(f"  {iso3}: NO DATA")
            overall_issues.append(f"{iso3}: no vault data")
            continue

        if "macro_metric_name" not in df_all.columns:
            print(f"  {iso3}: missing macro_metric_name column")
            continue

        # Classify rows
        df_unemp = df_all[df_all["macro_metric_name"].apply(lambda x: _matches(str(x), UNEMP_KEYWORDS))]
        df_empl  = df_all[df_all["macro_metric_name"].apply(
            lambda x: _matches(str(x), EMPL_KEYWORDS) and not _matches(str(x), UNEMP_KEYWORDS)
        )]
        df_wages = df_all[df_all["macro_metric_name"].apply(lambda x: _matches(str(x), WAGES_KEYWORDS))]

        # Series A: monthly unemployment (12 months/yr from 2000)
        unemp_gaps = _check_month_coverage(df_unemp, SERIES_A_START) if not df_unemp.empty else {"NO_DATA": []}
        unemp_ok = len(unemp_gaps) == 0
        unemp_label = "OK (12m/yr)" if unemp_ok else f"GAPS:{len(unemp_gaps)} yrs"

        # Series B: quarterly employment (12 months/yr from 2009 after carry-forward)
        empl_gaps = _check_month_coverage(df_empl, SERIES_B_START) if not df_empl.empty else {"NO_DATA": []}
        empl_ok = len(empl_gaps) == 0
        empl_label = "OK (12m/yr)" if empl_ok else f"GAPS:{len(empl_gaps)} yrs"

        # Series C: annual wages — check January present
        wages_label = "ANNUAL(Jan)" if not df_wages.empty else "MISSING"

        # Country status
        if not unemp_ok or not empl_ok:
            status = "WARN"
            if not unemp_ok:
                years_with_gaps = sorted(unemp_gaps.keys())[:3]
                overall_issues.append(f"{iso3} unemp gaps: {years_with_gaps}")
            if not empl_ok:
                years_with_gaps = sorted(empl_gaps.keys())[:3]
                overall_issues.append(f"{iso3} empl gaps: {years_with_gaps}")
        else:
            status = "OK"

        print(f"  {iso3:<4} {unemp_label:<16} {empl_label:<18} {wages_label:<12} {status}")

    print("-" * 75)
    print()

    if overall_issues:
        print(f"ISSUES ({len(overall_issues)} total):")
        for issue in overall_issues[:20]:
            print(f"  - {issue}")
        if len(overall_issues) > 20:
            print(f"  ... and {len(overall_issues)-20} more")
    else:
        print("ALL PASS — every EU27 country has complete monthly coverage.")

    print()
    print("Series definitions:")
    print("  UNEMP_MONTHLY  — une_rt_m / macro_employment. 12 months/yr expected 2000-2025.")
    print("  EMPL_QUARTERLY — lfsi_emp_q + carry-forward fill. 12 months/yr expected 2009-2025.")
    print("  WAGES_D11      — nama_10_a10 D11 annual. January only per year (ANNUAL by design).")
    print("=" * 75)


if __name__ == "__main__":
    # Support running from project root or from validations subfolder
    import os
    if not Path("lekwankwa-historical-vault").exists():
        # Try project root
        project_root = Path(__file__).resolve().parents[2]
        os.chdir(project_root)
    run()
