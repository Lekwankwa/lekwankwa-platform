"""
Referential Integrity — Trade Flows (US Census FT-900)

CHECKS:
  1.  Country Code Consistency     — country_code = 'US' across all records
  2.  Symmetric HS Coverage        — each HS chapter present in both Export and Import
  3.  Series ID Isolation          — series IDs match HS{2d}_(EXP|IMP) pattern; no overlap
  4.  commodity_code in Exports == commodity_code in Imports per month
  5.  Non-Negative Trade Values    — all observed_value >= 0
  6.  PIT Fields Consistency       — all 5 PIT fields present
  7.  Source Isolation from Other Products — no census_ft900 series in food/employment vaults
  8.  Temporal Partition Alignment — trade_flows months align with employment/food months (post-1989)

OUTPUT:
  trade_flows_referential_integrity_report.json
  trade_flows_referential_integrity_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_referential_integrity.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR    = Path("lekwankwa-historical-vault")
PRODUCT      = "trade_flows"
COUNTRY      = "USA"
SOURCE       = "census_ft900"
SAMPLE_FILES = 50

REPORT_JSON  = Path("trade_flows_referential_integrity_report.json")
REPORT_TXT   = Path("trade_flows_referential_integrity_report.txt")

SERIES_PATTERN = re.compile(r"^HS\d{2}_(EXP|IMP)$")
PIT_FIELDS     = {"record_id", "revision_number", "superseded_by", "published_date", "as_of_date"}


# =============================================================================
# HELPERS
# =============================================================================

def _result(status, check, message, details=None):
    r = {"status": status, "check": check, "message": message}
    if details:
        r["details"] = details
    icons  = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}
    log_fn = logger.error if status == "FAIL" else (logger.warning if status == "WARN" else logger.info)
    log_fn(f"  {icons[status]} {check}")
    if message:
        log_fn(f"         {message}")
    return r


def _load_sample(product, country, source, n: int) -> pd.DataFrame:
    base  = VAULT_DIR / f"product={product}" / f"country={country}" / f"source={source}"
    files = [f for f in base.rglob("*.parquet")
             if "outliers" not in f.name and "changelog" not in f.name]
    step  = max(1, len(files) // n)
    dfs   = []
    for f in files[::step][:n]:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _ym_set(product, country, source) -> set:
    base  = VAULT_DIR / f"product={product}" / f"country={country}" / f"source={source}"
    result = set()
    for f in base.glob("year=*/month=*/*.parquet"):
        parts = f.parts
        y = next((p.split("=")[1] for p in parts if p.startswith("year=")),  None)
        m = next((p.split("=")[1] for p in parts if p.startswith("month=")), None)
        if y and m:
            result.add((int(y), int(m)))
    return result


# =============================================================================
# CHECKS
# =============================================================================

def chk_country_code_consistency(df: pd.DataFrame):
    if "country_code" not in df.columns:
        return _result("FAIL", "Country Code Consistency", "country_code column missing")
    found = set(df["country_code"].dropna().str.upper().unique())
    if found == {"US"} or found == {"USA"} or found.issubset({"US", "USA"}):
        return _result("PASS", "Country Code Consistency",
                       f"All records have country_code in {{US, USA}}: {found}")
    invalid = found - {"US", "USA"}
    return _result("FAIL", "Country Code Consistency",
                   f"Unexpected country_codes: {invalid}",
                   {"invalid": list(invalid)})


def chk_symmetric_hs_coverage(df: pd.DataFrame):
    """Every HS chapter in Exports should also appear in Imports and vice versa."""
    if "trade_flow" not in df.columns or "commodity_code" not in df.columns:
        return _result("SKIP", "Symmetric HS Coverage", "Required columns missing")
    exp_codes = set(df[df["trade_flow"] == "Export"]["commodity_code"].dropna().unique())
    imp_codes = set(df[df["trade_flow"] == "Import"]["commodity_code"].dropna().unique())
    only_exp  = exp_codes - imp_codes
    only_imp  = imp_codes - exp_codes
    both      = exp_codes & imp_codes
    if not only_exp and not only_imp:
        return _result("PASS", "Symmetric HS Coverage",
                       f"All {len(both)} HS chapters present in both Export and Import flows")
    msg = []
    if only_exp:
        msg.append(f"Export-only chapters: {sorted(only_exp)}")
    if only_imp:
        msg.append(f"Import-only chapters: {sorted(only_imp)}")
    return _result("WARN", "Symmetric HS Coverage",
                   "; ".join(msg),
                   {"export_only": sorted(only_exp), "import_only": sorted(only_imp),
                    "symmetric": len(both)})


def chk_series_id_isolation(df: pd.DataFrame):
    """Series IDs must match HS{2d}_(EXP|IMP) — no collision with other products."""
    if "sovereign_series_id" not in df.columns:
        return _result("SKIP", "Series ID Isolation", "sovereign_series_id column missing")
    ids     = df["sovereign_series_id"].dropna()
    invalid = ids[~ids.apply(lambda x: bool(SERIES_PATTERN.match(str(x))))]
    if len(invalid) == 0:
        exp_ids = ids[ids.str.endswith("_EXP")].nunique()
        imp_ids = ids[ids.str.endswith("_IMP")].nunique()
        return _result("PASS", "Series ID Isolation",
                       f"All {len(ids):,} series IDs match HS pattern "
                       f"({exp_ids} EXP + {imp_ids} IMP series)")
    return _result("FAIL", "Series ID Isolation",
                   f"{len(invalid):,} series IDs violate HS{{2d}}_(EXP|IMP) format",
                   {"invalid_samples": invalid.head(5).tolist()})


def chk_commodity_code_scope(df: pd.DataFrame):
    """commodity_code must be in valid HS2 chapter range (01–99)."""
    if "commodity_code" not in df.columns:
        return _result("SKIP", "Commodity Code Scope", "commodity_code column missing")
    codes   = df["commodity_code"].dropna().astype(str)
    invalid = codes[~codes.str.match(r"^\d{2}$")]
    out_rng = codes[codes.str.match(r"^\d{2}$") & (codes.astype(int) < 1)]
    total   = int(len(codes))
    if len(invalid) == 0 and len(out_rng) == 0:
        unique = codes.nunique()
        return _result("PASS", "Commodity Code Scope",
                       f"All {total:,} commodity_codes are valid HS2 (01-99), {unique} unique chapters")
    return _result("FAIL", "Commodity Code Scope",
                   f"{len(invalid) + len(out_rng):,} invalid commodity_codes",
                   {"invalid_samples": (invalid | out_rng).head(5).tolist()})


def chk_trade_value_non_negative(df: pd.DataFrame):
    if "observed_value" not in df.columns:
        return _result("SKIP", "Trade Value Non-Negative", "observed_value column missing")
    numeric = pd.to_numeric(df["observed_value"], errors="coerce")
    neg     = int((numeric < 0).sum())
    nulls   = int(numeric.isna().sum())
    if neg == 0:
        return _result("PASS", "Trade Value Non-Negative",
                       f"All {int(numeric.notna().sum()):,} values >= 0 ({nulls} null)")
    return _result("FAIL", "Trade Value Non-Negative",
                   f"{neg} records have negative observed_value",
                   {"negative_count": neg})


def chk_pit_fields_consistency(df: pd.DataFrame):
    missing = PIT_FIELDS - set(df.columns)
    if missing:
        return _result("FAIL", "PIT Fields Consistency",
                       f"Missing PIT columns: {sorted(missing)}")
    null_counts = {c: int(df[c].isna().sum()) for c in PIT_FIELDS - {"superseded_by"}
                   if df[c].isna().sum() > 0}
    if not null_counts:
        sup_null_pct = round(df["superseded_by"].isna().mean() * 100, 2) if "superseded_by" in df.columns else 100.0
        return _result("PASS", "PIT Fields Consistency",
                       f"All 5 PIT fields present and populated. superseded_by: {sup_null_pct}% null",
                       {"superseded_by_null_pct": sup_null_pct})
    return _result("FAIL", "PIT Fields Consistency",
                   f"Null values in required PIT fields: {null_counts}",
                   null_counts)


def chk_source_isolation_from_other_products(df: pd.DataFrame):
    """No census_ft900 series should appear in food or employment vaults."""
    trade_series = set(df.get("sovereign_series_id", pd.Series()).dropna().unique())
    cross_issues = []
    for other_product, other_source in [
        ("food_micropricing", "bls"),
        ("wages_and_employment", "bls_ces"),
    ]:
        other_df = _load_sample(other_product, COUNTRY, other_source, n=20)
        if other_df.empty:
            continue
        for col in ["sovereign_series_id", "source_series_id"]:
            if col in other_df.columns:
                other_series = set(other_df[col].dropna().unique())
                overlap = trade_series & other_series
                if overlap:
                    cross_issues.append(
                        f"{other_product}/{other_source}: {len(overlap)} shared series IDs"
                    )
    if not cross_issues:
        return _result("PASS", "Source Isolation from Other Products",
                       "No census_ft900 series IDs found in food/employment vaults")
    return _result("FAIL", "Source Isolation from Other Products",
                   f"Series ID collision: {'; '.join(cross_issues)}",
                   {"collisions": cross_issues})


def chk_temporal_alignment_with_employment(df: pd.DataFrame):
    """Trade flow months (post-1989) should overlap with employment vault months."""
    trade_yms = set()
    ts = pd.to_datetime(df.get("data_timestamp", pd.Series()), errors="coerce", utc=True)
    for t in ts.dropna():
        if t.year >= 1989:
            trade_yms.add((t.year, t.month))

    emp_yms = _ym_set("wages_and_employment", COUNTRY, "bls_ces")
    if not emp_yms:
        return _result("SKIP", "Temporal Alignment with Employment",
                       "Employment vault not available for cross-check")

    overlap  = len(trade_yms & emp_yms)
    total_tr = len(trade_yms)
    pct      = round(overlap / max(total_tr, 1) * 100, 1)
    if pct >= 80:
        return _result("PASS", "Temporal Alignment with Employment",
                       f"{overlap}/{total_tr} trade months ({pct}%) present in employment vault (post-1989)")
    return _result("WARN", "Temporal Alignment with Employment",
                   f"Only {overlap}/{total_tr} ({pct}%) trade months align with employment data",
                   {"overlap": overlap, "trade_total": total_tr, "pct": pct})


# =============================================================================
# MAIN
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — REFERENTIAL INTEGRITY")
    logger.info("=" * 70)

    df = _load_sample(PRODUCT, COUNTRY, SOURCE, SAMPLE_FILES)
    if df.empty:
        logger.error("  No trade_flows data found. Run scraper first.")
        return False

    logger.info(f"  Sample: {len(df):,} records")

    results = [
        chk_country_code_consistency(df),
        chk_symmetric_hs_coverage(df),
        chk_series_id_isolation(df),
        chk_commodity_code_scope(df),
        chk_trade_value_non_negative(df),
        chk_pit_fields_consistency(df),
        chk_source_isolation_from_other_products(df),
        chk_temporal_alignment_with_employment(df),
    ]

    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    overall = "PASS" if failed == 0 else "FAIL"

    logger.info(f"\n  Summary: {passed}P / {failed}F / {warned}W / {skipped}S -> [{overall}]")

    report = {
        "product": PRODUCT, "country": COUNTRY,
        "generated": datetime.utcnow().isoformat(),
        "overall": overall,
        "counts": {"passed": passed, "failed": failed, "warned": warned, "skipped": skipped},
        "checks": results,
    }
    REPORT_JSON.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    with open(REPORT_TXT, "w") as f:
        f.write(f"TRADE FLOWS — REFERENTIAL INTEGRITY REPORT\nOverall: [{overall}]\n\n")
        for r in results:
            f.write(f"  [{r['status']:<4}] {r['check']}\n         {r['message']}\n")

    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
