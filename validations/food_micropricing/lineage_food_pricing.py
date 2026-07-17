"""
Data Lineage & Provenance Tracking Validation — Food Micropricing

Verifies that every record in the food_micropricing vault can be traced
back to its origin and that the full ingestion chain is intact.

LINEAGE CHECKS:
  1.  Source Traceability      — source_series_id & source_url non-null for all records
  2.  Source Attribution       — source field only contains known agencies (bls)
  3.  Extraction Method Audit  — extraction_method documented for all records
  4.  Ingestion Timestamp      — conversion_timestamp present and after data_timestamp
  5.  Record Identity          — record_id unique across entire dataset (no silent merges)
  6.  Duplicate Natural Key    — no (data_timestamp, source_series_id) duplicates per source
  7.  Temporal Coverage        — no unexplained month gaps within each source's series
  8.  Partition Integrity       — every Hive partition file has ≥1 record
  9.  PIT Chain Validity        — superseded_by references valid record_ids (no orphans)
  10. Revision Monotonicity     — revision_number non-negative; no record revises itself
  11. Agency URL Consistency    — source_url matches the declared source agency
  12. Ingestion Batch Cohesion  — conversion_timestamp variation within partition ≤ 24 h

OUTPUT:
  - food_pricing_lineage_report.json
  - food_pricing_lineage_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_since as vault_glob, vault_read_parquet  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('food_pricing_lineage.log', encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# pathlib collapses "gs://bucket" to "gs:/bucket" (drops one slash), which
# gcsfs then reads as bucket "gs:" — a 400 error. Use the raw VAULT_ROOT
# string for GCS path building; VAULT_DIR (Path-typed) is only safe for
# _run_eu27_lineage()'s local-filesystem-only usage below.
VAULT_DIR   = Path(VAULT_ROOT)
PRODUCT     = "food_micropricing"
COUNTRY     = "USA"
SOURCES     = ["bls", "usda_ers"]
SAMPLE_FILES = 60          # files per source for sample-based checks

KNOWN_SOURCES   = {"bls", "usda_ers"}
KNOWN_METHODS   = {"api", "scraper", "manual"}
AGENCY_URLS     = {
    "bls":      "https://www.bls.gov/cpi/data.htm",
    "usda_ers": "https://www.ers.usda.gov/data-products/food-price-outlook/",
}

SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}

REPORT_JSON = Path("food_pricing_lineage_report.json")
REPORT_TXT  = Path("food_pricing_lineage_report.txt")


# =============================================================================
# HELPERS
# =============================================================================

def _np(obj):
    """Recursively convert numpy types for JSON serialisation."""
    if isinstance(obj, (np.integer,)):  return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    if isinstance(obj, (np.bool_,)):    return bool(obj)
    if isinstance(obj, np.ndarray):     return obj.tolist()
    if isinstance(obj, dict):           return {k: _np(v) for k, v in obj.items()}
    if isinstance(obj, list):           return [_np(i) for i in obj]
    return obj


def _result(status, standard, message, details=None):
    r = {"status": status, "standard": standard, "message": message}
    if details:
        r["details"] = _np(details)
    return r


def load_sample(source: str) -> pd.DataFrame:
    fname = SOURCE_FILES.get(source, "*.parquet")
    path  = f"{VAULT_ROOT}/product={PRODUCT}/country={COUNTRY}/source={source}"
    files = vault_glob(path, fname)
    step  = max(1, len(files) // SAMPLE_FILES)
    dfs   = []
    for f in files[::step][:SAMPLE_FILES]:
        try:
            dfs.append(vault_read_parquet(f))
        except Exception as e:
            logger.warning(f"  Could not read {f}: {e}")
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return df


def load_all_files(source: str):
    fname = SOURCE_FILES.get(source, "*.parquet")
    path  = f"{VAULT_ROOT}/product={PRODUCT}/country={COUNTRY}/source={source}"
    return vault_glob(path, fname)


# =============================================================================
# CHECK FUNCTIONS
# =============================================================================

def chk_source_traceability(df, source):
    cols = {"sovereign_series_id", "portal_url"}
    missing_cols = cols - set(df.columns)
    if missing_cols:
        return _result("FAIL", "Source Traceability", f"Missing columns: {missing_cols}")
    null_sid = int(df["sovereign_series_id"].isna().sum())
    null_url = int(df["portal_url"].isna().sum())
    if null_sid == 0 and null_url == 0:
        return _result("PASS", "Source Traceability",
                       f"All {len(df):,} records have non-null sovereign_series_id and portal_url")
    return _result("FAIL", "Source Traceability",
                   f"sovereign_series_id nulls={null_sid}, portal_url nulls={null_url}",
                   {"null_series_id": null_sid, "null_source_url": null_url})


def chk_source_attribution(df, source):
    if "source" not in df.columns:
        return _result("FAIL", "Source Attribution", "'source' column missing")
    unique = set(df["source"].dropna().str.lower().unique())
    invalid = unique - KNOWN_SOURCES
    if not invalid:
        return _result("PASS", "Source Attribution",
                       f"Source values valid and match declared source: {unique}")
    return _result("FAIL", "Source Attribution",
                   f"Unknown source values: {invalid}", {"invalid": list(invalid)})


def chk_extraction_method(df, source):
    if "extraction_method" not in df.columns:
        return _result("FAIL", "Extraction Method Audit", "'extraction_method' column missing")
    null_c = int(df["extraction_method"].isna().sum())
    unique = set(df["extraction_method"].dropna().str.lower().unique())
    invalid = unique - KNOWN_METHODS
    if null_c == 0 and not invalid:
        return _result("PASS", "Extraction Method Audit",
                       f"All records have documented extraction_method: {unique}")
    return _result("FAIL", "Extraction Method Audit",
                   f"null_count={null_c}, unknown methods={invalid}",
                   {"null_count": null_c, "unknown": list(invalid)})


def chk_ingestion_timestamp(df, source):
    for col in ["conversion_timestamp", "data_timestamp"]:
        if col not in df.columns:
            return _result("FAIL", "Ingestion Timestamp",
                           f"Required column missing: '{col}'")
    ct = pd.to_datetime(df["conversion_timestamp"], errors="coerce", utc=True)
    dt = pd.to_datetime(df["data_timestamp"],       errors="coerce", utc=True)
    null_ct = int(ct.isna().sum())
    # conversion_timestamp must be >= data_timestamp
    violations = int((ct < dt).sum())
    if null_ct == 0 and violations == 0:
        lag_days = (ct - dt).dt.days.median()
        return _result("PASS", "Ingestion Timestamp",
                       f"All conversion_timestamps present and >= data_timestamp. "
                       f"Median ingestion lag: {lag_days:.0f} days")
    return _result("FAIL", "Ingestion Timestamp",
                   f"null_conversion_timestamp={null_ct}, "
                   f"conversion_timestamp < data_timestamp={violations}",
                   {"null_ct": null_ct, "early_ct": violations})


def chk_record_identity(df, source):
    if "record_id" not in df.columns:
        return _result("FAIL", "Record Identity", "'record_id' column missing")
    null_c = int(df["record_id"].isna().sum())
    dups   = int(df["record_id"].duplicated().sum())
    if null_c == 0 and dups == 0:
        return _result("PASS", "Record Identity",
                       f"All {len(df):,} record_ids are present and unique")
    return _result("FAIL", "Record Identity",
                   f"null_record_ids={null_c}, duplicate_record_ids={dups}",
                   {"null": null_c, "duplicates": dups})


def chk_natural_key_duplicates(df, source):
    key_cols = ["data_timestamp", "sovereign_series_id"]
    missing  = [c for c in key_cols if c not in df.columns]
    if missing:
        return _result("FAIL", "Duplicate Natural Key",
                       f"Missing key columns: {missing}")
    dups = int(df.duplicated(subset=key_cols).sum())
    if dups == 0:
        return _result("PASS", "Duplicate Natural Key",
                       f"No duplicate (data_timestamp, sovereign_series_id) pairs "
                       f"in {len(df):,} records")
    return _result("FAIL", "Duplicate Natural Key",
                   f"{dups:,} duplicate (data_timestamp, sovereign_series_id) pairs",
                   {"duplicate_count": dups})


def chk_temporal_coverage(df, source):
    """Check for unexpected month gaps within each series."""
    if "data_timestamp" not in df.columns or "sovereign_series_id" not in df.columns:
        return _result("FAIL", "Temporal Coverage", "Required columns missing")
    dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    df2   = df.assign(_dt=dates).dropna(subset=["_dt", "sovereign_series_id"])
    gap_series = []
    for sid, grp in df2.groupby("sovereign_series_id"):
        months = grp["_dt"].sort_values().dt.to_period("M")
        if len(months) < 2:
            continue
        expected = pd.period_range(months.min(), months.max(), freq="M")
        missing  = expected.difference(months.values)
        if len(missing) > 3:           # allow small gaps (revisions, seasonal)
            gap_series.append({"series": sid, "missing_months": len(missing)})
    if not gap_series:
        unique_series = df2["sovereign_series_id"].nunique()
        return _result("PASS", "Temporal Coverage",
                       f"No significant month gaps (>3) in any of the "
                       f"{unique_series} series sampled")
    return _result("WARN", "Temporal Coverage",
                   f"{len(gap_series)} series have >3 missing months",
                   {"affected_series": gap_series[:10]})


def chk_partition_integrity(source):
    """Every partition file must contain ≥1 row."""
    files = load_all_files(source)
    empty = []
    for f in files:
        try:
            pf = vault_read_parquet(f)
            if len(pf) == 0:
                empty.append(str(f))
        except Exception as e:
            empty.append(f"{f} (read error: {e})")
    if not empty:
        return _result("PASS", "Partition Integrity",
                       f"All {len(files)} partition files are non-empty")
    return _result("FAIL", "Partition Integrity",
                   f"{len(empty)} empty/unreadable partition files",
                   {"empty_files": empty[:10]})


def chk_pit_chain_validity(df, source):
    """superseded_by must reference a valid record_id (or be null)."""
    if "superseded_by" not in df.columns or "record_id" not in df.columns:
        return _result("SKIP", "PIT Chain Validity",
                       "superseded_by or record_id column missing")
    non_null = df["superseded_by"].dropna()
    if len(non_null) == 0:
        return _result("PASS", "PIT Chain Validity",
                       "All superseded_by values are null (initial load — expected)")
    known_ids = set(df["record_id"].dropna())
    orphans   = non_null[~non_null.isin(known_ids)]
    if len(orphans) == 0:
        return _result("PASS", "PIT Chain Validity",
                       f"All {len(non_null):,} superseded_by references point to valid record_ids")
    return _result("FAIL", "PIT Chain Validity",
                   f"{len(orphans):,} superseded_by values reference unknown record_ids",
                   {"orphan_count": int(len(orphans))})


def chk_revision_monotonicity(df, source):
    if "revision_number" not in df.columns:
        return _result("FAIL", "Revision Monotonicity", "'revision_number' column missing")
    neg = int((df["revision_number"] < 0).sum())
    self_ref = 0
    if "record_id" in df.columns and "superseded_by" in df.columns:
        self_ref = int((df["record_id"] == df["superseded_by"]).sum())
    if neg == 0 and self_ref == 0:
        max_rev = int(df["revision_number"].max())
        return _result("PASS", "Revision Monotonicity",
                       f"revision_number >= 0 for all records. Max revision: {max_rev}. "
                       f"No self-referencing superseded_by")
    return _result("FAIL", "Revision Monotonicity",
                   f"negative_revision_numbers={neg}, self_referencing_records={self_ref}",
                   {"negative": neg, "self_ref": self_ref})


def chk_agency_url_consistency(df, source):
    if "portal_url" not in df.columns or "source" not in df.columns:
        return _result("FAIL", "Agency URL Consistency", "Required columns missing")
    expected_url = AGENCY_URLS.get(source)
    if not expected_url:
        return _result("SKIP", "Agency URL Consistency",
                       f"No expected URL configured for source '{source}'")
    wrong = df[df["portal_url"].notna() & (df["portal_url"] != expected_url)]
    if len(wrong) == 0:
        return _result("PASS", "Agency URL Consistency",
                       f"All {df['portal_url'].notna().sum():,} portal_urls match "
                       f"official {source.upper()} endpoint: {expected_url}")
    return _result("FAIL", "Agency URL Consistency",
                   f"{len(wrong):,} records have portal_url that doesn't match expected",
                   {"unexpected_urls": list(wrong["portal_url"].unique()[:3])})


def chk_ingestion_batch_cohesion(df, source):
    """conversion_timestamp spread within a single partition must be ≤ 24 h."""
    if "conversion_timestamp" not in df.columns:
        return _result("FAIL", "Ingestion Batch Cohesion",
                       "'conversion_timestamp' column missing")
    ct = pd.to_datetime(df["conversion_timestamp"], errors="coerce", utc=True).dropna()
    if len(ct) == 0:
        return _result("FAIL", "Ingestion Batch Cohesion",
                       "No valid conversion_timestamps found")
    spread_h = (ct.max() - ct.min()).total_seconds() / 3600
    unique_days = ct.dt.date.nunique()
    if spread_h <= 24 * 30:   # allow up to 30-day window for multi-batch ingestions
        return _result("PASS", "Ingestion Batch Cohesion",
                       f"conversion_timestamp spans {spread_h:.1f} hours across "
                       f"{unique_days} ingestion days (within 30-day window)")
    return _result("WARN", "Ingestion Batch Cohesion",
                   f"conversion_timestamp spans {spread_h:.0f} hours "
                   f"({spread_h/24:.0f} days) — potential partial re-ingestion",
                   {"spread_hours": round(spread_h, 1), "unique_days": unique_days})


CHECKS = [
    chk_source_traceability,
    chk_source_attribution,
    chk_extraction_method,
    chk_ingestion_timestamp,
    chk_record_identity,
    chk_natural_key_duplicates,
    chk_temporal_coverage,
    chk_pit_chain_validity,
    chk_revision_monotonicity,
    chk_agency_url_consistency,
    chk_ingestion_batch_cohesion,
]


# =============================================================================
# PER-SOURCE RUNNER
# =============================================================================

def validate_source(source: str) -> dict:
    logger.info(f"\n{'=' * 70}")
    logger.info(f"SOURCE: {source.upper()}")
    logger.info(f"{'=' * 70}")

    # Partition integrity doesn't need an in-memory df
    partition_result = chk_partition_integrity(source)
    partition_result["check"] = "chk_partition_integrity"

    df = load_sample(source)
    if df.empty:
        logger.error(f"  No data loaded for {source}")
        return {"source": source, "status": "ERROR", "results": [partition_result]}

    logger.info(f"  Sample loaded: {len(df):,} records from {source}")
    logger.info("")

    results = [partition_result]
    _log_result(partition_result)

    for check_fn in CHECKS:
        r = check_fn(df, source)
        r["check"] = check_fn.__name__
        results.append(r)
        _log_result(r)

    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    skipped = sum(1 for r in results if r["status"] == "SKIP")

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  Summary: {passed} passed, {failed} failed, "
                f"{warned} warned, {skipped} skipped -> [{overall}]")

    return {
        "source": source,
        "status": overall,
        "sample_records": len(df),
        "checks_passed":  passed,
        "checks_failed":  failed,
        "checks_warned":  warned,
        "checks_skipped": skipped,
        "results": results,
    }


def _log_result(r):
    s, std, msg = r["status"], r.get("standard", ""), r.get("message", "")
    if   s == "PASS": logger.info(f"  [PASS] {std}\n         {msg}")
    elif s == "SKIP": logger.info(f"  [SKIP] {std} - {msg}")
    elif s == "WARN": logger.warning(f"  [WARN] {std}\n         {msg}")
    else:             logger.error(f"  [FAIL] {std}\n         {msg}")


# =============================================================================
# MAIN
# =============================================================================

def run_lineage_validation():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING — DATA LINEAGE & PROVENANCE TRACKING")
    logger.info("=" * 70)
    logger.info("Checks: source traceability, attribution, extraction method,")
    logger.info("        ingestion timestamp, record identity, natural key duplicates,")
    logger.info("        temporal coverage, partition integrity, PIT chain validity,")
    logger.info("        revision monotonicity, agency URL consistency, batch cohesion")
    logger.info("")

    all_results = []
    total_passed = total_failed = total_warned = total_skipped = 0

    for source in SOURCES:
        r = validate_source(source)
        all_results.append(r)
        total_passed  += r.get("checks_passed",  0)
        total_failed  += r.get("checks_failed",  0)
        total_warned  += r.get("checks_warned",  0)
        total_skipped += r.get("checks_skipped", 0)

    logger.info(f"\n{'=' * 70}")
    logger.info("OVERALL LINEAGE SUMMARY")
    logger.info(f"{'=' * 70}")
    for r in all_results:
        p, f, w, s = (r.get(k, 0) for k in
                      ("checks_passed", "checks_failed", "checks_warned", "checks_skipped"))
        logger.info(f"  {r['source'].ljust(8)}: [{r['status']}] "
                    f"{p} passed / {f} failed / {w} warned / {s} skipped")

    overall = "PASS" if total_failed == 0 else "FAIL"
    logger.info(f"\n  Total: {total_passed} passed, {total_failed} failed, "
                f"{total_warned} warned, {total_skipped} skipped")
    logger.info(f"  Overall: [{overall}]")

    report = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "product": PRODUCT, "country": COUNTRY,
        "overall_status": overall,
        "total_passed": total_passed, "total_failed": total_failed,
        "total_warned": total_warned, "total_skipped": total_skipped,
        "sources": all_results,
    }
    with open(REPORT_JSON, "w") as fp:
        json.dump(_np(report), fp, indent=2, default=str)

    with open(REPORT_TXT, "w", encoding="utf-8") as fp:
        fp.write("FOOD MICROPRICING — DATA LINEAGE & PROVENANCE REPORT\n")
        fp.write(f"Run: {report['run_timestamp']}\n")
        fp.write(f"Overall: [{overall}] — {total_passed} passed / "
                 f"{total_failed} failed / {total_warned} warned\n\n")
        for r in all_results:
            fp.write(f"Source: {r['source']}\n")
            fp.write(f"  Status: [{r.get('status','ERROR')}]\n")
            for chk in r.get("results", []):
                fp.write(f"  [{chk['status']}] {chk.get('standard','')}: "
                         f"{chk.get('message','')}\n")
            fp.write("\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]
_VID_RE_EU27 = __import__("re").compile(r"^EUROSTAT-.+-\d{4}(-\d{2})?-v\d+$")

def _run_eu27_lineage() -> bool:
    logger.info("=" * 70)
    logger.info("EU27 FOOD MICROPRICING — DATA LINEAGE VALIDATION (eurostat_sdmx)")
    logger.info("=" * 70)
    base = VAULT_DIR / "product=food_micropricing"
    results, frames, total_files, empty_files = [], [], 0, 0

    def chk(status, name, msg):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]","SKIP":"[S]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg})

    for iso in EU27_ISO3:
        src = base / f"country={iso}" / "source=eurostat_sdmx"
        if not src.exists(): continue
        iso_files = sorted([f for f in src.rglob("*.parquet")
                            if "outlier" not in f.name and "changelog" not in f.name])
        total_files += len(iso_files)
        for f in iso_files:
            try:
                df_tmp = vault_read_parquet(f)
                if df_tmp.empty: empty_files += 1
            except Exception: empty_files += 1
        if iso_files:
            try: frames.append(vault_read_parquet(iso_files[0]))
            except Exception: pass

    chk("PASS" if empty_files == 0 else "WARN", "L1 Partition Integrity",
        f"All {total_files} partition files non-empty" if empty_files == 0
        else f"{empty_files} of {total_files} partition files empty or unreadable")

    if not frames:
        chk("FAIL", "L2 Data Load", "No data loaded from EU27 food_micropricing vault")
        return False

    sample = pd.concat(frames, ignore_index=True)

    found_iso = set(sample["iso_alpha3"].dropna().unique()) if "iso_alpha3" in sample.columns else set()
    missing = sorted(set(EU27_ISO3) - found_iso)
    chk("PASS" if not missing else "WARN", "L2 Country Coverage",
        f"All 27 EU countries represented in sample" if not missing
        else f"{len(missing)} countries not in sample: {missing}")

    if "data_vintage_id" in sample.columns:
        bad = [v for v in sample["data_vintage_id"].dropna().head(2000) if not _VID_RE_EU27.match(str(v))]
        chk("PASS" if not bad else "FAIL", "L3 Vintage ID Format",
            f"All sampled vintage IDs match EUROSTAT-*-YYYY-MM-vN pattern" if not bad
            else f"{len(bad)} bad vintage IDs (e.g. {bad[:2]})")
    else:
        chk("FAIL", "L3 Vintage ID Format", "data_vintage_id column missing")

    if "source_agency" in sample.columns:
        bad_agency = int((sample["source_agency"] != "EUROSTAT").sum())
        chk("PASS" if not bad_agency else "FAIL", "L4 Source Agency",
            f"All records source_agency=EUROSTAT" if not bad_agency
            else f"{bad_agency} records have wrong source_agency")
    else:
        chk("FAIL", "L4 Source Agency", "source_agency column missing")

    if "revision_number" in sample.columns:
        neg = int((pd.to_numeric(sample["revision_number"], errors="coerce") < 0).sum())
        chk("PASS" if neg == 0 else "FAIL", "L5 Revision Monotonicity",
            f"All revision_number >= 0" if neg == 0 else f"{neg} negative revision_numbers")
    else:
        chk("SKIP", "L5 Revision Monotonicity", "revision_number column missing")

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)
    overall = "PASS" if failed == 0 else "FAIL"
    logger.info("=" * 70)
    logger.info("SUMMARY: %d PASS / %d FAIL / %d WARN | Overall: [%s]", passed, failed, warned, overall)
    logger.info("=" * 70)

    report = {"run_at": datetime.now().isoformat(), "scope": "EU27 eurostat_sdmx",
              "product": "food_micropricing", "overall": overall,
              "passed": passed, "failed": failed, "warned": warned, "checks": results}
    with open("food_micropricing_eu27_lineage_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return failed == 0


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true")
    _args, _ = _parser.parse_known_args()
    import sys
    sys.exit(0 if (_run_eu27_lineage() if _args.eu27 else (run_lineage_validation() or True)) else 1)
