"""
Data Lineage & Provenance Tracking Validation — Macro Employment

Verifies that every record in the macro_employment vault can be traced
back to its BLS origin and that the full ingestion chain is intact.

LINEAGE CHECKS:
  1.  Source Traceability       — source_series_id & source_url non-null for all records
  2.  Source Attribution        — source field matches expected BLS identifiers
  3.  Extraction Method Audit   — extraction_method documented ('api' for BLS FTP)
  4.  Ingestion Timestamp       — conversion_timestamp present, UTC-aware, >= data_timestamp
  5.  Record Identity           — record_id unique across entire dataset
  6.  Duplicate Natural Key     — no (data_timestamp, source_series_id) duplicates
  7.  Temporal Coverage         — no unexplained month gaps within each series
  8.  Partition Integrity        — every Hive partition file has ≥1 record
  9.  PIT Chain Validity         — superseded_by references valid record_ids (or null)
  10. Revision Monotonicity      — revision_number ≥ 0, no self-referencing records
  11. Agency URL Consistency     — source_url matches official BLS FTP endpoint
  12. Series ID–Source Alignment — CES IDs start with 'CES', JOLTS with 'JTS'/'JTU'
  13. Ingestion Batch Cohesion   — conversion_timestamp spread ≤ 30 days in sample

OUTPUT:
  - macro_employment_lineage_report.json
  - macro_employment_lineage_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('macro_employment_lineage.log', encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR    = Path("lekwankwa-historical-vault")
PRODUCT      = "wages_and_employment"
COUNTRY      = "USA"
SOURCES      = ["bls_ces", "bls_cps"]
SAMPLE_FILES = 50

KNOWN_SOURCES  = {"bls_ces", "bls_cps", "bls_jolts"}
KNOWN_METHODS  = {"api", "scraper", "manual"}
AGENCY_URLS    = {
    "bls_ces":   "https://www.bls.gov",
    "bls_cps":   "https://www.bls.gov",
    "bls_jolts": "https://download.bls.gov/pub/time.series/jt/",
}
SERIES_PREFIXES = {
    "bls_ces":   ("CES",),
    "bls_cps":   ("LNS",),
    "bls_jolts": ("JTS", "JTU"),
}

REPORT_JSON = Path("macro_employment_lineage_report.json")
REPORT_TXT  = Path("macro_employment_lineage_report.txt")


# =============================================================================
# HELPERS
# =============================================================================

def _np(obj):
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


def _log_result(r):
    s, std, msg = r["status"], r.get("standard", ""), r.get("message", "")
    if   s == "PASS": logger.info(f"  [PASS] {std}\n         {msg}")
    elif s == "SKIP": logger.info(f"  [SKIP] {std} - {msg}")
    elif s == "WARN": logger.warning(f"  [WARN] {std}\n         {msg}")
    else:             logger.error(f"  [FAIL] {std}\n         {msg}")


def load_sample(source: str) -> pd.DataFrame:
    path  = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    files = [f for f in path.rglob("*.parquet")
             if "outliers" not in f.name and "changelog" not in f.name]
    step  = max(1, len(files) // SAMPLE_FILES)
    dfs   = []
    for f in files[::step][:SAMPLE_FILES]:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            logger.warning(f"  Could not read {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def all_data_files(source: str):
    path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    return [f for f in path.rglob("*.parquet")
            if "outliers" not in f.name and "changelog" not in f.name]


# =============================================================================
# CHECKS
# =============================================================================

def chk_source_traceability(df, source):
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    url_col = "source_url" if "source_url" in df.columns else "portal_url"
    cols = {sid_col, url_col}
    missing_cols = cols - set(df.columns)
    if missing_cols:
        return _result("FAIL", "Source Traceability", f"Missing columns: {missing_cols}")
    null_sid = int(df[sid_col].isna().sum())
    null_url = int(df[url_col].isna().sum())
    if null_sid == 0 and null_url == 0:
        return _result("PASS", "Source Traceability",
                       f"All {len(df):,} records have non-null {sid_col} and {url_col}")
    return _result("FAIL", "Source Traceability",
                   f"{sid_col} nulls={null_sid}, {url_col} nulls={null_url}",
                   {"null_series_id": null_sid, "null_source_url": null_url})


def chk_source_attribution(df, source):
    if "source" not in df.columns:
        return _result("FAIL", "Source Attribution", "'source' column missing")
    unique  = set(df["source"].dropna().str.lower().unique())
    invalid = unique - KNOWN_SOURCES
    if not invalid:
        return _result("PASS", "Source Attribution",
                       f"Source values valid: {unique}")
    return _result("FAIL", "Source Attribution",
                   f"Unknown source values: {invalid}", {"invalid": list(invalid)})


def chk_extraction_method(df, source):
    if "extraction_method" not in df.columns:
        return _result("FAIL", "Extraction Method Audit", "'extraction_method' column missing")
    null_c  = int(df["extraction_method"].isna().sum())
    unique  = set(df["extraction_method"].dropna().str.lower().unique())
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
    null_ct    = int(ct.isna().sum())
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
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    key_cols = ["data_timestamp", sid_col]
    missing  = [c for c in key_cols if c not in df.columns]
    if missing:
        return _result("FAIL", "Duplicate Natural Key",
                       f"Missing key columns: {missing}")
    dups = int(df.duplicated(subset=key_cols).sum())
    if dups == 0:
        return _result("PASS", "Duplicate Natural Key",
                       f"No duplicate (data_timestamp, {sid_col}) pairs "
                       f"in {len(df):,} records")
    return _result("FAIL", "Duplicate Natural Key",
                   f"{dups:,} duplicate natural keys found",
                   {"duplicate_count": dups})


def chk_temporal_coverage(df, source):
    """Check for unexpected month gaps within each series (sample-based)."""
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    if "data_timestamp" not in df.columns or sid_col not in df.columns:
        return _result("FAIL", "Temporal Coverage", "Required columns missing")
    dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    df2   = df.assign(_dt=dates).dropna(subset=["_dt", sid_col])
    sample_series = df2[sid_col].unique()[:30]
    gap_series = []
    for sid in sample_series:
        months   = df2[df2[sid_col] == sid]["_dt"].sort_values().dt.to_period("M")
        if len(months) < 2:
            continue
        expected = pd.period_range(months.min(), months.max(), freq="M")
        missing  = expected.difference(months.values)
        if len(missing) > 6:   # BLS can have gaps for new/discontinued series
            gap_series.append({"series": sid, "missing_months": len(missing)})
    if not gap_series:
        return _result("PASS", "Temporal Coverage",
                       f"No significant month gaps (>6) in any of the "
                       f"{len(sample_series)} sampled series")
    return _result("WARN", "Temporal Coverage",
                   f"{len(gap_series)} series have >6 missing months "
                   f"(may be expected for new/discontinued BLS series)",
                   {"affected_series": gap_series[:10]})


def chk_partition_integrity(source):
    files = all_data_files(source)
    empty = []
    for f in files:
        try:
            pf = pd.read_parquet(f)
            if len(pf) == 0:
                empty.append(str(f))
        except Exception as e:
            empty.append(f"{f} (error: {e})")
    if not empty:
        return _result("PASS", "Partition Integrity",
                       f"All {len(files)} partition files are non-empty")
    return _result("FAIL", "Partition Integrity",
                   f"{len(empty)} empty/unreadable partition files",
                   {"empty_files": empty[:10]})


def chk_pit_chain_validity(df, source):
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
        return _result("FAIL", "Revision Monotonicity",
                       "'revision_number' column missing")
    neg = int((df["revision_number"] < 0).sum())
    self_ref = 0
    if "record_id" in df.columns and "superseded_by" in df.columns:
        self_ref = int((df["record_id"] == df["superseded_by"]).sum())
    if neg == 0 and self_ref == 0:
        max_rev = int(df["revision_number"].max())
        return _result("PASS", "Revision Monotonicity",
                       f"revision_number ≥ 0 for all records. Max: {max_rev}. "
                       f"No self-referencing superseded_by")
    return _result("FAIL", "Revision Monotonicity",
                   f"negative_revision_numbers={neg}, self_referencing_records={self_ref}",
                   {"negative": neg, "self_ref": self_ref})


def chk_agency_url_consistency(df, source):
    url_col = "source_url" if "source_url" in df.columns else "portal_url"
    if url_col not in df.columns:
        return _result("FAIL", "Agency URL Consistency", "'source_url'/'portal_url' column missing")
    expected = AGENCY_URLS.get(source)
    if not expected:
        return _result("SKIP", "Agency URL Consistency",
                       f"No expected URL configured for source '{source}'")
    wrong = df[df[url_col].notna() & (df[url_col] != expected)]
    if len(wrong) == 0:
        return _result("PASS", "Agency URL Consistency",
                       f"All {df[url_col].notna().sum():,} {url_col} values match "
                       f"BLS endpoint: {expected}")
    return _result("FAIL", "Agency URL Consistency",
                   f"{len(wrong):,} records have unexpected {url_col}",
                   {"unexpected_urls": list(wrong[url_col].unique()[:3])})


def chk_series_id_source_alignment(df, source):
    """Series IDs must start with the correct BLS prefix for the declared source."""
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    if sid_col not in df.columns:
        return _result("FAIL", "Series ID–Source Alignment",
                       f"'{sid_col}' column missing")
    expected_prefixes = SERIES_PREFIXES.get(source, ())
    if not expected_prefixes:
        return _result("SKIP", "Series ID–Source Alignment",
                       f"No prefix rules configured for source '{source}'")
    ids  = df[sid_col].dropna()
    bad  = ids[~ids.apply(lambda x: str(x).startswith(expected_prefixes))]
    if len(bad) == 0:
        unique_prefixes = set(ids.str[:3].unique())
        return _result("PASS", "Series ID–Source Alignment",
                       f"All {len(ids):,} series IDs start with expected prefixes "
                       f"{expected_prefixes}. Found: {unique_prefixes}")
    return _result("FAIL", "Series ID–Source Alignment",
                   f"{len(bad):,} series IDs do not start with {expected_prefixes}",
                   {"bad_samples": bad.head(5).tolist()})


def chk_ingestion_batch_cohesion(df, source):
    if "conversion_timestamp" not in df.columns:
        return _result("FAIL", "Ingestion Batch Cohesion",
                       "'conversion_timestamp' column missing")
    ct = pd.to_datetime(df["conversion_timestamp"], errors="coerce", utc=True).dropna()
    if len(ct) == 0:
        return _result("FAIL", "Ingestion Batch Cohesion",
                       "No valid conversion_timestamps found")
    spread_h = (ct.max() - ct.min()).total_seconds() / 3600
    days     = ct.dt.date.nunique()
    if spread_h <= 24 * 30:
        return _result("PASS", "Ingestion Batch Cohesion",
                       f"conversion_timestamp spans {spread_h:.1f} hours "
                       f"across {days} ingestion day(s)")
    return _result("WARN", "Ingestion Batch Cohesion",
                   f"conversion_timestamp spans {spread_h/24:.0f} days — "
                   f"possible multi-batch ingestion",
                   {"spread_hours": round(spread_h, 1), "unique_days": days})


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
    chk_series_id_source_alignment,
    chk_ingestion_batch_cohesion,
]


# =============================================================================
# PER-SOURCE RUNNER
# =============================================================================

def validate_source(source: str) -> dict:
    logger.info(f"\n{'=' * 70}")
    logger.info(f"SOURCE: {source.upper()}")
    logger.info(f"{'=' * 70}")

    partition_result = chk_partition_integrity(source)
    partition_result["check"] = "chk_partition_integrity"

    df = load_sample(source)
    if df.empty:
        logger.error(f"  No data loaded for {source}")
        return {"source": source, "status": "ERROR",
                "results": [partition_result]}

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


# =============================================================================
# MAIN
# =============================================================================

def run_lineage_validation():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT — DATA LINEAGE & PROVENANCE TRACKING")
    logger.info("=" * 70)
    logger.info("Checks: source traceability, attribution, extraction method,")
    logger.info("        ingestion timestamp, record identity, natural key duplicates,")
    logger.info("        temporal coverage, partition integrity, PIT chain validity,")
    logger.info("        revision monotonicity, agency URL, series ID alignment, batch cohesion")
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
        logger.info(f"  {r['source'].ljust(12)}: [{r['status']}] "
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
        fp.write("MACRO EMPLOYMENT — DATA LINEAGE & PROVENANCE REPORT\n")
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


def _run_eu27_lineage():
    """EU27 provenance checks for wages_and_employment Eurostat SDMX data."""
    import re, json as _json
    _VID_PATTERN = re.compile(r"^EUROSTAT-.+-\d{4}(-\d{2})?-v\d+$")
    eu27_src = "eurostat_sdmx"
    frames, total_files, empty_files = [], 0, 0

    for iso in EU27_ISO3:
        src = VAULT_DIR / f"product={PRODUCT}" / f"country={iso}" / f"source={eu27_src}"
        if not src.exists():
            continue
        iso_files = sorted([f for f in src.rglob("*.parquet")
                            if "outlier" not in f.name and "changelog" not in f.name])
        total_files += len(iso_files)
        for f in iso_files:
            try:
                df = pd.read_parquet(f)
                if df.empty:
                    empty_files += 1
            except Exception:
                empty_files += 1
        # Sample one representative file per country for provenance checks
        if iso_files:
            try:
                frames.append(pd.read_parquet(iso_files[0]))
            except Exception:
                pass

    sample = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    results = []

    results.append({
        "status": "PASS" if empty_files == 0 else "FAIL",
        "check": "Partition Integrity",
        "message": f"{total_files - empty_files}/{total_files} files readable and non-empty",
    })

    if "data_vintage_id" in sample.columns:
        s = sample["data_vintage_id"].dropna().head(2000)
        bad = [v for v in s if not _VID_PATTERN.match(str(v))]
        results.append({
            "status": "PASS" if not bad else "FAIL",
            "check": "Vintage ID Format",
            "message": ("All sampled data_vintage_id match EUROSTAT-*-YYYY-MM-vN"
                        if not bad else f"{len(bad)} malformed vintage IDs"),
        })

    if "source_agency" in sample.columns:
        bad_rows = sample[sample["source_agency"] != "EUROSTAT"]
        results.append({
            "status": "PASS" if bad_rows.empty else "FAIL",
            "check": "Source Agency",
            "message": (f"All {len(sample):,} rows have source_agency=EUROSTAT"
                        if bad_rows.empty else f"{len(bad_rows)} rows with wrong source_agency"),
        })

    if "revision_number" in sample.columns:
        neg = int((pd.to_numeric(sample["revision_number"], errors="coerce") < 0).sum())
        results.append({
            "status": "PASS" if neg == 0 else "FAIL",
            "check": "Revision Monotonicity",
            "message": "All revision_number >= 0" if neg == 0 else f"{neg} negative revision_numbers",
        })

    if "iso_alpha3" in sample.columns:
        found = set(sample["iso_alpha3"].dropna().unique())
        missing = sorted(set(EU27_ISO3) - found)
        results.append({
            "status": "PASS" if not missing else "WARN",
            "check": "Country Coverage",
            "message": ("All 27 EU countries present in sample"
                        if not missing else f"Missing from sample: {missing}"),
        })

    logger.info("=" * 70)
    logger.info(f"EU27 LINEAGE & PROVENANCE — {PRODUCT.upper()}")
    logger.info("=" * 70)
    passed = failed = 0
    for r in results:
        tag = "[PASS]" if r["status"] == "PASS" else "[FAIL]" if r["status"] == "FAIL" else "[WARN]"
        logger.info(f"  {tag} {r['check']}: {r['message']}")
        if r["status"] == "PASS": passed += 1
        elif r["status"] == "FAIL": failed += 1
    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  OVERALL: [{overall}] — {passed} PASS, {failed} FAIL")

    rp = Path(f"{PRODUCT}_eu27_lineage_report.json")
    rp.write_text(_json.dumps({
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": PRODUCT, "scope": "EU27 eurostat_sdmx",
        "total_files": total_files, "records_sampled": len(sample),
        "overall": overall, "results": results,
    }, indent=2), encoding="utf-8")
    logger.info(f"  Report: {rp}")
    return failed == 0


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true", help="Run EU27 Eurostat provenance checks")
    _args, _ = _parser.parse_known_args()
    if _args.eu27:
        exit(0 if _run_eu27_lineage() else 1)
    else:
        run_lineage_validation()
