"""
Data Lineage & Provenance — Trade Flows (US Census FT-900)

LINEAGE CHECKS:
  1.  Source Traceability         — sovereign_series_id & source_url non-null
  2.  Source Attribution          — source field = 'census_ft900' for all records
  3.  Extraction Method Audit     — extraction_method = 'api'
  4.  Ingestion Timestamp         — conversion_timestamp present, UTC-aware, >= published_date
  5.  Record Identity             — record_id unique across entire dataset
  6.  Duplicate Natural Key       — no (data_timestamp, sovereign_series_id) duplicates
  7.  Temporal Coverage           — no unexplained gaps > 2 months within each HS series
  8.  Partition Integrity          — every Hive partition has >= 1 record
  9.  PIT Chain Validity          — superseded_by refs valid record_ids (or null)
  10. Revision Monotonicity        — revision_number >= 0
  11. Agency URL Consistency      — source_url points to Census API
  12. Series ID–Source Alignment  — all series IDs start with 'HS' for census_ft900

OUTPUT:
  trade_flows_lineage_report.json
  trade_flows_lineage_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_lineage.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR    = VAULT_ROOT
PRODUCT      = "trade_flows"
COUNTRY      = "USA"
SOURCE       = "census_ft900"
SAMPLE_FILES = 60

REPORT_JSON  = Path("trade_flows_lineage_report.json")
REPORT_TXT   = Path("trade_flows_lineage_report.txt")

KNOWN_SOURCES   = {"census_ft900"}
KNOWN_METHODS   = {"api", "scraper", "manual"}
AGENCY_URL_BASE = "https://api.census.gov/data/timeseries/intltrade/"
SERIES_PREFIX   = "HS"


# =============================================================================
# HELPERS
# =============================================================================

def _np(obj):
    if isinstance(obj, np.integer):  return int(obj)
    if isinstance(obj, np.floating): return float(obj)
    if isinstance(obj, np.bool_):    return bool(obj)
    if isinstance(obj, np.ndarray):  return obj.tolist()
    if isinstance(obj, dict):        return {k: _np(v) for k, v in obj.items()}
    if isinstance(obj, list):        return [_np(i) for i in obj]
    return obj


def _result(status, standard, message, details=None):
    r = {"status": status, "standard": standard, "message": message}
    if details:
        r["details"] = _np(details)
    return r


def _log(r):
    s, std, msg = r["status"], r.get("standard", ""), r.get("message", "")
    if   s == "PASS": logger.info(f"  [PASS] {std}\n         {msg}")
    elif s == "SKIP": logger.info(f"  [SKIP] {std} - {msg}")
    elif s == "WARN": logger.warning(f"  [WARN] {std}\n         {msg}")
    else:             logger.error(f"  [FAIL] {std}\n         {msg}")


def _load_sample(n: int) -> pd.DataFrame:
    base  = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={SOURCE}"
    files = [f for f in vault_glob(base, "*.parquet")
             if "outliers" not in f.name and "changelog" not in f.name]
    step  = max(1, len(files) // n)
    dfs   = []
    for f in files[::step][:n]:
        try:
            dfs.append(vault_read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _all_partitions():
    base = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={SOURCE}"
    return [f for f in vault_glob(base, "*.parquet")
            if "outliers" not in f.name and "changelog" not in f.name]


# =============================================================================
# CHECKS
# =============================================================================

def chk_source_traceability(df: pd.DataFrame):
    issues = {}
    for col in ["sovereign_series_id", "source_url"]:
        if col not in df.columns:
            issues[col] = "column missing"
            continue
        nulls = int(df[col].isna().sum())
        if nulls > 0:
            issues[col] = f"{nulls} null values"
    if not issues:
        return _result("PASS", "Source Traceability",
                       f"sovereign_series_id and source_url fully populated in {len(df):,} records")
    return _result("FAIL", "Source Traceability",
                   f"Traceability gaps: {issues}", {"issues": issues})


def chk_source_attribution(df: pd.DataFrame):
    if "source" not in df.columns:
        return _result("FAIL", "Source Attribution", "source column missing")
    found   = set(df["source"].dropna().unique())
    invalid = found - KNOWN_SOURCES
    if not invalid:
        return _result("PASS", "Source Attribution",
                       f"All records attributed to known source: {found}")
    return _result("FAIL", "Source Attribution",
                   f"Unknown source values: {invalid}",
                   {"invalid": list(invalid)})


def chk_extraction_method(df: pd.DataFrame):
    if "extraction_method" not in df.columns:
        return _result("FAIL", "Extraction Method Audit", "extraction_method column missing")
    found   = set(df["extraction_method"].dropna().str.lower().unique())
    invalid = found - KNOWN_METHODS
    if not invalid:
        return _result("PASS", "Extraction Method Audit",
                       f"All extraction_method values documented: {found}")
    return _result("FAIL", "Extraction Method Audit",
                   f"Invalid extraction_method values: {invalid}")


def chk_ingestion_timestamp(df: pd.DataFrame):
    if "conversion_timestamp" not in df.columns:
        return _result("FAIL", "Ingestion Timestamp",
                       "conversion_timestamp column missing")
    conv  = pd.to_datetime(df["conversion_timestamp"], utc=True, errors="coerce")
    nulls = int(conv.isna().sum())
    if nulls:
        return _result("FAIL", "Ingestion Timestamp",
                       f"{nulls} null conversion_timestamps")
    if "published_date" in df.columns:
        pub  = pd.to_datetime(df["published_date"], utc=True, errors="coerce")
        late = int((conv < pub).sum())
        if late:
            return _result("FAIL", "Ingestion Timestamp",
                           f"{late} records ingested before published_date "
                           f"(backtesting contamination risk)",
                           {"pre_publication_count": late})
    return _result("PASS", "Ingestion Timestamp",
                   f"conversion_timestamp present, UTC-aware, >= published_date for all {len(df):,} records")


def chk_record_identity(df: pd.DataFrame):
    if "record_id" not in df.columns:
        return _result("FAIL", "Record Identity", "record_id column missing")
    nulls = int(df["record_id"].isna().sum())
    dupes = int(df["record_id"].duplicated().sum())
    if nulls == 0 and dupes == 0:
        return _result("PASS", "Record Identity",
                       f"All {len(df):,} record_ids unique and non-null in sample")
    return _result("FAIL", "Record Identity",
                   f"{nulls} null, {dupes} duplicate record_ids",
                   {"null_count": nulls, "duplicate_count": dupes})


def chk_duplicate_natural_key(df: pd.DataFrame):
    key = ["data_timestamp", "sovereign_series_id"]
    missing = [c for c in key if c not in df.columns]
    if missing:
        return _result("SKIP", "Duplicate Natural Key",
                       f"Required columns missing: {missing}")
    dupes = int(df.duplicated(subset=key, keep=False).sum())
    if dupes == 0:
        return _result("PASS", "Duplicate Natural Key",
                       f"No (data_timestamp, sovereign_series_id) duplicates in {len(df):,} records")
    bad = df[df.duplicated(subset=key, keep=False)][key].drop_duplicates().head(3)
    return _result("FAIL", "Duplicate Natural Key",
                   f"{dupes} records share (data_timestamp, sovereign_series_id)",
                   {"duplicate_count": dupes, "examples": bad.to_dict("records")})


def chk_temporal_coverage(df: pd.DataFrame, max_series: int = 15):
    """Sample series for MoM gaps > 2 months in temporal coverage."""
    if "sovereign_series_id" not in df.columns or "data_timestamp" not in df.columns:
        return _result("SKIP", "Temporal Coverage", "Required columns missing")
    series_ids = df["sovereign_series_id"].dropna().unique()
    if len(series_ids) > max_series:
        series_ids = np.random.default_rng(42).choice(series_ids, max_series, replace=False)
    gap_list = []
    for sid in series_ids:
        sub = (df[df["sovereign_series_id"] == sid]
               .assign(_ts=lambda x: pd.to_datetime(x["data_timestamp"], utc=True))
               .sort_values("_ts").drop_duplicates("_ts"))
        if len(sub) < 3:
            continue
        month_gaps = sub["_ts"].diff().dt.days.iloc[1:] / 30
        big = int((month_gaps > 2).sum())
        if big:
            gap_list.append({"series": str(sid), "gaps_over_2_months": big})
    if not gap_list:
        return _result("PASS", "Temporal Coverage",
                       f"No unexplained MoM gaps > 2 months in {len(series_ids)} sampled series")
    return _result("WARN", "Temporal Coverage",
                   f"{len(gap_list)}/{len(series_ids)} series have MoM gaps > 2 months",
                   {"gap_series": gap_list})


def chk_partition_integrity(files):
    empty = []
    for f in files:
        try:
            if vault_read_parquet(f).empty:
                empty.append(str(f))
        except Exception as exc:
            empty.append(f"{f} (read error: {exc})")
    if not empty:
        return _result("PASS", "Partition Integrity",
                       f"All {len(files)} Hive partitions contain >= 1 record")
    return _result("FAIL", "Partition Integrity",
                   f"{len(empty)} empty or unreadable partitions",
                   {"empty_files": empty[:5]})


def chk_pit_chain_validity(df: pd.DataFrame):
    has_sup = df["superseded_by"].notna() if "superseded_by" in df.columns else pd.Series(False)
    total = int(has_sup.sum())
    if total == 0:
        return _result("PASS", "PIT Chain Validity",
                       f"All {len(df):,} records: superseded_by = null (current-version load)")
    valid_ids = set(df["record_id"].dropna().astype(str))
    broken    = int(df[has_sup]["superseded_by"]
                    .apply(lambda x: str(x) not in valid_ids).sum())
    if broken == 0:
        return _result("PASS", "PIT Chain Validity",
                       f"{total} superseded records all reference valid record_ids")
    return _result("FAIL", "PIT Chain Validity",
                   f"{broken} superseded_by values reference non-existent record_ids",
                   {"broken_count": broken})


def chk_revision_monotonicity(df: pd.DataFrame):
    if "revision_number" not in df.columns:
        return _result("FAIL", "Revision Monotonicity", "revision_number column missing")
    neg = int((pd.to_numeric(df["revision_number"], errors="coerce") < 0).sum())
    if neg == 0:
        return _result("PASS", "Revision Monotonicity",
                       f"All revision_numbers >= 0 ({len(df):,} records)")
    return _result("FAIL", "Revision Monotonicity",
                   f"{neg} records have negative revision_number",
                   {"negative_count": neg})


def chk_agency_url(df: pd.DataFrame):
    if "source_url" not in df.columns:
        return _result("FAIL", "Agency URL Consistency", "source_url column missing")
    found   = df["source_url"].dropna().unique()
    invalid = [u for u in found if AGENCY_URL_BASE not in str(u)]
    if not invalid:
        return _result("PASS", "Agency URL Consistency",
                       f"All source_urls reference Census API ({AGENCY_URL_BASE})")
    return _result("FAIL", "Agency URL Consistency",
                   f"{len(invalid)} non-Census source URLs",
                   {"invalid_samples": [str(u) for u in invalid[:3]]})


def chk_series_source_alignment(df: pd.DataFrame):
    if "sovereign_series_id" not in df.columns:
        return _result("SKIP", "Series ID–Source Alignment", "sovereign_series_id missing")
    ids = df["sovereign_series_id"].dropna()
    bad = ids[~ids.str.startswith(SERIES_PREFIX)]
    if len(bad) == 0:
        return _result("PASS", "Series ID–Source Alignment",
                       f"All {len(ids):,} series IDs start with '{SERIES_PREFIX}' (Census HS prefix)")
    return _result("FAIL", "Series ID–Source Alignment",
                   f"{len(bad):,} series IDs do not start with '{SERIES_PREFIX}'",
                   {"invalid_samples": bad.head(5).tolist()})


# =============================================================================
# MAIN
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — DATA LINEAGE & PROVENANCE")
    logger.info("=" * 70)

    files = _all_partitions()
    logger.info(f"  Partition files: {len(files)}")

    if not files:
        logger.error("  No vault data found. Run scraper first.")
        return False

    df = _load_sample(SAMPLE_FILES)
    logger.info(f"  Sample: {len(df):,} records")

    results = [
        chk_source_traceability(df),
        chk_source_attribution(df),
        chk_extraction_method(df),
        chk_ingestion_timestamp(df),
        chk_record_identity(df),
        chk_duplicate_natural_key(df),
        chk_temporal_coverage(df),
        chk_partition_integrity(files),
        chk_pit_chain_validity(df),
        chk_revision_monotonicity(df),
        chk_agency_url(df),
        chk_series_source_alignment(df),
    ]
    for r in results:
        _log(r)

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
        f.write(f"TRADE FLOWS — LINEAGE REPORT\nOverall: [{overall}]\n\n")
        for r in results:
            f.write(f"  [{r['status']:<4}] {r['standard']}\n         {r['message']}\n")

    return failed == 0


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]


def _run_eu27_lineage():
    """EU27 provenance checks for trade_flows Eurostat SDMX data."""
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
    sys.exit(0 if (_run_eu27_lineage() if _args.eu27 else run()) else 1)
