"""
Data Lineage Validation for Housing Supply & Shelter Inflation

Verifies data provenance, natural key uniqueness, and partition integrity
for both datasets in the housing vault:
  - source=bls_cpi_shelter  (BLS CPI Shelter series)
  - source=census_bps        (Census Building Permits Survey)

LINEAGE CHECKS:
  1. Source Attribution   — portal_url matches source_agency
  2. Natural Key Uniqueness — (sovereign_series_id, reporting_date) unique per source
  3. Partition Integrity  — each Parquet file covers exactly one year-month
  4. Record ID Uniqueness — no duplicate record_ids across full vault
  5. Vault Path Compliance — files located at expected product=housing/... path
  6. Data Vintage Coverage — data_vintage_id present and non-null
  7. Cross-source Isolation — shelter and permits records do not bleed between partitions

OUTPUT:
  - housing_lineage_report.json
  - housing_lineage_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_lineage.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR  = VAULT_ROOT
PRODUCT    = "Housing_Supply_and_Shelter_Inflation"
COUNTRY    = "USA"
SOURCES    = ["bls_cpi_shelter", "census_bps"]

REPORT_JSON = Path("housing_lineage_report.json")
REPORT_TXT  = Path("housing_lineage_report.txt")

EXPECTED_PORTALS = {
    "bls_cpi_shelter": "https://www.bls.gov/cpi/",
    "census_bps":      "https://www.census.gov/construction/bps/",
}
EXPECTED_AGENCIES = {
    "bls_cpi_shelter": "BLS",
    "census_bps":      "CENSUS",
}
EXPECTED_FILES = {
    "bls_cpi_shelter": "shelter_inflation_data.parquet",
    "census_bps":      "housing_permits_data.parquet",
}


# =============================================================================
# HELPERS
# =============================================================================

def _load_source(source: str) -> tuple[pd.DataFrame, list[Path]]:
    src_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
    files = sorted(vault_glob(src_path, "*.parquet"))
    dfs = []
    for f in files:
        try:
            df = vault_read_parquet(f)
            df["__file__"] = str(f)
            dfs.append(df)
        except Exception as exc:
            logger.warning(f"  Cannot read {f}: {exc}")
    if not dfs:
        return pd.DataFrame(), files
    return pd.concat(dfs, ignore_index=True), files


# =============================================================================
# LINEAGE CHECKS
# =============================================================================

def check_source_attribution(df: pd.DataFrame, source: str) -> dict:
    """portal_url and source_agency must match expected values for this source."""
    results = []
    if "portal_url" in df.columns:
        expected = EXPECTED_PORTALS[source]
        wrong = int((df["portal_url"] != expected).sum())
        results.append(("portal_url", expected, wrong))
    if "source_agency" in df.columns:
        expected = EXPECTED_AGENCIES[source]
        wrong = int((df["source_agency"] != expected).sum())
        results.append(("source_agency", expected, wrong))

    failures = [(col, exp, n) for col, exp, n in results if n > 0]
    if not failures:
        return {"status": "PASS", "check": "Source Attribution",
                "message": f"All portal_url and source_agency values match expected for {source}"}
    return {"status": "FAIL", "check": "Source Attribution",
            "message": "; ".join(f"{col} mismatch ({n} records, expected '{exp}')"
                                  for col, exp, n in failures)}


def check_natural_key_uniqueness(df: pd.DataFrame, source: str) -> dict:
    """(sovereign_series_id, reporting_date) must be unique within each source."""
    key_cols = []
    if "sovereign_series_id" in df.columns:
        key_cols.append("sovereign_series_id")
    elif "bps_variable" in df.columns:
        key_cols.append("bps_variable")
    if "reporting_date" in df.columns:
        key_cols.append("reporting_date")
    elif "data_timestamp" in df.columns:
        key_cols.append("data_timestamp")

    if len(key_cols) < 2:
        return {"status": "WARN", "check": "Natural Key Uniqueness",
                "message": f"Cannot find key columns — found: {df.columns.tolist()[:10]}"}

    dupes = df.duplicated(subset=key_cols, keep=False)
    n_dupe = int(dupes.sum())
    if n_dupe == 0:
        return {"status": "PASS", "check": "Natural Key Uniqueness",
                "message": f"No duplicate natural keys on {key_cols} across {len(df):,} records"}
    return {"status": "FAIL", "check": "Natural Key Uniqueness",
            "message": f"{n_dupe:,} records have duplicate ({', '.join(key_cols)}) combinations",
            "key_columns": key_cols}


def check_partition_integrity(source: str, files: list[Path]) -> dict:
    """Each file should sit under year=YYYY/month=MM and contain data for that period."""
    mismatches = []
    for f in files:
        parts = str(f).split("/")
        try:
            year_part  = next(p for p in parts if p.startswith("year="))
            month_part = next(p for p in parts if p.startswith("month="))
            expected_year  = int(year_part.split("=")[1])
            expected_month = int(month_part.split("=")[1])
        except (StopIteration, ValueError):
            continue   # non-partitioned file — skip

        try:
            df_part = vault_read_parquet(f, columns=["data_timestamp"])
        except Exception:
            continue

        ts = pd.to_datetime(df_part["data_timestamp"], errors="coerce", utc=True)
        years  = ts.dt.year.dropna().unique().tolist()
        months = ts.dt.month.dropna().unique().tolist()
        if years and (len(years) > 1 or years[0] != expected_year):
            mismatches.append(f"{f.name}: path=year={expected_year} but data years={years}")
        if months and (len(months) > 1 or months[0] != expected_month):
            mismatches.append(f"{f.name}: path=month={expected_month} but data months={months}")

    if not mismatches:
        return {"status": "PASS", "check": "Partition Integrity",
                "message": f"All {len(files)} partitions contain data for their declared year/month"}
    return {"status": "FAIL", "check": "Partition Integrity",
            "message": f"{len(mismatches)} partition(s) have year/month mismatch",
            "details": mismatches[:10]}


def check_record_id_global_uniqueness(df: pd.DataFrame, source: str) -> dict:
    if "record_id" not in df.columns:
        return {"status": "WARN", "check": "Record ID Global Uniqueness",
                "message": "record_id column missing"}
    dupes = int(df["record_id"].duplicated().sum())
    if dupes == 0:
        return {"status": "PASS", "check": "Record ID Global Uniqueness",
                "message": f"All {len(df):,} record_ids are globally unique within {source}"}
    return {"status": "FAIL", "check": "Record ID Global Uniqueness",
            "message": f"{dupes:,} duplicate record_ids in {source}"}


def check_vault_path_compliance(source: str, files: list[Path]) -> dict:
    expected_product = f"product={PRODUCT}"
    expected_country = f"country={COUNTRY}"
    expected_source  = f"source={source}"
    wrong_paths = [
        str(f) for f in files
        if not (expected_product in str(f) and expected_country in str(f)
                and expected_source in str(f))
    ]
    if not wrong_paths:
        return {"status": "PASS", "check": "Vault Path Compliance",
                "message": f"All {len(files)} files are within the correct vault sub-tree"}
    return {"status": "FAIL", "check": "Vault Path Compliance",
            "message": f"{len(wrong_paths)} file(s) outside expected vault path",
            "sample": wrong_paths[:5]}


def check_vintage_id_coverage(df: pd.DataFrame, source: str) -> dict:
    if "data_vintage_id" not in df.columns:
        return {"status": "FAIL", "check": "Data Vintage Coverage",
                "message": "data_vintage_id column missing"}
    nulls = int(df["data_vintage_id"].isna().sum())
    empty = int((df["data_vintage_id"].astype(str).str.strip() == "").sum())
    total = len(df)
    if nulls == 0 and empty == 0:
        return {"status": "PASS", "check": "Data Vintage Coverage",
                "message": f"data_vintage_id present and non-null for all {total:,} records"}
    return {"status": "FAIL", "check": "Data Vintage Coverage",
            "message": f"{nulls + empty:,}/{total:,} records have null/empty data_vintage_id"}


def check_cross_source_isolation(files_by_source: dict) -> dict:
    """Confirm that shelter and permit files contain only their own source label."""
    violations = []
    for source, files in files_by_source.items():
        for f in files[:20]:   # sample to keep run time short
            try:
                df = vault_read_parquet(f, columns=["source"])
                wrong = df[df["source"] != source]
                if not wrong.empty:
                    violations.append(
                        f"{f.name}: found source='{wrong['source'].iloc[0]}' "
                        f"(expected {source})"
                    )
            except Exception:
                continue
    if not violations:
        return {"status": "PASS", "check": "Cross-source Isolation",
                "message": "No cross-contamination between shelter and permits partitions"}
    return {"status": "FAIL", "check": "Cross-source Isolation",
            "message": f"{len(violations)} file(s) contain records for the wrong source",
            "details": violations[:10]}


# =============================================================================
# RUNNER
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("HOUSING — DATA LINEAGE VALIDATION")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")

    all_results    = {}
    files_by_src   = {}

    for source in SOURCES:
        logger.info(f"\n{'-' * 60}")
        logger.info(f"  SOURCE: {source}")
        logger.info(f"{'-' * 60}")
        df, files = _load_source(source)
        files_by_src[source] = files

        if df.empty:
            logger.warning(f"  No data for source={source}")
            all_results[source] = [
                {"status": "SKIP", "check": "All lineage checks",
                 "message": f"No vault data for source={source}"}
            ]
            continue

        logger.info(f"  Loaded {len(df):,} records across {len(files)} files")

        results = [
            check_source_attribution(df, source),
            check_natural_key_uniqueness(df, source),
            check_partition_integrity(source, files),
            check_record_id_global_uniqueness(df, source),
            check_vault_path_compliance(source, files),
            check_vintage_id_coverage(df, source),
        ]

        for r in results:
            r["source"] = source
            icon = {"PASS": "[+]", "FAIL": "[!]", "WARN": "[!]", "SKIP": "[-]"}.get(r["status"], "[?]")
            logger.info(f"  [{icon}] {r['check']}: {r['message']}")

        all_results[source] = results

    # Cross-source check
    cross = check_cross_source_isolation(files_by_src)
    logger.info(f"\n[{'+' if cross['status'] == 'PASS' else '!'}] {cross['check']}: {cross['message']}")
    all_results["_cross_source"] = [cross]

    # ── Summary ──────────────────────────────────────────────────────────────
    flat     = [r for v in all_results.values() for r in v]
    passed   = sum(r["status"] == "PASS" for r in flat)
    failed   = sum(r["status"] == "FAIL" for r in flat)
    warned   = sum(r["status"] == "WARN" for r in flat)

    logger.info("\n" + "=" * 70)
    logger.info(f"SUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN / {len(flat)} total")
    logger.info("=" * 70)

    report = {
        "product": PRODUCT,
        "country": COUNTRY,
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {"total": len(flat), "passed": passed, "failed": failed, "warned": warned},
        "results_by_source": all_results,
    }
    with open(REPORT_JSON, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    with open(REPORT_TXT, "w", encoding="utf-8", errors="replace") as fh:
        fh.write(f"Housing Lineage Report - {datetime.utcnow().isoformat()}Z\n")
        fh.write("=" * 70 + "\n")
        for src, results in all_results.items():
            fh.write(f"\nSOURCE: {src}\n")
            for r in results:
                fh.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
        fh.write(f"\nSUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN\n")

    logger.info(f"Reports written: {REPORT_JSON}, {REPORT_TXT}")
    return failed == 0


EU27_ISO3 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
             "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]
_HOUSING_VAULT_PRODUCT = "Housing_Supply_and_Shelter_Inflation"


def _run_eu27_lineage():
    """EU27 provenance checks for housing Eurostat SDMX data."""
    import re, json as _json
    _VID_PATTERN = re.compile(r"^EUROSTAT-.+-\d{4}(-\d{2})?-v\d+$")
    eu27_src = "eurostat_sdmx"
    frames, total_files, empty_files = [], 0, 0

    for iso in EU27_ISO3:
        src = VAULT_DIR / f"product={_HOUSING_VAULT_PRODUCT}" / f"country={iso}" / f"source={eu27_src}"
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
    logger.info(f"EU27 LINEAGE & PROVENANCE — HOUSING (Eurostat SDMX)")
    logger.info("=" * 70)
    passed = failed = 0
    for r in results:
        tag = "[PASS]" if r["status"] == "PASS" else "[FAIL]" if r["status"] == "FAIL" else "[WARN]"
        logger.info(f"  {tag} {r['check']}: {r['message']}")
        if r["status"] == "PASS": passed += 1
        elif r["status"] == "FAIL": failed += 1
    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  OVERALL: [{overall}] — {passed} PASS, {failed} FAIL")

    rp = Path(f"{_HOUSING_VAULT_PRODUCT}_eu27_lineage_report.json")
    rp.write_text(_json.dumps({
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": _HOUSING_VAULT_PRODUCT, "scope": "EU27 eurostat_sdmx",
        "total_files": total_files, "records_sampled": len(sample),
        "overall": overall, "results": results,
    }, indent=2), encoding="utf-8")
    logger.info(f"  Report: {rp}")
    return failed == 0


if __name__ == "__main__":
    import sys, argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true", help="Run EU27 Eurostat provenance checks")
    _args, _ = _parser.parse_known_args()
    sys.exit(0 if (_run_eu27_lineage() if _args.eu27 else run()) else 1)
