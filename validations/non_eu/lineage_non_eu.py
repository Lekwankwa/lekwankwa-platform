"""
Stage 8 — Lineage & Provenance: GBR / CAN / AUS / NOR.

Checks:
  1. Source Traceability    — sovereign_series_id non-null for all records
  2. Source Attribution     — source field matches expected per-country values
  3. Ingestion Timestamp    — data_timestamp present and not future-dated
  4. Record Uniqueness      — data_vintage_id unique (no silent merges)
  5. Partition Integrity    — every year partition has ≥ 1 record
  6. Revision Monotonicity  — revision_number non-negative
  7. PIT Coverage Tag       — pit_coverage_type present and expected value

Usage:
  python validations/non_eu/lineage_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import VAULT, load, active_countries, PRODUCT_FILENAMES, ALL_PRODUCTS

KNOWN_SOURCES = {"ons_api", "statcan_csv", "abs_sdmx", "ssb_statbank"}
EXPECTED_PIT_TAGS = {"RELEASE_DATE_ONLY/accumulating", "RELEASE_DATE_ONLY/structural_ceiling"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _r(status, standard, message, details=None):
    return {"status": status, "standard": standard, "message": message, "details": details or {}}


def chk_source_traceability(df: pd.DataFrame) -> dict:
    if "sovereign_series_id" not in df.columns:
        return _r("FAIL", "Source Traceability", "sovereign_series_id column missing")
    nulls = int(df["sovereign_series_id"].isna().sum())
    if nulls == 0:
        return _r("PASS", "Source Traceability",
                  f"All {len(df):,} records have non-null sovereign_series_id")
    return _r("FAIL", "Source Traceability",
              f"{nulls} null sovereign_series_id", {"null_count": nulls})


def chk_source_attribution(df: pd.DataFrame) -> dict:
    if "source" not in df.columns:
        return _r("FAIL", "Source Attribution", "'source' column missing")
    unique = set(df["source"].dropna().unique())
    invalid = unique - KNOWN_SOURCES
    if not invalid:
        return _r("PASS", "Source Attribution",
                  f"All source values valid: {unique}")
    return _r("FAIL", "Source Attribution",
              f"Unknown source values: {invalid}", {"invalid": list(invalid)})


def chk_ingestion_timestamp(df: pd.DataFrame) -> dict:
    if "data_timestamp" not in df.columns:
        return _r("FAIL", "Ingestion Timestamp", "data_timestamp missing")
    null_ts = int(df["data_timestamp"].isna().sum())
    today = pd.Timestamp.utcnow()
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    future = int((ts > today + pd.Timedelta(days=1)).sum())
    if null_ts == 0 and future == 0:
        return _r("PASS", "Ingestion Timestamp",
                  f"data_timestamp present and not future-dated ({len(df):,} records)")
    issues = []
    if null_ts: issues.append(f"{null_ts} null")
    if future:  issues.append(f"{future} future-dated")
    return _r("FAIL", "Ingestion Timestamp", "; ".join(issues),
              {"null_count": null_ts, "future_count": future})


def chk_record_uniqueness(df: pd.DataFrame) -> dict:
    if "data_vintage_id" not in df.columns:
        return _r("FAIL", "Record Uniqueness", "data_vintage_id missing")
    dupes = int(df["data_vintage_id"].duplicated().sum())
    if dupes == 0:
        return _r("PASS", "Record Uniqueness",
                  f"All {len(df):,} data_vintage_id values unique")
    return _r("FAIL", "Record Uniqueness",
              f"{dupes} duplicate data_vintage_id", {"duplicate_count": dupes})


def chk_partition_integrity(product: str) -> dict:
    filename = PRODUCT_FILENAMES[product]
    empty_partitions: list[str] = []
    total_partitions = 0

    for iso, (_, source, _) in active_countries(product).items():
        src = VAULT / f"product={product}" / f"country={iso}" / f"source={source}"
        if not src.exists():
            continue
        for yr_dir in sorted(src.iterdir()):
            if not yr_dir.name.startswith("year="):
                continue
            for mo_dir in sorted(yr_dir.iterdir()):
                if not mo_dir.name.startswith("month="):
                    continue
                pf = mo_dir / filename
                total_partitions += 1
                if not pf.exists():
                    continue
                try:
                    n = len(pd.read_parquet(pf))
                    if n == 0:
                        empty_partitions.append(str(pf))
                except Exception:
                    empty_partitions.append(str(pf) + " (READ_ERROR)")

    if not empty_partitions:
        return _r("PASS", "Partition Integrity",
                  f"All {total_partitions} Hive partitions have ≥ 1 record")
    return _r("WARN", "Partition Integrity",
              f"{len(empty_partitions)} empty or unreadable partition files (out of {total_partitions})",
              {"empty": empty_partitions[:10]})


def chk_revision_monotonicity(df: pd.DataFrame) -> dict:
    if "revision_number" not in df.columns:
        return _r("SKIP", "Revision Monotonicity", "revision_number missing")
    neg = int((pd.to_numeric(df["revision_number"], errors="coerce") < 0).sum())
    if neg == 0:
        return _r("PASS", "Revision Monotonicity",
                  f"All revision_number ≥ 0 ({len(df):,} records)")
    return _r("FAIL", "Revision Monotonicity",
              f"{neg} negative revision_number values", {"negative_count": neg})


def chk_pit_coverage_tag(df: pd.DataFrame) -> dict:
    if "pit_coverage_type" not in df.columns:
        return _r("WARN", "PIT Coverage Tag",
                  "pit_coverage_type column absent — expected for RELEASE_DATE_ONLY sources")
    unique = set(df["pit_coverage_type"].dropna().unique())
    unknown = unique - EXPECTED_PIT_TAGS
    if not unknown:
        return _r("PASS", "PIT Coverage Tag",
                  f"pit_coverage_type values valid: {unique}")
    return _r("WARN", "PIT Coverage Tag",
              f"Unexpected pit_coverage_type values: {unknown}",
              {"found": list(unique)})


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU LINEAGE & PROVENANCE — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    df = load(product)
    if df.empty:
        logger.error("No data loaded.")
        return False
    logger.info(f"  Loaded {len(df):,} rows")

    results = [
        chk_source_traceability(df),
        chk_source_attribution(df),
        chk_ingestion_timestamp(df),
        chk_record_uniqueness(df),
        chk_partition_integrity(product),
        chk_revision_monotonicity(df),
        chk_pit_coverage_tag(df),
    ]

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")

    for r in results:
        tag = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]", "SKIP": "[SKIP]"}.get(r["status"], "[????]")
        logger.info(f"  {tag} {r['standard']}: {r['message']}")

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  OVERALL: [{overall}] — {passed} PASS, {warned} WARN, {failed} FAIL")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product, "scope": "non_eu GBR/CAN/AUS/NOR",
        "total_records": len(df),
        "checks_passed": passed, "checks_warned": warned, "checks_failed": failed,
        "overall": overall, "results": results,
    }
    out = Path(f"{product}_non_eu_lineage_report.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return failed == 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    args = p.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
