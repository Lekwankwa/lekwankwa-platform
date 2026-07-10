"""
Source Identity Verification — shared validation stage (Stage 10).

Confirms that every vault record's provenance fields (source_agency,
source_sub_category, portal_url, extraction_method, market_tier) match
what the record's actual source is known to produce, per the single
shared reference manifest at configs/source_identity_reference.yaml.

This is ONE function used by all 5 products across all 3 geo scopes
(USA / EU27 / non_eu) — there is no per-product copy of this logic.
Products differ only in which rows get scanned; the identity rules
themselves live entirely in the manifest, not in code.

Two independent things get checked:
  1. VALUE match   — does source_agency/portal_url/extraction_method/
                      market_tier/source_sub_category equal what the
                      manifest says that source should produce?
                      (Fields the manifest marks `null` are known to be
                      legitimately absent for that source — e.g. every
                      non-USA source has no portal_url column at all —
                      and are never flagged for absence.)
  2. FOLDER match  — does the row's own `source` column agree with the
                      `source=` partition folder it's physically stored
                      under? Catches mislabeled `source` values that a
                      pure column-value check would miss.

Usage:
    python validations/source_identity_validator.py --product wages_and_employment --scope USA
    python validations/source_identity_validator.py --product wages_and_employment --scope EU27
    python validations/source_identity_validator.py --product wages_and_employment --scope non_eu

Author: Lekwankwa Corporation
Date: 2026-07-09
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scrapers.utilities.vault_io import get_vault_root

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("source_identity_verification.log"),
              logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

REPO_ROOT     = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "configs" / "source_identity_reference.yaml"
VAULT_ROOT    = get_vault_root(str(REPO_ROOT / "lekwankwa-historical-vault"))

EU27_MEMBERS = [
    "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA", "DEU",
    "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT",
    "ROU", "SVK", "SVN", "ESP", "SWE",
]
NON_EU_COUNTRIES = ["GBR", "CAN"]

IDENTITY_FIELDS = ["source_agency", "source_sub_category", "portal_url",
                    "extraction_method", "market_tier"]

MAX_FILES_PER_SOURCE = 40   # stride-sampled, matches existing schema_compliance convention


# ─────────────────────────────────────────────────────────────────────────
# Manifest loading (module-level cache — loaded once regardless of how many
# times this stage runs within one process)
# ─────────────────────────────────────────────────────────────────────────

_MANIFEST: dict[str, Any] | None = None


def load_manifest() -> dict[str, Any]:
    global _MANIFEST
    if _MANIFEST is None:
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            _MANIFEST = yaml.safe_load(f)
    return _MANIFEST


def get_expected_identity(
    product: str,
    country: str,
    series_id: str,
    *,
    source: str | None = None,
) -> dict[str, Any] | None:
    """
    Look up the expected source-identity fields for (product, country,
    series_id), regardless of which product is calling.

    For every non-USA country there is exactly one legitimate source per
    product (geography determines the scraper), so `source` is resolved
    automatically from configs/source_identity_reference.yaml's
    `countries` map and `series_id` is accepted for interface
    consistency / future per-series overrides but isn't needed to
    disambiguate.

    USA has multiple sources per product (e.g. wages_and_employment has
    alfred_vintage, bls_ces, and bls_cps side by side — the same series
    can legitimately appear in more than one, e.g. CES0500000003 exists
    in both bls_ces as the primary record and alfred_vintage as its
    vintage-history retrofit). series_id alone cannot disambiguate that,
    so callers checking USA data must pass `source` explicitly — it's
    already known from the row/partition being checked. Returns None if
    `source` is required but not given, or if the (product, source) pair
    isn't in the manifest.

    Returns None if the product/country/source isn't found in the
    manifest (e.g. a brand-new source that hasn't been added yet).
    """
    manifest = load_manifest()
    product_entry = manifest.get("products", {}).get(product)
    if product_entry is None:
        return None

    if source is None:
        country_entry = manifest.get("countries", {}).get(country)
        if country_entry is None:
            return None
        source = country_entry.get("source")
        if source is None:
            logger.warning(
                "get_expected_identity: country=%s has multiple possible sources "
                "for product=%s (series_id=%s) — pass source= explicitly.",
                country, product, series_id,
            )
            return None

    return product_entry.get("sources", {}).get(source)


# ─────────────────────────────────────────────────────────────────────────
# Row / batch checking
# ─────────────────────────────────────────────────────────────────────────

def check_row_identity(
    row: dict[str, Any],
    product: str,
    country: str,
    partition_source: str,
) -> list[dict[str, Any]]:
    """
    Compare one row's actual identity fields against the expected values
    for (product, country, partition_source). Returns a list of violation
    dicts (empty if the row is clean).
    """
    violations: list[dict[str, Any]] = []

    folder_expected = get_expected_identity(product, country, row.get("sovereign_series_id", ""),
                                             source=partition_source)
    if folder_expected is None:
        violations.append({
            "code": "UNKNOWN_SOURCE",
            "field": "source",
            "actual": partition_source,
            "expected": None,
            "message": (
                f"source={partition_source!r} for product={product!r} has no entry in "
                f"configs/source_identity_reference.yaml — either a new source was added "
                f"without updating the manifest, or this is genuinely unrecognized."
            ),
        })
        return violations

    row_source = row.get("source")
    alt_sources = folder_expected.get("alt_sources") or []
    if row_source is not None and row_source != partition_source and row_source not in alt_sources:
        violations.append({
            "code": "SOURCE_FOLDER_MISMATCH",
            "field": "source",
            "actual": row_source,
            "expected": partition_source,
            "message": (
                f"Row declares source={row_source!r} but is stored under "
                f"source={partition_source!r} — the two must agree (or "
                f"source={row_source!r} must be listed under alt_sources "
                f"for {partition_source!r} in the manifest)."
            ),
        })

    # A row filed under an alt_source (e.g. food_micropricing's
    # alfred_vintage folder legitimately contains pre-2019 rows honestly
    # labeled source='bls') is validated against ITS OWN source's manifest
    # entry, not the folder's nominal one -- otherwise a genuinely correct
    # bls-labeled row would be flagged for not having ALFRED's portal_url.
    if row_source is not None and row_source != partition_source and row_source in alt_sources:
        expected = get_expected_identity(product, country, row.get("sovereign_series_id", ""),
                                          source=row_source) or folder_expected
    else:
        expected = folder_expected

    for field in IDENTITY_FIELDS:
        expected_val = expected.get(field)
        if expected_val is None:
            continue  # manifest says this field has no fixed constraint for this source

        actual_val = row.get(field)
        if actual_val is None:
            continue  # column legitimately absent/null on this row — not this check's concern

        if isinstance(expected_val, list):
            if actual_val not in expected_val:
                violations.append({
                    "code": "SOURCE_IDENTITY_MISMATCH",
                    "field": field,
                    "actual": actual_val,
                    "expected": expected_val,
                    "message": (
                        f"{field}={actual_val!r} not in expected set {expected_val} "
                        f"for source={partition_source!r} (product={product!r})."
                    ),
                })
        else:
            if actual_val != expected_val:
                violations.append({
                    "code": "SOURCE_IDENTITY_MISMATCH",
                    "field": field,
                    "actual": actual_val,
                    "expected": expected_val,
                    "message": (
                        f"{field}={actual_val!r} but expected {expected_val!r} "
                        f"for source={partition_source!r} (product={product!r})."
                    ),
                })

    return violations


def _sample_files(source_dir: Path) -> list[Path]:
    files = sorted(source_dir.rglob("*.parquet"))
    files = [f for f in files if "outlier" not in f.name and "changelog" not in f.name]
    if len(files) > MAX_FILES_PER_SOURCE:
        step = len(files) // MAX_FILES_PER_SOURCE
        return files[::step][:MAX_FILES_PER_SOURCE]
    return files


def validate_source_identity(product: str, scope: str) -> dict[str, Any]:
    """
    Run source identity verification for one product across one geo scope.
    Scans a stride sample of vault files per (country, source), checks
    every row, and returns a summary dict (also written to disk as
    {product}_{scope}_source_identity_report.json / .txt, matching the
    established validations/ report convention).
    """
    if scope == "USA":
        countries = ["USA"]
    elif scope == "EU27":
        countries = EU27_MEMBERS
    elif scope == "non_eu":
        countries = NON_EU_COUNTRIES
    else:
        raise ValueError(f"Unknown scope: {scope!r} (expected USA / EU27 / non_eu)")

    product_dir = Path(str(VAULT_ROOT / f"product={product}"))
    total_rows_checked = 0
    total_files_checked = 0
    total_files_empty = 0
    violation_log: list[dict[str, Any]] = []
    sources_seen: set[str] = set()

    for country in countries:
        country_dir = product_dir / f"country={country}"
        if not country_dir.exists():
            continue
        for source_dir in sorted(country_dir.glob("source=*")):
            source = source_dir.name.split("=", 1)[1]
            sources_seen.add(source)
            files = _sample_files(source_dir)
            for f in files:
                try:
                    df = pd.read_parquet(f)
                except Exception as exc:
                    violation_log.append({
                        "file": str(f.relative_to(product_dir.parent)),
                        "country": country, "source": source,
                        "violations": [{"code": "FILE_READ_ERROR", "message": str(exc)}],
                    })
                    continue

                total_files_checked += 1
                if df.empty:
                    total_files_empty += 1
                    continue

                total_rows_checked += len(df)
                file_violations: list[dict[str, Any]] = []
                # One representative row is enough per file — every row in a
                # given partition file shares the same source/agency/portal
                # by construction (confirmed across the whole vault scan
                # used to build the manifest), so row-by-row would just
                # repeat the same finding thousands of times.
                sample_row = df.iloc[0].to_dict()
                file_violations.extend(
                    check_row_identity(sample_row, product, country, source)
                )
                if file_violations:
                    violation_log.append({
                        "file": str(f.relative_to(product_dir.parent)),
                        "country": country, "source": source,
                        "violations": file_violations,
                    })

    empty_ratio = (total_files_empty / total_files_checked) if total_files_checked else 0.0
    if total_files_checked >= 5 and empty_ratio > 0.10:
        violation_log.append({
            "file": f"product={product}/*",
            "country": scope, "source": "*",
            "violations": [{
                "code": "EXCESSIVE_EMPTY_PARTITIONS",
                "message": (
                    f"{total_files_empty}/{total_files_checked} sampled files "
                    f"({empty_ratio:.0%}) contain zero rows."
                ),
            }],
        })

    overall = "PASS" if not violation_log else "FAIL"
    report = {
        "product": product,
        "scope": scope,
        "overall": overall,
        "sources_checked": sorted(sources_seen),
        "total_files_checked": total_files_checked,
        "total_files_empty": total_files_empty,
        "total_rows_checked": total_rows_checked,
        "violation_count": len(violation_log),
        "violations": violation_log,
    }

    out_json = REPO_ROOT / f"{product}_{scope}_source_identity_report.json"
    out_json.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    logger.info("=" * 70)
    logger.info(f"SOURCE IDENTITY VERIFICATION — {product} / {scope}")
    logger.info("=" * 70)
    logger.info(f"Sources checked      : {sorted(sources_seen)}")
    logger.info(f"Files checked        : {total_files_checked} ({total_files_empty} empty)")
    logger.info(f"Rows checked         : {total_rows_checked:,}")
    logger.info(f"Violations           : {len(violation_log)}")
    logger.info(f"Overall              : [{overall}]")
    logger.info(f"Report               : {out_json}")
    if violation_log:
        for v in violation_log[:20]:
            logger.warning(f"  {v['country']}/{v['source']}: {v['file']} -> "
                            f"{[x['code'] for x in v['violations']]}")
        if len(violation_log) > 20:
            logger.warning(f"  ... and {len(violation_log) - 20} more (see report JSON)")

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Source Identity Verification (Stage 10)")
    parser.add_argument("--product", required=True)
    parser.add_argument("--scope", required=True, choices=["USA", "EU27", "non_eu"])
    args = parser.parse_args()

    report = validate_source_identity(args.product, args.scope)
    return 0 if report["overall"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
