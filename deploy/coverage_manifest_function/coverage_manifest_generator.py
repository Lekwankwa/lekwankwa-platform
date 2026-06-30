"""
coverage_manifest_generator.py — Lekwankwa Corporation
=======================================================

Event-driven coverage manifest builder.  Runs automatically after each
successful vault_extractor ingestion (NOT on a fixed calendar schedule).
Generates a master manifest and geo-split granular files that mirror the
release_calendar folder structure exactly.

TRIGGER CHAIN (same event-driven pattern as quality_report_generator)
----------------------------------------------------------------------
1.  vault_extractor --mode live/full writes a completion marker on success:
      {vault_root}/run_markers/extractor_{product}_{YYYY-MM-DD}.complete
2.  A Cloud Storage OBJECT_FINALIZE trigger (or the same marker-check that
    drives quality_report_generator) fires this tool.
3.  Reads catalog_manifest.yaml for authoritative coverage metadata.
4.  Writes master + 20 granular JSON files to metadata/coverage_manifest/
    and uploads to GCS.

OUTPUT STRUCTURE (metadata/coverage_manifest/)
----------------------------------------------
  coverage_manifest_master.json
  coverage_manifest_dataset_1_food_micropricing_usa_only.json
  coverage_manifest_dataset_1_food_micropricing_eu27_only.json
  coverage_manifest_dataset_1_food_micropricing_non_eu_block.json
  coverage_manifest_dataset_1_food_micropricing_full_32_country.json
  coverage_manifest_dataset_2_wages_labor_<geo>.json   (×4)
  coverage_manifest_dataset_3_housing_credit_<geo>.json (×4)
  coverage_manifest_dataset_4_trade_flows_<geo>.json    (×4)
  coverage_manifest_dataset_5_global_macro_<geo>.json   (×4)

DEPLOYMENT
----------
  gcloud functions deploy coverage-manifest-generator \\
    --runtime python311 --trigger-resource lekwankwa-historical-vault \\
    --trigger-event google.storage.object.finalize \\
    --entry-point cloud_function_handler \\
    --set-env-vars VAULT_ROOT=gs://lekwankwa-historical-vault \\
    --memory 512Mi --timeout 300s --region us-central1

LOCAL USAGE
-----------
  python tools/coverage_manifest_generator.py \\
      --catalog backtesting/backtest_engine/config/catalog_manifest.yaml \\
      --out-dir metadata/coverage_manifest \\
      [--gcs-bucket gs://lekwankwa-vault] \\
      [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Product catalogue
# ---------------------------------------------------------------------------

SCHEMA_STANDARD = "v5.0"
CATALOG_VERSION  = "v5.0"
DELIVERY_FORMAT  = "Compressed Parquet via Google Cloud Storage"
DELIVERY_SLA     = "Delta files delivered within 24 hours of source publication via GCS (Parquet)."

#  product_key → (number, file_stem, label, mode)
PRODUCTS: dict[str, tuple[int, str, str, str]] = {
    "food_micropricing": (
        1, "dataset_1_food_micropricing",
        "DATASET 1 — Food & Micropricing Archive v5.0", "live",
    ),
    "wages_and_employment": (
        2, "dataset_2_wages_labor",
        "DATASET 2 — Wages & Labor Archive v5.0", "live",
    ),
    "Housing_Supply_and_Shelter_Inflation": (
        3, "dataset_3_housing_credit",
        "DATASET 3 — Housing & Credit Archive v5.0", "archive",
    ),
    "trade_flows": (
        4, "dataset_4_trade_flows",
        "DATASET 4 — Trade Flows Archive v5.0", "live",
    ),
    "global_macro": (
        5, "dataset_5_global_macro",
        "DATASET 5 — Global Macro Archive v5.0", "archive",
    ),
}

# catalog_manifest.yaml dataset names → vault product_key
_DATASET_TO_PRODUCT: dict[str, str] = {
    "food_pricing":               "food_micropricing",
    "eu27_food_pricing":          "food_micropricing",
    "non_eu_food_pricing":        "food_micropricing",
    "wages_and_employment":       "wages_and_employment",
    "eu27_wages_and_employment":  "wages_and_employment",
    "non_eu_wages_and_employment":"wages_and_employment",
    "housing":                    "Housing_Supply_and_Shelter_Inflation",
    "eu27_housing":               "Housing_Supply_and_Shelter_Inflation",
    "eu27_hpi":                   "Housing_Supply_and_Shelter_Inflation",
    "non_eu_housing":             "Housing_Supply_and_Shelter_Inflation",
    "trade_flows":                "trade_flows",
    "eu27_trade_flows":           "trade_flows",
    "non_eu_trade_flows":         "trade_flows",
    "global_macro":               "global_macro",
    "eu27_global_macro":          "global_macro",
    "non_eu_global_macro":        "global_macro",
}

# Source agency labels per country
_SOURCE_AGENCIES: dict[str, str] = {
    "USA":  "BLS / ALFRED / Census / BEA",
    "GBR":  "ONS",
    "CAN":  "Statistics Canada",
    "AUS":  "Australian Bureau of Statistics (ABS)",
    "NOR":  "Statistics Norway (SSB)",
    "EU27": "Eurostat",
}

# ---------------------------------------------------------------------------
# Geo bundles (identical to release_calendar)
# ---------------------------------------------------------------------------

EU27_MEMBERS = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
    "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK",
    "SVN","ESP","SWE",
]
NON_EU_COUNTRIES = ["GBR", "CAN", "AUS", "NOR"]

GEO_BUNDLES: list[tuple[str, str, list[str]]] = [
    ("usa_only",        "USA Only",                      ["USA"]),
    ("eu27_only",       "EU27 Only",                     EU27_MEMBERS),
    ("non_eu_block",    "Non-EU Block (GBR / CAN / AUS / NOR)", NON_EU_COUNTRIES),
    ("full_32_country", "Full 32-Country Coverage",
     ["USA"] + EU27_MEMBERS + NON_EU_COUNTRIES),
]

# Dataset subfolder names — mirrors release_calendar structure exactly
_DATASET_FOLDER: dict[str, str] = {
    "food_micropricing":                    "Dataset 1 - Food Micropricing",
    "wages_and_employment":                 "Dataset 2 - Wages Labor",
    "Housing_Supply_and_Shelter_Inflation": "Dataset 3 - Housing Credit",
    "trade_flows":                          "Dataset 4 - Trade Flows",
    "global_macro":                         "Dataset 5 - Global Macro",
}


# ---------------------------------------------------------------------------
# Catalog reader
# ---------------------------------------------------------------------------

def _load_catalog(catalog_path: Path) -> list[dict]:
    with catalog_path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("catalog", [])


def _iso_for_country(entry: dict) -> str | None:
    """Return the canonical ISO3 from a catalog entry."""
    iso = entry.get("iso_alpha3", "")
    if iso in ("NON_EU", "EU27", ""):
        return None  # panel/aggregate entries handled separately
    return iso


def _infer_country_group(iso: str) -> str:
    if iso == "USA":
        return "USA"
    if iso in NON_EU_COUNTRIES:
        return iso
    if iso in EU27_MEMBERS:
        return "EU27"
    return iso


def _infer_frequency(entry: dict) -> str:
    pit = entry.get("pit_coverage_type", "")
    series = entry.get("series", []) or []
    notes  = (entry.get("notes", "") or "").lower()
    dataset = (entry.get("dataset", "") or "").lower()
    if "quarterly" in notes or "_q" in dataset or "quarter" in notes:
        return "Quarterly"
    if "monthly" in notes:
        return "Monthly"
    # Infer from known sources
    source = entry.get("vault_source", "")
    if source in ("abs_sdmx",) and "food" in dataset:
        return "Quarterly"
    if source == "ssb_statbank" and "wages" in dataset:
        return "Quarterly"
    return "Monthly"


def _infer_pit_type(entry: dict) -> str:
    raw = entry.get("pit_coverage_type", "")
    if "FULL_VINTAGE" in raw:
        return "FULL_VINTAGE"
    if raw:
        return raw
    # ALFRED sources = FULL_VINTAGE
    source = entry.get("vault_source", "")
    if source == "alfred_vintage":
        return "FULL_VINTAGE"
    return "RELEASE_DATE_ONLY/accumulating"


# ---------------------------------------------------------------------------
# Build per-country coverage entries
# ---------------------------------------------------------------------------

def build_coverage_entries(catalog: list[dict]) -> dict[str, dict[str, dict]]:
    """
    Returns: {product_key: {iso3: coverage_entry_dict}}
    Aggregates individual country + panel entries.
    """
    result: dict[str, dict[str, dict]] = {pk: {} for pk in PRODUCTS}

    for entry in catalog:
        dataset  = entry.get("dataset", "")
        pk       = _DATASET_TO_PRODUCT.get(dataset)
        if pk is None:
            continue

        status = entry.get("status", "UNKNOWN")
        iso    = _iso_for_country(entry)

        # Skip panel/aggregate entries here — they're used only for notes
        if iso is None:
            continue

        if status in ("BLOCKED",):
            continue

        cg = _infer_country_group(iso)
        freq = _infer_frequency(entry)
        pit  = _infer_pit_type(entry)
        series = entry.get("series") or []
        notes  = (entry.get("notes") or "").strip()
        # Truncate long notes
        if notes and len(notes) > 200:
            notes = notes[:197] + "..."

        entry_out = {
            "iso_alpha3":       iso,
            "country_group":    cg,
            "source_agency":    _SOURCE_AGENCIES.get(cg, cg),
            "vault_source":     entry.get("vault_source", ""),
            "frequency":        freq,
            "pit_coverage_type": pit,
            "data_from":        entry.get("backtest_start", ""),
            "data_through":     entry.get("backtest_end", ""),
            "series_tracked":   len(series) if series else 1,
            "catalog_status":   status,
            "delivery_format":  DELIVERY_FORMAT,
            "notes":            notes,
        }

        # Keep the entry with the widest date range if duplicated (e.g. eu27_hpi + eu27_housing)
        existing = result[pk].get(iso)
        if existing is None:
            result[pk][iso] = entry_out
        else:
            existing_from = existing.get("data_from", "9999")
            new_from      = entry_out.get("data_from", "9999")
            if new_from < existing_from:
                result[pk][iso] = entry_out

    return result


# ---------------------------------------------------------------------------
# Geo-filter
# ---------------------------------------------------------------------------

def _filter_for_geo(
    coverage_map: dict[str, dict],
    geo_iso_list: list[str],
) -> list[dict]:
    """Return ordered coverage entries for the given geo bundle."""
    entries = []
    for iso in geo_iso_list:
        if iso in coverage_map:
            entries.append(coverage_map[iso])
        elif iso in EU27_MEMBERS:
            # EU27 members may all share same entry if loaded at group level
            pass
    # Fallback: for EU27, find all matching country_group=="EU27"
    if not entries:
        for iso, entry in coverage_map.items():
            if entry.get("iso_alpha3") in geo_iso_list:
                entries.append(entry)
    return entries


def _summarise(entries: list[dict]) -> dict:
    if not entries:
        return {}
    dates_from   = [e["data_from"]   for e in entries if e.get("data_from")]
    dates_through = [e["data_through"] for e in entries if e.get("data_through")]
    ready = sum(1 for e in entries if e.get("catalog_status") == "READY")
    return {
        "total_countries":       len(entries),
        "countries_ready":       ready,
        "countries_pending":     len(entries) - ready,
        "earliest_data_from":    min(dates_from)    if dates_from    else None,
        "latest_data_through":   max(dates_through) if dates_through else None,
        "pit_coverage_types":    list({e.get("pit_coverage_type","") for e in entries}),
        "delivery_format":       DELIVERY_FORMAT,
    }


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def _write_granular(
    out_dir: Path,
    product_key: str,
    geo_key: str,
    geo_label: str,
    entries: list[dict],
    generated_at: str,
    as_of_date: str,
) -> None:
    num, stem, label, mode = PRODUCTS[product_key]

    if geo_key != "full_32_country":
        product_label = f"{label} — {geo_label}"
    else:
        product_label = label

    doc = {
        "document_type":   "Coverage Manifest — Data Availability & Series Coverage",
        "product_number":  num,
        "geo_bundle":      geo_label,
        "geo_key":         geo_key,
        "product_label":   product_label,
        "schema_standard": SCHEMA_STANDARD,
        "catalog_version": CATALOG_VERSION,
        "generated_at":    generated_at,
        "as_of_date":      as_of_date,
        "delivery_format": DELIVERY_FORMAT,
        "delivery_sla":    DELIVERY_SLA,
        "delivery_mode":   mode,
        "coverage":        entries,
        "summary":         _summarise(entries),
    }

    fname = f"coverage_manifest_{stem}_{geo_key}.json"
    ds_folder = _DATASET_FOLDER.get(product_key, stem)
    target_dir = out_dir / ds_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / fname
    out_path.write_text(
        json.dumps(doc, indent=2, default=str, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  Written: {ds_folder}/{fname}")


def _write_master(
    out_dir: Path,
    all_product_entries: dict[str, dict[str, dict]],
    generated_at: str,
    as_of_date: str,
) -> None:
    products_out: dict[str, Any] = {}
    total_countries: set[str] = set()

    for pk, coverage_map in all_product_entries.items():
        if not coverage_map:
            continue
        num, stem, label, mode = PRODUCTS[pk]
        entries = list(coverage_map.values())
        total_countries.update(e["iso_alpha3"] for e in entries)
        products_out[pk] = {
            "product_number":  num,
            "product_label":   label,
            "delivery_mode":   mode,
            "total_countries": len(entries),
            "coverage":        entries,
            "summary":         _summarise(entries),
        }

    master = {
        "document_type":   "Coverage Manifest — Master",
        "schema_standard": SCHEMA_STANDARD,
        "catalog_version": CATALOG_VERSION,
        "generated_at":    generated_at,
        "as_of_date":      as_of_date,
        "scope": {
            "total_countries":  len(total_countries),
            "total_products":   len(products_out),
            "delivery_format":  DELIVERY_FORMAT,
            "delivery_sla":     DELIVERY_SLA,
            "schema_standard":  SCHEMA_STANDARD,
        },
        "products": products_out,
    }

    out_path = out_dir / "coverage_manifest_master.json"
    out_path.write_text(
        json.dumps(master, indent=2, default=str, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  Written: {out_path.name}")


# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------

def _upload_to_gcs(
    local_dir: Path,
    gcs_bucket: str,
    gcs_prefix: str,
    dry_run: bool = False,
) -> None:
    try:
        from google.cloud import storage  # type: ignore
    except ImportError:
        print("ERROR: google-cloud-storage not installed. Run: pip install google-cloud-storage")
        return

    bucket_name = gcs_bucket.lstrip("gs://")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for local_file in sorted(local_dir.rglob("*.json")):
        rel_path = local_file.relative_to(local_dir)
        blob_name = f"{gcs_prefix.rstrip('/')}/{rel_path.as_posix()}"
        if dry_run:
            print(f"[DRY-RUN] Would upload: {rel_path} → gs://{bucket_name}/{blob_name}")
        else:
            bucket.blob(blob_name).upload_from_filename(
                str(local_file), content_type="application/json"
            )
            print(f"  Uploaded: gs://{bucket_name}/{blob_name}")


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------

def cloud_function_handler(event: dict, context: Any) -> None:  # type: ignore
    """
    GCS OBJECT_FINALIZE trigger.
    Fires when any extractor completion marker is written to the vault bucket.
    Regenerates coverage manifests on every new data release.
    """
    import re as _re
    name = event.get("name", "")
    if not _re.match(r"run_markers/extractor_.+\.complete$", name):
        return

    vault_root_env = __import__("os").environ.get("VAULT_ROOT", "gs://lekwankwa-historical-vault")
    catalog_path   = Path("/tmp/catalog_manifest.yaml")

    # Fetch catalog from GCS if not already present
    if not catalog_path.exists():
        try:
            from google.cloud import storage  # type: ignore
            client = storage.Client()
            bucket_name = vault_root_env.replace("gs://", "").split("/")[0]
            client.bucket(bucket_name).blob(
                "backtesting/backtest_engine/config/catalog_manifest.yaml"
            ).download_to_filename(str(catalog_path))
        except Exception as exc:
            print(f"ERROR fetching catalog: {exc}")
            return

    out_dir = Path("/tmp/coverage_manifest")
    out_dir.mkdir(parents=True, exist_ok=True)

    run(
        catalog_path=catalog_path,
        out_dir=out_dir,
        gcs_bucket=vault_root_env,
        gcs_prefix="metadata/coverage_manifest",
        dry_run=False,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(
    catalog_path: Path,
    out_dir: Path,
    gcs_bucket: str | None,
    gcs_prefix: str,
    dry_run: bool,
) -> int:
    generated_at = datetime.now(timezone.utc).isoformat()
    as_of_date   = date.today().isoformat()

    print("=" * 70)
    print("LEKWANKWA COVERAGE MANIFEST GENERATOR")
    print(f"Catalog: {catalog_path}  |  Out: {out_dir}")
    if dry_run:
        print("DRY RUN — no files written")
    print("=" * 70)

    catalog = _load_catalog(catalog_path)
    print(f"  Loaded {len(catalog)} catalog entries")

    all_product_entries = build_coverage_entries(catalog)

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

        # Master
        _write_master(out_dir, all_product_entries, generated_at, as_of_date)

        # Granular: 5 products × 4 geo bundles = 20 files
        for pk, coverage_map in all_product_entries.items():
            if not coverage_map:
                print(f"  SKIP {pk} — no catalog entries found")
                continue
            for geo_key, geo_label, geo_iso_list in GEO_BUNDLES:
                entries = _filter_for_geo(coverage_map, geo_iso_list)
                if not entries:
                    continue
                _write_granular(
                    out_dir=out_dir,
                    product_key=pk,
                    geo_key=geo_key,
                    geo_label=geo_label,
                    entries=entries,
                    generated_at=generated_at,
                    as_of_date=as_of_date,
                )
    else:
        for pk, coverage_map in all_product_entries.items():
            for geo_key, geo_label, geo_iso_list in GEO_BUNDLES:
                entries = _filter_for_geo(coverage_map, geo_iso_list)
                num, stem, _, _ = PRODUCTS[pk]
                fname = f"coverage_manifest_{stem}_{geo_key}.json"
                ds_folder = _DATASET_FOLDER.get(pk, stem)
                print(f"  [DRY-RUN] Would write: {ds_folder}/{fname} ({len(entries)} countries)")

    if gcs_bucket and not dry_run:
        print(f"\nUploading to GCS: {gcs_bucket}/{gcs_prefix}")
        _upload_to_gcs(out_dir, gcs_bucket, gcs_prefix)
    elif gcs_bucket and dry_run:
        _upload_to_gcs(out_dir, gcs_bucket, gcs_prefix, dry_run=True)

    print("\nDone.")
    return 0


def main(argv: list | None = None) -> None:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    p = argparse.ArgumentParser(
        description="Lekwankwa Coverage Manifest Generator — event-driven, GCS-ready",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--catalog", required=True, metavar="YAML",
        help="Path to catalog_manifest.yaml (e.g. backtesting/backtest_engine/config/catalog_manifest.yaml)",
    )
    p.add_argument(
        "--out-dir", default="metadata/coverage_manifest", metavar="DIR",
        help="Output directory (default: metadata/coverage_manifest)",
    )
    p.add_argument(
        "--gcs-bucket", default=None, metavar="BUCKET",
        help="GCS bucket for upload, e.g. gs://lekwankwa-vault (omit to skip upload)",
    )
    p.add_argument(
        "--gcs-prefix", default="metadata/coverage_manifest", metavar="PREFIX",
        help="GCS object prefix (default: metadata/coverage_manifest)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be written/uploaded without writing anything",
    )
    args = p.parse_args(argv)

    sys.exit(
        run(
            catalog_path=Path(args.catalog),
            out_dir=Path(args.out_dir),
            gcs_bucket=args.gcs_bucket,
            gcs_prefix=args.gcs_prefix,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
