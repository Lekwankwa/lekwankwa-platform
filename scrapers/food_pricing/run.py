"""
scrapers/food_pricing/run.py — Lekwankwa Corporation
Cloud Scheduler entry point for food_micropricing across all countries.

Usage:
    python scrapers/food_pricing/run.py --country USA
    python scrapers/food_pricing/run.py --country EU27
    python scrapers/food_pricing/run.py --country ALL_EU
    python scrapers/food_pricing/run.py --country GBR --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Add repo root to path when running directly
from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation
from tools.live_feed_audit import run_post_delta_audit
from tools.vault_schema_fix import normalize_partition_schemas
log = logging.getLogger(__name__)

PRODUCT = "food_micropricing"
PRODUCT = "food_micropricing"
)

load_all_secrets_to_env()

from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation
from tools.live_feed_audit import run_post_delta_audit
log = logging.getLogger(__name__)


PRODUCT = "food_micropricing"
TODAY   = date.today().isoformat()

# Countries routed through each underlying scraper
COUNTRY_ROUTER: dict[str, dict] = {
    "USA": {
        "module":  "scrapers.food_pricing.usa_food_scraper",
        "fn":      "scrape_usa_food_pricing",
        "sources": ["bls_cpi"],
    },
    "GBR": {
        "module":  "scrapers.food_pricing.ons_food_scraper",
        "fn":      "scrape_gbr_food_pricing",
        "sources": ["ons"],
    },
    "CAN": {
        "module":  "scrapers.food_pricing.statcan_food_scraper",
        "fn":      "scrape_can_food_pricing",
        "sources": ["statcan"],
    },
    "AUS": {
        "module":  "scrapers.food_pricing.abs_food_scraper",
        "fn":      "scrape_aus_food_pricing",
        "sources": ["abs"],
    },
    "NOR": {
        "module":  "scrapers.food_pricing.ssb_food_scraper",
        "fn":      "scrape_nor_food_pricing",
    "ROU","SVK","SVN","ESP","SWE",
]


def _normalize_parquet_schema(product: str, country: str, source: str) -> None:
    """Ensure the 'source' column is a plain string (never dictionary-encoded)
    across all vault parquet partitions for this product/country/source.

    Upstream writers occasionally emit dictionary-encoded 'source' columns,
    which causes downstream validation merges to fail with an
    'incompatible types: string vs dictionary<...>' error, silently
    skipping partitions and leading to false 'no data found' failures.
    """
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    try:
        client = storage.Client()
        bucket = client.bucket("lekwankwa-vault")
        prefix = f"product={product}/country={country}/source={source}/"
        for blob in bucket.list_blobs(prefix=prefix):
            if not blob.name.endswith(".parquet"):
                continue
            data = blob.download_as_bytes()
            table = pq.read_table(io.BytesIO(data))
            if "source" not in table.column_names:
                continue
            idx = table.schema.get_field_index("source")
            field_type = table.schema.field(idx).type
            if pa.types.is_dictionary(field_type):
                col = table.column("source").cast(pa.string())
                table = table.set_column(idx, "source", table.schema.field(idx).with_type(pa.string()), col)
                buf = io.BytesIO()
                pq.write_table(table, buf)
                blob.upload_from_string(buf.getvalue(),
                                         content_type="application/octet-stream")
                log.info("Normalized dictionary-encoded 'source' column in %s",
                         blob.name)
    except Exception as norm_exc:
        log.warning("Schema normalization skipped for %s/%s/%s: %s",
                    product, country, source, norm_exc)


def run_country(country: str, mode: str, since: str | None, dry_run: bool) -> bool:
    cfg = COUNTRY_ROUTER.get(country)
    if cfg is None:
}

EU_MEMBERS = [
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU",
    "GRC","HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT",
    "ROU","SVK","SVN","ESP","SWE",
]


def run_country(country: str, mode: str, since: str | None, dry_run: bool) -> bool:
    cfg = COUNTRY_ROUTER.get(country)
    if cfg is None:
        log.error("No router entry for country=%s", country)
        return False
            fn  = getattr(mod, cfg["fn"])
            fn(mode=mode, since=since)
            log.info("Completed scrape %s/%s/%s", PRODUCT, country, source)
            _normalize_parquet_schema(PRODUCT, country, source)
        except Exception as exc:
            log.error("Scraper failed for %s/%s/%s: %s", PRODUCT, country, source, exc,
                      exc_info=True)            continue

        if dry_run:
            log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, country, source)
            continue

        try:
            import importlib
            mod = importlib.import_module(cfg["module"])
            fn  = getattr(mod, cfg["fn"])
            except ImportError:
                pass
            return False

        # Normalize vault parquet schema for this product/country/source before
        # validation runs, to prevent Arrow dictionary-vs-string merge failures
        # caused by inconsistent encodings across historical files.
        try:
            normalize_partition_schema(
                product=PRODUCT,
                country=country,
                source=source,
                columns=["source"],
            )
        except Exception as norm_exc:
            return False

        # Normalize Parquet column schemas (e.g. dictionary-encoded vs plain
        # string 'source' column) across all partitions for this
        # product/country/source before running validation, to prevent
        # PyArrow merge failures during PIT validation loading.
        try:
            normalize_partition_schemas(product=PRODUCT, country=country,
                                         source=source)
        except Exception as norm_exc:
            log.warning("Schema normalization failed for %s/%s/%s: %s",
                        PRODUCT, country, source, norm_exc, exc_info=True)

        # Post-scrape: 9-stage + GX + Bitemporal Core validation
        val = run_9_stage_validation(product=PRODUCT, country=country)
        if val.severity in ("CRITICAL", "HIGH"):                handle_exception(
                    program=__file__,
                    exception=exc,
                    context={
                        "product": PRODUCT, "country": country,
                        "source": source, "run_date": TODAY,
                        "layer": "SCRAPER",
                    },
                )
            except ImportError:
                pass
            return False

        # Post-scrape: 9-stage + GX + Bitemporal Core validation
        val = run_9_stage_validation(product=PRODUCT, country=country)
        if val.severity in ("CRITICAL", "HIGH"):
            from tools.self_healing.handler import handle_validation_finding
            handle_validation_finding(
                program=__file__,
                context={
                    "product": PRODUCT, "country": country,
                    "source": source, "run_date": TODAY,
                    "layer": "VALIDATION",
                },
                result=val,
            )
            return False

        # Post-delta live feed audit (live products only)
        if PRODUCT in ("food_micropricing", "wages_and_employment", "trade_flows"):
            audit = run_post_delta_audit(product=PRODUCT, country=country)
            if audit.severity in ("CRITICAL", "HIGH"):
                from tools.self_healing.handler import handle_validation_finding
                handle_validation_finding(
                    program=__file__,
                    context={
                        "product": PRODUCT, "country": country,
                        "source": source, "run_date": TODAY,
                        "layer": "LIVE_FEED_AUDIT",
                    },
                    result=audit,
                )
                return False
    from tools.trigger_downstream import trigger_all_metadata
    trigger_all_metadata()
    return True


def main():
    parser = argparse.ArgumentParser(
        description="food_micropricing cloud scraper entry point"
    )
    parser.add_argument("--country", required=True,
                        help="ISO alpha-3 country code, EU27, ALL_EU, or ALL")
    parser.add_argument("--mode", choices=["incremental", "full"],
                        default="incremental")
    parser.add_argument("--since", type=str, default=None, metavar="YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("FOOD MICROPRICING — run.py  [%s]", TODAY)
    log.info("Country: %s | Mode: %s", args.country, args.mode)
    log.info("=" * 60)

    if args.country == "ALL_EU":
        countries = EU_MEMBERS
    elif args.country == "ALL":
        countries = list(COUNTRY_ROUTER.keys()) + EU_MEMBERS
    else:
        countries = [args.country]

    ok = all(run_country(c, args.mode, args.since, args.dry_run)
             for c in countries)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
