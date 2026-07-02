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
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from tools.secrets import load_all_secrets_to_env
load_all_secrets_to_env()

from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation

# Countries routed through each underlying scraper
COUNTRY_ROUTER: dict[str, dict] = {
def _normalize_parquet_schema(product: str, country: str, source: str) -> None:
    """
    Ensure freshly-written parquet partitions for this product/country/source
    use a plain Arrow `string` dtype for the `source` column instead of a
    dictionary/categorical type. Mixed dtypes across monthly partitions
    (string vs dictionary<values=string,...>) cause pyarrow dataset merges
    to fail during downstream PIT validation, which silently skips every
    partition and ultimately raises FileNotFoundError("No food data found
    in vault").
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tools.vault_io import list_vault_files, write_vault_file

    try:
        files = list_vault_files(product=product, country=country, source=source)
    except Exception as exc:
        log.warning("Could not list vault files for %s/%s/%s: %s",
                    product, country, source, exc)
        return

    for uri in files:
        try:
            table = pq.read_table(uri)
            if "source" not in table.column_names:
                continue
            idx = table.schema.get_field_index("source")
            field_type = table.schema.field(idx).type
            if pa.types.is_dictionary(field_type):
                col = table.column("source").cast(pa.string())
                table = table.set_column(idx, "source", col)
                write_vault_file(uri, table)
                log.info("Normalized 'source' column dtype in %s", uri)
        except Exception as exc:
            log.warning("Schema normalization failed for %s: %s", uri, exc)


    "USA": {
        "module":  "scrapers.food_pricing.usa_food_scraper",
        "fn":      "scrape_usa_food_pricing",
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
        "sources": ["ssb"],
    },
    "EU27": {
        "module":  "scrapers.food_pricing.eurostat_food_scraper",
        "fn":      "scrape_eu27_food_pricing",
        "sources": ["eurostat"],
    },
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

    for source in cfg["sources"]:
        if not is_release_due(product=PRODUCT, country=country,
                              source=source, as_of=TODAY):
            log.info("No release due today for %s/%s/%s — skipping.",
                     PRODUCT, country, source)
            continue

        if dry_run:
            log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, country, source)
            continue

        try:
            import importlib
            mod = importlib.import_module(cfg["module"])
            mod = importlib.import_module(cfg["module"])
            fn  = getattr(mod, cfg["fn"])
            fn(mode=mode, since=since)
            _normalize_parquet_schema(PRODUCT, country, source)
            log.info("Completed scrape %s/%s/%s", PRODUCT, country, source)
        except Exception as exc:
            log.error("Scraper failed for %s/%s/%s: %s", PRODUCT, country, source, exc,            try:
                from tools.self_healing.handler import handle_exception
                handle_exception(
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
