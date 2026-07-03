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
import time
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
from tools.live_feed_audit import run_post_delta_audit
PRODUCT = "food_micropricing"
TODAY   = date.today().isoformat()

# Retry policy for transient validation-harness failures (e.g. empty
# `stages` payload + VALIDATION_FAIL_RC1 caused by subprocess timeouts or
# slow GCS partition scans), as opposed to genuine data-quality failures.
VALIDATION_MAX_RETRIES = 3
VALIDATION_RETRY_BACKOFF_SECONDS = 30

# Countries routed through each underlying scraper
COUNTRY_ROUTER: dict[str, dict] = {
    "USA": {
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
            fn  = getattr(mod, cfg["fn"])
            fn(mode=mode, since=since)
            log.info("Completed scrape %s/%s/%s", PRODUCT, country, source)
        except Exception as exc:
            log.error("Scraper failed for %s/%s/%s: %s", PRODUCT, country, source, exc,
                      exc_info=True)
            try:
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
            return False

        # Post-scrape: 9-stage + GX + Bitemporal Core validation
        # Retry on transient validation-harness failures (empty `stages`
        # payload with code VALIDATION_FAIL_RC1) before treating the run as
        # a genuine data failure — these are typically caused by network or
        # GCS-listing stalls inside the validation subprocess, not by actual
        # broken data (all individual stage logs show PASS/WARN, not FAIL).
        val = None
        for attempt in range(1, VALIDATION_MAX_RETRIES + 1):
            val = run_9_stage_validation(product=PRODUCT, country=country)
            transient = (
                val.severity in ("CRITICAL", "HIGH")
                and not getattr(val, "stages", None)
                and getattr(val, "code", "") == "VALIDATION_FAIL_RC1"
            )
            if not transient:
                break
            log.warning(
                "Transient validation harness failure (empty stages, RC1) "
                "for %s/%s/%s — retry %d/%d",
                PRODUCT, country, source, attempt, VALIDATION_MAX_RETRIES,
            )
            time.sleep(VALIDATION_RETRY_BACKOFF_SECONDS * attempt)

        if val.severity in ("CRITICAL", "HIGH"):
            from tools.self_healing.handler import handle_validation_finding
            handle_validation_finding(
                program=__file__,        # Post-delta live feed audit (live products only)
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
