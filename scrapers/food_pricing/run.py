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
import os
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

from tools.secret_manager import load_all_secrets_to_env
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
                        "module": cfg["module"], "fn": cfg["fn"],
                        "mode": mode, "since": since,
                    },
                )
            except ImportError:
                pass
            return False

        # Scope validation to the incremental window instead of the full
        # ~46-year vault — the whole history has already been validated
        # once; re-checking all of it on every run is what was making
        if mode == "incremental":
            from scrapers.utilities.incremental import get_vault_latest_month
            vault_root_env = os.environ.get("VAULT_ROOT", "").strip().rstrip("/") or "lekwankwa-historical-vault"
            # Guard against a malformed VAULT_ROOT (e.g. "gs://" with no bucket
            # name, which .rstrip("/") collapses down to the invalid "gs:").
            # A bad value here is inherited by the downstream validation
            # subprocess — including the lineage check spawned from it —
            # which then globs against bucket "gs:" and dies with an
            # HttpError: Invalid bucket name: 'gs:'. Detect and repair it
            # before it ever reaches the environment used by validation.
            if vault_root_env in ("gs:", "gs://", "") or not vault_root_env.replace("gs://", "").strip():
                log.error(
                    "VAULT_ROOT resolved to invalid value %r for %s/%s/%s — "
                    "falling back to default vault root.",
                    vault_root_env, PRODUCT, country, source,
                )
                vault_root_env = "lekwankwa-historical-vault"
            os.environ["VAULT_ROOT"] = vault_root_env
            scan_root = f"{vault_root_env}/product={PRODUCT}/country={country}/source={source}"
            # Always clear first: if latest comes back falsy (empty vault,
            # first-ever run for this country/source), neither branch below
            # would otherwise touch the var, letting a stale scoping value
            # from an earlier country/source in this same process (main()
            # loops over multiple countries in one Python process) leak
            # into this source's validation and live-feed-audit calls.
            os.environ.pop("VALIDATION_SINCE_YEAR", None)
            latest = get_vault_latest_month(scan_root)
            if latest:
                os.environ["VALIDATION_SINCE_YEAR"] = str(max(1, latest[0] - 2))
        else:
            os.environ.pop("VALIDATION_SINCE_YEAR", None)
            vault_root_env = os.environ.get("VAULT_ROOT", "").strip().rstrip("/") or "lekwankwa-historical-vault"
            if vault_root_env in ("gs:", "gs://", "") or not vault_root_env.replace("gs://", "").strip():
                log.error(
                    "VAULT_ROOT resolved to invalid value %r for %s/%s/%s — "
                    "falling back to default vault root.",
                    vault_root_env, PRODUCT, country, source,
                )
                vault_root_env = "lekwankwa-historical-vault"
            os.environ["VAULT_ROOT"] = vault_root_env            latest = get_vault_latest_month(scan_root)
            if latest:
                os.environ["VALIDATION_SINCE_YEAR"] = str(max(1, latest[0] - 2))
        else:
            os.environ.pop("VALIDATION_SINCE_YEAR", None)

        # Post-scrape: 9-stage + GX + Bitemporal Core validation
        try:
            val = run_9_stage_validation(product=PRODUCT, country=country)
        except Exception as exc:
            log.error("run_9_stage_validation raised for %s/%s/%s: %s",
                      PRODUCT, country, source, exc, exc_info=True)
            try:
                from tools.self_healing.handler import handle_exception
                handle_exception(
                    program=__file__,
                    exception=exc,
                    context={
                        "product": PRODUCT, "country": country,
                        "source": source, "run_date": TODAY,
                        "layer": "VALIDATION",
                    },
                )
            except ImportError:
                pass
            return False

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
            try:
                audit = run_post_delta_audit(product=PRODUCT, country=country)
            except Exception as exc:
                log.error("run_post_delta_audit raised for %s/%s/%s: %s",
                          PRODUCT, country, source, exc, exc_info=True)
                try:
                    from tools.self_healing.handler import handle_exception
                    handle_exception(
                        program=__file__,
                        exception=exc,
                        context={
                            "product": PRODUCT, "country": country,
                            "source": source, "run_date": TODAY,
                            "layer": "LIVE_FEED_AUDIT",
                        },
                    )
                except ImportError:
                    pass
                return False

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
