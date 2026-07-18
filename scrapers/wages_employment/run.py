from __future__ import annotations

import argparse
import time
import logging
import sys
from datetime import date
    python scrapers/wages_employment/run.py --country USA --source bls_cps
    python scrapers/wages_employment/run.py --country GBR
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation
from tools.live_feed_audit import run_post_delta_audit
from tools.vault_audit import vault_has_data
log = logging.getLogger(__name__)

PRODUCT = "wages_and_employment"
)

from tools.secret_manager import load_all_secrets_to_env
load_all_secrets_to_env()

from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation
from tools.live_feed_audit import run_post_delta_audit
log = logging.getLogger(__name__)

PRODUCT = "wages_and_employment"
TODAY   = date.today().isoformat()

COUNTRY_ROUTER: dict[str, list[dict]] = {
    "USA": [
        {
            "source":  "bls_ces",
            "module":  "scrapers.wages_employment.bls_ces_cps_usa_scraper",
            "fn":      "main",
            "kwargs":  {"dataset": "ces"},
        },
        {
            "source":  "bls_cps",
            "module":  "scrapers.wages_employment.bls_ces_cps_usa_scraper",
            "fn":      "main",
            "kwargs":  {"dataset": "cps"},
        },
    ],
    "GBR": [{"source": "ons",     "module": "scrapers.wages_employment.ons_wages_scraper",    "fn": "scrape_gbr_wages", "kwargs": {}}],
    "CAN": [{"source": "statcan", "module": "scrapers.wages_employment.statcan_wages_scraper", "fn": "scrape_can_wages", "kwargs": {}}],
    "EU27": [{"source": "eurostat", "module": "scrapers.wages_employment.eurostat_wages_scraper", "fn": "scrape_eu27_wages", "kwargs": {}}],
}


def run_source(country: str, cfg: dict, source_filter: str | None,
               mode: str, since: str | None, dry_run: bool) -> bool:
    source = cfg["source"]
    if source_filter and source != source_filter:
        return True   # not requested — skip cleanly

    if not is_release_due(product=PRODUCT, country=country,
                          source=source, as_of=TODAY):
        log.info("No release due today for %s/%s/%s — skipping.",
                 PRODUCT, country, source)
        return True

    if dry_run:
        log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, country, source)
        return True

    try:
        import importlib
            pass
        return False

    # Guard against eventual-consistency / silent no-op writes: confirm the
    # scraper actually landed data in the vault before handing off to the
    # validation pipeline, which otherwise fails with an opaque
    # FileNotFoundError at the PIT stage.
    max_attempts = 5
    wait_seconds = 3
    for attempt in range(1, max_attempts + 1):
        if vault_has_data(product=PRODUCT, country=country,
                           source=source, as_of=TODAY):
            break
        log.warning("Vault data not yet visible for %s/%s/%s (attempt %d/%d) — "
                    "waiting %ds before re-check.",
                    PRODUCT, country, source, attempt, max_attempts, wait_seconds)
        time.sleep(wait_seconds)
    else:
        no_data_exc = RuntimeError(
            f"No vault data found for {PRODUCT}/{country}/{source} after "
            f"scraper reported success — treating as scraper write failure."
        )
        log.error(str(no_data_exc))
        try:
            from tools.self_healing.handler import handle_exception
            handle_exception(
                program=__file__, exception=no_data_exc,
                context={"product": PRODUCT, "country": country,
                         "source": source, "run_date": TODAY, "layer": "SCRAPER"},
            )
        except ImportError:
            pass
        return False

    val = run_9_stage_validation(product=PRODUCT, country=country)
    if val.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding        log.error("Scraper failed %s/%s/%s: %s", PRODUCT, country, source, exc,
                  exc_info=True)
        try:
            from tools.self_healing.handler import handle_exception
            handle_exception(
                program=__file__, exception=exc,
                context={"product": PRODUCT, "country": country,
                         "source": source, "run_date": TODAY, "layer": "SCRAPER"},
            )
        except ImportError:
            pass
        return False

    val = run_9_stage_validation(product=PRODUCT, country=country)
    if val.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(
            program=__file__,
            context={"product": PRODUCT, "country": country, "source": source,
                     "run_date": TODAY, "layer": "VALIDATION"},
            result=val,
        )
        return False

    audit = run_post_delta_audit(product=PRODUCT, country=country)
    if audit.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(
            program=__file__,
            context={"product": PRODUCT, "country": country, "source": source,
                     "run_date": TODAY, "layer": "LIVE_FEED_AUDIT"},
            result=audit,
        )
        return False
    from tools.trigger_downstream import trigger_all_metadata
    trigger_all_metadata()
    return True


def main():
    parser = argparse.ArgumentParser(
        description="wages_and_employment cloud scraper entry point"
    )
    parser.add_argument("--country", required=True)
    parser.add_argument("--source", default=None,
                        help="Filter to one source (bls_ces / bls_cps)")
    parser.add_argument("--mode", choices=["incremental", "full"],
                        default="incremental")
    parser.add_argument("--since", type=str, default=None, metavar="YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("WAGES & EMPLOYMENT — run.py  [%s]", TODAY)
    log.info("Country: %s | Source: %s | Mode: %s",
             args.country, args.source or "all", args.mode)
    log.info("=" * 60)

    cfgs = COUNTRY_ROUTER.get(args.country, [])
    if not cfgs:
        log.error("No router entry for country=%s", args.country)
        sys.exit(1)

    ok = all(run_source(args.country, cfg, args.source,
                        args.mode, args.since, args.dry_run)
             for cfg in cfgs)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
