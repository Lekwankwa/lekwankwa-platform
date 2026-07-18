"""
scrapers/wages_employment/run.py — Lekwankwa Corporation
Cloud Scheduler entry point for wages_and_employment across all countries.

Usage:
    python scrapers/wages_employment/run.py --country USA --source bls_ces
    python scrapers/wages_employment/run.py --country USA --source bls_cps
    python scrapers/wages_employment/run.py --country GBR
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

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
}


# PIT validation on large bitemporal sources (e.g. USA BLS CES/CPS) can
# legitimately exceed the default 600s validation timeout. Give these
# sources a longer budget and allow one automatic retry on TIMEOUT before
# treating it as a hard failure.
EXTENDED_VALIDATION_TIMEOUT_SOURCES = {"bls_ces", "bls_cps"}
DEFAULT_VALIDATION_TIMEOUT_SECS = 600
EXTENDED_VALIDATION_TIMEOUT_SECS = 1800


def run_source(country: str, cfg: dict, source_filter: str | None,
               mode: str, since: str | None, dry_run: bool) -> bool:
    source = cfg["source"]
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
        from scrapers.utilities.call_scraper_entry import call_scraper_entry
        mod = importlib.import_module(cfg["module"])
        fn  = getattr(mod, cfg["fn"])
        call_scraper_entry(fn, mode, since, cfg["kwargs"])
            pass
        return False

    timeout_secs = (
        EXTENDED_VALIDATION_TIMEOUT_SECS
        if source in EXTENDED_VALIDATION_TIMEOUT_SOURCES
        else DEFAULT_VALIDATION_TIMEOUT_SECS
    )

    val = run_9_stage_validation(product=PRODUCT, country=country,
                                 timeout_secs=timeout_secs)

    if getattr(val, "code", None) == "VALIDATION_FAIL_RC1" and \
            "TIMEOUT" in str(getattr(val, "stdout_tail", "")):
        log.warning(
            "PIT validation timed out at %ss for %s/%s/%s — retrying once with %ss.",
            timeout_secs, PRODUCT, country, source, EXTENDED_VALIDATION_TIMEOUT_SECS,
        )
        val = run_9_stage_validation(product=PRODUCT, country=country,
                                     timeout_secs=EXTENDED_VALIDATION_TIMEOUT_SECS)

    if val.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(                program=__file__, exception=exc,
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
