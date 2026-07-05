from __future__ import annotations

import argparse
import inspect
import logging
import sys
from datetime import date
    python scrapers/housing/run.py --country USA --source census_bps
    python scrapers/housing/run.py --country GBR
    python scrapers/housing/run.py --country EU27
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

from tools.secrets import load_all_secrets_to_env
load_all_secrets_to_env()

from tools.release_calendar_extractor import is_release_due
from tools.vault_audit import run_9_stage_validation
from tools.live_feed_audit import run_post_delta_audit
log = logging.getLogger(__name__)

PRODUCT = "Housing_Supply_and_Shelter_Inflation"
TODAY   = date.today().isoformat()

COUNTRY_ROUTER: dict[str, list[dict]] = {
    "USA": [
        {
            "source": "bls_cpi_shelter",
            "module": "scrapers.housing.bls_census_housing_usa_scraper",
            "fn":     "main",
            "kwargs": {"dataset": "shelter"},
        },
        {
            "source": "census_bps",
            "module": "scrapers.housing.bls_census_housing_usa_scraper",
            "fn":     "main",
            "kwargs": {"dataset": "permits"},
        },
    ],
    "GBR": [{"source": "ons",      "module": "scrapers.housing.ons_housing_scraper",      "fn": "scrape_gbr_housing", "kwargs": {}}],
    "AUS": [{"source": "abs",      "module": "scrapers.housing.abs_housing_scraper",      "fn": "scrape_aus_housing", "kwargs": {}}],
    "NOR": [{"source": "ssb",      "module": "scrapers.housing.ssb_housing_scraper",      "fn": "scrape_nor_housing", "kwargs": {}}],
    "EU27": [{"source": "eurostat", "module": "scrapers.housing.eurostat_housing_scraper", "fn": "scrape_eu27_housing", "kwargs": {}}],
}


def run_source(country: str, cfg: dict, source_filter: str | None,
               mode: str, since: str | None, dry_run: bool) -> bool:
    source = cfg["source"]
    if source_filter and source != source_filter:
        return True

    if not is_release_due(product=PRODUCT, country=country,
                          source=source, as_of=TODAY):
        log.info("No release due today for %s/%s/%s — skipping.",
                 PRODUCT, country, source)
        return True

    if dry_run:
        log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, country, source)
    try:
        import importlib
        mod = importlib.import_module(cfg["module"])
        fn  = getattr(mod, cfg["fn"])

        sig = inspect.signature(fn)
        accepts_var_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        call_kwargs = dict(cfg["kwargs"])
        if accepts_var_kwargs or "mode" in sig.parameters:
            call_kwargs["mode"] = mode
        if accepts_var_kwargs or "since" in sig.parameters:
            call_kwargs["since"] = since

        fn(**call_kwargs)
        log.info("Completed scrape %s/%s/%s", PRODUCT, country, source)
    except Exception as exc:
        log.error("Failed %s/%s/%s: %s", PRODUCT, country, source, exc, exc_info=True)    except Exception as exc:
        log.error("Failed %s/%s/%s: %s", PRODUCT, country, source, exc, exc_info=True)
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
        description="Housing_Supply_and_Shelter_Inflation cloud scraper entry point"
    )
    parser.add_argument("--country", required=True)
    parser.add_argument("--source", default=None)
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None, metavar="YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("HOUSING & SHELTER INFLATION — run.py  [%s]", TODAY)
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
