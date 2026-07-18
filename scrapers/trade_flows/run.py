"""
scrapers/trade_flows/run.py — Lekwankwa Corporation
Cloud Scheduler entry point for trade_flows across all countries.

Usage:
    python scrapers/trade_flows/run.py --country USA
    python scrapers/trade_flows/run.py --country GBR --mode full
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
}


def _repair_vault_schema(country: str, source: str) -> None:
    """Normalize dtype of the 'source' column across existing vault parquet
    parts to prevent ArrowTypeError when pandas/pyarrow attempt to merge
    schemas across partitions during read_parquet. Some historical parts were
    written with a dictionary-encoded 'source' column while newer parts use a
    plain string column; this repairs any drifted parts in place before the
    scraper reads/merges them.
    """
    import glob

    import pyarrow as pa
    import pyarrow.parquet as pq

    vault_glob = f"data_vault/{PRODUCT}/{country}/{source}/*.parquet"
    for fp in glob.glob(vault_glob):
        try:
            table = pq.read_table(fp)
        except Exception:
            continue
        if "source" in table.column_names:
            idx = table.schema.get_field_index("source")
            col = table.column("source")
            if pa.types.is_dictionary(col.type):
                table = table.set_column(idx, "source", col.cast(pa.string()))
                pq.write_table(table, fp)


def main():
    parser = argparse.ArgumentParser(
        description="trade_flows cloud scraper entry point"
    "GBR":  {"source": "ons",          "module": "scrapers.trade_flows.ons_trade_scraper",           "fn": "scrape_gbr_trade"},
    "CAN":  {"source": "statcan",      "module": "scrapers.trade_flows.statcan_trade_scraper",       "fn": "scrape_can_trade"},
    "EU27": {"source": "eurostat",     "module": "scrapers.trade_flows.eurostat_trade_scraper",      "fn": "scrape_eu27_trade"},
}


def main():
    parser = argparse.ArgumentParser(
        description="trade_flows cloud scraper entry point"
    )
    parser.add_argument("--country", required=True)
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None, metavar="YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TRADE FLOWS — run.py  [%s]", TODAY)
    log.info("Country: %s | Mode: %s", args.country, args.mode)
    log.info("=" * 60)

    cfg = COUNTRY_ROUTER.get(args.country)
    if not cfg:
        log.error("No router entry for country=%s", args.country)
        sys.exit(1)

    source = cfg["source"]
    if not is_release_due(product=PRODUCT, country=args.country,
                          source=source, as_of=TODAY):
        log.info("No release due today for %s/%s/%s — skipping.",
                 PRODUCT, args.country, source)
        sys.exit(0)
    try:
        import importlib
        from scrapers.utilities.call_scraper_entry import call_scraper_entry
        try:
            _repair_vault_schema(args.country, source)
        except Exception as repair_exc:
            log.warning("Vault schema repair skipped for %s/%s: %s",
                        args.country, source, repair_exc)
        mod = importlib.import_module(cfg["module"])
        fn  = getattr(mod, cfg["fn"])
        call_scraper_entry(fn, args.mode, args.since, cfg.get("kwargs", {}))        import importlib
        from scrapers.utilities.call_scraper_entry import call_scraper_entry
        mod = importlib.import_module(cfg["module"])
        fn  = getattr(mod, cfg["fn"])
        call_scraper_entry(fn, args.mode, args.since, cfg.get("kwargs", {}))
        log.info("Completed scrape %s/%s", PRODUCT, args.country)
    except Exception as exc:
        log.error("Failed %s/%s: %s", PRODUCT, args.country, exc, exc_info=True)
        try:
            from tools.self_healing.handler import handle_exception
            handle_exception(
                program=__file__, exception=exc,
                context={"product": PRODUCT, "country": args.country,
                         "source": source, "run_date": TODAY, "layer": "SCRAPER"},
            )
        except ImportError:
            pass
        sys.exit(1)

    val = run_9_stage_validation(product=PRODUCT, country=args.country)
    if val.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(
            program=__file__,
            context={"product": PRODUCT, "country": args.country, "source": source,
                     "run_date": TODAY, "layer": "VALIDATION"},
            result=val,
        )
        sys.exit(1)

    audit = run_post_delta_audit(product=PRODUCT, country=args.country)
    if audit.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(
            program=__file__,
            context={"product": PRODUCT, "country": args.country, "source": source,
                     "run_date": TODAY, "layer": "LIVE_FEED_AUDIT"},
            result=audit,
        )
        sys.exit(1)

    from tools.trigger_downstream import trigger_all_metadata
    trigger_all_metadata()
    sys.exit(0)


if __name__ == "__main__":
    main()
