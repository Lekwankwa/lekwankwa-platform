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
}


def _install_parquet_schema_safety_patch() -> None:
    """
    Guard against ArrowTypeError when a vault's partitioned parquet dataset
    contains mixed-encoded columns (e.g. plain `string` vs
    `dictionary<values=string, indices=int32>` for the same field, such as
    `source` in census_ft900). PyArrow's dataset factory refuses to unify
    those types by default; this patch falls back to a manual per-file
    read + cast + concat with schema promotion.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    if getattr(pd.read_parquet, "_lekwankwa_schema_safe", False):
        return  # already patched

    _original_read_parquet = pd.read_parquet

    def _schema_safe_read_parquet(path, *args, **kwargs):
        try:
            return _original_read_parquet(path, *args, **kwargs)
        except pa.ArrowTypeError:
            log.warning("Schema drift detected reading %s — normalizing "
                        "dictionary vs string columns before merge.", path)
            dataset = pq.ParquetDataset(path, use_legacy_dataset=False)
            tables = []
            for frag in dataset.fragments:
                t = frag.to_table()
                for i, field in enumerate(t.schema):
                    if pa.types.is_dictionary(field.type):
                        t = t.set_column(i, field.name, t.column(i).cast(pa.string()))
                tables.append(t)
            merged = pa.concat_tables(tables, promote=True)
            return merged.to_pandas()

    _schema_safe_read_parquet._lekwankwa_schema_safe = True
    pd.read_parquet = _schema_safe_read_parquet


def main():
    parser = argparse.ArgumentParser(
        description="trade_flows cloud scraper entry point"
COUNTRY_ROUTER: dict[str, dict] = {
    "USA":  {"source": "census_ft900", "module": "scrapers.trade_flows.census_ft900_usa_scraper",   "fn": "main"},
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
        log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, args.country, source)
        sys.exit(0)

    _install_parquet_schema_safety_patch()

    try:
        import importlib
        from scrapers.utilities.call_scraper_entry import call_scraper_entry                          source=source, as_of=TODAY):
        log.info("No release due today for %s/%s/%s — skipping.",
                 PRODUCT, args.country, source)
        sys.exit(0)

    if args.dry_run:
        log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, args.country, source)
        sys.exit(0)

    try:
        import importlib
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
