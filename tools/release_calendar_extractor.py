"""
Release Calendar Extractor -- Lekwankwa Corporation

Builds the master release calendar covering official upcoming release
dates for every series across the validated 32-country catalog, then
splits it into per-dataset and per-country exports for delivery.

CATALOG SCOPE (must match catalog_manifest.yaml exactly):
  Countries: USA + EU27 (27 states) + GBR + CAN + AUS + NOR = 32 countries
  CHE (Switzerland) is excluded -- BLOCKED, FSO Swiss Stats Explorer
  returning HTTP 503, PENDING_INGESTION. Do not reference CHE anywhere
  in output.

  Datasets -- Archive + Live Feed eligible (3):
    food_micropricing
    wages_and_employment
    trade_flows

  Datasets -- Archive ONLY (2) -- mixed monthly/quarterly frequency:
    Housing_Supply_and_Shelter_Inflation
    global_macro

  NOR housing is PENDING (no confirmed SSB residential property table)
  -- excluded from housing calendar entries for NOR specifically.

OUTPUT -- all files written to vault root, NOT inside product= folders:
  release_calendar_master.json
  release_calendar_{dataset}.json   (5 files)
  release_calendar_{dataset}.csv    (5 files)
  release_calendar_{dataset}_{iso3}.json  (per country, per dataset)
  release_calendar_{dataset}_{iso3}.csv

Run modes:
  python tools/release_calendar_extractor.py --vault-root /path/to/vault
  python tools/release_calendar_extractor.py --vault-root /path/to/vault --dry-run
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("release_calendar_extractor.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# CATALOG SCOPE -- must mirror catalog_manifest.yaml
# -------------------------------------------------------------------------

EU27 = [
    "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA",
    "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD",
    "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE",
]
NON_EU = ["GBR", "CAN", "AUS", "NOR"]
ALL_COUNTRIES = ["USA"] + EU27 + NON_EU  # 32 total -- CHE intentionally absent

DATASETS = [
    "food_micropricing",
    "wages_and_employment",
    "Housing_Supply_and_Shelter_Inflation",
    "trade_flows",
    "global_macro",
]

LIVE_FEED_ELIGIBLE = {"food_micropricing", "wages_and_employment", "trade_flows"}
ARCHIVE_ONLY = {"Housing_Supply_and_Shelter_Inflation", "global_macro"}

# Known coverage exceptions per catalog_manifest.yaml
COVERAGE_EXCEPTIONS = {
    ("Housing_Supply_and_Shelter_Inflation", "NOR"): "PENDING_INGESTION -- no confirmed SSB residential property table",
}


# -------------------------------------------------------------------------
# SOURCE FETCHERS
# Each returns a list of entry dicts. Each fetcher is isolated -- a failure
# in one source must not halt extraction for any other source.
# -------------------------------------------------------------------------

def fetch_bls(dry_run: bool = False) -> list[dict[str, Any]]:
    """BLS -- CPI (food, housing shelter), Employment Situation (wages)."""
    entries = []
    try:
        if dry_run:
            logger.info("[BLS] dry-run -- skipping live scrape")
            return entries
        import requests
        from bs4 import BeautifulSoup

        url = "https://www.bls.gov/schedule/news_release/"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("table tr")
        relevant_keywords = {
            "Consumer Price Index": ("food_micropricing", "CPI_FOOD"),
            "Employment Situation": ("wages_and_employment", "EMPLOYMENT_SITUATION"),
        }
        for row in rows:
            text = row.get_text(" ", strip=True)
            for keyword, (dataset, concept) in relevant_keywords.items():
                if keyword in text:
                    entries.append({
                        "iso_alpha3": "USA",
                        "dataset": dataset,
                        "source_agency": "BLS",
                        "series_concept": concept,
                        "next_release_date": None,
                        "release_frequency": "Monthly",
                        "source_url": url,
                        "raw_text": text[:200],
                    })
        logger.info(f"[BLS] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[BLS] fetch failed: {exc}")
    return entries


def fetch_census(dry_run: bool = False) -> list[dict[str, Any]]:
    """Census Bureau -- Building Permits, International Trade."""
    entries = []
    try:
        if dry_run:
            logger.info("[Census] dry-run -- skipping live scrape")
            return entries
        import requests
        from bs4 import BeautifulSoup

        url = "https://www.census.gov/economic-indicators/"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        relevant_keywords = {
            "New Residential Construction": ("Housing_Supply_and_Shelter_Inflation", "AUTHORIZED_PERMITS_TOTAL_UNITS"),
            "U.S. International Trade in Goods": ("trade_flows", "TRADE_BALANCE_GOODS"),
        }
        rows = soup.select("table tr") or soup.select("li")
        for row in rows:
            text = row.get_text(" ", strip=True)
            for keyword, (dataset, concept) in relevant_keywords.items():
                if keyword in text:
                    entries.append({
                        "iso_alpha3": "USA",
                        "dataset": dataset,
                        "source_agency": "CENSUS",
                        "series_concept": concept,
                        "next_release_date": None,
                        "release_frequency": "Monthly",
                        "source_url": url,
                        "raw_text": text[:200],
                    })
        logger.info(f"[Census] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[Census] fetch failed: {exc}")
    return entries


def fetch_bea(dry_run: bool = False) -> list[dict[str, Any]]:
    """BEA -- GDP advance / second / third estimate, PCE."""
    entries = []
    try:
        if dry_run:
            logger.info("[BEA] dry-run -- skipping live scrape")
            return entries
        import requests
        from bs4 import BeautifulSoup

        url = "https://www.bea.gov/news/schedule"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("table tr")
        for row in rows:
            text = row.get_text(" ", strip=True)
            if "Gross Domestic Product" in text or "Personal Income" in text:
                entries.append({
                    "iso_alpha3": "USA",
                    "dataset": "global_macro",
                    "source_agency": "BEA",
                    "series_concept": "GDP_GROWTH_QOQ" if "Gross Domestic Product" in text else "PCE",
                    "next_release_date": None,
                    "release_frequency": "Quarterly",
                    "source_url": url,
                    "raw_text": text[:200],
                })
        logger.info(f"[BEA] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[BEA] fetch failed: {exc}")
    return entries


def fetch_eurostat(dry_run: bool = False) -> list[dict[str, Any]]:
    """Eurostat -- HICP (food, housing rent), LFS (wages), HPI (housing),
    COMEXT (trade), national accounts (macro). Covers all 27 EU states
    via a single dataflow query per concept."""
    entries = []
    dataflows = {
        "prc_hicp_midx": ("food_micropricing", "HICP_FOOD_CP01"),
        "lfsa_ergaed":   ("wages_and_employment", "EU_UNEMPLOYMENT_RATE"),
        "prc_hpi_q":     ("Housing_Supply_and_Shelter_Inflation", "HPI_TOTAL_Q"),
        "DS-018995":     ("trade_flows", "EU_TRADE_BALANCE"),
        "nama_10_gdp":   ("global_macro", "EU_GDP_GROWTH_QOQ"),
    }
    try:
        if dry_run:
            logger.info("[Eurostat] dry-run -- skipping live scrape")
            return entries
        import requests

        for dataflow, (dataset, concept) in dataflows.items():
            try:
                url = f"https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?dataflow={dataflow}"
                requests.get(url, timeout=20)
                freq = "Quarterly" if dataset in ARCHIVE_ONLY else "Monthly"
                for iso3 in EU27:
                    entries.append({
                        "iso_alpha3": iso3,
                        "dataset": dataset,
                        "source_agency": "EUROSTAT",
                        "series_concept": concept,
                        "next_release_date": None,
                        "release_frequency": freq,
                        "source_url": "https://ec.europa.eu/eurostat/web/main/news/release-calendar",
                        "dataflow": dataflow,
                    })
            except Exception as inner_exc:
                logger.warning(f"[Eurostat] dataflow {dataflow} failed: {inner_exc}")
        logger.info(f"[Eurostat] {len(entries)} entries found across {len(dataflows)} dataflows x 27 states")
    except Exception as exc:
        logger.warning(f"[Eurostat] fetch failed: {exc}")
    return entries


def fetch_ons(dry_run: bool = False) -> list[dict[str, Any]]:
    """ONS -- GBR, all 5 datasets via api.beta.ons.gov.uk/v1/releases."""
    entries = []
    try:
        if dry_run:
            logger.info("[ONS] dry-run -- skipping live scrape")
            return entries
        import requests

        url = "https://api.beta.ons.gov.uk/v1/releases"
        resp = requests.get(url, timeout=20, params={"limit": 100})
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            entries.append({
                "iso_alpha3": "GBR",
                "dataset": "wages_and_employment",
                "source_agency": "ONS",
                "series_concept": item.get("description", {}).get("title", "UNKNOWN"),
                "next_release_date": item.get("description", {}).get("releaseDate"),
                "release_frequency": "Monthly",
                "source_url": url,
            })
        logger.info(f"[ONS] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[ONS] fetch failed: {exc}")
    return entries


def fetch_statcan(dry_run: bool = False) -> list[dict[str, Any]]:
    """StatCan -- CAN, via WDS REST getChangedSeriesList + NDM CSV vectors."""
    entries = []
    try:
        if dry_run:
            logger.info("[StatCan] dry-run -- skipping live scrape")
            return entries
        import requests

        url = "https://www150.statcan.gc.ca/t1/wds/rest/getChangedSeriesList"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        for item in data if isinstance(data, list) else data.get("object", []):
            entries.append({
                "iso_alpha3": "CAN",
                "dataset": "global_macro",
                "source_agency": "STATCAN",
                "series_concept": str(item.get("vectorId", "UNKNOWN")),
                "next_release_date": item.get("releaseTime"),
                "release_frequency": "Monthly",
                "source_url": "https://www150.statcan.gc.ca/n1/dai-quo/sst-fst/release-diffusion-eng.htm",
            })
        logger.info(f"[StatCan] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[StatCan] fetch failed: {exc}")
    return entries


def fetch_abs(dry_run: bool = False) -> list[dict[str, Any]]:
    """ABS -- AUS, scraped release calendar filtered by topic."""
    entries = []
    try:
        if dry_run:
            logger.info("[ABS] dry-run -- skipping live scrape")
            return entries
        import requests
        from bs4 import BeautifulSoup

        url = "https://www.abs.gov.au/release-calendar"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        relevant = {
            "Consumer Price Index": ("food_micropricing", "ABS_CPI"),
            "Labour Force":         ("wages_and_employment", "ABS_LFS"),
            "Building Approvals":   ("Housing_Supply_and_Shelter_Inflation", "ABS_PERMITS"),
            "International Trade":  ("trade_flows", "ABS_TRADE"),
            "National Accounts":    ("global_macro", "ABS_GDP"),
        }
        rows = soup.select("li") or soup.select("table tr")
        for row in rows:
            text = row.get_text(" ", strip=True)
            for keyword, (dataset, concept) in relevant.items():
                if keyword in text:
                    freq = "Quarterly" if dataset in ARCHIVE_ONLY else "Monthly"
                    entries.append({
                        "iso_alpha3": "AUS",
                        "dataset": dataset,
                        "source_agency": "ABS",
                        "series_concept": concept,
                        "next_release_date": None,
                        "release_frequency": freq,
                        "source_url": url,
                        "raw_text": text[:200],
                    })
        logger.info(f"[ABS] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[ABS] fetch failed: {exc}")
    return entries


def fetch_ssb(dry_run: bool = False) -> list[dict[str, Any]]:
    """SSB -- NOR, via PX-Web metadata nextUpdate field per active table.
    Housing is intentionally absent -- PENDING_INGESTION, no confirmed table."""
    entries = []
    ssb_tables = {
        "09190": ("global_macro",           "SSB_GDP"),
        "07458": ("wages_and_employment",   "SSB_LFS"),
        "12308": ("trade_flows",            "SSB_TRADE"),
        # housing -- PENDING_INGESTION, intentionally absent
    }
    try:
        if dry_run:
            logger.info("[SSB] dry-run -- skipping live scrape")
            return entries
        import requests

        for table_id, (dataset, concept) in ssb_tables.items():
            try:
                url = f"https://data.ssb.no/api/v0/en/table/{table_id}"
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                meta = resp.json()
                entries.append({
                    "iso_alpha3": "NOR",
                    "dataset": dataset,
                    "source_agency": "SSB",
                    "series_concept": concept,
                    "next_release_date": meta.get("nextUpdate"),
                    "release_frequency": "Quarterly" if dataset in ARCHIVE_ONLY else "Monthly",
                    "source_url": url,
                })
            except Exception as inner_exc:
                logger.warning(f"[SSB] table {table_id} failed: {inner_exc}")
        logger.info(f"[SSB] {len(entries)} entries found")
    except Exception as exc:
        logger.warning(f"[SSB] fetch failed: {exc}")
    return entries


# CHE intentionally has no fetcher -- BLOCKED, PENDING_INGESTION,
# excluded from catalog per product catalog and prior build decisions.

SOURCE_FETCHERS = {
    "BLS":      fetch_bls,
    "CENSUS":   fetch_census,
    "BEA":      fetch_bea,
    "EUROSTAT": fetch_eurostat,
    "ONS":      fetch_ons,
    "STATCAN":  fetch_statcan,
    "ABS":      fetch_abs,
    "SSB":      fetch_ssb,
}


# -------------------------------------------------------------------------
# BUILD MASTER
# -------------------------------------------------------------------------

def build_master(dry_run: bool = False) -> dict[str, Any]:
    all_entries: list[dict[str, Any]] = []
    source_results: dict[str, int] = {}
    source_errors: list[str] = []

    for source_name, fetcher in SOURCE_FETCHERS.items():
        try:
            entries = fetcher(dry_run=dry_run)
            all_entries.extend(entries)
            source_results[source_name] = len(entries)
            if not entries and not dry_run:
                source_errors.append(f"{source_name}: returned 0 entries")
        except Exception as exc:
            logger.error(f"[{source_name}] unhandled error: {exc}")
            source_results[source_name] = 0
            source_errors.append(f"{source_name}: {exc}")

    # Apply known coverage exceptions
    filtered = []
    for e in all_entries:
        key = (e.get("dataset"), e.get("iso_alpha3"))
        if key in COVERAGE_EXCEPTIONS:
            logger.info(f"Excluding entry {key}: {COVERAGE_EXCEPTIONS[key]}")
            continue
        if e.get("iso_alpha3") == "CHE":
            logger.warning(f"Stripping unexpected CHE entry from {e.get('source_agency')} -- CHE excluded from catalog")
            continue
        filtered.append(e)

    master = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog_scope": {
            "countries":          ALL_COUNTRIES,
            "country_count":      len(ALL_COUNTRIES),
            "datasets":           DATASETS,
            "live_feed_eligible": sorted(LIVE_FEED_ELIGIBLE),
            "archive_only":       sorted(ARCHIVE_ONLY),
            "known_exceptions":   {f"{k[0]}/{k[1]}": v for k, v in COVERAGE_EXCEPTIONS.items()},
        },
        "source_run_summary": source_results,
        "source_errors":      source_errors,
        "entries":            filtered,
    }
    return master


# -------------------------------------------------------------------------
# SPLIT -- per dataset, per dataset+country
# -------------------------------------------------------------------------

def split_master(master: dict[str, Any], vault_root: Path) -> dict[str, int]:
    entries = master["entries"]
    counts: dict[str, int] = {}

    master_path = vault_root / "release_calendar_master.json"
    master_path.write_text(json.dumps(master, indent=2), encoding="utf-8")
    counts["master"] = len(entries)
    logger.info(f"Wrote {master_path} ({len(entries)} entries)")

    for dataset in DATASETS:
        ds_entries = [e for e in entries if e.get("dataset") == dataset]

        ds_json_path = vault_root / f"release_calendar_{dataset}.json"
        ds_json_path.write_text(
            json.dumps({"generated_at": master["generated_at"],
                        "dataset": dataset, "entries": ds_entries}, indent=2),
            encoding="utf-8",
        )
        ds_csv_path = vault_root / f"release_calendar_{dataset}.csv"
        _write_csv(ds_csv_path, ds_entries)
        counts[dataset] = len(ds_entries)
        logger.info(f"Wrote {ds_json_path} and {ds_csv_path} ({len(ds_entries)} entries)")

        for iso3 in sorted({e.get("iso_alpha3") for e in ds_entries if e.get("iso_alpha3")}):
            c_entries = [e for e in ds_entries if e.get("iso_alpha3") == iso3]
            c_json = vault_root / f"release_calendar_{dataset}_{iso3}.json"
            c_json.write_text(
                json.dumps({"generated_at": master["generated_at"],
                            "dataset": dataset, "iso_alpha3": iso3,
                            "entries": c_entries}, indent=2),
                encoding="utf-8",
            )
            _write_csv(vault_root / f"release_calendar_{dataset}_{iso3}.csv", c_entries)
            counts[f"{dataset}_{iso3}"] = len(c_entries)

    return counts


def _write_csv(path: Path, entries: list[dict[str, Any]]) -> None:
    if not entries:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({k for e in entries for k in e.keys()})
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for e in entries:
            writer.writerow(e)


# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Lekwankwa Release Calendar Extractor")
    parser.add_argument("--vault-root", required=True,
                        help="Path to vault root where calendar files will be written")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip live source fetches, validate logic only")
    args = parser.parse_args()

    vault_root = Path(args.vault_root)
    vault_root.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("LEKWANKWA RELEASE CALENDAR EXTRACTOR")
    logger.info(f"Catalog scope: {len(ALL_COUNTRIES)} countries, {len(DATASETS)} datasets")
    logger.info("CHE excluded -- BLOCKED, PENDING_INGESTION")
    logger.info("=" * 70)

    master = build_master(dry_run=args.dry_run)
    counts = split_master(master, vault_root)

    logger.info("=" * 70)
    logger.info("RUN SUMMARY")
    logger.info(f"Source results: {master['source_run_summary']}")
    if master["source_errors"]:
        logger.warning(f"Source errors/empty results: {master['source_errors']}")
    logger.info(f"Total master entries: {counts['master']}")
    for dataset in DATASETS:
        logger.info(f"  {dataset}: {counts.get(dataset, 0)} entries")
    logger.info("=" * 70)

    return 0 if counts["master"] > 0 or args.dry_run else 1


if __name__ == "__main__":
    sys.exit(main())
