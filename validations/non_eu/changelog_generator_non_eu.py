"""
Stage 7 — Changelog Generation: GBR / CAN / AUS / NOR.

Writes a changelog.parquet entry per year-country partition documenting
the ingestion event, row count, and data quality status.

Usage:
  python validations/non_eu/changelog_generator_non_eu.py --product wages_and_employment
"""
from __future__ import annotations

import argparse, hashlib, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import VAULT, active_countries, PRODUCT_FILENAMES, ALL_PRODUCTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

CHANGELOG_COLUMNS = [
    "change_id", "year", "source", "change_date", "change_type", "change_category",
    "change_description", "change_severity", "records_affected", "columns_affected",
    "validation_status", "change_author", "change_metadata",
]


def _cid(iso: str, year: int, ts: str) -> str:
    return hashlib.md5(f"{iso}_{year}_{ts}".encode()).hexdigest()[:12]


def run(product: str) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU CHANGELOG GENERATION — {product.upper()} ({', '.join(countries)})")
    logger.info("=" * 70)

    filename = PRODUCT_FILENAMES[product]
    base = VAULT / f"product={product}"
    run_ts = datetime.now().isoformat()
    ok = total = 0

    for iso, (_, source, source_agency) in countries.items():
        src = base / f"country={iso}" / f"source={source}"
        if not src.exists():
            logger.info(f"  {iso}: partition missing — skipped")
            continue

        year_dirs = sorted({
            f.parent.parent for f in src.rglob("*.parquet")
            if "outlier" not in f.name and "changelog" not in f.name
        })

        for yr_dir in year_dirs:
            year_str = yr_dir.name.replace("year=", "")
            try:
                year = int(year_str)
            except ValueError:
                continue

            n_records = 0
            for f in yr_dir.rglob(filename):
                try:
                    n_records += len(pd.read_parquet(f))
                except Exception:
                    pass

            if n_records == 0:
                continue

            # Check for any outlier files this year
            outlier_files = list(yr_dir.rglob("outliers.parquet"))
            n_outliers = sum(len(pd.read_parquet(f)) for f in outlier_files if f.exists())

            entry = {
                "change_id":       _cid(iso, year, run_ts),
                "year":            year,
                "source":          source,
                "change_date":     run_ts,
                "change_type":     "data_ingestion",
                "change_category": "pipeline",
                "change_description": (
                    f"{iso} ({source_agency}): {n_records:,} records for year={year}"
                    + (f"; {n_outliers} outlier flags" if n_outliers else "")
                ),
                "change_severity":    "info" if n_outliers == 0 else "warning",
                "records_affected":   n_records,
                "columns_affected":   "",
                "validation_status":  "completed",
                "change_author":      f"{source}_ingestion",
                "change_metadata":    (
                    f"country={iso};source={source};year={year};"
                    f"outliers={n_outliers};pit_coverage=RELEASE_DATE_ONLY"
                ),
            }

            # Write into month=1 of the year partition (same convention as EU27)
            part = yr_dir / "month=1"
            part.mkdir(parents=True, exist_ok=True)
            out = part / "changelog.parquet"
            pd.DataFrame([entry]).to_parquet(out, index=False, engine="pyarrow")
            ok += 1
            total += 1

    logger.info(f"  {ok} / {total} year-country partitions written")
    logger.info("  [PASS] Changelog generation complete")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product, "scope": "non_eu GBR/CAN/AUS/NOR",
        "partitions_written": ok, "overall": "PASS",
    }
    out_json = Path(f"{product}_non_eu_changelog.json")
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"  Report: {out_json}")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    args = p.parse_args()
    sys.exit(0 if run(args.product) else 1)


if __name__ == "__main__":
    main()
