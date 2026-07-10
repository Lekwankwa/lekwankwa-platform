"""
Stage 6 — Outlier Extraction: GBR / CAN.

Flags PRIMARY rows where observed_value deviates > 3σ from the
per-series rolling mean (window=12). Writes outliers.parquet per year partition.

Usage:
  python validations/non_eu/outlier_extractor_non_eu.py --product wages_and_employment
  python validations/non_eu/outlier_extractor_non_eu.py --product wages_and_employment --dry-run
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from validations.non_eu._loader import VAULT, active_countries, PRODUCT_FILENAMES, ALL_PRODUCTS

Z_THRESHOLD = 3.0
WINDOW      = 12

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _detect_outliers(df: pd.DataFrame) -> pd.DataFrame:
    if "sovereign_series_id" not in df.columns or "observed_value" not in df.columns:
        return pd.DataFrame()

    primary = df[df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"].copy()
    if primary.empty:
        return pd.DataFrame()

    primary["reporting_date"] = pd.to_datetime(primary["reporting_date"], errors="coerce")
    primary = primary.sort_values(["sovereign_series_id", "reporting_date"])

    parts = []
    for sid, grp in primary.groupby("sovereign_series_id", sort=False):
        vals = pd.to_numeric(grp["observed_value"], errors="coerce")
        if len(vals) < WINDOW + 1:
            continue
        roll_mean = vals.rolling(WINDOW, min_periods=3).mean()
        roll_std  = vals.rolling(WINDOW, min_periods=3).std()
        with np.errstate(divide="ignore", invalid="ignore"):
            z = ((vals - roll_mean) / roll_std).abs()
        flagged = grp[z > Z_THRESHOLD].copy()
        if not flagged.empty:
            flagged["outlier_z_score"]     = z[z > Z_THRESHOLD].values
            flagged["outlier_flag"]        = True
            flagged["outlier_detected_at"] = datetime.utcnow().isoformat() + "Z"
            parts.append(flagged)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def run(product: str, dry_run: bool = False) -> bool:
    countries = active_countries(product)
    logger.info("=" * 70)
    logger.info(f"NON-EU OUTLIER EXTRACTION — {product.upper()} (dry_run={dry_run})")
    logger.info("=" * 70)

    filename = PRODUCT_FILENAMES[product]
    base = VAULT / f"product={product}"
    total_flagged = 0
    year_reports: list[dict] = []

    for iso, (_, source, _) in countries.items():
        src = base / f"country={iso}" / f"source={source}"
        if not src.exists():
            continue

        frames = []
        for f in sorted(src.rglob(filename)):
            if "outlier" in f.name or "changelog" in f.name:
                continue
            try:
                frames.append(pd.read_parquet(f))
            except Exception:
                pass

        if not frames:
            continue

        country_df = pd.concat(frames, ignore_index=True)
        outliers   = _detect_outliers(country_df)
        if outliers.empty:
            logger.info(f"  {iso}: 0 outliers")
            continue

        total_flagged += len(outliers)
        logger.info(f"  {iso}: {len(outliers)} outlier rows flagged")

        if not dry_run and "reporting_date" in outliers.columns:
            outliers["_year"] = pd.to_datetime(outliers["reporting_date"], errors="coerce").dt.year
            for yr, grp in outliers.groupby("_year"):
                yr_dir = src / f"year={int(yr)}"
                if yr_dir.exists():
                    out_path = yr_dir / "outliers.parquet"
                    grp.drop(columns=["_year"]).to_parquet(out_path, index=False, compression="snappy")
                    year_reports.append({"iso": iso, "year": int(yr), "flagged": len(grp)})

    logger.info(f"\n  Total outlier rows flagged : {total_flagged:,}")
    logger.info(f"  Outlier files written      : {len(year_reports)}"
                + (" (dry run — skipped)" if dry_run else ""))
    logger.info("  [PASS] Outlier extraction complete (non-blocking)")

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product": product, "scope": "non_eu GBR/CAN",
        "z_threshold": Z_THRESHOLD, "total_flagged": total_flagged,
        "dry_run": dry_run, "files_written": year_reports, "overall": "PASS",
    }
    out = Path(f"{product}_non_eu_outlier_report.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--product", required=True, choices=ALL_PRODUCTS)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    sys.exit(0 if run(args.product, args.dry_run) else 1)


if __name__ == "__main__":
    main()
