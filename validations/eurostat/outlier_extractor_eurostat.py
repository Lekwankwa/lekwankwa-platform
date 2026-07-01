"""
Outlier extraction — EU27 Eurostat products.

Flags PRIMARY rows where observed_value deviates > 3 standard deviations
from the per-series rolling mean (window=12 months / 4 quarters).
Writes flagged rows to outliers.parquet in each year partition.

Usage:
  python validations/eurostat/outlier_extractor_eurostat.py --product wages_and_employment
  python validations/eurostat/outlier_extractor_eurostat.py --product wages_and_employment --dry-run
"""
from __future__ import annotations

import argparse, io, json, logging, sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

VAULT  = Path("lekwankwa-historical-vault")
SOURCE = "eurostat_sdmx"
EU27   = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
          "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

Z_THRESHOLD = 3.0   # flag if |z-score| > 3
WINDOW      = 12    # rolling window in periods

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)


def _normalize_schema(df: pd.DataFrame, product: str) -> pd.DataFrame:
    if product != "food_micropricing":
        return df
    rename = {}
    if "observation_period" in df.columns and "reporting_date" not in df.columns:
        rename["observation_period"] = "reporting_date"
    if "observed_price_local" in df.columns and "observed_value" not in df.columns:
        rename["observed_price_local"] = "observed_value"
    if "standard_name" in df.columns and "macro_metric_name" not in df.columns:
        rename["standard_name"] = "macro_metric_name"
    return df.rename(columns=rename) if rename else df


def _detect_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Return subset of df with outlier_flag=True added."""
    if "sovereign_series_id" not in df.columns or "observed_value" not in df.columns:
        return pd.DataFrame()

    primary = df[df.get("confidence_tier", pd.Series(["PRIMARY"] * len(df))) == "PRIMARY"].copy()
    if primary.empty:
        return pd.DataFrame()

    primary["reporting_date"] = pd.to_datetime(primary["reporting_date"], errors="coerce")
    primary = primary.sort_values(["sovereign_series_id", "reporting_date"])

    outlier_parts = []
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
            flagged["outlier_z_score"]    = z[z > Z_THRESHOLD].values
            flagged["outlier_flag"]       = True
            flagged["outlier_detected_at"] = datetime.utcnow().isoformat() + "Z"
            outlier_parts.append(flagged)

    return pd.concat(outlier_parts, ignore_index=True) if outlier_parts else pd.DataFrame()


def run(product: str, dry_run: bool = False) -> bool:
    logger.info("=" * 70)
    logger.info(f"EU27 OUTLIER EXTRACTION — {product.upper()} (dry_run={dry_run})")
    logger.info("=" * 70)

    base = VAULT / f"product={product}"
    total_flagged = 0
    year_reports: list[dict] = []

    for iso in EU27:
        src = base / f"country={iso}" / f"source={SOURCE}"
        if not src.exists(): continue

        # Collect all data for this country to compute rolling z-scores across time
        frames = []
        for f in sorted(src.rglob("*.parquet")):
            if "outlier" in f.name or "changelog" in f.name: continue
            try: frames.append(pd.read_parquet(f))
            except Exception: pass

        if not frames: continue
        country_df = _normalize_schema(pd.concat(frames, ignore_index=True), product)
        outliers   = _detect_outliers(country_df)
        if outliers.empty: continue

        total_flagged += len(outliers)

        if not dry_run:
            # Write outliers partitioned by year into the year= directories
            if "reporting_date" in outliers.columns:
                outliers["_year"] = pd.to_datetime(outliers["reporting_date"], errors="coerce").dt.year
                for yr, grp in outliers.groupby("_year"):
                    yr_dir = src / f"year={int(yr)}"
                    if yr_dir.exists():
                        out_path = yr_dir / "outliers.parquet"
                        grp.drop(columns=["_year"]).to_parquet(out_path, index=False, compression="snappy")
                        year_reports.append({"iso": iso, "year": int(yr), "flagged": len(grp)})

    logger.info(f"  Total outlier rows flagged : {total_flagged:,}")
    logger.info(f"  Outlier files written      : {len(year_reports)}" + (" (dry run — skipped)" if dry_run else ""))

    overall = "PASS"  # outlier extraction is non-blocking
    report  = {
        "timestamp":    datetime.utcnow().isoformat() + "Z",
        "product":      product,
        "scope":        "EU27 eurostat_sdmx",
        "z_threshold":  Z_THRESHOLD,
        "total_flagged": total_flagged,
        "dry_run":      dry_run,
        "files_written": year_reports,
        "overall":      overall,
    }
    out = Path(f"{product}_eu27_outlier_report.json")
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"  Report: {out}")
    return True


def main():
    parser = argparse.ArgumentParser(description="EU27 outlier extraction")
    parser.add_argument("--product", required=True,
                        choices=["wages_and_employment",
                                 "Housing_Supply_and_Shelter_Inflation",
                                 "trade_flows", "global_macro",
                                 "food_micropricing"])
    parser.add_argument("--dry-run", action="store_true",
                        help="Detect but do not write outliers.parquet files")
    args = parser.parse_args()
    sys.exit(0 if run(args.product, args.dry_run) else 1)


if __name__ == "__main__":
    main()
