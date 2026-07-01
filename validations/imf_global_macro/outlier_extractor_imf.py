"""Stage 8 — Outlier Extraction for imf_global_macro (annual Z-score, YoY % change)."""
import json, logging, sys, uuid
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_outlier_extraction.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT         = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
ZSCORE_THRESH = 3.0
YOY_THRESH    = 0.5   # 50% year-on-year change

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — OUTLIER EXTRACTION")
    logger.info("=" * 70)
    files = sorted(VAULT.rglob("*_data.parquet"))
    df    = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["_year"] = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
    df["observed_value"] = pd.to_numeric(df["observed_value"], errors="coerce")

    indicators = sorted(df["sovereign_series_id"].dropna().unique())
    total_outliers, by_year = 0, {}
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

    for ind in indicators:
        sub = df[df["sovereign_series_id"] == ind].sort_values("_year").copy()
        if len(sub) < 5:
            continue
        vals = sub["observed_value"].dropna()

        # Z-score on raw values
        mean, std = vals.mean(), vals.std()
        if std == 0:
            continue
        sub["_zscore"] = (sub["observed_value"] - mean) / std

        # YoY % change
        sub["_yoy"] = sub["observed_value"].pct_change().abs()

        outliers = sub[(sub["_zscore"].abs() >= ZSCORE_THRESH) | (sub["_yoy"] >= YOY_THRESH)].copy()
        if outliers.empty:
            continue

        outliers["outlier_type"]     = np.where(outliers["_zscore"].abs() >= ZSCORE_THRESH, "ZSCORE", "YOY_SPIKE")
        outliers["outlier_score"]    = outliers["_zscore"].round(3)
        outliers["outlier_yoy_pct"]  = outliers["_yoy"].round(4)
        outliers["outlier_detected_at"] = run_ts
        outliers = outliers.drop(columns=["_zscore","_yoy","_year"], errors="ignore")

        for _, row in outliers.iterrows():
            year = int(str(row.get("data_timestamp","2000"))[:4])
            by_year.setdefault(year, []).append(row.to_dict())

        total_outliers += len(outliers)
        logger.info("  %s: %d outliers", ind, len(outliers))

    # Write outlier files per year, grouped by month
    for year, rows in by_year.items():
        odf = pd.DataFrame(rows)
        if not odf.empty:
            odf['_month'] = pd.to_datetime(odf['data_timestamp']).dt.month
            for month, group in odf.groupby('_month'):
                part = VAULT / f"year={year}" / f"month={month:02d}"
                part.mkdir(parents=True, exist_ok=True)
                out_path = part / "outliers.parquet"
                group.drop(columns=['_month']).to_parquet(out_path, index=False, engine="pyarrow")
        else:
            part = VAULT / f"year={year}" / "month=01"
            part.mkdir(parents=True, exist_ok=True)
            out_path = part / "outliers.parquet"
            odf.to_parquet(out_path, index=False, engine="pyarrow")

    logger.info("=" * 70)
    logger.info("SUMMARY: %d total outliers across %d years", total_outliers, len(by_year))
    logger.info("=" * 70)
    return True

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
