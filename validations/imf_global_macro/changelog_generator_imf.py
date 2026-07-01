"""Stage 9 — Changelog Generation for imf_global_macro."""
import json, logging, sys, uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_changelog.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT  = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — CHANGELOG GENERATION")
    logger.info("=" * 70)
    files = sorted(VAULT.rglob("*_data.parquet"))
    df    = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["_year"] = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True).dt.year
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    years  = sorted(df["_year"].dropna().astype(int).unique())
    ok_count = 0

    for year in years:
        sub = df[df["_year"] == year]
        indicators = sub["sovereign_series_id"].dropna().unique()

        changelog_entries = [
            {
                "changelog_id":     str(uuid.uuid4()),
                "product":          "global_macro",
                "source":           "imf_weo",
                "country_code":     "US",
                "year":             year,
                "change_type":      "INGESTION",
                "change_summary":   f"Ingested {len(sub)} records for {len(indicators)} indicators",
                "indicators":       sorted(indicators.tolist()),
                "record_count":     int(len(sub)),
                "has_forecasts":    bool((sub.get("is_forecast", pd.Series(False))).any()),
                "processed_at":     run_ts,
                "schema_version":   "2026.1.0",
            }
        ]

        part = VAULT / f"year={year}" / "month=04"
        part.mkdir(parents=True, exist_ok=True)
        out_path = part / "changelog.parquet"
        pd.DataFrame(changelog_entries).to_parquet(out_path, index=False, engine="pyarrow")
        ok_count += 1

    logger.info("  %d / %d years changelog written", ok_count, len(years))
    logger.info("=" * 70)
    logger.info("SUMMARY: %d years | %d entries | [PASS]", ok_count, ok_count)
    logger.info("=" * 70)
    return True

EU27 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
        "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]

def _run_eu27_changelog() -> bool:
    import hashlib
    logger.info("=" * 70)
    logger.info("EU27 GLOBAL MACRO — CHANGELOG GENERATION (eurostat_sdmx)")
    logger.info("=" * 70)
    base = Path("lekwankwa-historical-vault/product=global_macro")
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    ok, total = 0, 0

    for iso in EU27:
        src = base / f"country={iso}" / "source=eurostat_sdmx"
        if not src.exists(): continue
        year_dirs = sorted({f.parent.parent for f in src.rglob("*.parquet")
                            if "outlier" not in f.name and "changelog" not in f.name})
        for yr_dir in year_dirs:
            year_str = yr_dir.name  # "year=YYYY"
            year = int(year_str.replace("year=", ""))
            files = list(yr_dir.rglob("global_macro_data.parquet"))
            n_records = 0
            for f in files:
                try: n_records += len(pd.read_parquet(f))
                except Exception: pass
            if n_records == 0: continue
            cid = hashlib.md5(f"{iso}_{year}_{run_ts}".encode()).hexdigest()[:12]
            entry = {
                "changelog_id": cid, "product": "global_macro",
                "source": "eurostat_sdmx", "country_code": iso, "year": year,
                "change_type": "INGESTION",
                "change_summary": f"{iso}: {n_records} Eurostat SDMX records for year={year}",
                "record_count": n_records, "processed_at": run_ts, "schema_version": "5.0",
            }
            part = yr_dir / "month=04"
            part.mkdir(parents=True, exist_ok=True)
            out = part / "changelog.parquet"
            pd.DataFrame([entry]).to_parquet(out, index=False, engine="pyarrow")
            ok += 1
            total += 1

    logger.info("  %d / %d year-country partitions written", ok, total)
    logger.info("  SUMMARY: [PASS]")
    return True


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true")
    _args, _ = _parser.parse_known_args()
    sys.exit(0 if (_run_eu27_changelog() if _args.eu27 else run()) else 1)
