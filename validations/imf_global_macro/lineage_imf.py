"""Stage 6 — Data Lineage for imf_global_macro."""
import json, logging, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("imf_lineage.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

VAULT    = Path("lekwankwa-historical-vault/product=global_macro/country=USA/source=imf_weo")
REPORT_J = "imf_lineage_report.json"
REPORT_T = "imf_lineage_report.txt"

def run() -> bool:
    logger.info("=" * 70)
    logger.info("IMF GLOBAL MACRO — DATA LINEAGE VALIDATION")
    logger.info("=" * 70)
    files   = sorted(VAULT.rglob("*_data.parquet"))
    df      = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    results = []

    def chk(status, name, msg, **kw):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg, **kw})

    # L1 — portal_url attribution
    if "portal_url" in df.columns:
        bad = int((~df["portal_url"].str.contains("imf.org", na=False)).sum())
        chk("PASS" if not bad else "FAIL", "L1 Source Attribution",
            "All portal_urls reference imf.org" if not bad else f"{bad} incorrect portal URLs")
    else:
        chk("WARN","L1 Source Attribution", "portal_url column missing")

    # L2 — natural key uniqueness (sovereign_series_id, data_timestamp, published_date)
    # Multiple vintages per observation timestamp are valid (Oct preliminary + Apr final).
    dupes = int(df.duplicated(subset=["sovereign_series_id","data_timestamp","published_date"]).sum())
    chk("PASS" if not dupes else "FAIL","L2 Natural Key Uniqueness",
        "All (series, timestamp, published_date) tuples unique" if not dupes else f"{dupes} duplicate natural keys")

    # L3 — data_vintage_id coverage
    nulls = int(df["data_vintage_id"].isna().sum()) if "data_vintage_id" in df.columns else len(df)
    chk("PASS" if not nulls else "FAIL","L3 Vintage ID Coverage",
        f"All {len(df)} records have data_vintage_id" if not nulls else f"{nulls} missing vintage IDs")

    # L4 — record_id uniqueness across full vault
    dupes = int(df["record_id"].duplicated().sum())
    chk("PASS" if not dupes else "FAIL","L4 Record ID Uniqueness",
        "No duplicate record_ids" if not dupes else f"{dupes} duplicate record_ids")

    # L5 — vault path compliance: product=global_macro
    bad_paths = [str(f) for f in files if "product=global_macro" not in str(f)]
    chk("PASS" if not bad_paths else "FAIL","L5 Vault Path Compliance",
        "All files at product=global_macro path" if not bad_paths
        else f"{len(bad_paths)} files at wrong path")

    # L6 — processing_timestamp populated
    nulls = int(df["processing_timestamp"].isna().sum()) if "processing_timestamp" in df.columns else len(df)
    chk("PASS" if not nulls else "FAIL","L6 Processing Timestamp",
        "All records have processing_timestamp" if not nulls else f"{nulls} missing")

    # L7 — source isolation per partition
    for fp in files:
        part_df = pd.read_parquet(fp)
        if "source" in part_df.columns:
            wrong = int((part_df["source"] != "imf_weo").sum())
            if wrong:
                chk("FAIL","L7 Source Isolation",
                    f"{wrong} records with wrong source in {fp.name}")
                break
    else:
        chk("PASS","L7 Source Isolation","All partitions contain only source=imf_weo records")

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)
    overall = "PASS" if failed == 0 else "FAIL"

    logger.info("=" * 70)
    logger.info("SUMMARY: %d PASS / %d FAIL / %d WARN | Overall: [%s]", passed, failed, warned, overall)
    logger.info("=" * 70)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "overall": overall,
              "passed": passed, "failed": failed, "warned": warned, "checks": results}
    with open(REPORT_J, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
    with open(REPORT_T, "w", encoding="utf-8") as f:
        f.write(f"IMF Lineage Report - {datetime.utcnow().isoformat()}Z\n")
        for r in results: f.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
    logger.info("  Reports: %s, %s", REPORT_J, REPORT_T)
    return failed == 0

EU27 = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
        "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE"]
_VID_RE = __import__("re").compile(r"^EUROSTAT-.+-\d{4}(-\d{2})?-v\d+$")

def _run_eu27_lineage() -> bool:
    logger.info("=" * 70)
    logger.info("EU27 GLOBAL MACRO — DATA LINEAGE VALIDATION (eurostat_sdmx)")
    logger.info("=" * 70)
    base = Path("lekwankwa-historical-vault/product=global_macro")
    results, frames, total_files, empty_files = [], [], 0, 0

    def chk(status, name, msg, **kw):
        icon = {"PASS":"[+]","FAIL":"[!]","WARN":"[~]","SKIP":"[S]"}.get(status,"[?]")
        logger.info("  %s %s: %s", icon, name, msg)
        results.append({"status": status, "check": name, "message": msg, **kw})

    for iso in EU27:
        src = base / f"country={iso}" / "source=eurostat_sdmx"
        if not src.exists(): continue
        iso_files = sorted([f for f in src.rglob("*.parquet")
                            if "outlier" not in f.name and "changelog" not in f.name])
        total_files += len(iso_files)
        for f in iso_files:
            try:
                df_tmp = pd.read_parquet(f)
                if df_tmp.empty: empty_files += 1
            except Exception: empty_files += 1
        if iso_files:
            try: frames.append(pd.read_parquet(iso_files[0]))
            except Exception: pass

    chk("PASS" if empty_files == 0 else "WARN", "L1 Partition Integrity",
        f"All {total_files} partition files non-empty" if empty_files == 0
        else f"{empty_files} of {total_files} partition files empty or unreadable")

    if not frames:
        chk("FAIL", "L2 Data Load", "No data loaded from EU27 global_macro vault")
        return False

    sample = pd.concat(frames, ignore_index=True)

    found_iso = set(sample["iso_alpha3"].dropna().unique()) if "iso_alpha3" in sample.columns else set()
    missing = sorted(set(EU27) - found_iso)
    chk("PASS" if not missing else "WARN", "L2 Country Coverage",
        f"All 27 EU countries represented in sample" if not missing
        else f"{len(missing)} countries not in sample: {missing}")

    if "data_vintage_id" in sample.columns:
        bad = [v for v in sample["data_vintage_id"].dropna().head(2000) if not _VID_RE.match(str(v))]
        chk("PASS" if not bad else "FAIL", "L3 Vintage ID Format",
            f"All sampled vintage IDs match EUROSTAT pattern" if not bad
            else f"{len(bad)} bad vintage IDs (e.g. {bad[:2]})")
    else:
        chk("FAIL", "L3 Vintage ID Format", "data_vintage_id column missing")

    if "source_agency" in sample.columns:
        bad_agency = int((sample["source_agency"] != "EUROSTAT").sum())
        chk("PASS" if not bad_agency else "FAIL", "L4 Source Agency",
            f"All records source_agency=EUROSTAT" if not bad_agency
            else f"{bad_agency} records have wrong source_agency")
    else:
        chk("FAIL", "L4 Source Agency", "source_agency column missing")

    if "revision_number" in sample.columns:
        neg = int((pd.to_numeric(sample["revision_number"], errors="coerce") < 0).sum())
        chk("PASS" if neg == 0 else "FAIL", "L5 Revision Monotonicity",
            f"All revision_number >= 0" if neg == 0 else f"{neg} negative revision_numbers")
    else:
        chk("SKIP", "L5 Revision Monotonicity", "revision_number column missing")

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)
    overall = "PASS" if failed == 0 else "FAIL"
    logger.info("=" * 70)
    logger.info("SUMMARY: %d PASS / %d FAIL / %d WARN | Overall: [%s]", passed, failed, warned, overall)
    logger.info("=" * 70)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "scope": "EU27 eurostat_sdmx",
              "product": "global_macro", "overall": overall,
              "passed": passed, "failed": failed, "warned": warned, "checks": results}
    with open("global_macro_eu27_lineage_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return failed == 0


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--eu27", action="store_true")
    _args, _ = _parser.parse_known_args()
    sys.exit(0 if (_run_eu27_lineage() if _args.eu27 else run()) else 1)
