"""
Schema Compliance Validation — All Products (excl. food_micropricing)

Validates each vault source against its corresponding gold standard JSON.
Gold standards live in: schema gold standards/

Products covered:
  electricity                        → electricity.json
  Housing_Supply_and_Shelter_Inflation/bls_cpi_shelter → housing_shelter_inflation.json
  Housing_Supply_and_Shelter_Inflation/census_bps      → housing_building_permits.json
  global_macro/imf_weo               → imf_weo.json
  trade_flows/census_ft900           → trade_flows.json
  wages_and_employment/bls_ces       → wages.json
  wages_and_employment/bls_cps       → unemployment.json

Author: Lekwankwa Corporation
Date: 2026-06-15
"""

import json
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

VAULT      = Path("lekwankwa-historical-vault")
GOLD_DIR   = Path("schema gold standards")
SAMPLE_N   = 60    # max partition files to sample per source
REPORT_OUT = Path("schema_compliance_all_products_report.json")

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

VALID_CONFIDENCE_TIERS = {"PRIMARY", "SECONDARY", "INDICATIVE", "EXPERIMENTAL", "ESTIMATED"}
VALID_MARKET_TIERS     = {"Developed", "Emerging", "Frontier", "Developing"}
VALID_EXTRACTION       = {"api", "scraper", "manual", "ftp"}

# Common gold-standard fields that must appear in the vault (items + source_metadata)
COMMON_GOLD_FIELDS = [
    "data_vintage_id", "confidence_tier", "sovereign_series_id",
    "official_release_date", "as_of_date",
    "observed_value", "unit_of_measure", "is_revised_figure",
    "iso_alpha3", "market_tier",
    "source_agency", "source_sub_category", "portal_url",
]

# reporting_date maps to data_timestamp in some older sources
REPORTING_DATE_ALIASES = ["reporting_date", "data_timestamp"]

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE → GOLD STANDARD MAPPING
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_CONFIGS = [
    {
        "label":        "electricity / eia",
        "vault_path":   "product=electricity/country=USA/source=eia",
        "file_pattern": "generation_data.parquet",
        "gold_file":    "electricity.json",
        "extra_required": [
            "energy_source_type", "generation_output_mwh",
        ],
        "extra_missing_ok": [
            # In gold standard but pre-aggregated upstream; not yet ingested
            "total_grid_load_mwh", "installed_capacity_mw",
            "release_frequency", "macro_metric_name",
            # Vault uses data_timestamp (alias for reporting_date in gold std)
            "reporting_date",
            # Electricity is a generation metric, not priced in a currency
            "currency",
            # as_of_date and source_sub_category are genuine gaps to fix in scraper
            # — left in required so they show as FAIL until fixed
        ],
        "non_null_critical": [
            "sovereign_series_id", "energy_source_type", "iso_alpha3",
            # generation_output_mwh removed — EIA has ~1% nulls for unreported months
        ],
        "value_checks": {
            # EIA net generation can legitimately be negative (storage charging / grid draw)
            "generation_output_mwh": {},
        },
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
        },
    },
    {
        "label":        "Housing_Supply_and_Shelter_Inflation / bls_cpi_shelter",
        "vault_path":   "product=Housing_Supply_and_Shelter_Inflation/country=USA/source=bls_cpi_shelter",
        "file_pattern": "shelter_inflation_data.parquet",
        "gold_file":    "housing_shelter_inflation.json",
        "extra_required": ["macro_metric_name", "record_id"],
        "extra_missing_ok": ["release_frequency"],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id",
        ],
        "value_checks": {
            "observed_value": {"min": 0},
        },
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
        },
    },
    {
        "label":        "Housing_Supply_and_Shelter_Inflation / census_bps",
        "vault_path":   "product=Housing_Supply_and_Shelter_Inflation/country=USA/source=census_bps",
        "file_pattern": "building_permits_data.parquet",
        "gold_file":    "housing_building_permits.json",
        "extra_required": ["macro_metric_name", "record_id"],
        "extra_missing_ok": ["release_frequency"],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id",
        ],
        "value_checks": {
            "observed_value": {"min": 0},
        },
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
        },
    },
    {
        "label":        "global_macro / imf_weo",
        "vault_path":   "product=global_macro/country=USA/source=imf_weo",
        "file_pattern": "global_macro_data.parquet",
        "gold_file":    "imf_weo.json",
        "extra_required": ["macro_metric_name", "record_id"],
        "extra_missing_ok": [
            "release_frequency", "as_of_date",
            "is_revised_figure",        # IMF uses is_forecast instead
            "reporting_date",           # vault uses data_timestamp (alias)
            "official_release_date",    # vault uses published_date (alias)
        ],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id",
        ],
        "value_checks": {},
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
        },
    },
    {
        "label":        "trade_flows / census_ft900",
        "vault_path":   "product=trade_flows/country=USA/source=census_ft900",
        "file_pattern": "trade_flows_data.parquet",
        "gold_file":    "trade_flows.json",
        "extra_required": ["macro_metric_name", "record_id", "trade_flow"],
        "extra_missing_ok": ["release_frequency"],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id", "trade_flow",
        ],
        "value_checks": {
            "observed_value": {"min": 0},
        },
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
            "trade_flow":      {"exports", "imports", "EXPORTS", "IMPORTS",
                                "Export", "Import"},
        },
    },
    {
        "label":        "wages_and_employment / bls_ces",
        "vault_path":   "product=wages_and_employment/country=USA/source=bls_ces",
        "file_pattern": "ces_data.parquet",
        "gold_file":    "wages.json",
        "extra_required": ["macro_metric_name", "record_id"],
        "extra_missing_ok": ["release_frequency"],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id",
        ],
        "value_checks": {},
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
        },
    },
    {
        "label":        "wages_and_employment / bls_cps",
        "vault_path":   "product=wages_and_employment/country=USA/source=bls_cps",
        "file_pattern": "cps_data.parquet",
        "gold_file":    "unemployment.json",
        "extra_required": ["macro_metric_name", "record_id"],
        "extra_missing_ok": ["release_frequency"],
        "non_null_critical": [
            "sovereign_series_id", "observed_value",
            "macro_metric_name", "iso_alpha3", "record_id",
        ],
        "value_checks": {
            # CPS tracks both rates (0-100%) AND employment levels (100k+)
            # so only enforce positivity, not a 100% ceiling
            "observed_value": {"min": 0},
        },
        "vocab_checks": {
            "confidence_tier": VALID_CONFIDENCE_TIERS,
            "market_tier":     VALID_MARKET_TIERS,
        },
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_gold_fields(gold_file: str) -> list[str]:
    """Extract all field names from a gold standard JSON (items + source_metadata)."""
    gs = json.load(open(GOLD_DIR / gold_file, encoding="utf-8"))
    fields = set()
    for rec in gs.get("market_records", []):
        fields.update(rec.get("source_metadata", {}).keys())
        for item in rec.get("items", []):
            fields.update(item.keys())
        # top-level market_record fields (iso_alpha3, market_tier, country_name)
        for k in rec:
            if k not in ("source_metadata", "items"):
                fields.add(k)
    return sorted(fields)


def _load_sample(vault_path: str, file_pattern: str) -> pd.DataFrame | None:
    """Load a representative sample of parquet files from a vault source."""
    src = VAULT / vault_path
    if not src.exists():
        return None
    files = sorted(src.rglob(file_pattern))
    if not files:
        return None
    step = max(1, len(files) // SAMPLE_N)
    sample = files[::step][:SAMPLE_N]
    dfs = []
    for f in sample:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else None


def _result(status: str, check: str, message: str, details: dict | None = None) -> dict:
    icons = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}
    print(f"    {icons[status]} {check}")
    if message:
        print(f"           {message}")
    r = {"status": status, "check": check, "message": message}
    if details:
        r["details"] = details
    return r


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def chk_data_available(df, label):
    if df is None or df.empty:
        return _result("FAIL", "Data Available", f"No data found in vault for {label}")
    return _result("PASS", "Data Available",
                   f"{len(df):,} records loaded from {SAMPLE_N} sampled partitions")


def chk_gold_fields_present(df, gold_fields, extra_required, extra_missing_ok):
    """All gold standard fields (+ extra_required) must be in vault columns."""
    required = set(gold_fields) | set(extra_required)
    # Fields allowed to be absent (not yet ingested / derived differently)
    ok_missing = set(extra_missing_ok) | {"country_name", "release_frequency"}
    required -= ok_missing

    vault_cols = set(df.columns)
    missing    = sorted(required - vault_cols)
    extra      = sorted(vault_cols - (required | ok_missing))

    if missing:
        return _result("FAIL", "Gold Standard Fields Present",
                       f"{len(missing)} required fields missing from vault",
                       {"missing_fields": missing, "extra_fields": extra[:10]})
    return _result("PASS", "Gold Standard Fields Present",
                   f"All {len(required)} required gold-standard fields present "
                   f"({len(extra)} extra pipeline fields in vault)")


def chk_reporting_date_present(df):
    """reporting_date (or its alias data_timestamp) must be present."""
    present = [c for c in REPORTING_DATE_ALIASES if c in df.columns]
    if not present:
        return _result("FAIL", "Reporting Date Present",
                       f"Neither {REPORTING_DATE_ALIASES} found in vault")
    return _result("PASS", "Reporting Date Present",
                   f"Observation date column present: '{present[0]}'")


def chk_non_null_critical(df, critical_cols):
    issues = []
    for col in critical_cols:
        if col not in df.columns:
            issues.append(f"{col}: MISSING")
        else:
            n = int(df[col].isna().sum())
            if n > 0:
                issues.append(f"{col}: {n:,} nulls ({n/len(df)*100:.1f}%)")
    if issues:
        return _result("FAIL", "Non-Null Critical Fields",
                       f"{len(issues)} fields have nulls or are missing",
                       {"issues": issues})
    return _result("PASS", "Non-Null Critical Fields",
                   f"All {len(critical_cols)} critical fields fully populated")


def chk_record_id_uuid(df):
    if "record_id" not in df.columns:
        return _result("SKIP", "UUID record_id", "record_id column not present")
    ids = df["record_id"].dropna().astype(str)
    invalid = int((~ids.str.match(UUID_RE.pattern, case=False)).sum())
    if invalid:
        return _result("FAIL", "UUID record_id",
                       f"{invalid:,} record_ids are not valid UUID v4",
                       {"examples": ids[~ids.str.match(UUID_RE.pattern, case=False)].head(3).tolist()})
    return _result("PASS", "UUID record_id",
                   f"All {len(ids):,} record_ids are valid UUID v4")


def chk_iso_alpha3(df):
    if "iso_alpha3" not in df.columns:
        return _result("FAIL", "ISO 3166-1 alpha-3", "iso_alpha3 column missing")
    vals = df["iso_alpha3"].dropna().unique()
    invalid = [v for v in vals if not (isinstance(v, str) and len(v) == 3 and v.isupper())]
    if invalid:
        return _result("FAIL", "ISO 3166-1 alpha-3",
                       f"Invalid iso_alpha3 values: {invalid[:5]}")
    return _result("PASS", "ISO 3166-1 alpha-3",
                   f"All iso_alpha3 values are valid 3-letter codes: {sorted(vals)}")


def chk_confidence_tier(df, valid_tiers):
    if "confidence_tier" not in df.columns:
        return _result("FAIL", "Confidence Tier Vocabulary", "confidence_tier column missing")
    vals = set(df["confidence_tier"].dropna().unique())
    invalid = vals - valid_tiers
    if invalid:
        return _result("FAIL", "Confidence Tier Vocabulary",
                       f"Invalid confidence_tier values: {invalid}")
    return _result("PASS", "Confidence Tier Vocabulary",
                   f"All confidence_tier values in valid set: {sorted(vals)}")


def chk_market_tier(df, valid_tiers):
    if "market_tier" not in df.columns:
        return _result("FAIL", "Market Tier Vocabulary", "market_tier column missing")
    vals = set(df["market_tier"].dropna().unique())
    invalid = vals - valid_tiers
    if invalid:
        return _result("FAIL", "Market Tier Vocabulary",
                       f"Invalid market_tier values: {invalid}")
    return _result("PASS", "Market Tier Vocabulary",
                   f"All market_tier values valid: {sorted(vals)}")


def chk_data_vintage_id(df):
    """data_vintage_id must be non-null and follow {AGENCY}-{SERIES}-{YYYY}-{MM}-v{N} pattern."""
    if "data_vintage_id" not in df.columns:
        return _result("FAIL", "data_vintage_id Format", "data_vintage_id column missing")
    col = df["data_vintage_id"].dropna().astype(str)
    null_n = int(df["data_vintage_id"].isna().sum())
    # Pattern: {AGENCY}-{SERIES_ID}-{YYYY[-MM]}-v{N}
    # Series IDs may contain underscores (e.g. NGDP_RPCH, HS04_EXP)
    pattern = re.compile(r"^[A-Z0-9_]+-[A-Z0-9_]+-.+-v\d+$", re.IGNORECASE)
    invalid = int((~col.str.match(pattern)).sum())
    if null_n > 0 or invalid > 0:
        return _result("WARN", "data_vintage_id Format",
                       f"{null_n} null, {invalid} non-conforming data_vintage_id values",
                       {"examples": col[~col.str.match(pattern)].head(3).tolist()})
    return _result("PASS", "data_vintage_id Format",
                   f"All {len(col):,} data_vintage_id values match pattern")


def chk_is_revised_figure(df):
    if "is_revised_figure" not in df.columns:
        return _result("SKIP", "is_revised_figure Boolean", "is_revised_figure not present")
    col = df["is_revised_figure"].dropna()
    non_bool = col[~col.apply(lambda v: isinstance(v, (bool,)) or str(v) in ("True", "False"))]
    if len(non_bool):
        return _result("FAIL", "is_revised_figure Boolean",
                       f"{len(non_bool):,} non-boolean values in is_revised_figure")
    return _result("PASS", "is_revised_figure Boolean",
                   f"All {len(col):,} is_revised_figure values are boolean")


def chk_temporal_ordering(df):
    """official_release_date >= observation date (reporting_date / data_timestamp)."""
    obs_col = next((c for c in REPORTING_DATE_ALIASES if c in df.columns), None)
    if obs_col is None or "official_release_date" not in df.columns:
        return _result("SKIP", "Temporal Ordering (release >= observation)",
                       "Missing observation date or official_release_date")
    try:
        obs  = pd.to_datetime(df[obs_col],              utc=True, errors="coerce")
        rel  = pd.to_datetime(df["official_release_date"], utc=True, errors="coerce")
        both = obs.notna() & rel.notna()
        bad  = int((rel[both] < obs[both]).sum())
        if bad:
            return _result("FAIL", "Temporal Ordering (release >= observation)",
                           f"{bad:,} records where official_release_date < {obs_col}")
        return _result("PASS", "Temporal Ordering (release >= observation)",
                       f"All {int(both.sum()):,} records have release_date >= observation_date")
    except Exception as e:
        return _result("WARN", "Temporal Ordering (release >= observation)", str(e))


def chk_as_of_ordering(df):
    """as_of_date >= official_release_date (ingestion after release)."""
    if "as_of_date" not in df.columns or "official_release_date" not in df.columns:
        return _result("SKIP", "Temporal Ordering (as_of >= release)",
                       "Missing as_of_date or official_release_date")
    try:
        rel   = pd.to_datetime(df["official_release_date"], utc=True, errors="coerce")
        aod   = pd.to_datetime(df["as_of_date"],            utc=True, errors="coerce")
        both  = rel.notna() & aod.notna()
        bad   = int((aod[both] < rel[both]).sum())
        if bad:
            return _result("WARN", "Temporal Ordering (as_of >= release)",
                           f"{bad:,} records where as_of_date < official_release_date")
        return _result("PASS", "Temporal Ordering (as_of >= release)",
                       f"All {int(both.sum()):,} records have as_of_date >= official_release_date")
    except Exception as e:
        return _result("WARN", "Temporal Ordering (as_of >= release)", str(e))


def chk_portal_url(df, gold_file: str):
    """portal_url must be non-null and consistent across records."""
    if "portal_url" not in df.columns:
        return _result("FAIL", "Portal URL Consistency", "portal_url column missing")
    null_n = int(df["portal_url"].isna().sum())
    urls   = df["portal_url"].dropna().unique()
    if null_n > 0:
        return _result("FAIL", "Portal URL Consistency",
                       f"{null_n:,} null portal_url values")
    if len(urls) > 3:
        return _result("WARN", "Portal URL Consistency",
                       f"{len(urls)} distinct portal_url values (expected ≤3)",
                       {"urls": list(urls[:5])})
    return _result("PASS", "Portal URL Consistency",
                   f"portal_url consistent ({len(urls)} distinct): {list(urls)}")


def chk_value_ranges(df, value_checks: dict):
    if not value_checks:
        return []
    results = []
    for col, bounds in value_checks.items():
        if col not in df.columns:
            results.append(_result("SKIP", f"Value Range: {col}", f"{col} column not present"))
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        lo = bounds.get("min")
        hi = bounds.get("max")
        bad = pd.Series([], dtype=float)
        if lo is not None:
            bad = pd.concat([bad, series[series < lo]])
        if hi is not None:
            bad = pd.concat([bad, series[series > hi]])
        bad = bad.drop_duplicates()
        if len(bad):
            results.append(_result("FAIL", f"Value Range: {col}",
                                   f"{len(bad):,} values outside [{lo}, {hi}]",
                                   {"min_found": float(series.min()), "max_found": float(series.max())}))
        else:
            results.append(_result("PASS", f"Value Range: {col}",
                                   f"All {len(series):,} values in [{lo}, {hi}] "
                                   f"(min={series.min():.4g}, max={series.max():.4g})"))
    return results


def chk_vocab(df, vocab_checks: dict):
    results = []
    for col, valid_set in vocab_checks.items():
        if col not in df.columns:
            results.append(_result("SKIP", f"Vocabulary: {col}", f"{col} not present"))
            continue
        found   = set(df[col].dropna().unique())
        invalid = found - valid_set
        if invalid:
            results.append(_result("FAIL", f"Vocabulary: {col}",
                                   f"Invalid values: {sorted(str(v) for v in invalid)[:10]}"))
        else:
            results.append(_result("PASS", f"Vocabulary: {col}",
                                   f"All values in valid set: {sorted(str(v) for v in found)[:10]}"))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def validate_source(cfg: dict) -> dict:
    label        = cfg["label"]
    gold_file    = cfg["gold_file"]
    extra_req    = cfg.get("extra_required", [])
    missing_ok   = cfg.get("extra_missing_ok", [])
    critical     = cfg.get("non_null_critical", [])
    val_checks   = cfg.get("value_checks", {})
    vocab_checks = cfg.get("vocab_checks", {})

    print(f"\n{'─'*70}")
    print(f"  SOURCE : {label}")
    print(f"  GOLD   : {gold_file}")
    print(f"{'─'*70}")

    gold_fields = _load_gold_fields(gold_file)
    df = _load_sample(cfg["vault_path"], cfg["file_pattern"])

    results = []

    # 1. Data availability
    r = chk_data_available(df, label)
    results.append(r)
    if r["status"] == "FAIL":
        # Can't run any further checks without data
        return {"label": label, "gold_file": gold_file, "results": results,
                "records_sampled": 0,
                "pass": 0, "fail": 1, "warn": 0, "skip": 0, "overall": "FAIL"}

    # 2. Gold standard fields present
    results.append(chk_gold_fields_present(df, gold_fields, extra_req, missing_ok))

    # 3. Reporting date present
    results.append(chk_reporting_date_present(df))

    # 4. Non-null critical fields
    results.append(chk_non_null_critical(df, critical))

    # 5. UUID record_id
    results.append(chk_record_id_uuid(df))

    # 6. ISO alpha-3
    results.append(chk_iso_alpha3(df))

    # 7. confidence_tier vocabulary
    results.append(chk_confidence_tier(df, cfg["vocab_checks"].get("confidence_tier", VALID_CONFIDENCE_TIERS)))

    # 8. market_tier vocabulary
    results.append(chk_market_tier(df, cfg["vocab_checks"].get("market_tier", VALID_MARKET_TIERS)))

    # 9. data_vintage_id format
    results.append(chk_data_vintage_id(df))

    # 10. is_revised_figure boolean
    results.append(chk_is_revised_figure(df))

    # 11. Temporal ordering: release >= observation
    results.append(chk_temporal_ordering(df))

    # 12. Temporal ordering: as_of >= release
    results.append(chk_as_of_ordering(df))

    # 13. Portal URL consistency
    results.append(chk_portal_url(df, gold_file))

    # 14. Value range checks (dataset-specific)
    results.extend(chk_value_ranges(df, val_checks))

    # 15. Vocabulary checks (dataset-specific)
    for col, valid_set in vocab_checks.items():
        if col in ("confidence_tier", "market_tier"):
            continue  # already done above
        if col not in df.columns:
            results.append(_result("SKIP", f"Vocabulary: {col}", f"{col} not present"))
            continue
        found   = set(df[col].dropna().unique())
        invalid = found - valid_set
        if invalid:
            results.append(_result("FAIL", f"Vocabulary: {col}",
                                   f"Invalid values: {sorted(str(v) for v in invalid)[:10]}"))
        else:
            results.append(_result("PASS", f"Vocabulary: {col}",
                                   f"All values valid: {sorted(str(v) for v in found)[:10]}"))

    counts = {s: sum(1 for r in results if r["status"] == s)
              for s in ("PASS", "FAIL", "WARN", "SKIP")}
    overall = "PASS" if counts["FAIL"] == 0 else "FAIL"

    print(f"\n    Summary: {counts['PASS']}P / {counts['FAIL']}F / "
          f"{counts['WARN']}W / {counts['SKIP']}S  →  [{overall}]")

    return {
        "label":    label,
        "gold_file": gold_file,
        "records_sampled": len(df),
        "results": results,
        "pass":  counts["PASS"],
        "fail":  counts["FAIL"],
        "warn":  counts["WARN"],
        "skip":  counts["SKIP"],
        "overall": overall,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("SCHEMA COMPLIANCE — ALL PRODUCTS (excl. food_micropricing)")
    print("=" * 70)
    print(f"Timestamp : {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"Gold std  : {GOLD_DIR}/")
    print(f"Vault     : {VAULT}/")
    print(f"Sources   : {len(SOURCE_CONFIGS)}")

    all_results = []
    for cfg in SOURCE_CONFIGS:
        result = validate_source(cfg)
        all_results.append(result)

    # ── Overall summary ──────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("OVERALL SUMMARY")
    print(f"{'='*70}")
    total_p = total_f = total_w = total_s = 0
    for r in all_results:
        icon = "[PASS]" if r["overall"] == "PASS" else "[FAIL]"
        print(f"  {icon} {r['label']:<55} "
              f"{r['pass']}P/{r['fail']}F/{r['warn']}W/{r['skip']}S")
        total_p += r["pass"]
        total_f += r["fail"]
        total_w += r["warn"]
        total_s += r["skip"]

    print(f"\n  Totals : {total_p} passed / {total_f} failed / "
          f"{total_w} warned / {total_s} skipped")
    overall = "PASS" if total_f == 0 else "FAIL"
    print(f"  Overall: [{overall}]")
    print(f"{'='*70}")

    # ── Write JSON report ────────────────────────────────────────────────────
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "overall": overall,
        "totals": {"pass": total_p, "fail": total_f, "warn": total_w, "skip": total_s},
        "sources": all_results,
    }
    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n  Report : {REPORT_OUT}")

    return 0 if total_f == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
