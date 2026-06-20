"""
Lekwankwa Corporation — Live Feed Post-Delta Audit
===================================================
Run automatically after vault_extractor --mode live AND the 9-stage validation
suite both PASS. This is NOT a replacement for the 9-stage suite — it is a
separate fast sweep checking for the specific failure patterns discovered
during the vault/extractor build.

Checks
------
  C1  Non-null           — non-nullable schema fields must have zero None/NaN
  C2  Scraper-placeholder — PRIMARY/SECONDARY rows with data_quality_certified=False
  C3  Cross-pipeline dup  — same (series, date) with conflicting value from different source
  C4  Timestamp contam.   — as_of_date or conversion_timestamp equals pipeline run date
  C5  Filename/content    — macro_metric_name values consistent with declared product

Usage
-----
  python live_feed_audit.py \\
      --delta extracts/food_pricing_20260620_120000.parquet \\
      --product food_pricing \\
      --validation-summary validation_summary_food_eu27_20260620_115900.json \\
      [--vault-root lekwankwa-historical-vault] \\
      [--run-date 2026-06-20] \\
      [--log-dir audit_logs] \\
      [--skip-vault-check]

Exit codes
----------
  0  All checks PASS — safe to proceed with GCS write
  1  One or more ERRORs flagged — GCS write HALTED
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# ── Paths ──────────────────────────────────────────────────────────────────────

_ROOT = Path(__file__).resolve().parent
_SCHEMA_PATH = _ROOT / "backtesting" / "backtest_engine" / "config" / "SCHEMA_STANDARD.yaml"
_VAULT_DEFAULT = _ROOT / "lekwankwa-historical-vault"

# ── Vault product-folder names ─────────────────────────────────────────────────
# extractor product key → vault folder name (product={name})
_VAULT_PRODUCT_FOLDER: dict[str, str] = {
    "food_pricing":         "food_micropricing",
    "wages_and_employment": "wages_and_employment",
    "housing":              "Housing_Supply_and_Shelter_Inflation",
    "trade_flows":          "trade_flows",
    "global_macro":         "global_macro",
}

# ── Metric name keywords that must NEVER appear in a given product's delta ─────
# Upper-case substring match against macro_metric_name values.
_WRONG_METRICS_FOR_PRODUCT: dict[str, list[str]] = {
    "food_pricing": [
        "PERMIT", "HPI", "HOUSE_PRICE_INDEX", "SHELTER_INFLATION",
        "TOTAL_NONFARM", "NONFARM_PAYROLL", "GDP", "TRADE_BALANCE",
        "EXPORT_VALUE", "IMPORT_VALUE", "UNEMPLOYMENT_RATE",
    ],
    "wages_and_employment": [
        "PERMIT", "HPI", "HOUSE_PRICE", "SHELTER_INFLATION",
        "FOOD_PRICE", "FOOD_INDEX", "GDP", "TRADE_BALANCE",
        "EXPORT_VALUE", "IMPORT_VALUE",
    ],
    "housing": [
        "FOOD_PRICE", "FOOD_INDEX",
        "TOTAL_NONFARM_PAYROLLS", "EMPLOYMENT_LEVEL",
        "GDP", "TRADE_BALANCE", "EXPORT_VALUE", "IMPORT_VALUE",
    ],
    "trade_flows": [
        "FOOD_PRICE", "FOOD_INDEX",
        "PERMIT", "HOUSING_STARTS", "HPI", "HOUSE_PRICE",
        "SHELTER_INFLATION", "TOTAL_NONFARM", "NONFARM_PAYROLL",
        "GDP", "INFLATION_RATE", "UNEMPLOYMENT_RATE",
    ],
    "global_macro": [
        "FOOD_PRICE", "FOOD_INDEX",
        "PERMIT", "HOUSING_STARTS", "HPI", "HOUSE_PRICE_INDEX", "SHELTER_INFLATION",
        "TOTAL_NONFARM", "NONFARM_PAYROLL",
        "EXPORT_VALUE", "IMPORT_VALUE", "TRADE_BALANCE",
    ],
}

# Columns required from vault for C3 vault-scan
_VAULT_SCAN_COLS = [
    "sovereign_series_id", "reporting_date", "observed_value",
    "source", "iso_alpha3", "confidence_tier",
]

log = logging.getLogger("live_feed_audit")


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _load_non_nullable_fields(schema_path: Path) -> set[str]:
    """Parse SCHEMA_STANDARD.yaml; return field names where nullable: false."""
    with schema_path.open(encoding="utf-8") as fh:
        schema = yaml.safe_load(fh)

    non_nullable: set[str] = set()
    # Don't check computed or source-specific sections — they are optional by design
    skip = {"backtest_computed", "source_specific_fields", "version"}
    for section_name, section in schema.items():
        if section_name in skip or not isinstance(section, dict):
            continue
        for field, defn in section.items():
            if isinstance(defn, dict) and defn.get("nullable") is False:
                non_nullable.add(field)
    return non_nullable


# ── Check C1: Non-null ─────────────────────────────────────────────────────────

def check_non_null(
    df: pd.DataFrame, non_nullable: set[str], filename: str
) -> list[dict[str, Any]]:
    """
    For every non-nullable schema field, assert zero None/NaN in the delta.
    is_interpolated is only checked when the column is present in the file
    (the schema explicitly allows its absence for monthly-publisher partitions).
    """
    violations: list[dict[str, Any]] = []

    for field in sorted(non_nullable):
        # is_interpolated: field-presence rule — only check when column exists
        if field == "is_interpolated":
            if field in df.columns:
                n = int(df[field].isna().sum())
                if n:
                    violations.append(_v("C1_NON_NULL", "ERROR", field, filename,
                        f"{n} null(s) in is_interpolated — must be True/False when column is present",
                        null_count=n))
            continue

        if field not in df.columns:
            violations.append(_v("C1_NON_NULL", "ERROR", field, filename,
                f"Non-nullable field '{field}' is ABSENT from delta file"))
            continue

        n = int(df[field].isna().sum())
        if n:
            violations.append(_v("C1_NON_NULL", "ERROR", field, filename,
                f"{n} null(s) in non-nullable field '{field}'", null_count=n))

    return violations


# ── Check C2: Scraper-placeholder dqc ─────────────────────────────────────────

def check_scraper_placeholder(
    df: pd.DataFrame, filename: str
) -> list[dict[str, Any]]:
    """
    Flag any PRIMARY/SECONDARY row with data_quality_certified=False.
    Pattern confirmed in food/USA, wages/USA, housing/USA — scrapers hardcode
    dqc=False at write time; the backfill corrects it after 9-stage PASS.
    A 4th occurrence would be caught here before delivery.
    """
    if "data_quality_certified" not in df.columns or "confidence_tier" not in df.columns:
        return []

    mask = (
        df["data_quality_certified"].eq(False) &
        df["confidence_tier"].isin(["PRIMARY", "SECONDARY"])
    )
    affected = df[mask]
    if affected.empty:
        return []

    by_cs: dict[str, int] = {}
    if "iso_alpha3" in affected.columns and "source" in affected.columns:
        by_cs = {
            f"{k[0]}/{k[1]}": int(v)
            for k, v in affected.groupby(["iso_alpha3", "source"]).size().items()
        }

    return [_v("C2_SCRAPER_PLACEHOLDER_DQC", "ERROR", "data_quality_certified", filename,
        f"{len(affected)} PRIMARY/SECONDARY row(s) have dqc=False. "
        f"Run the appropriate backfill script before GCS delivery. "
        f"Affected (country/source -> row count): {by_cs}",
        affected_rows=int(len(affected)), by_country_source=by_cs)]


# ── Check C3: Cross-pipeline duplicate ────────────────────────────────────────

def check_cross_pipeline_duplicates(
    df: pd.DataFrame,
    product: str,
    vault_root: Path,
    filename: str,
    skip_vault_scan: bool = False,
) -> list[dict[str, Any]]:
    """
    C3a — Within-delta: same (iso, series, date) from two sources with different values.
    C3b — Delta vs vault: delta row conflicts with an existing vault row from a different source.
    C3b is skipped when --skip-vault-check is set.

    This is the Sweden permits pattern: two pipeline paths writing the same real-world
    observation under different source conventions with diverging observed values.
    """
    violations: list[dict[str, Any]] = []
    required = {"sovereign_series_id", "reporting_date", "observed_value"}
    if not required.issubset(df.columns):
        return violations

    # Work on PRIMARY/SECONDARY only; DERIVED fill rows legitimately duplicate dates
    primary = (
        df[df["confidence_tier"].isin(["PRIMARY", "SECONDARY"])].copy()
        if "confidence_tier" in df.columns else df.copy()
    )
    if primary.empty:
        return violations

    key_cols = [c for c in ["iso_alpha3", "sovereign_series_id", "reporting_date"] if c in primary.columns]
    rdate_str = primary["reporting_date"].astype(str).str[:10]

    # ── C3a: within-delta conflicts ───────────────────────────────────────────
    if len(key_cols) == 3 and "source" in primary.columns:
        tmp = primary.copy()
        tmp["_rdate"] = rdate_str

        # Find (iso, series, date) that appear under ≥2 distinct sources
        group_key = ["iso_alpha3", "sovereign_series_id", "_rdate"]
        n_sources = tmp.groupby(group_key)["source"].nunique()
        multi = n_sources[n_sources > 1]

        within_conflicts: list[dict] = []
        for (iso, sid, rd) in multi.index:
            rows = tmp[
                (tmp["iso_alpha3"] == iso) &
                (tmp["sovereign_series_id"] == sid) &
                (tmp["_rdate"] == rd)
            ]
            if rows["observed_value"].nunique() > 1:
                by_src = rows.groupby("source")["observed_value"].first().to_dict()
                within_conflicts.append({
                    "iso_alpha3": iso, "sovereign_series_id": sid,
                    "reporting_date": rd, "values_by_source": by_src,
                })

        if within_conflicts:
            violations.append(_v(
                "C3_CROSS_PIPELINE_DUPLICATE", "ERROR", "observed_value", filename,
                f"{len(within_conflicts)} (series, date) pair(s) appear in this delta from "
                f"two different sources with conflicting observed_value. "
                f"First conflict: {within_conflicts[0]}",
                sub_check="C3a_within_delta",
                conflict_count=len(within_conflicts),
                conflicts=within_conflicts[:5],
            ))

    if skip_vault_scan:
        return violations

    # ── C3b: delta vs existing vault ─────────────────────────────────────────
    vault_folder = _VAULT_PRODUCT_FOLDER.get(product, product)
    product_dir = vault_root / f"product={vault_folder}"
    if not product_dir.exists():
        log.debug("C3b: vault product dir missing (%s) — skipping vault scan", product_dir)
        return violations

    delta_countries = (
        primary["iso_alpha3"].dropna().unique().tolist()
        if "iso_alpha3" in primary.columns else []
    )
    if not delta_countries:
        return violations

    # Build lookup from delta: (iso, sid, rdate, source) → observed_value
    delta_idx: dict[tuple[str, str, str, str], Any] = {}
    delta_keys: set[tuple[str, str, str]] = set()   # (iso, sid, rdate) for fast vault filter
    for _, row in primary.iterrows():
        iso  = str(row.get("iso_alpha3", ""))
        sid  = str(row.get("sovereign_series_id", ""))
        rd   = str(row.get("reporting_date", ""))[:10]
        src  = str(row.get("source", ""))
        delta_idx[(iso, sid, rd, src)] = row.get("observed_value")
        delta_keys.add((iso, sid, rd))

    vault_conflicts: list[dict] = []

    for iso in delta_countries:
        iso_dir = product_dir / f"country={iso}"
        if not iso_dir.exists():
            continue

        frames: list[pd.DataFrame] = []
        for pq_file in iso_dir.rglob("*.parquet"):
            if any(skip in pq_file.name for skip in ("outlier", "changelog", "fill")):
                continue
            try:
                full = pd.read_parquet(pq_file)
                avail = [c for c in _VAULT_SCAN_COLS if c in full.columns]
                frames.append(full[avail])
            except Exception:
                continue

        if not frames:
            continue

        vault_iso = pd.concat(frames, ignore_index=True)

        if "confidence_tier" in vault_iso.columns:
            vault_iso = vault_iso[vault_iso["confidence_tier"].isin(["PRIMARY", "SECONDARY"])].copy()
        if vault_iso.empty:
            continue

        # Filter to rows matching (iso, sid, rdate) in the delta
        v_sid   = vault_iso["sovereign_series_id"].astype(str)
        v_rdate = vault_iso["reporting_date"].astype(str).str[:10]
        v_iso_c = (
            vault_iso["iso_alpha3"].astype(str)
            if "iso_alpha3" in vault_iso.columns
            else pd.Series([iso] * len(vault_iso), index=vault_iso.index)
        )
        match = pd.Series(
            [(i, s, r) in delta_keys for i, s, r in zip(v_iso_c, v_sid, v_rdate)],
            index=vault_iso.index,
        )
        relevant = vault_iso[match]
        if relevant.empty:
            continue

        for _, vrow in relevant.iterrows():
            v_iso_val = str(vrow.get("iso_alpha3", iso))
            v_sid_val = str(vrow.get("sovereign_series_id", ""))
            v_rd      = str(vrow.get("reporting_date", ""))[:10]
            v_src     = str(vrow.get("source", ""))
            v_val     = vrow.get("observed_value")

            # Compare against every delta row for the same (iso, sid, date) from a different source
            for (d_iso, d_sid, d_rd, d_src), d_val in delta_idx.items():
                if not (d_iso == v_iso_val and d_sid == v_sid_val and d_rd == v_rd):
                    continue
                if d_src == v_src:
                    continue   # Same source → legitimate revision, not a cross-pipeline conflict
                if d_val is None or v_val is None:
                    continue
                try:
                    if abs(float(d_val) - float(v_val)) > 1e-6:
                        vault_conflicts.append({
                            "iso_alpha3": v_iso_val, "sovereign_series_id": v_sid_val,
                            "reporting_date": v_rd,
                            "delta_source": d_src, "delta_value": float(d_val),
                            "vault_source":  v_src, "vault_value":  float(v_val),
                        })
                except (TypeError, ValueError):
                    pass

    if vault_conflicts:
        # Deduplicate
        seen: set[tuple] = set()
        unique_vc: list[dict] = []
        for c in vault_conflicts:
            k = (c["iso_alpha3"], c["sovereign_series_id"], c["reporting_date"],
                 c["delta_source"], c["vault_source"])
            if k not in seen:
                seen.add(k)
                unique_vc.append(c)

        violations.append(_v(
            "C3_CROSS_PIPELINE_DUPLICATE", "ERROR", "observed_value", filename,
            f"{len(unique_vc)} (series, date) pair(s) in this delta conflict with "
            f"existing vault records from a different source pipeline. "
            f"First conflict: {unique_vc[0]}",
            sub_check="C3b_delta_vs_vault",
            conflict_count=len(unique_vc),
            conflicts=unique_vc[:5],
        ))

    return violations


# ── Check C4: Timestamp contamination ─────────────────────────────────────────

def check_timestamp_contamination(
    df: pd.DataFrame, run_date: str, filename: str
) -> list[dict[str, Any]]:
    """
    Flag records where as_of_date or conversion_timestamp carries today's pipeline
    run date rather than the historically correct publication date.

    as_of_date contamination pattern (housing/USA census_bps, June 2026):
      Scraper sets as_of_date = extraction_ts (now()) instead of official_release_date.
      Result: as_of_date == run_date but official_release_date is historical.

    conversion_timestamp is a WARN not ERROR — it legitimately records when the
    scraper wrote the row, but a historical observation_date + today conversion_ts
    indicates data that was already in the vault and should not be re-delivered.
    """
    violations: list[dict[str, Any]] = []

    # ── as_of_date ERROR check ────────────────────────────────────────────────
    if "as_of_date" in df.columns and "official_release_date" in df.columns:
        aod  = df["as_of_date"].astype(str).str[:10]
        ord_ = df["official_release_date"].astype(str).str[:10]
        # Contaminated: scraper stamped today but the release was historical
        mask = (aod == run_date) & (ord_ != run_date)
        n = int(mask.sum())
        if n:
            examples = (
                df[mask][["iso_alpha3", "sovereign_series_id", "reporting_date",
                           "as_of_date", "official_release_date"]]
                .head(3).to_dict("records")
            )
            violations.append(_v(
                "C4_TIMESTAMP_CONTAMINATION", "ERROR", "as_of_date", filename,
                f"{n} row(s) have as_of_date={run_date} (pipeline run date) but "
                f"official_release_date != {run_date}. Scraper set as_of_date=extraction_ts "
                f"instead of official_release_date. Run backfill before delivery. "
                f"Examples: {examples}",
                affected_rows=n,
            ))

    # ── conversion_timestamp WARN check ──────────────────────────────────────
    if "conversion_timestamp" in df.columns and "reporting_date" in df.columns:
        ct = df["conversion_timestamp"].astype(str).str[:10]
        rd = df["reporting_date"].astype(str).str[:10]
        try:
            # Suspicious: today's conversion_ts on an observation older than 60 days
            cutoff = (pd.Timestamp(run_date) - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
            mask = (ct == run_date) & (rd < cutoff)
            n = int(mask.sum())
            if n:
                examples = (
                    df[mask][["iso_alpha3", "sovereign_series_id",
                               "reporting_date", "conversion_timestamp"]]
                    .head(3).to_dict("records")
                )
                violations.append(_v(
                    "C4_TIMESTAMP_CONTAMINATION", "WARN", "conversion_timestamp", filename,
                    f"{n} row(s) have conversion_timestamp={run_date} (today) on observations "
                    f"older than 60 days. These rows may be stale vault records included in "
                    f"the delta by mistake. Verify --since date is correct. "
                    f"Examples: {examples}",
                    affected_rows=n,
                ))
        except Exception:
            pass

    return violations


# ── Check C5: Filename / content match ────────────────────────────────────────

def check_filename_content_match(
    df: pd.DataFrame, product: str, delta_path: Path
) -> list[dict[str, Any]]:
    """
    Confirm macro_metric_name values are consistent with the declared product.
    Catches the permits_monthly_fill.parquet containing HPI data pattern:
    a fill file was misnamed and its content was not verified against the
    product's expected metric vocabulary.
    """
    violations: list[dict[str, Any]] = []
    stem = delta_path.stem   # e.g. food_pricing_20260620_120000

    # ── C5a: filename product vs declared product ────────────────────────────
    m = re.match(r"^(.+?)_\d{8}_\d{6}$", stem)
    if m:
        filename_product = m.group(1)
        if filename_product != product:
            violations.append(_v(
                "C5_FILENAME_CONTENT_MATCH", "ERROR", "filename", delta_path.name,
                f"Filename implies product='{filename_product}' but --product='{product}'. "
                f"Mismatch — verify vault_extractor was invoked with the correct --product flag.",
            ))

    # ── C5b: macro_metric_name keyword exclusion ─────────────────────────────
    if "macro_metric_name" not in df.columns:
        return violations

    forbidden = _WRONG_METRICS_FOR_PRODUCT.get(product, [])
    if not forbidden:
        return violations

    bad: list[str] = []
    for metric in df["macro_metric_name"].dropna().unique():
        mu = str(metric).upper()
        for kw in forbidden:
            if kw in mu:
                bad.append(f"{metric!r} (matched keyword '{kw}')")
                break

    if bad:
        violations.append(_v(
            "C5_FILENAME_CONTENT_MATCH", "ERROR", "macro_metric_name", delta_path.name,
            f"Delta declared as product='{product}' but contains {len(bad)} metric name(s) "
            f"that belong to a different product. This is the permits_monthly_fill pattern. "
            f"Wrong metrics: {bad}",
            wrong_metrics=bad,
        ))

    return violations


# ── Violation builder ─────────────────────────────────────────────────────────

def _v(check: str, severity: str, field: str, filename: str,
       detail: str, **extra: Any) -> dict[str, Any]:
    """Build a violation dict with standard keys."""
    return {"check": check, "severity": severity, "field": field,
            "file": filename, "detail": detail, **extra}


# ── Pre-flight: 9-stage PASS guard ────────────────────────────────────────────

def _nine_stage_passed(summary_path: Path) -> bool:
    try:
        data = json.loads(summary_path.read_text(encoding="utf-8"))
        return data.get("overall") == "PASS"
    except Exception as exc:
        log.warning("Cannot read validation summary %s: %s", summary_path, exc)
        return False


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_audit(
    delta_path: Path,
    product: str,
    validation_summary: Path | None,
    vault_root: Path,
    run_date: str,
    log_dir: Path,
    skip_vault_check: bool = False,
) -> int:
    """Run all 5 checks; write audit log JSON; return 0=PASS, 1=FLAG."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"live_feed_audit_log_{product}_{ts}.json"

    # Load schema
    try:
        non_nullable = _load_non_nullable_fields(_SCHEMA_PATH)
    except Exception as exc:
        log.error("Cannot load SCHEMA_STANDARD.yaml: %s", exc)
        non_nullable = set()

    # Load delta
    try:
        df = pd.read_parquet(delta_path)
    except Exception as exc:
        log.error("Cannot read delta file %s: %s", delta_path, exc)
        _write_log(log_path, {"overall": "ERROR", "error": str(exc),
                               "delta_file": str(delta_path), "product": product})
        return 1

    log.info("Auditing %s -- %d rows, %d cols, product=%s, run_date=%s",
             delta_path.name, len(df), len(df.columns), product, run_date)

    # ── Pre-flight ────────────────────────────────────────────────────────────
    if validation_summary and not _nine_stage_passed(validation_summary):
        log.error(
            "9-stage validation suite shows non-PASS result in %s. "
            "Audit aborted — GCS write HALTED.",
            validation_summary,
        )
        _write_log(log_path, {
            "overall": "FLAG",
            "pre_flight": "9-STAGE-NOT-PASS",
            "delta_file": str(delta_path),
            "product": product,
            "validation_summary": str(validation_summary),
        })
        return 1

    # ── Run checks ────────────────────────────────────────────────────────────
    t0 = datetime.now()
    all_violations: list[dict] = []
    check_results: dict[str, str] = {}

    def _run(name: str, fn, *args) -> list[dict]:
        result = fn(*args)
        status = "PASS" if not any(v["severity"] == "ERROR" for v in result) else "FLAG"
        if any(v["severity"] == "WARN" for v in result) and status == "PASS":
            status = "WARN"
        check_results[name] = status
        return result

    all_violations += _run("C1_NON_NULL",
        check_non_null, df, non_nullable, delta_path.name)

    all_violations += _run("C2_SCRAPER_PLACEHOLDER_DQC",
        check_scraper_placeholder, df, delta_path.name)

    all_violations += _run("C3_CROSS_PIPELINE_DUPLICATE",
        check_cross_pipeline_duplicates, df, product, vault_root,
        delta_path.name, skip_vault_check)

    all_violations += _run("C4_TIMESTAMP_CONTAMINATION",
        check_timestamp_contamination, df, run_date, delta_path.name)

    all_violations += _run("C5_FILENAME_CONTENT_MATCH",
        check_filename_content_match, df, product, delta_path)

    duration = round((datetime.now() - t0).total_seconds(), 2)

    # ── Outcome ───────────────────────────────────────────────────────────────
    errors = [v for v in all_violations if v.get("severity") == "ERROR"]
    warns  = [v for v in all_violations if v.get("severity") == "WARN"]
    overall = "PASS" if not errors else "FLAG"

    # ── Console summary ───────────────────────────────────────────────────────
    bar = "-" * 64
    log.info(bar)
    log.info("LIVE FEED AUDIT -- %s", delta_path.name)
    log.info(bar)
    for check, status in check_results.items():
        log.info("  %-42s [%s]", check, status)
    log.info(bar)
    log.info("  Overall : [%s]  |  %d error(s)  %d warning(s)  |  %.1fs",
             overall, len(errors), len(warns), duration)
    log.info("  Log     : %s", log_path)

    if errors:
        log.error("")
        log.error("GCS WRITE HALTED — resolve the following before delivery:")
        for v in errors:
            log.error("  [%s] %s", v["check"], v["detail"])
    else:
        log.info("All checks PASS -- delta file cleared for GCS write.")

    # ── Write audit log ───────────────────────────────────────────────────────
    audit_log: dict[str, Any] = {
        "audit_timestamp":    datetime.now(timezone.utc).isoformat(),
        "delta_file":         str(delta_path),
        "product":            product,
        "run_date":           run_date,
        "validation_summary": str(validation_summary) if validation_summary else None,
        "rows_audited":       int(len(df)),
        "duration_sec":       duration,
        "vault_scan_skipped": skip_vault_check,
        "overall":            overall,
        "error_count":        len(errors),
        "warn_count":         len(warns),
        "checks":             check_results,
        "violations":         all_violations,
    }
    _write_log(log_path, audit_log)

    return 0 if overall == "PASS" else 1


def _write_log(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        log.error("Could not write audit log to %s: %s", path, exc)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args(argv: list | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Post-delta audit for Lekwankwa live feed pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--delta", required=True,
        help="Path to the delta parquet file produced by vault_extractor --mode live",
    )
    p.add_argument(
        "--product", required=True,
        choices=list(_VAULT_PRODUCT_FOLDER),
        help="Product key used with vault_extractor (must match filename prefix)",
    )
    p.add_argument(
        "--validation-summary", default=None, metavar="JSON",
        help="Path to the 9-stage validation_summary_*.json for this run (PASS guard)",
    )
    p.add_argument(
        "--vault-root", default=None, metavar="DIR",
        help=f"Vault root directory (default: {_VAULT_DEFAULT})",
    )
    p.add_argument(
        "--run-date", default=None, metavar="YYYY-MM-DD",
        help="Pipeline run date for timestamp contamination check (default: today UTC)",
    )
    p.add_argument(
        "--log-dir", default="audit_logs", metavar="DIR",
        help="Directory for audit log JSON files (default: ./audit_logs/)",
    )
    p.add_argument(
        "--skip-vault-check", action="store_true",
        help="Skip C3b vault-scan for faster runs; C3a within-delta check still runs",
    )
    return p.parse_args(argv)


def main(argv: list | None = None) -> None:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    args = _parse_args(argv)
    sys.exit(run_audit(
        delta_path=Path(args.delta),
        product=args.product,
        validation_summary=Path(args.validation_summary) if args.validation_summary else None,
        vault_root=Path(args.vault_root) if args.vault_root else _VAULT_DEFAULT,
        run_date=args.run_date or date.today().isoformat(),
        log_dir=Path(args.log_dir),
        skip_vault_check=args.skip_vault_check,
    ))


if __name__ == "__main__":
    main()
