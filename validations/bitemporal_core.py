"""
Shared bitemporal PIT validation checks.

All product PIT validators import from here.  No product-specific logic lives here —
only the 12 generic checks, a numpy serialiser, and a report writer.

BITEMPORAL SAFETY RULE  (used by all checks):
  A backtest is bias-free iff  data_timestamp <= query_date
                               AND published_date  <= query_date
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

TODAY = pd.Timestamp.utcnow().normalize()
TODAY_END = TODAY + pd.Timedelta(days=1)  # end-of-today ceiling for ingestion timestamps
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _r(status, check, message, details=None):
    return {"status": status, "check": check, "message": message, "details": details or {}}

def np_safe(obj):
    if isinstance(obj, np.integer):  return int(obj)
    if isinstance(obj, np.floating): return float(obj)
    if isinstance(obj, np.bool_):    return bool(obj)
    if isinstance(obj, np.ndarray):  return obj.tolist()
    if isinstance(obj, dict):        return {k: np_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):        return [np_safe(v) for v in obj]
    return obj

# ─────────────────────────────────────────────────────────────────────────────
# CHECK FUNCTIONS  (pure: accept a DataFrame, return a result dict)
# ─────────────────────────────────────────────────────────────────────────────

def check_unique_record_ids(df):
    null_ids = int(df["record_id"].isna().sum())
    dupes    = int(df["record_id"].duplicated().sum())
    if null_ids == 0 and dupes == 0:
        return _r("PASS", "Unique Record IDs",
                  f"All {len(df):,} record_ids unique and non-null")
    return _r("FAIL", "Unique Record IDs",
              f"{null_ids} null, {dupes} duplicate record_ids",
              {"null_count": null_ids, "duplicate_count": dupes})


def check_knowledge_completeness(df):
    """published_date and as_of_date non-null — required for PIT slice queries."""
    np_ = int(df["published_date"].isna().sum())
    na_ = int(df["as_of_date"].isna().sum())
    if np_ == 0 and na_ == 0:
        return _r("PASS", "Knowledge Time Completeness",
                  f"published_date and as_of_date fully populated ({len(df):,} records)")
    return _r("FAIL", "Knowledge Time Completeness",
              f"{np_} null published_date, {na_} null as_of_date "
              f"— records unqueryable on knowledge-time axis",
              {"null_published_date": np_, "null_as_of_date": na_})


def check_valid_to_knowledge_ordering(df):
    """published_date >= data_timestamp — knowledge time must not precede valid time."""
    pub = pd.to_datetime(df["published_date"], utc=True, errors="coerce")
    dat = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    v   = int((pub < dat).sum())
    if v == 0:
        return _r("PASS", "Valid → Knowledge Ordering",
                  f"All {len(df):,} records: published_date >= data_timestamp")
    bad = df.loc[pub < dat, ["source_series_id", "data_timestamp", "published_date"]].head(3)
    return _r("FAIL", "Valid → Knowledge Ordering",
              f"{v:,} records have published_date < data_timestamp "
              f"(retroactive knowledge — backtesting contamination risk)",
              {"violation_count": v, "examples": bad.to_dict("records")})


def check_knowledge_ordering(df):
    """as_of_date >= published_date — knowledge window opens at publication."""
    aod = pd.to_datetime(df["as_of_date"],     utc=True, errors="coerce")
    pub = pd.to_datetime(df["published_date"], utc=True, errors="coerce")
    v   = int((aod < pub).sum())
    if v == 0:
        return _r("PASS", "Knowledge Ordering",
                  f"All {len(df):,} records: as_of_date >= published_date")
    return _r("FAIL", "Knowledge Ordering",
              f"{v:,} records have as_of_date < published_date", {"violation_count": v})


def check_as_of_published_cohesion(df):
    """For revision_number=0, as_of_date == published_date (no artificial offset)."""
    rev0 = df[df["revision_number"] == 0]
    if rev0.empty:
        return _r("SKIP", "as_of / published Cohesion", "No revision_number=0 records found")
    aod = pd.to_datetime(rev0["as_of_date"],     utc=True, errors="coerce")
    pub = pd.to_datetime(rev0["published_date"], utc=True, errors="coerce")
    n   = int((aod != pub).sum())
    if n == 0:
        return _r("PASS", "as_of / published Cohesion",
                  f"All {len(rev0):,} revision-0 records: as_of_date == published_date "
                  f"(correct initial-load bitemporal convention)")
    return _r("WARN", "as_of / published Cohesion",
              f"{n:,}/{len(rev0):,} revision-0 records have as_of_date ≠ published_date",
              {"mismatch_count": n, "total_rev0": len(rev0)})


def check_knowledge_horizon(df):
    """No published_date or as_of_date may be in the future."""
    pub = pd.to_datetime(df["published_date"], utc=True, errors="coerce")
    aod = pd.to_datetime(df["as_of_date"],     utc=True, errors="coerce")
    fp, fa = int((pub > TODAY).sum()), int((aod > TODAY_END).sum())
    if fp == 0 and fa == 0:
        return _r("PASS", "Knowledge Time Horizon",
                  f"No future-dated published_date or as_of_date (cutoff: {TODAY.date()})")
    return _r("FAIL", "Knowledge Time Horizon",
              f"{fp} future published_date, {fa} future as_of_date — vault knows the future",
              {"future_published_date_count": fp, "future_as_of_date_count": fa,
               "cutoff": str(TODAY.date())})


def check_anti_retroactive_ingestion(df):
    """conversion_timestamp >= published_date — records can't be ingested before publication."""
    conv = pd.to_datetime(df["conversion_timestamp"], utc=True, errors="coerce")
    pub  = pd.to_datetime(df["published_date"],       utc=True, errors="coerce")
    v    = int((conv < pub).sum())
    if v == 0:
        return _r("PASS", "Anti-Retroactive Ingestion",
                  f"All {len(df):,} records: conversion_timestamp >= published_date "
                  f"(no pre-publication ingestion — backtesting-safe)")
    bad = df.loc[conv < pub, ["source_series_id", "published_date", "conversion_timestamp"]].head(3)
    return _r("FAIL", "Anti-Retroactive Ingestion",
              f"{v:,} records ingested before their published_date (backtesting contamination risk)",
              {"violation_count": v, "examples": bad.to_dict("records")})


def check_conversion_horizon(df):
    """conversion_timestamp must not be after end of today."""
    conv = pd.to_datetime(df["conversion_timestamp"], utc=True, errors="coerce")
    n    = int((conv > TODAY_END).sum())
    if n == 0:
        return _r("PASS", "Conversion Time Horizon",
                  f"No future-dated conversion_timestamp (cutoff: {TODAY.date()}+1d)")
    return _r("FAIL", "Conversion Time Horizon",
              f"{n} records have conversion_timestamp beyond end of today",
              {"count": n, "cutoff": str(TODAY.date())})


def check_publication_lag(data, pub_lag_bounds):
    """
    Per-source publication lag must match the source's release schedule.

    data: dict {source_name: DataFrame}  OR  a single DataFrame with a 'source' column.
    pub_lag_bounds: {source_name: (lo_months, hi_months)}
    """
    if isinstance(data, dict):
        frames = data
    else:
        frames = {src: data[data["source"] == src] for src in pub_lag_bounds
                  if not data[data["source"] == src].empty}

    src_stats, issues = {}, []
    for src, (lo, hi) in pub_lag_bounds.items():
        sub = frames.get(src, pd.DataFrame())
        if sub.empty:
            continue
        pub  = pd.to_datetime(sub["published_date"], utc=True, errors="coerce")
        dat  = pd.to_datetime(sub["data_timestamp"], utc=True, errors="coerce")
        mask = pub.notna() & dat.notna()
        lag  = ((pub[mask].dt.year  - dat[mask].dt.year) * 12 +
                (pub[mask].dt.month - dat[mask].dt.month))
        total    = int(mask.sum())
        in_range = int(((lag >= lo) & (lag <= hi)).sum())
        pct      = round(in_range / total * 100, 2) if total else 0.0
        src_stats[src] = {
            "total_valid": total, "in_range": in_range, "pct_in_range": pct,
            "expected_range_months": f"{lo}–{hi}",
            "lag_min_months":    int(lag.min())      if len(lag) else None,
            "lag_max_months":    int(lag.max())      if len(lag) else None,
            "lag_median_months": float(lag.median()) if len(lag) else None,
        }
        if pct < 90:
            issues.append(f"{src}: only {pct:.1f}% within expected {lo}–{hi} month lag")

    summary = "; ".join(
        f"{s}: {v['pct_in_range']:.1f}% in range (median {v['lag_median_months']} mo)"
        for s, v in src_stats.items()
    )
    if not issues:
        return _r("PASS", "Publication Lag by Source", summary, {"sources": src_stats})
    return _r("WARN", "Publication Lag by Source",
              f"Unexpected lag distribution: {'; '.join(issues)}", {"sources": src_stats})


def check_knowledge_monotonicity(df, sample=50, min_len=4, min_year=None):
    """
    For each series, published_date must be non-decreasing as data_timestamp advances.
    A retrograde step signals a vintage mismatch that breaks PIT slice queries.
    """
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    subset = df
    if min_year:
        subset = df[pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce").dt.year >= min_year]
    series_ids = subset[sid_col].dropna().unique()
    if len(series_ids) > sample:
        series_ids = np.random.default_rng(42).choice(series_ids, sample, replace=False)

    violations, checked = [], 0
    for sid in series_ids:
        grp = (subset[subset[sid_col] == sid]
               .assign(_dt=lambda x: pd.to_datetime(x["data_timestamp"], utc=True, errors="coerce"),
                       _pub=lambda x: pd.to_datetime(x["published_date"],  utc=True, errors="coerce"))
               .dropna(subset=["_dt", "_pub"])
               .sort_values("_dt").drop_duplicates(subset=["_dt"]))
        if len(grp) < min_len:
            continue
        checked += 1
        n = int((grp["_pub"].diff().iloc[1:] < pd.Timedelta(0)).sum())
        if n > 0:
            violations.append({"series": str(sid), "non_monotone_steps": n})

    if not violations:
        return _r("PASS", "Knowledge Time Monotonicity",
                  f"published_date non-decreasing in all {checked} sampled series "
                  f"(no vintage mismatch detected)")
    return _r("WARN", "Knowledge Time Monotonicity",
              f"{len(violations)}/{checked} sampled series have retrograde published_date "
              f"(possible vintage mismatch)",
              {"violation_count": len(violations), "checked": checked, "examples": violations[:5]})


def check_bitemporal_uniqueness(df, skip_sources=None):
    """
    (source_series_id, data_timestamp, revision_number) must be unique.
    Duplicates make any PIT slice non-deterministic.

    skip_sources: sources whose series_id is a category-level key (e.g. USDA).
                  Those sources are excluded from this check; row uniqueness is
                  guaranteed via check_unique_record_ids instead.
    """
    sid_col = "source_series_id" if "source_series_id" in df.columns else "sovereign_series_id"
    key = [sid_col, "data_timestamp", "revision_number"]
    skip_sources = skip_sources or []
    evaluated = (df[~df["source"].isin(skip_sources)] if "source" in df.columns and skip_sources
                 else df)
    dupes = int(evaluated.duplicated(subset=key, keep=False).sum())
    skip_note = (f" ({', '.join(skip_sources)} excluded — category-level series key)"
                 if skip_sources else "")
    if dupes == 0:
        return _r("PASS", "Bitemporal Uniqueness",
                  f"All {len(evaluated):,} evaluated records unique on "
                  f"({sid_col}, data_timestamp, revision_number){skip_note} "
                  f"— PIT reconstruction is deterministic")
    bad = (evaluated[evaluated.duplicated(subset=key, keep=False)][key]
           .drop_duplicates().head(5).to_dict("records"))
    return _r("FAIL", "Bitemporal Uniqueness",
              f"{dupes:,} records share ({sid_col}, data_timestamp, revision_number) "
              f"— PIT query results are ambiguous",
              {"duplicate_count": dupes, "examples": bad})


def check_supersession_integrity(df):
    """superseded_by must be null or point to a valid record_id."""
    has_sup = df["superseded_by"].notna()
    total   = int(has_sup.sum())
    if total == 0:
        return _r("PASS", "Supersession Integrity",
                  f"All {len(df):,} records: superseded_by = null (current-version only)")
    valid_ids   = set(df["record_id"].dropna().astype(str))
    invalid_ref = int(df[has_sup]["superseded_by"]
                      .apply(lambda x: str(x) not in valid_ids if pd.notna(x) else False).sum())
    if invalid_ref == 0:
        return _r("PASS", "Supersession Integrity",
                  f"{total} superseded records all point to valid record_ids")
    return _r("FAIL", "Supersession Integrity",
              f"{invalid_ref} superseded_by values reference non-existent record_ids "
              f"(broken revision chain)",
              {"invalid_ref_count": invalid_ref, "total_superseded": total})


# ─────────────────────────────────────────────────────────────────────────────
# REPORT WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_report(report_json: Path, report_txt: Path, product, country, results):
    """Persist JSON + TXT report and log a summary line. Returns True if overall PASS."""
    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    overall = "PASS" if failed == 0 else "FAIL"

    report = {
        "product":   product,
        "country":   country,
        "generated": datetime.now(timezone.utc).isoformat(),
        "overall":   overall,
        "counts":    {"passed": passed, "failed": failed, "warned": warned, "skipped": skipped},
        "bitemporal_dimensions": {
            "valid_time":      "data_timestamp",
            "knowledge_time":  "published_date / as_of_date",
            "ingestion_time":  "conversion_timestamp",
            "pit_safety_rule": "data_timestamp <= query_date AND published_date <= query_date",
        },
        "checks": np_safe(results),
    }
    report_json.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    lines = [
        f"{product.upper()} — BITEMPORAL PIT VALIDATION REPORT",
        f"Generated : {report['generated']}",
        f"Overall   : {overall}",
        f"Counts    : {passed}P / {failed}F / {warned}W / {skipped}S",
        "─" * 70,
        "PIT SAFETY RULE:  data_timestamp <= query_date  AND  published_date <= query_date",
        "─" * 70,
    ]
    for r in results:
        lines.append(f"[{r['status']}] {r['check']}: {r['message']}")
    report_txt.write_text("\n".join(lines), encoding="utf-8")

    logger.info(f"  {passed} passed / {failed} failed / {warned} warned / {skipped} skipped")
    logger.info(f"  Overall: [{overall}]")
    logger.info(f"  Reports: {report_json}, {report_txt}")
    return overall == "PASS"
