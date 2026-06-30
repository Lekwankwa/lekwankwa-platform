"""
Building Permits Quarterly Ingest — 14 countries with no sts_cobp_m data

Fetches sts_cobp_q for the 14 EU countries that have no monthly permit data
in the Eurostat sts_cobp_m dataflow.  Applies quarterly carry-forward to fill
inter-quarter months so all 12 calendar months are represented.

Target countries:
  AUT, BGR, HRV, CZE, DNK, EST, IRL, ITA, LVA, LTU, LUX, MLT, POL, SVK

sovereign_series_id:  EUROSTAT_PERMIT_{cpa_suffix}_{ISO3}
vintage_id:          EUROSTAT-PERMIT-{ISO3}-{YYYY-MM}-v1
                     EUROSTAT-PERMIT-{ISO3}-{YYYY-MM}-CF-v1  (carry-forward)
macro_metric_name:   AUTHORIZED_PERMITS_TOTAL_UNITS
Output file:         permits_eu27_data.parquet  (same as main v2 ingestor)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "backtesting"))

from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import write_partition, _estimate_release_date
from scrapers.eurostat.country_map import GEO2_TO_ISO3, ISO3_TO_NAME

VAULT = _ROOT / "lekwankwa-historical-vault"
PRODUCT = "Housing_Supply_and_Shelter_Inflation"
SOURCE = "eurostat_sdmx"
FILENAME = "permits_eu27_data.parquet"
RELEASE_LAG_DAYS = 90
START_PERIOD_Q = "2000-Q1"
MACRO_METRIC = "AUTHORIZED_PERMITS_TOTAL_UNITS"

# 14 countries with no sts_cobp_m data
MISSING_GEO2 = ["AT","BG","HR","CZ","DK","EE","IE","IT","LV","LT","LU","MT","PL","SK"]
MISSING_ISO3 = {GEO2_TO_ISO3[g] for g in MISSING_GEO2 if g in GEO2_TO_ISO3}

QUARTER_MONTHS = {1, 4, 7, 10}
_VID_DATE_RE = re.compile(r"^(.*?)(\d{4}-\d{2})(-CF-v\d+|-v\d+)$")


def _make_carry_vid(source_vid: str, carry_date: pd.Timestamp) -> str:
    m = _VID_DATE_RE.match(str(source_vid))
    if m:
        return f"{m.group(1)}{carry_date.strftime('%Y-%m')}-CF-v1"
    return f"{source_vid}-{carry_date.strftime('%Y-%m')}-CF"


def _build_vintage_id(iso3: str, obs_date: pd.Timestamp) -> str:
    return f"EUROSTAT-PERMIT-{iso3}-{obs_date.strftime('%Y-%m')}-v1"


def run() -> int:
    print("=" * 70)
    print(f"Quarterly Permits Ingest — {len(MISSING_ISO3)} countries")
    print(f"Source: sts_cobp_q | Target: {FILENAME}")
    print("=" * 70)

    df_raw = fetch_dataset(
        dataset_id=  "sts_cobp_q",
        filters=      {"freq": "Q", "s_adj": "NSA", "indic_bt": "BPRM_DW", "unit": "I15"},
        geo_list=     MISSING_GEO2,
        start_period= START_PERIOD_Q,
    )

    if df_raw.empty:
        print("ERROR: sts_cobp_q returned 0 rows")
        return 0

    print(f"Raw rows from API: {len(df_raw):,}")

    geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")
    rows = []

    for _, r in df_raw.iterrows():
        geo  = str(r.get(geo_col, ""))
        iso3 = GEO2_TO_ISO3.get(geo)
        if iso3 not in MISSING_ISO3:
            continue

        period   = str(r.get("time", ""))
        obs_date = period_to_date(period)
        if obs_date is None:
            continue

        val = r.get("value")
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        cpa_suffix = ""
        if "cpa2_1" in df_raw.columns:
            cpa_val = str(r.get("cpa2_1", "")).replace("CPA_", "").replace("-", "_")
            if cpa_val and cpa_val != "nan":
                cpa_suffix = f"_{cpa_val}"

        rdate = _estimate_release_date(obs_date, RELEASE_LAG_DAYS)
        vid   = _build_vintage_id(iso3, obs_date)
        sid   = f"EUROSTAT_PERMIT{cpa_suffix}_{iso3}"

        rows.append({
            "data_vintage_id":       vid,
            "confidence_tier":       "PRIMARY",
            "sovereign_series_id":   sid,
            "macro_metric_name":     MACRO_METRIC,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "official_release_date": rdate,
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       "INDEX_2015_100",
            "is_revised_figure":     False,
            "data_timestamp":        obs_date.isoformat() + "Z",
            "revision_number":       1,
            "iso_alpha3":            iso3,
            "country_name":          ISO3_TO_NAME.get(iso3, iso3),
            "source":                SOURCE,
            "source_agency":         "EUROSTAT",
            "source_sub_category":   "HOUSING",
            "sdmx_dataflow":         "sts_cobp_q",
            "observation_period":    obs_date.strftime("%Y-%m"),
            "sdmx_frequency":        "Q",
            "published_date":        rdate,
            "data_quality_certified": True,
            "is_forecast":           False,
            "is_interpolated":       False,
            "interpolation_method":  None,
        })

    if not rows:
        print("ERROR: schema mapping produced 0 rows")
        return 0

    df_vault = pd.DataFrame(rows)
    print(f"Quarterly rows mapped: {len(df_vault):,} across "
          f"{df_vault['iso_alpha3'].nunique()} countries")

    # Generate carry-forward rows for months +1 and +2 within each quarter
    df_vault["_rd"] = pd.to_datetime(df_vault["reporting_date"], errors="coerce")
    carry_rows = []
    generated: dict[str, set] = {}

    for _, row in df_vault[df_vault["_rd"].dt.month.isin(QUARTER_MONTHS)].iterrows():
        q_date = row["_rd"]
        sid = str(row["sovereign_series_id"])
        if sid not in generated:
            generated[sid] = set(
                df_vault[df_vault["sovereign_series_id"] == sid]["_rd"]
                .dropna().dt.strftime("%Y-%m-%d")
            )

        for offset in (1, 2):
            mo = q_date.month + offset
            yr = q_date.year
            if mo > 12:
                mo -= 12
                yr += 1
            carry_date = pd.Timestamp(yr, mo, 1)
            carry_str = carry_date.strftime("%Y-%m-%d")
            if carry_str in generated[sid]:
                continue
            nr = row.drop(labels=["_rd"]).to_dict()
            nr["reporting_date"] = carry_str
            nr["data_timestamp"] = carry_date.isoformat() + "Z"
            nr["data_vintage_id"] = _make_carry_vid(row["data_vintage_id"], carry_date)
            nr["interpolation_method"] = "QUARTERLY_CARRY_FORWARD"
            nr["is_interpolated"] = True
            nr["data_quality_certified"] = False
            carry_rows.append(nr)
            generated[sid].add(carry_str)

    df_carry = pd.DataFrame(carry_rows) if carry_rows else pd.DataFrame()
    df_all = pd.concat([df_vault.drop(columns=["_rd"]), df_carry], ignore_index=True) \
             if not df_carry.empty else df_vault.drop(columns=["_rd"])

    print(f"  Quarterly:     {len(df_vault):,} rows")
    print(f"  Carry-forward: {len(df_carry):,} rows")
    print(f"  Total to write:{len(df_all):,} rows")

    df_all["_obs_ts"] = pd.to_datetime(df_all["data_timestamp"], errors="coerce", utc=True)

    total_written = 0
    for iso3, grp_iso in df_all.groupby("iso_alpha3"):
        vault_root = (
            VAULT / f"product={PRODUCT}" / f"country={iso3}" / f"source={SOURCE}"
        )
        for (yr, mo), grp in grp_iso.groupby([
            grp_iso["_obs_ts"].dt.year, grp_iso["_obs_ts"].dt.month
        ]):
            out = grp.drop(columns=["_obs_ts"])
            write_partition(out, vault_root, int(yr), int(mo), FILENAME)
            total_written += len(out)

    print(f"\nTotal written: {total_written:,} rows across {len(MISSING_ISO3)} countries")

    # Summary per country
    df_all["_obs_ts"] = pd.to_datetime(df_all["data_timestamp"], errors="coerce", utc=True)
    for iso3, grp in df_all.groupby("iso_alpha3"):
        months_per_yr = grp.groupby(grp["_obs_ts"].dt.year)["_obs_ts"].apply(
            lambda x: x.dt.month.nunique()
        )
        min_months = months_per_yr.min()
        max_months = months_per_yr.max()
        print(f"  {iso3}: {len(grp):>6} rows | {min_months}-{max_months} months/yr")

    print("=" * 70)
    return total_written


if __name__ == "__main__":
    run()
