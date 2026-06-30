# Point-in-Time (PIT) Coverage Disclosure

**Document Version:** 5.0 (Comprehensive Disclosure)
**Effective Date:** June 30, 2026
**Owner:** Lehlogonolo Kgato Maabane — Lekwankwa Corporation
**Scope:** All 32 countries across 5 data products

---

## Executive Summary

Lekwankwa data products are built with **point-in-time (PIT) awareness** across all 32 countries and 5 data products. PIT coverage enables users to reconstruct the **exact information set available at any historical date** — the foundation of look-ahead-free backtesting and institutional-grade quant research.

PIT coverage is structured in three tiers, reflecting what each source's API can provide:

| Tier | Scope | Coverage Type | Revision History |
|------|-------|--------------|-----------------|
| **Tier 1** | USA — 5 products via ALFRED | Full bitemporal archive | Complete: every revision since initial publication |
| **Tier 2** | Global macro — IMF WEO | QUAD_VINTAGE snapshots | 4 named vintages per observation year |
| **Tier 3** | 31 countries — EU27 + GBR/CAN/AUS/NOR | Release-date-stamped snapshots | Forward-going from first ingestion |

All tiers populate the same four PIT schema fields on every vault record: `official_release_date`, `as_of_date`, `revision_number`, and `is_revised_figure`. The difference is the **source and precision** of those values.

---

## Tier 1: USA Full Bitemporal Archive (ALFRED)

### What is captured

The St. Louis Fed's **Archival FRED (ALFRED)** API provides the complete revision history for every series published on FRED. For each observation period, ALFRED returns every value ever published — not just the most recent one. This enables true bitemporal queries: "What value was published on date X for observation period Y?"

**Products covered:**
- Food micropricing (BLS CPI-U item-level series)
- Wages & employment (BLS CES / CPS)
- Trade flows (Census FT-900 monthly trade data)
- Global macro / USA (FRED macro indicators)
- Housing (BLS shelter CPI, Census building permits, housing starts)

### How ALFRED works

Each ALFRED observation has:

| ALFRED Field | Vault Field | Meaning |
|-------------|-------------|---------|
| `realtime_start` | `official_release_date` | Date this value first appeared in FRED (true publication date) |
| `realtime_end` | *(used for `superseded_by` linkage)* | Date this value was superseded (`9999-12-31` = still current) |
| `date` | `data_timestamp` | Observation period date (e.g., `2020-03-01` for March 2020) |
| `value` | `observed_value` | The value as published in that vintage |

**Example — BLS food price revised:**

```
Observation: CPI Eggs, March 2020

Vintage 1  (revision_number=0):
  official_release_date = 2020-04-15   ← BLS published this on April 15
  observed_value = 1.84                ← first-release value
  is_revised_figure = False

Vintage 2  (revision_number=1):
  official_release_date = 2020-05-13   ← BLS revised this on May 13
  observed_value = 1.87                ← corrected value
  is_revised_figure = True

Vintage 3  (revision_number=2):
  official_release_date = 2021-01-15   ← annual benchmark revision
  observed_value = 1.86                ← benchmark-adjusted value
  is_revised_figure = True
```

A backtest running on April 20, 2020 will see `observed_value = 1.84` — the only value known at that time. A backtest running on June 1, 2020 will see `1.87`. This is true point-in-time accuracy.

### Live revision detection (ongoing)

After initial ALFRED backfill, `live_revision_detector.py` runs monthly. For each USA series observation:
1. Fetches the current value from the FRED API
2. Compares against the most recent stored revision
3. If changed: writes a new row with `revision_number = N+1`, `is_revised_figure = True`, `as_of_date = UTC now`
4. If unchanged: no write (the vault row is immutable once stored)

This ensures USA vault data accumulates revisions forward from the scrape date indefinitely.

---

## Tier 2: IMF Global Macro — QUAD_VINTAGE

### What is captured

The IMF's **World Economic Outlook (WEO)** is published four times per year. Each publication is a named vintage that supersedes the previous one. Lekwankwa captures all four vintages per observation year, creating four separate vault records per indicator-year pair.

**Products covered:**
- Global macro (GDP, CPI, unemployment, current account, government debt, fiscal balance) for all IMF member countries where data is available

### How QUAD_VINTAGE works

| Vintage | Publication Month | `official_release_date` |
|---------|-----------------|------------------------|
| April WEO (primary) | April | `{year}-04-01` |
| July Update | July | `{year}-07-01` |
| October WEO | October | `{year}-10-01` |
| January Update | January (next year) | `{year+1}-01-01` |

Each vintage produces a unique `data_vintage_id`:
```
IMF-WEO-{INDICATOR}-{OBS_YEAR}-Apr    (April WEO)
IMF-WEO-{INDICATOR}-{OBS_YEAR}-Jul    (July Update)
IMF-WEO-{INDICATOR}-{OBS_YEAR}-Oct    (October WEO)
IMF-WEO-{INDICATOR}-{OBS_YEAR}-Jan    (January Update)
```

**Example — IMF GDP forecast revised across vintages:**
```
Observation: USA GDP growth, 2024

April 2024 WEO:   official_release_date=2024-04-01  observed_value=2.7%
July 2024 Update: official_release_date=2024-07-01  observed_value=2.6%
Oct 2024 WEO:     official_release_date=2024-10-01  observed_value=2.8%
Jan 2025 Update:  official_release_date=2025-01-01  observed_value=2.9%  ← final
```

A backtest using data as of May 2024 will correctly see the April estimate (2.7%), not the October revision.

### Scope note

The IMF DataMapper API provides only **current-vintage values**. QUAD_VINTAGE captures the four named publications rather than continuous intraday revisions. Between publications, the IMF does not revise WEO data, making four vintages per year the natural granularity.

---

## Tier 3: 31-Country Live Release-Date-Stamped Snapshots

### What is captured

For the 31 countries outside the ALFRED-covered USA datasets, PIT coverage is provided via **release-date-stamped snapshots**: every record carries an `official_release_date` derived from the known publication schedule of the source statistical agency, and the vault record is written once at that estimated date (plus forward-going revision detection thereafter).

**Countries and sources:**

| Source | Countries | Products |
|--------|-----------|---------|
| **Eurostat SDMX** | 27 EU member states (AUT, BEL, BGR, CYP, CZE, DEU, DNK, ESP, EST, FIN, FRA, GRC, HRV, HUN, IRL, ITA, LTU, LVA, MLT, NLD, POL, PRT, ROU, SVK, SVN, SWE, + EU27 aggregate) | Food pricing, wages, housing, trade flows, global macro |
| **ONS** | GBR | Food pricing, wages, housing, trade flows, global macro |
| **Statistics Canada (StatCan)** | CAN | Food pricing, wages, housing, trade flows, global macro |
| **ABS** | AUS | Food pricing, wages, housing, trade flows, global macro |
| **SSB** | NOR | Food pricing, wages, housing, trade flows, global macro |

### How release dates are estimated

Each source agency publishes on a predictable schedule. When a series value is ingested, `official_release_date` is computed as:

```
official_release_date = obs_date + release_lag_days
```

Source-specific lag schedules embedded in the scrapers:

| Source | Indicator Type | Release Lag | Basis |
|--------|---------------|-------------|-------|
| Eurostat | HICP (monthly CPI) | 30 days | Flash HICP published ~15th of following month |
| Eurostat | Unemployment (monthly) | 45 days | LFS flash ~6 weeks after month-end |
| Eurostat | Labour Cost Index (quarterly) | 75 days | Eurostat Q-data release schedule |
| Eurostat | House Price Index (quarterly) | 90 days | Eurostat Q-data release schedule |
| Eurostat | Building Permits (quarterly) | 75 days | Eurostat Q-data release schedule |
| Eurostat | Trade / Balance of Payments (quarterly) | 90 days | Eurostat BOP publication schedule |
| Eurostat | GDP / National Accounts (quarterly) | 90 days | Eurostat national accounts flash estimate |
| ONS | All series | Per CDID series-level lag | ONS release calendar |
| StatCan | All series | Per table-level lag | StatCan release calendar |
| ABS | All series | Per series-level lag | ABS release calendar |
| SSB | All series | Per series-level lag | SSB (Statistics Norway) release calendar |

The `as_of_date` field is set equal to `official_release_date` for initial ingestion. For subsequent detected revisions, `as_of_date` is updated to the UTC timestamp when the revision was detected and written.

### Why estimated rather than exact dates

The public statistical APIs for Eurostat, ONS, StatCan, ABS, and SSB **do not embed a publication timestamp** in each API response. They expose current-vintage data only. The exact intraday publication time is not machine-readable from these APIs.

Lekwankwa uses statistically validated lag schedules — agency-published release calendars cross-referenced against actual historical publication patterns — to assign `official_release_date`. The uncertainty is at the day level (not week or month). For monthly series, the release date is typically accurate to ±3 days.

### Historical vintages: irrecoverable for Tier 3

The Eurostat SDMX API has no mechanism for vintage retrieval:
- `includeHistory=true` is silently ignored (v2.1) or returns HTTP 400 (v1.0)
- `updatedAfter` detects that a revision occurred but cannot return the superseded value
- No dedicated revision dataset (`namq_10_revise`) exists on the dissemination API

ONS, StatCan, ABS, and SSB have similar constraints: their public APIs return only the current time series, with no access to prior publication vintages.

**This means:** For Tier 3 data, revision history before the Lekwankwa first-ingestion date (June 2026) is permanently unrecoverable via the public API. Historical pre-ingestion data in the vault is written with `revision_number=1` and `is_revised_figure=False`, representing the snapshot as it existed at ingestion time. True initial-vs-revised history is not available for observations before first scrape.

**Going forward** (from June 2026 onward), `revision_detector.py` (Eurostat) and `live_revision_detector.py` (ONS/StatCan/ABS/SSB) track revisions by comparing each new API pull against the stored vault value for that observation period. When a changed value is detected:
- A new vault row is appended with `revision_number = N+1`
- `is_revised_figure = True`
- `as_of_date` set to the UTC detection timestamp
- The prior row is never modified (bitemporal append-only design)

---

## PIT Field Reference

All vault records across all tiers carry these four PIT fields:

| Field | Type | Definition |
|-------|------|------------|
| `official_release_date` | date | When the source agency first published this value. **Exact** for ALFRED/USA. **Estimated** for EU27/GBR/CAN/AUS/NOR. **Named vintage** for IMF. |
| `as_of_date` | timestamp (UTC) | The earliest date a trader/researcher could have known this value. Equals `official_release_date` on initial publication; equals detection timestamp on subsequent revisions. |
| `revision_number` | int | 0 = original publication, 1+ = revision. For Tier 1 (ALFRED), this reflects the true FRED revision count. For Tier 2/3, starts at 1 (initial) and increments on detected changes. |
| `is_revised_figure` | bool | False for initial publication rows, True for all revision rows. |
| `data_vintage_id` | string | Unique identifier encoding source + country + metric + period + version. Enables stable referencing across runs. |

---

## Query Patterns

### Backtest-safe query (as-of date filtering)

```sql
-- What data was available on April 20, 2020?
SELECT *
FROM vault
WHERE as_of_date <= '2020-04-20T00:00:00Z'
  AND (superseded_by IS NULL
       OR superseded_by IN (
         SELECT data_vintage_id FROM vault WHERE as_of_date > '2020-04-20T00:00:00Z'
       ));
```

### Latest values (no PIT filter)

```sql
-- Current best estimates, all revisions applied
SELECT *
FROM vault
WHERE is_revised_figure = FALSE OR revision_number = (
  SELECT MAX(revision_number) FROM vault v2
  WHERE v2.sovereign_series_id = vault.sovereign_series_id
    AND v2.data_timestamp = vault.data_timestamp
);
```

### First-published only (original estimates, no revisions)

```sql
-- Original-publication values — shows initial estimates before any revision
SELECT *
FROM vault
WHERE revision_number = 0        -- ALFRED/USA exact
   OR (revision_number = 1 AND is_revised_figure = FALSE);  -- Tier 2/3 initial
```

### Revision impact analysis

```sql
-- Measure how much values changed from first publish to current
SELECT
  sovereign_series_id,
  data_timestamp,
  MIN(observed_value)  AS first_published,
  MAX(observed_value)  AS latest_value,
  MAX(revision_number) AS total_revisions,
  MAX(observed_value) - MIN(observed_value) AS revision_delta
FROM vault
GROUP BY sovereign_series_id, data_timestamp
HAVING MAX(revision_number) > 0
ORDER BY ABS(revision_delta) DESC;
```

---

## Coverage Limitations

The following limitations apply. Lekwankwa does not misrepresent these constraints.

| Limitation | Tier | Detail |
|-----------|------|--------|
| Historical vintages not available | Tier 3 | Eurostat, ONS, StatCan, ABS, SSB APIs do not expose prior publication vintages. Pre-June 2026 revision history is unrecoverable. |
| Estimated release dates | Tier 3 | Dates are computed from agency lag schedules, not extracted from API timestamps. Accuracy is ±3 days for monthly series, ±7 days for quarterly. |
| IMF intra-vintage changes | Tier 2 | The IMF does not revise WEO data between the four named publications. Changes are only captured at publication boundaries. |
| BLS appropriations lapse | Tier 1 | BLS did not publish CPI data for October 2025 due to a government funding lapse. These gaps are documented in `catalog_expected_series.yaml` under `known_gaps`. |
| Single snapshot per SDMX fetch | Tier 3 | Each scheduled run produces one snapshot. If a source agency revises and then re-revises a value between two Lekwankwa runs, only the latest revision is captured; the intermediate value is lost. |

---

## Summary: Accurate Disclosure Statements

### USA (Tier 1)

> Full point-in-time revision history via ALFRED for food micropricing, wages and employment, trade flows, and global macro (USA). Every official revision since original BLS/Census/FRED publication is captured with its exact publication date. Backtests can reconstruct the exact data available on any historical date with day-level precision.

### IMF Global Macro (Tier 2)

> IMF World Economic Outlook data is captured in QUAD_VINTAGE format: four named vintages per observation year (April, July, October, January). Each vintage carries its exact scheduled publication date, enabling backtests to use only the WEO vintage that was available at decision time.

### All 32 countries / new releases (Tier 3)

> All 32 country datasets (EU27 + GBR, CAN, AUS, NOR) carry a release-date estimate on every observation, computed from each source agency's known publication schedule. Every new release is captured with its release-date timestamp, enabling point-in-time filtering on live data going forward from June 2026. Revision detection is active: when a source agency revises a previously published value, a new versioned record is appended to the vault with the revision detection date as `as_of_date` and `is_revised_figure=True`. Historical pre-ingestion revisions are not available for non-ALFRED sources due to API limitations.

---

**Document Version:** 5.0
**Last Updated:** June 30, 2026
**Next Review:** September 30, 2026
**Owner:** Lehlogonolo Kgato Maabane — Lekwankwa Corporation
