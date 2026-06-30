"""
FSO Switzerland — BLOCKED (as of 2026-06-18)

Status: PENDING_INGESTION / BLOCKED

Root cause:
  FSO national accounts data migrated from STAT-TAB to Swiss Stats Explorer (SSE).
  The new SDMX endpoint (dam-api.bfs.admin.ch) returns HTTP 503 (server down).
  STAT-TAB still operates but has ZERO theme-04 (national economy) tables —
  all national accounts data has been removed.

  Legacy STAT-TAB endpoint:    https://www.pxweb.bfs.admin.ch/api/v1/en/
  New SSE SDMX endpoint:       https://dam-api.bfs.admin.ch/hub/api/rest/sdmx/v2/
  SSE download portal:         https://www.bfs.admin.ch/bfs/en/home/statistics.html

Tested endpoints (2026-06-18):
  dam-api.bfs.admin.ch/hub/api/dam/assets/orderNr:do-e-04.02.01.10-q/master
    → HTTP 503 (server down, 57kb error page)
  dam-api.bfs.admin.ch/hub/api/rest/sdmx/v2/data/CH1,BFS,...
    → HTTP 503

Alternative route:
  SIX Swiss Exchange / SNB data portal sometimes mirrors FSO national accounts.
  SNB BankingStatistics API: https://data.snb.ch/api/cube/
  Candidate series: GDP from SNB cube "snbdep" or FSO table px-x-0401010000_01

Action required:
  Re-test dam-api.bfs.admin.ch in 4+ weeks to check if SSE is back online.
  If SSE remains down, pivot to SNB GDP series as substitute source.

This file serves as documentation only. No data is ingested for CHE until
the SSE endpoint is accessible or an SNB substitute is configured.
"""

PIT_COVERAGE  = "BLOCKED"
SOURCE        = "fso_sse"
SOURCE_AGENCY = "FSO"
ISO3          = "CHE"
BLOCKED_REASON = (
    "FSO national accounts migrated to Swiss Stats Explorer (SSE) "
    "which returns HTTP 503. STAT-TAB has no theme-04 (national economy) tables."
)

# Placeholder — no series to ingest until SSE is accessible
SERIES: list = []
