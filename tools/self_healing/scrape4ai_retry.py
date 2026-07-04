"""
tools/self_healing/scrape4ai_retry.py — Lekwankwa Corporation
Layer 2: Scrape4AI self-healing retry for transient and schema-change errors.

Attempts up to 3 retries with exponential backoff before declaring
MAJOR_EXCEPTION and escalating to Layer 3 (Claude diagnosis).
"""
from __future__ import annotations

import importlib
import logging
import sys
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

TRANSIENT_CODES = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES     = 3
BACKOFF_SECS    = [30, 120, 300]   # 30s → 2m → 5m


def _is_transient(exc: Exception) -> bool:
    exc_class = type(exc).__name__
    return (
        "Timeout"    in exc_class or
        "Connection" in exc_class or
        "SSL"        in exc_class or
        getattr(exc, "status_code", None) in TRANSIENT_CODES or
        getattr(exc, "code",        None) in TRANSIENT_CODES
    )


def _rerun_with_scrape4ai(program: str, context: dict[str, Any]) -> bool:
    """
    Re-run ONLY the failed scraper function, in-process — not the whole
    run.py pipeline (scrape + validation) as a nested subprocess.

    The original implementation re-ran the entire run.py via subprocess
    with a 600s timeout. Once validation legitimately started taking up
    to 30+ minutes (see run_9_stage_validation), that made retries
    structurally broken: a retry whose scrape half succeeded would still
    get killed by TimeoutExpired once it reached validation, reporting a
    false failure, and 3 such retries could exhaust the job's entire
    1-hour Cloud Run task timeout on retries alone before validation
    ever got a chance to run in the (outer) calling process. Retrying
    just the scraper call lets the caller's own run_country() loop
    proceed to validation normally afterward, in the original process.
    """
    product = context.get("product", "")
    country = context.get("country", "")
    source  = context.get("source", "")
    module_path = context.get("module")
    fn_name     = context.get("fn")
    mode        = context.get("mode", "incremental")
    since       = context.get("since")

    if not module_path or not fn_name:
        raise RuntimeError(
            "Layer 1 retry requires 'module' and 'fn' in context to "
            "re-call the scraper directly — caller must pass these "
            "through to handle_exception()."
        )

    # Notify Scrape4AI to re-crawl this source endpoint if available
    try:
        crawl4ai_path = Path(__file__).resolve().parents[2] / "crawl4ai-main"
        if crawl4ai_path.exists():
            sys.path.insert(0, str(crawl4ai_path))
        from crawl4ai import WebCrawler
        # Simple re-crawl to refresh cached page structure
        crawler = WebCrawler()
        crawler.warmup()
        log.info("  [Scrape4AI] Warmed up crawler for %s/%s/%s", product, country, source)
    except ImportError:
        log.debug("  [Scrape4AI] crawl4ai not installed — proceeding with direct retry")
    except Exception as crawl_exc:
        log.warning("  [Scrape4AI] Crawler warmup failed (non-fatal): %s", crawl_exc)

    log.info("  [Scrape4AI] Re-calling %s.%s directly for %s/%s",
             module_path, fn_name, product, country)
    mod = importlib.import_module(module_path)
    fn  = getattr(mod, fn_name)
    fn(mode=mode, since=since)
    return True


def attempt_scrape4ai_retry(
    program: str,
    context: dict[str, Any],
    exception: Exception,
) -> bool:
    """
    Layer 2 entry point. Returns True if any retry succeeded.
    Returns False if all MAX_RETRIES exhausted (caller escalates to Layer 3).
    """
    product = context.get("product")
    country = context.get("country")

    log.info("[SELF-HEAL] Layer 2 — Scrape4AI retry starting for %s/%s",
             product, country)

    for attempt in range(1, MAX_RETRIES + 1):
        sleep_secs = BACKOFF_SECS[attempt - 1]
        log.info("  Retry %d/%d — waiting %ds before attempt...",
                 attempt, MAX_RETRIES, sleep_secs)
        time.sleep(sleep_secs)

        try:
            success = _rerun_with_scrape4ai(program, context)
            if success:
                log.info("  [Scrape4AI] Retry %d SUCCEEDED for %s/%s",
                         attempt, product, country)
                return True
        except Exception as retry_exc:
            log.warning("  [Scrape4AI] Retry %d failed: %s", attempt, retry_exc)
            exception = retry_exc

    log.error("[SELF-HEAL] All %d Scrape4AI retries exhausted for %s/%s."
              " Escalating to MAJOR_EXCEPTION.", MAX_RETRIES, product, country)
    return False
