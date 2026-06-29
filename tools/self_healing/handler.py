"""
tools/self_healing/handler.py — Lekwankwa Corporation
Complete 3-layer self-healing orchestrator.

Flow (scraper exception):
  SCRAPER RUNS
      ↓ exception detected
  handle_exception() called
      ↓
  LAYER 2 — Scrape4AI retry (3 attempts with backoff)
      ↓ success → log RESOLVED_LAYER2, done
      ↓ all fail
  _escalate_to_layer3()
      ↓
  Claude Sonnet 4.6 diagnosis → Firestore token → approval email

Flow (audit / validation / quality report — skips Layer 2):
  handle_audit_finding()  — live_feed_audit.py C1-C5 ERRORs
  handle_quality_finding() — quality_report_generator.py CRITICAL/HIGH
      ↓ both call _escalate_to_layer3() directly
"""
from __future__ import annotations

import json
import logging
import traceback as tb
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

LOG_DIR = Path("logs/self_healing")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LIVE_FEED_PRODUCTS = frozenset({
    "food_micropricing",
    "wages_and_employment",
    "trade_flows",
})

SEVERITY_MAP = {
    "ValidationError":  "CRITICAL",
    "SchemaError":      "CRITICAL",
    "DataGap":          "CRITICAL",
    "Timeout":          "HIGH",
    "Connection":       "HIGH",
    "SSL":              "HIGH",
    "HTTPError":        "HIGH",
    "KeyError":         "HIGH",
    "ValueError":       "HIGH",
}


def classify_severity(exception: Exception, context: dict[str, Any]) -> str:
    exc_class = type(exception).__name__
    for keyword, severity in SEVERITY_MAP.items():
        if keyword.lower() in exc_class.lower():
            return severity
    if context.get("layer") in ("VALIDATION", "LIVE_FEED_AUDIT", "QUALITY_REPORT"):
        return "CRITICAL"
    return "HIGH"


def log_event(
    program: str,
    context: dict[str, Any],
    status: str,
    diagnosis: str | None = None,
) -> None:
    """Append structured event to logs/self_healing/events.jsonl."""
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "program":   program,
        "status":    status,
        "context":   context,
        "diagnosis": diagnosis,
    }
    with open(LOG_DIR / "events.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")
    log.info("[SELF-HEAL] Event logged: %s — %s", status, context.get("product"))


def trigger_quality_report(
    mode: str,
    products: list[str],
    countries: list[str],
) -> None:
    """Re-run quality report for affected product after fix is applied."""
    try:
        import subprocess, sys
        cmd = [sys.executable, "tools/quality_report_generator.py",
               "--mode", mode,
               "--products", ",".join(products),
               "--countries", ",".join(countries)]
        log.info("[SELF-HEAL] Re-running quality report: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            log.info("[SELF-HEAL] Quality report re-run succeeded.")
        else:
            log.warning("[SELF-HEAL] Quality report re-run exit %d: %s",
                        result.returncode, result.stderr[:300])
    except Exception as exc:
        log.error("[SELF-HEAL] Quality report re-run failed: %s", exc)


# ---------------------------------------------------------------------------
# Core Layer 3 escalation (shared by all entry points)
# ---------------------------------------------------------------------------

def _escalate_to_layer3(
    program: str,
    exception: Exception,
    context: dict[str, Any],
    tb_str: str,
) -> None:
    """
    Direct escalation to Layer 3. No Layer 2 retries.
    Called by handle_exception() after Layer 2 exhausts, and directly by
    handle_audit_finding() / handle_quality_finding().
    """
    layer = context.get("layer", "?")
    log.info("[SELF-HEAL] Layer 3 escalation — %s / %s",
             layer, Path(program).name)

    # Claude Sonnet 4.6 diagnosis
    try:
        from tools.self_healing.claude_diagnosis import diagnose_with_claude
        diagnosis = diagnose_with_claude(program, exception, context, tb_str)
    except Exception as l3_exc:
        log.error("[SELF-HEAL] Layer 3 Claude call failed: %s", l3_exc)
        diagnosis = f"[Claude unavailable: {l3_exc}]"

    # ── Auto-apply via GitHub → open PR → notify (always attempted) ─────────
    try:
        from tools.self_healing.claude_diagnosis import parse_diff, apply_simple_fix
        diff = parse_diff(diagnosis)
        if diff:
            log.info("[SELF-HEAL] Attempting auto-apply via GitHub PR...")
            pr_url = apply_simple_fix(diff, program, context)
            if pr_url:
                log.info("[SELF-HEAL] PR opened: %s", pr_url)
                from tools.self_healing.gmail_notifier import send_auto_fix_notification
                send_auto_fix_notification(program, context, diagnosis, pr_url)
                log_event(program, context, "AUTO_FIX_PR_OPENED", diagnosis=diagnosis)
                return
            log.warning("[SELF-HEAL] Auto-apply failed — falling back to approval email")
        else:
            log.warning("[SELF-HEAL] No diff in diagnosis — falling back to approval email")
    except Exception as auto_exc:
        log.warning("[SELF-HEAL] Auto-apply attempt failed: %s", auto_exc)

    # ── Fallback: token + approval email (only if auto-apply failed) ─────────
    try:
        from tools.self_healing.firestore_tokens import (
            generate_approval_token, store_in_firestore,
        )
        token = generate_approval_token(program, context, diagnosis)
        store_in_firestore(token, program, context, diagnosis)
    except Exception as fs_exc:
        log.error("[SELF-HEAL] Token storage failed: %s", fs_exc)
        import uuid
        token = str(uuid.uuid4()).replace("-", "")[:32]

    log.info("[SELF-HEAL] Sending approval email to info@lekwankwa.com...")
    try:
        from tools.self_healing.gmail_notifier import send_approval_email
        send_approval_email(program, context, diagnosis, token)
        log.info("[SELF-HEAL] Approval email sent. Awaiting response.")
    except Exception as mail_exc:
        log.error("[SELF-HEAL] Email send failed: %s", mail_exc)

    log_event(program, context, "PENDING_APPROVAL", diagnosis=diagnosis)


# ---------------------------------------------------------------------------
# Entry point 1: Scraper exception (with Layer 2 retry)
# ---------------------------------------------------------------------------

def handle_exception(
    program: str,
    exception: Exception,
    context: dict[str, Any],
) -> None:
    """
    Main entry point for scraper exceptions.

    program:   __file__ of the calling script
    exception: the caught exception
    context:   dict with keys: product, country, source, run_date, layer
               optionally: finding (validation result dict)
    """
    tb_str = tb.format_exc()
    context["severity"] = classify_severity(exception, context)
    product = context.get("product", "unknown")
    country = context.get("country", "unknown")

    log.error("[SELF-HEAL] Exception in %s — %s/%s — %s: %s",
              Path(program).name, product, country,
              type(exception).__name__, str(exception))
    log.debug("[SELF-HEAL] Traceback:\n%s", tb_str)

    # Layer 2: Scrape4AI retry
    log.info("[SELF-HEAL] Layer 2 — Scrape4AI retry starting...")
    try:
        from tools.self_healing.scrape4ai_retry import attempt_scrape4ai_retry
        layer2_ok = attempt_scrape4ai_retry(program, context, exception)
    except Exception as l2_exc:
        log.error("[SELF-HEAL] Layer 2 itself crashed: %s", l2_exc)
        layer2_ok = False

    if layer2_ok:
        log.info("[SELF-HEAL] Layer 2 resolved. No escalation needed.")
        log_event(program, context, "RESOLVED_LAYER2")
        return

    log.info("[SELF-HEAL] Layer 2 exhausted — MAJOR_EXCEPTION. Escalating to Layer 3.")
    _escalate_to_layer3(program, exception, context, tb_str)


# ---------------------------------------------------------------------------
# Entry point 2: Live feed audit / validation errors (no Layer 2)
# ---------------------------------------------------------------------------

def handle_audit_finding(
    program: str,
    context: dict[str, Any],
    violations: list[dict[str, Any]],
) -> None:
    """
    Entry point for live_feed_audit.py C1-C5 ERRORs and validation failures.
    Data integrity violations are not retryable — goes directly to Layer 3.

    violations: list of violation dicts from run_audit() (check, severity, detail, ...)
    """
    error_violations = [v for v in violations if v.get("severity") == "ERROR"]
    if not error_violations:
        return

    error_checks = list({v["check"] for v in error_violations})
    context.setdefault("layer", "LIVE_FEED_AUDIT")
    context["severity"] = "CRITICAL"

    synthetic_exc = RuntimeError(
        f"Audit FLAG — {len(error_violations)} violation(s): "
        f"{', '.join(error_checks)}"
    )
    tb_str = "\n".join(
        f"[{v['check']}] {v.get('severity', '?')} — {v['detail']}"
        for v in error_violations[:10]
    )

    log.error("[SELF-HEAL] Audit violations in %s — %s",
              context.get("product", "?"), ", ".join(error_checks))
    log_event(program, context, "AUDIT_FLAG")
    _escalate_to_layer3(program, synthetic_exc, context, tb_str)


# ---------------------------------------------------------------------------
# Entry point 3: Quality report CRITICAL/HIGH findings (no Layer 2)
# ---------------------------------------------------------------------------

def handle_quality_finding(
    program: str,
    context: dict[str, Any],
    findings: list[dict[str, Any]],
) -> None:
    """
    Entry point for quality_report_generator.py CRITICAL/HIGH findings.
    Skips Layer 2 — analytical findings are not retryable.

    findings: list of Finding dataclass dicts (from dataclasses.asdict()).
    """
    high_crit = [f for f in findings if f.get("severity") in ("CRITICAL", "HIGH")]
    if not high_crit:
        return

    context.setdefault("layer", "QUALITY_REPORT")
    context["severity"] = (
        "CRITICAL" if any(f.get("severity") == "CRITICAL" for f in high_crit)
        else "HIGH"
    )

    codes    = list({f.get("code", "?") for f in high_crit})
    products = list({f.get("product", "?") for f in high_crit})

    synthetic_exc = RuntimeError(
        f"Quality report — {len(high_crit)} CRITICAL/HIGH finding(s) "
        f"in {', '.join(products)}: {', '.join(codes[:5])}"
    )
    tb_str = "\n".join(
        f"[{f.get('severity', '?')}] {f.get('product', '?')}/"
        f"{f.get('country_group', '?')} {f.get('code', '?')} — "
        f"{f.get('message', '?')}"
        for f in high_crit[:10]
    )

    log.error("[SELF-HEAL] Quality report: %d CRITICAL/HIGH in %s",
              len(high_crit), ", ".join(products))
    log_event(program, context, "QUALITY_ALERT")
    _escalate_to_layer3(program, synthetic_exc, context, tb_str)


