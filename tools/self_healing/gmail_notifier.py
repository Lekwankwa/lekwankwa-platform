"""
tools/self_healing/gmail_notifier.py — Lekwankwa Corporation
Approval email sender for self-healing pipeline (Layer 3).

Sends an approval request to info@lekwankwa.com via Gmail SMTP.
Approval and reject links point to Cloud Functions endpoint.
"""
from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

log = logging.getLogger(__name__)

RECIPIENT = "info@lekwankwa.com"


def _approval_service_base() -> str:
    """Return the Cloud Run approval service base URL from Secret Manager."""
    from tools.self_healing.secret_manager import get_secret
    try:
        return get_secret("approval-service-url").rstrip("/")
    except Exception:
        import os
        return os.environ.get(
            "APPROVAL_SERVICE_URL",
            "https://fix-approval-service-CONFIGURE_ME.a.run.app",
        )


def send_approval_email(
    program: str,
    context: dict[str, Any],
    diagnosis: str,
    approval_token: str,
) -> None:
    """
    Send HTML + plain-text approval email to info@lekwankwa.com.
    Raise on SMTP failure (caller logs and continues).
    """
    from tools.self_healing.secret_manager import get_secret

    sender   = get_secret("gmail-sender-address")
    password = get_secret("gmail-app-password")

    base = _approval_service_base()

    severity  = context.get("severity", "HIGH")
    product   = context.get("product", "unknown")
    country   = context.get("country", "unknown")
    source    = context.get("source", "unknown")
    run_date  = context.get("run_date", "unknown")
    layer     = context.get("layer", "SCRAPER")

    approve_url = f"{base}/approve?token={approval_token}"
    reject_url  = f"{base}/reject?token={approval_token}"

    # Layer labels — reflect which pipeline stage triggered self-healing
    _layer_labels = {
        "SCRAPER":         "Scraper",
        "VALIDATION":      "Validation Suite",
        "LIVE_FEED_AUDIT": "Live Feed Audit (C1-C5)",
        "QUALITY_REPORT":  "Quality Report Generator",
    }
    layer_label  = _layer_labels.get(layer, layer)
    # Layer 2 only runs for scraper exceptions; audit/quality go straight to Layer 3
    layer2_text  = (
        "FAILED — 3 retries exhausted"
        if layer == "SCRAPER"
        else "SKIPPED — not applicable for this trigger source"
    )
    subject = (
        f"[LEKWANKWA SELF-HEAL] {severity} — "
        f"{product} / {country} — {layer_label} failed — approval required"
    )

    body_plain = f"""
LEKWANKWA PIPELINE SELF-HEALING — APPROVAL REQUIRED
=====================================================

EXCEPTION SUMMARY
-----------------
Program:   {program}
Product:   {product}
Country:   {country}
Source:    {source}
Date:      {run_date}
Severity:  {severity}
Layer:     {layer_label}

Layer 1 ({layer_label}):  FAILED — error detected
Layer 2 (Scrape4AI retry):           {layer2_text}
Layer 3 (Claude Sonnet 4.6 diagnosis): COMPLETE — see below

CLAUDE SONNET 4.6 DIAGNOSIS
----------------------------
{diagnosis}

ACTION REQUIRED
---------------
Click one link below. Do not reply to this email.

APPROVE FIX (apply proposed fix and redeploy):
{approve_url}

REJECT (log only, no changes made):
{reject_url}

This token expires in 24 hours. If no action is taken,
the fix is rejected and logged for manual review.

— Lekwankwa Automated Pipeline
"""

    body_html = f"""
<html><body>
<h2 style="color:#c0392b">LEKWANKWA PIPELINE — APPROVAL REQUIRED</h2>
<table border="1" cellpadding="6" style="border-collapse:collapse;font-family:monospace">
  <tr><td><b>Program</b></td><td>{program}</td></tr>
  <tr><td><b>Product</b></td><td>{product}</td></tr>
  <tr><td><b>Country</b></td><td>{country}</td></tr>
  <tr><td><b>Source</b></td><td>{source}</td></tr>
  <tr><td><b>Date</b></td><td>{run_date}</td></tr>
  <tr><td><b>Severity</b></td><td style="color:{'#c0392b' if severity=='CRITICAL' else '#e67e22'}">{severity}</td></tr>
</table>

<h3>Layer Status</h3>
<ul>
  <li>Layer 1 ({layer_label}): <b style="color:#c0392b">FAILED</b> — error detected</li>
  <li>Layer 2 (Scrape4AI retry): <b>{layer2_text}</b></li>
  <li>Layer 3 (Claude Sonnet 4.6 diagnosis): <b style="color:#27ae60">COMPLETE</b></li>
</ul>

<h3>Claude Sonnet 4.6 Diagnosis</h3>
<pre style="background:#f4f4f4;padding:12px;border-radius:4px">{diagnosis}</pre>

<h3>Action Required</h3>
<p>
  <a href="{approve_url}"
     style="background:#27ae60;color:white;padding:10px 20px;text-decoration:none;border-radius:4px;margin-right:12px">
    ✓ APPROVE FIX
  </a>
  &nbsp;&nbsp;
  <a href="{reject_url}"
     style="background:#c0392b;color:white;padding:10px 20px;text-decoration:none;border-radius:4px">
    ✗ REJECT
  </a>
</p>
<p><small>Token expires in 24 hours. Do not reply to this email.</small></p>
</body></html>
"""

    msg = MIMEMultipart("alternative")
    msg["From"]    = sender
    msg["To"]      = RECIPIENT
    msg["Subject"] = subject
    msg.attach(MIMEText(body_plain, "plain"))
    msg.attach(MIMEText(body_html,  "html"))

    log.info("[EMAIL] Sending approval email to %s", RECIPIENT)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.send_message(msg)
    log.info("[EMAIL] Approval email sent — token %s", approval_token[:8] + "...")


def send_auto_fix_notification(
    program: str,
    context: dict[str, Any],
    diagnosis: str,
    pr_url: str,
) -> None:
    """
    FYI email (no approval needed) for SIMPLE auto-fixes.
    Sent after a branch + PR have already been opened on GitHub.
    """
    from tools.self_healing.secret_manager import get_secret

    sender   = get_secret("gmail-sender-address")
    password = get_secret("gmail-app-password")

    product  = context.get("product", "unknown")
    country  = context.get("country", "unknown")
    source   = context.get("source", "unknown")
    run_date = context.get("run_date", "unknown")
    layer    = context.get("layer", "SCRAPER")

    _layer_labels = {
        "SCRAPER":         "Scraper",
        "VALIDATION":      "Validation Suite",
        "LIVE_FEED_AUDIT": "Live Feed Audit (C1-C5)",
        "QUALITY_REPORT":  "Quality Report Generator",
    }
    layer_label = _layer_labels.get(layer, layer)

    subject = (
        f"[LEKWANKWA AUTO-FIX] {product} / {country} — "
        f"SIMPLE fix applied — PR opened"
    )

    body_plain = f"""
LEKWANKWA PIPELINE SELF-HEALING — AUTO-FIX APPLIED
====================================================

A SIMPLE fix was detected and automatically applied via a GitHub pull request.
No approval is required — review and merge the PR below.

FIX SUMMARY
-----------
Program:   {program}
Product:   {product}
Country:   {country}
Source:    {source}
Date:      {run_date}
Triggered: {layer_label}

PULL REQUEST
------------
{pr_url}

CLAUDE SONNET 4.6 DIAGNOSIS
----------------------------
{diagnosis}

Review the PR, confirm the change looks correct, and merge to apply.

— Lekwankwa Automated Pipeline
"""

    body_html = f"""
<html><body>
<h2 style="color:#27ae60">LEKWANKWA PIPELINE — AUTO-FIX APPLIED</h2>
<p>A <strong>SIMPLE</strong> fix was automatically applied. Review and merge the PR below.</p>

<table border="1" cellpadding="6" style="border-collapse:collapse;font-family:monospace">
  <tr><td><b>Program</b></td><td>{program}</td></tr>
  <tr><td><b>Product</b></td><td>{product}</td></tr>
  <tr><td><b>Country</b></td><td>{country}</td></tr>
  <tr><td><b>Source</b></td><td>{source}</td></tr>
  <tr><td><b>Date</b></td><td>{run_date}</td></tr>
  <tr><td><b>Triggered by</b></td><td>{layer_label}</td></tr>
</table>

<h3>Pull Request</h3>
<p>
  <a href="{pr_url}"
     style="background:#27ae60;color:white;padding:10px 20px;text-decoration:none;border-radius:4px">
    View Pull Request on GitHub
  </a>
</p>

<h3>Claude Sonnet 4.6 Diagnosis</h3>
<pre style="background:#f4f4f4;padding:12px;border-radius:4px">{diagnosis}</pre>

<p><small>No approval action required — merge the PR to apply the fix.</small></p>
</body></html>
"""

    msg = MIMEMultipart("alternative")
    msg["From"]    = sender
    msg["To"]      = RECIPIENT
    msg["Subject"] = subject
    msg.attach(MIMEText(body_plain, "plain"))
    msg.attach(MIMEText(body_html,  "html"))

    log.info("[EMAIL] Sending auto-fix notification to %s", RECIPIENT)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.send_message(msg)
    log.info("[EMAIL] Auto-fix notification sent for PR: %s", pr_url)
