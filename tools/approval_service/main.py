"""
approval_service/main.py — Lekwankwa Fix Approval Gateway
==========================================================
Cloud Run HTTP service. Receives approve/reject clicks from quality alert
emails and writes GCS markers. Token validity is verified via Firestore
(the same collection that handler.py writes to).

DEPLOYMENT
----------
  gcloud run deploy fix-approval-service \
    --source tools/approval_service \
    --region africa-south1 \
    --allow-unauthenticated \
    --set-env-vars GCS_BUCKET=lekwankwa-historical-vault,FIRESTORE_PROJECT=fluted-alloy-498317-u0

ENDPOINTS
---------
  GET /approve?token=<token>   Verify via Firestore + write .approved to GCS
  GET /reject?token=<token>    Verify via Firestore + write .rejected to GCS
  GET /health                  200 OK
"""

from __future__ import annotations

import datetime
import json
import os

from flask import Flask, request, Response

app = Flask(__name__)

GCS_BUCKET        = os.environ.get("GCS_BUCKET", "lekwankwa-historical-vault")
FIRESTORE_PROJECT = os.environ.get("FIRESTORE_PROJECT", "fluted-alloy-498317-u0")
COLLECTION        = "lekwankwa_self_healing_tokens"


# ---------------------------------------------------------------------------
# Firestore token verification
# ---------------------------------------------------------------------------

def _get_firestore_client():
    from google.cloud import firestore
    return firestore.Client(project=FIRESTORE_PROJECT)


def _verify_token(token: str) -> dict | None:
    """
    Look up token in Firestore. Returns the token document dict if the token
    exists, is PENDING, and has not expired. Returns None otherwise.
    """
    if not token or len(token) < 8:
        return None
    try:
        db  = _get_firestore_client()
        doc = db.collection(COLLECTION).document(token).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data.get("status") != "PENDING":
            return None
        expiry = datetime.datetime.fromisoformat(data["expires_at"])
        if datetime.datetime.now(datetime.timezone.utc) > expiry:
            return None
        return data
    except Exception as exc:
        app.logger.error("Firestore token lookup failed: %s", exc)
        return None


def _update_token_status(token: str, status: str) -> None:
    try:
        db = _get_firestore_client()
        db.collection(COLLECTION).document(token).update({
            "status":      status,
            "resolved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
    except Exception as exc:
        app.logger.warning("Failed to update token status in Firestore: %s", exc)


# ---------------------------------------------------------------------------
# GCS write
# ---------------------------------------------------------------------------

def _write_gcs_marker(blob_name: str, data: dict) -> str:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    bucket.blob(blob_name).upload_from_string(
        json.dumps(data, indent=2, default=str),
        content_type="application/json",
    )
    return f"gs://{GCS_BUCKET}/{blob_name}"


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_PAGE = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{title}</title>
<style>
body{{font-family:Arial,sans-serif;max-width:560px;margin:80px auto;padding:0 24px;color:#202124;}}
h1{{color:{color};font-size:22px;}}
.card{{background:#f8f9fa;border-radius:8px;padding:20px 24px;margin-top:20px;line-height:1.6;}}
code{{background:#e8eaed;padding:2px 6px;border-radius:3px;font-size:13px;}}
.footer{{font-size:11px;color:#9aa0a6;margin-top:40px;border-top:1px solid #e8eaed;padding-top:14px;}}
</style>
</head>
<body>
<h1>{heading}</h1>
<div class="card">{body}</div>
<p class="footer">Lekwankwa Corporation &mdash; Data Quality Operations</p>
</body>
</html>"""


def _page(title: str, heading: str, body: str, color: str, status: int) -> Response:
    return Response(
        _PAGE.format(title=title, heading=heading, body=body, color=color),
        status=status,
        content_type="text/html",
    )


def _token_error() -> Response:
    return _page(
        "Invalid link",
        "Link invalid or expired",
        "This approval link is invalid or has already been used. "
        "Links expire after 24 hours.<br><br>"
        "Check the latest quality alert email for a fresh link.",
        "#d93025",
        400,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health() -> Response:
    return Response("OK", status=200, content_type="text/plain")


@app.route("/approve")
def approve() -> Response:
    token = request.args.get("token", "")
    data  = _verify_token(token)
    if not data:
        return _token_error()

    context  = data.get("context", {})
    product  = context.get("product", data.get("product", "unknown"))
    country  = context.get("country", data.get("country", "unknown"))
    program  = data.get("program", "unknown")
    now      = datetime.datetime.utcnow().isoformat() + "Z"

    marker = {
        "action":      "APPROVED",
        "token":       token,
        "product":     product,
        "country":     country,
        "program":     program,
        "run_date":    context.get("run_date"),
        "severity":    context.get("severity"),
        "approved_at": now,
    }

    try:
        gcs_path = _write_gcs_marker(f"fix_approvals/{token}.approved", marker)
        detail   = f"Approval marker written to <code>{gcs_path}</code>"
    except Exception as exc:
        detail = f"<em>Warning: GCS write failed ({exc}). Fix approval logged in Firestore only.</em>"

    _update_token_status(token, "APPROVED")

    return _page(
        "Fix approved",
        "✓ Fix approved",
        f"<strong>{product} / {country}</strong><br><br>"
        f"The fix has been approved and will be applied in the next scheduled run.<br><br>"
        f"{detail}",
        "#137333",
        200,
    )


@app.route("/reject")
def reject() -> Response:
    token = request.args.get("token", "")
    data  = _verify_token(token)
    if not data:
        return _token_error()

    context = data.get("context", {})
    product = context.get("product", data.get("product", "unknown"))
    country = context.get("country", data.get("country", "unknown"))
    now     = datetime.datetime.utcnow().isoformat() + "Z"

    marker = {
        "action":      "REJECTED",
        "token":       token,
        "product":     product,
        "country":     country,
        "run_date":    context.get("run_date"),
        "severity":    context.get("severity"),
        "rejected_at": now,
    }

    try:
        _write_gcs_marker(f"fix_approvals/{token}.rejected", marker)
    except Exception:
        pass

    _update_token_status(token, "REJECTED")

    return _page(
        "Fix rejected",
        "✗ Fix rejected",
        f"<strong>{product} / {country}</strong><br><br>"
        "The fix has been rejected. This finding will remain OPEN "
        "in the next quality report and must be resolved manually.",
        "#e37400",
        200,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
