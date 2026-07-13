"""
tools/self_healing/firestore_tokens.py — Lekwankwa Corporation
Approval token management via Google Cloud Storage.

Tokens are stored as JSON files in:
  gs://lekwankwa-historical-vault/self_healing_tokens/<token>.json

This replaces the Firestore implementation to avoid dependency on
the Firestore API, which has had repeated provisioning issues.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

BUCKET          = "lekwankwa-pipeline-ops"
TOKEN_PREFIX    = "self_healing_tokens"
TOKEN_TTL_HOURS = 24


def _get_storage_client():
    from google.cloud import storage
    return storage.Client(project="fluted-alloy-498317-u0")


def generate_approval_token(
    program: str,
    context: dict[str, Any],
    diagnosis: str,
) -> str:
    """Generate a deterministic 32-char token from program + context + timestamp."""
    raw = json.dumps(
        {"program": program, "context": context,
         "ts": datetime.now(timezone.utc).isoformat()},
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def compute_fingerprint(
    program: str,
    context: dict[str, Any],
    tb_str: str,
) -> str:
    """
    Stable identifier for "the same underlying issue", independent of the
    per-run timestamp/token. Used to detect that a recurring, still-unresolved
    finding (e.g. a quality-report CRITICAL/HIGH that hasn't been fixed yet)
    is identical to one already awaiting approval, so we don't re-send an
    approval email every time the same scheduled job re-detects it.

    Deliberately excludes context["run_date"] (varies every run) but includes
    everything that identifies *what* is wrong (program, layer, product,
    country, source, severity, and the finding detail text).
    """
    stable_context = {
        k: v for k, v in context.items() if k != "run_date"
    }
    raw = json.dumps(
        {"program": program, "context": stable_context, "detail": tb_str},
        sort_keys=True, default=str,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def store_in_firestore(
    token: str,
    program: str,
    context: dict[str, Any],
    diagnosis: str,
    fingerprint: str | None = None,
) -> None:
    """Persist approval token as a JSON file in GCS."""
    try:
        client = _get_storage_client()
        now    = datetime.now(timezone.utc)
        expiry = now + timedelta(hours=TOKEN_TTL_HOURS)
        doc = {
            "token":       token,
            "program":     program,
            "context":     context,
            "diagnosis":   diagnosis,
            "status":      "PENDING",
            "fingerprint": fingerprint,
            "created_at":  now.isoformat(),
            "expires_at":  expiry.isoformat(),
        }
        blob = client.bucket(BUCKET).blob(f"{TOKEN_PREFIX}/{token}.json")
        blob.upload_from_string(
            json.dumps(doc, indent=2, default=str),
            content_type="application/json",
        )
        log.info("[TOKEN] Stored token %s in GCS (expires %s)", token, expiry.date())
    except Exception as exc:
        log.error("[TOKEN] Failed to store token %s: %s", token, exc)
        raise


# Upper bound on how many token blobs we'll inspect per dedup lookup. Tokens
# expire after TOKEN_TTL_HOURS (24h) and scheduled jobs create at most a
# handful per day, so the bucket should stay small — this cap just prevents
# an unbounded/expensive scan if stale tokens ever accumulate (e.g. cleanup
# job not running).
MAX_DEDUP_SCAN_BLOBS = 500


def find_active_token_for_fingerprint(fingerprint: str) -> dict[str, Any] | None:
    """
    Return the token doc for an existing PENDING, unexpired escalation with
    the same fingerprint, or None if there isn't one.

    Used to suppress duplicate approval emails: if the same still-unresolved
    finding was already escalated and is awaiting a response, we skip sending
    another one on the next scheduled run.
    """
    try:
        client = _get_storage_client()
        now = datetime.now(timezone.utc)
        scanned = 0
        for blob in client.bucket(BUCKET).list_blobs(prefix=f"{TOKEN_PREFIX}/"):
            scanned += 1
            if scanned > MAX_DEDUP_SCAN_BLOBS:
                log.warning(
                    "[TOKEN] Dedup scan hit MAX_DEDUP_SCAN_BLOBS=%d — "
                    "stopping early; consider cleaning up expired tokens in "
                    "gs://%s/%s/",
                    MAX_DEDUP_SCAN_BLOBS, BUCKET, TOKEN_PREFIX,
                )
                break
            try:
                doc = json.loads(blob.download_as_text())
            except Exception:
                continue
            if doc.get("fingerprint") != fingerprint:
                continue
            if doc.get("status") != "PENDING":
                continue
            try:
                expiry = datetime.fromisoformat(doc["expires_at"])
            except Exception:
                continue
            if now < expiry:
                return doc
        return None
    except Exception as exc:
        log.error("[TOKEN] Failed to search for active token (fingerprint=%s): %s",
                   fingerprint, exc)
        return None


def update_token_status(token: str, status: str, note: str = "") -> None:
    """Update token status by rewriting the GCS file."""
    try:
        client = _get_storage_client()
        blob   = client.bucket(BUCKET).blob(f"{TOKEN_PREFIX}/{token}.json")
        data   = json.loads(blob.download_as_text())
        data["status"]      = status
        data["resolved_at"] = datetime.now(timezone.utc).isoformat()
        if note:
            data["note"] = note
        blob.upload_from_string(
            json.dumps(data, indent=2, default=str),
            content_type="application/json",
        )
        log.info("[TOKEN] Token %s -> %s", token, status)
    except Exception as exc:
        log.error("[TOKEN] Failed to update token %s: %s", token, exc)
        raise


def get_token_doc(token: str) -> dict[str, Any] | None:
    """Retrieve a token document from GCS. Returns None if not found."""
    try:
        client = _get_storage_client()
        blob   = client.bucket(BUCKET).blob(f"{TOKEN_PREFIX}/{token}.json")
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())
    except Exception as exc:
        log.error("[TOKEN] Failed to get token %s: %s", token, exc)
        return None


def is_token_valid(token: str) -> bool:
    """Return True if token exists, is PENDING, and has not expired."""
    doc = get_token_doc(token)
    if not doc:
        return False
    if doc.get("status") != "PENDING":
        return False
    expiry = datetime.fromisoformat(doc["expires_at"])
    return datetime.now(timezone.utc) < expiry
