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


def store_in_firestore(
    token: str,
    program: str,
    context: dict[str, Any],
    diagnosis: str,
) -> None:
    """Persist approval token as a JSON file in GCS."""
    try:
        client = _get_storage_client()
        now    = datetime.now(timezone.utc)
        expiry = now + timedelta(hours=TOKEN_TTL_HOURS)
        doc = {
            "token":      token,
            "program":    program,
            "context":    context,
            "diagnosis":  diagnosis,
            "status":     "PENDING",
            "created_at": now.isoformat(),
            "expires_at": expiry.isoformat(),
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
