"""
Trigger downstream Cloud Run Jobs after a successful vault write.
Called from scraper entry points when new data is confirmed in the vault.

The quality report job (job-quality-live) requires gcsfuse mounts so it
runs as its own Cloud Run Job rather than inline. This module fires it
via the Cloud Run Jobs API whenever a scraper confirms new rows were written.
"""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

_PROJECT  = os.environ.get("GOOGLE_CLOUD_PROJECT", "fluted-alloy-498317-u0")
_REGION   = "africa-south1"
_JOB_LIVE = "job-quality-live"


def trigger_quality_live() -> None:
    """Fire job-quality-live via Cloud Run Jobs API. Non-fatal on any error."""
    try:
        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)

        url = (
            f"https://{_REGION}-run.googleapis.com/apis/run.googleapis.com/v1"
            f"/namespaces/{_PROJECT}/jobs/{_JOB_LIVE}:run"
        )
        resp = requests.post(url, headers={"Authorization": f"Bearer {creds.token}"})
        if resp.status_code in (200, 201):
            log.info("✓ Triggered %s (HTTP %s)", _JOB_LIVE, resp.status_code)
        else:
            log.warning(
                "trigger_quality_live: HTTP %s — %s",
                resp.status_code, resp.text[:300],
            )
    except Exception as exc:
        log.warning("trigger_quality_live: failed (non-fatal) — %s", exc)
