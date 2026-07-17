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

_METADATA_JOBS = [
    "job-quality-live",
    "job-quality-archive",
    "job-coverage-manifest",
    "job-release-calendar",
    "job-pit-disclosure",
]


def _is_already_running(job_name: str, headers: dict) -> bool:
    """
    True if the most recent execution of job_name has no completionTime yet.

    Every scraper (8 entry points) calls trigger_all_metadata() independently
    on its own schedule. Without this guard, two scrapers finishing within
    the same 1-3.5hr window each fire their own overlapping copy of the same
    metadata job — observed 2026-07-17 causing duplicate self-healing PRs
    from concurrent job-quality-live/archive runs scanning the same data.
    """
    try:
        url = (
            f"https://{_REGION}-run.googleapis.com/apis/run.googleapis.com/v1"
            f"/namespaces/{_PROJECT}/executions"
        )
        resp = requests.get(
            url, headers=headers,
            params={"labelSelector": f"run.googleapis.com/job={job_name}"},
        )
        if resp.status_code != 200:
            return False  # can't tell — don't block firing on an API hiccup
        items = resp.json().get("items", [])
        if not items:
            return False
        latest = max(items, key=lambda it: it["metadata"]["creationTimestamp"])
        return "completionTime" not in latest.get("status", {})
    except Exception as exc:
        log.warning("running-check %s: failed (assuming not running) — %s", job_name, exc)
        return False


def _fire_job(job_name: str) -> None:
    """POST to Cloud Run Jobs API to execute a named job. Non-fatal on any error."""
    try:
        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        headers = {"Authorization": f"Bearer {creds.token}"}

        if _is_already_running(job_name, headers):
            log.info("skip %s: an execution is already running", job_name)
            return

        url = (
            f"https://{_REGION}-run.googleapis.com/apis/run.googleapis.com/v1"
            f"/namespaces/{_PROJECT}/jobs/{job_name}:run"
        )
        resp = requests.post(url, headers=headers)
        if resp.status_code in (200, 201):
            log.info("✓ Triggered %s (HTTP %s)", job_name, resp.status_code)
        else:
            log.warning("trigger %s: HTTP %s — %s", job_name, resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("trigger %s: failed (non-fatal) — %s", job_name, exc)


def trigger_all_metadata() -> None:
    """Fire all metadata jobs after a confirmed vault write. Non-fatal on any error."""
    for job in _METADATA_JOBS:
        _fire_job(job)


def trigger_quality_live() -> None:
    """Fire job-quality-live only. Kept for backwards compatibility."""
    _fire_job("job-quality-live")
