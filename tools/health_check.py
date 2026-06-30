"""
Lekwankwa Platform Health Check — Cloud Run Job: job-health-check

Queries Cloud Run Jobs API, Cloud Scheduler API, Cloud Functions API, and
GCS vault/metadata to produce a single health_status.json snapshot.

Full component inventory tracked (33 total):
  16 Cloud Run Jobs   (10 scrapers + 5 metadata + 1 health)
  16 Cloud Schedulers (one per job, fired from europe-west1)
   1 Cloud Function   (pit-disclosure-generator, GCS event trigger)

Covers three failure surfaces not caught by the self-healing layer:
  1. Cloud Run Job crashes (container exits non-zero before reaching audit)
  2. Scheduler goes silent (last_attempt too old)
  3. Vault data goes stale (API source down — scraper returns 0 rows)

Output: gs://lekwankwa-metadata/health/health_status.json
Schedule: every hour (0 * * * *), job name: job-health-check
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import requests as req

log = logging.getLogger(__name__)

_PROJECT      = os.environ.get("GOOGLE_CLOUD_PROJECT", "fluted-alloy-498317-u0")
_REGION       = "africa-south1"
_SCHED_REGION = "europe-west1"
_VAULT_BUCKET = os.environ.get("VAULT_BUCKET", "lekwankwa-vault")
_META_BUCKET  = os.environ.get("META_BUCKET",  "lekwankwa-metadata")

# ── Full component inventory ──────────────────────────────────────────────
_SCRAPER_JOBS = [
    "job-food-usa",
    "job-wages-usa",
    "job-trade-usa",
    "job-housing-usa",
    "job-imf",
    "job-eurostat",
    "job-ons",
    "job-statcan",
    "job-abs",
    "job-ssb",
]

_META_JOBS = [
    "job-quality-live",
    "job-quality-archive",
    "job-coverage-manifest",
    "job-release-calendar",
    "job-pit-disclosure",
]

_HEALTH_JOBS = ["job-health-check"]

_ALL_JOBS = _SCRAPER_JOBS + _META_JOBS + _HEALTH_JOBS   # 16 total

_CLOUD_FUNCTIONS = [
    "pit-disclosure-generator",   # GCS event trigger on lekwankwa-vault
]

# Staleness thresholds
_STALE_VAULT = {
    "food_pricing":      35,
    "wages_employment":  45,
    "trade_flows":       60,
    "housing":           45,
    "global_macro":     120,   # IMF quarterly
}

_SCHED_STALE_HOURS: dict[str, int] = {
    "sched-imf": 24 * 95,          # quarterly — only alert if >95 days silent
}
_SCHED_DEFAULT_STALE_HOURS = 48


# ── Auth ──────────────────────────────────────────────────────────────────

def _token() -> str:
    import google.auth, google.auth.transport.requests
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def _hdr() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}"}


# ── 1. Cloud Run Jobs ──────────────────────────────────────────────────────

def _check_jobs() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    hdr = _hdr()
    now = datetime.now(timezone.utc)

    for job_name in _ALL_JOBS:
        if job_name in _SCRAPER_JOBS:
            jtype = "scraper"
        elif job_name in _META_JOBS:
            jtype = "metadata"
        else:
            jtype = "health"

        url = (
            f"https://run.googleapis.com/v2/projects/{_PROJECT}"
            f"/locations/{_REGION}/jobs/{job_name}/executions"
            f"?pageSize=5"  # last 5 runs for trend
        )
        entry: dict[str, Any] = {"job": job_name, "type": jtype}

        try:
            r = req.get(url, headers=hdr, timeout=15)
            if r.status_code == 200:
                execs = r.json().get("executions", [])
                if not execs:
                    entry.update({"status": "NEVER_RUN", "last_run": None,
                                  "recent_statuses": []})
                else:
                    recent_statuses = []
                    for ex in execs:
                        s = "UNKNOWN"
                        for c in ex.get("conditions", []):
                            if c.get("type") == "Completed":
                                s = "SUCCEEDED" if c.get("status") == "True" else "FAILED"
                                break
                        recent_statuses.append(s)

                    latest = execs[0]
                    status = recent_statuses[0]

                    # Duration in seconds
                    duration_s = None
                    start = latest.get("createTime")
                    end   = latest.get("completionTime")
                    if start and end:
                        try:
                            s_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                            e_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                            duration_s = int((e_dt - s_dt).total_seconds())
                        except Exception:
                            pass

                    # Hours since last run
                    age_hours = None
                    if start:
                        try:
                            s_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                            age_hours = round((now - s_dt).total_seconds() / 3600, 1)
                        except Exception:
                            pass

                    success_rate = (
                        round(recent_statuses.count("SUCCEEDED") / len(recent_statuses) * 100)
                        if recent_statuses else None
                    )

                    entry.update({
                        "status":          status,
                        "last_run":        start,
                        "completion_time": end,
                        "duration_s":      duration_s,
                        "age_hours":       age_hours,
                        "recent_statuses": recent_statuses,
                        "success_rate_pct": success_rate,
                    })
            else:
                entry.update({"status": "API_ERROR", "last_run": None,
                              "error": r.text[:200], "recent_statuses": []})
        except Exception as exc:
            entry.update({"status": "ERROR", "last_run": None,
                          "error": str(exc), "recent_statuses": []})

        results.append(entry)
    return results


# ── 2. Cloud Schedulers ────────────────────────────────────────────────────

def _check_schedulers() -> list[dict[str, Any]]:
    url = (
        f"https://cloudscheduler.googleapis.com/v1/projects/{_PROJECT}"
        f"/locations/{_SCHED_REGION}/jobs"
    )
    try:
        r = req.get(url, headers=_hdr(), timeout=15)
        if r.status_code != 200:
            return [{"error": f"HTTP {r.status_code}: {r.text[:200]}"}]

        now     = datetime.now(timezone.utc)
        results = []
        for job in r.json().get("jobs", []):
            name  = job.get("name", "").split("/")[-1]
            last  = job.get("lastAttemptTime")
            stale_h = _SCHED_STALE_HOURS.get(name, _SCHED_DEFAULT_STALE_HOURS)

            stale = True
            age_hours: float | None = None
            if last:
                last_dt   = datetime.fromisoformat(last.replace("Z", "+00:00"))
                age_hours = (now - last_dt).total_seconds() / 3600
                stale     = age_hours > stale_h

            results.append({
                "scheduler":             name,
                "state":                 job.get("state", "UNKNOWN"),
                "last_attempt":          last,
                "next_run":              job.get("scheduleTime"),
                "age_hours":             round(age_hours, 1) if age_hours else None,
                "stale":                 stale,
                "stale_threshold_hours": stale_h,
                "last_status":           job.get("status", {}).get("code"),
            })
        return results
    except Exception as exc:
        return [{"error": str(exc)}]


# ── 3. Cloud Function ──────────────────────────────────────────────────────

def _check_functions() -> list[dict[str, Any]]:
    results = []
    hdr = _hdr()

    for fn_name in _CLOUD_FUNCTIONS:
        url = (
            f"https://cloudfunctions.googleapis.com/v2/projects/{_PROJECT}"
            f"/locations/{_REGION}/functions/{fn_name}"
        )
        try:
            r = req.get(url, headers=hdr, timeout=15)
            if r.status_code == 200:
                data    = r.json()
                state   = data.get("state", "UNKNOWN")
                build   = data.get("buildConfig", {})
                runtime = build.get("runtime", "?")
                results.append({
                    "function": fn_name,
                    "status":   "OK" if state == "ACTIVE" else "DEGRADED",
                    "state":    state,
                    "runtime":  runtime,
                    "update_time": data.get("updateTime"),
                })
            elif r.status_code == 404:
                results.append({
                    "function": fn_name,
                    "status":   "MISSING",
                    "state":    "NOT_DEPLOYED",
                })
            else:
                results.append({
                    "function": fn_name,
                    "status":   "API_ERROR",
                    "error":    r.text[:200],
                })
        except Exception as exc:
            results.append({"function": fn_name, "status": "ERROR", "error": str(exc)})

    return results


# ── 4. Vault data freshness ────────────────────────────────────────────────

def _check_vault_freshness() -> list[dict[str, Any]]:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(_VAULT_BUCKET)
    now    = datetime.now(timezone.utc)

    latest: dict[tuple[str, str], datetime] = {}
    for blob in bucket.list_blobs(prefix="product="):
        if not blob.name.endswith(".parquet"):
            continue
        parts = blob.name.split("/")
        if len(parts) < 2:
            continue
        product = parts[0].replace("product=", "")
        country = parts[1].replace("country=", "") if len(parts) > 1 else "?"
        key = (product, country)
        if key not in latest or blob.updated > latest[key]:
            latest[key] = blob.updated

    results = []
    for (product, country), last_write in sorted(latest.items()):
        age_days  = (now - last_write).days
        threshold = _STALE_VAULT.get(product, 60)
        health    = "OK" if age_days <= threshold * 2 else "STALE"
        results.append({
            "product":              product,
            "country":              country,
            "last_write":           last_write.isoformat(),
            "age_days":             age_days,
            "stale_threshold_days": threshold * 2,
            "health":               health,
        })
    return results


# ── 5. Metadata folder freshness ──────────────────────────────────────────

def _check_metadata_freshness() -> list[dict[str, Any]]:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(_META_BUCKET)
    now    = datetime.now(timezone.utc)

    _FOLDERS = [
        ("quality_reports/",   "Quality Reports",   26),
        ("coverage_manifest/", "Coverage Manifest", 26),
        ("release_calendar/",  "Release Calendar",  26),
        ("pit_disclosure/",    "PIT Disclosure",    26),
        ("health/",            "Health Snapshots",   2),
    ]

    results = []
    for prefix, label, stale_hours in _FOLDERS:
        latest_dt: datetime | None = None
        for blob in bucket.list_blobs(prefix=prefix):
            if latest_dt is None or blob.updated > latest_dt:
                latest_dt = blob.updated

        if latest_dt:
            age_h  = (now - latest_dt).total_seconds() / 3600
            health = "OK" if age_h <= stale_hours else "STALE"
            results.append({
                "folder":      prefix,
                "label":       label,
                "last_update": latest_dt.isoformat(),
                "age_hours":   round(age_h, 1),
                "health":      health,
            })
        else:
            results.append({
                "folder":      prefix,
                "label":       label,
                "last_update": None,
                "age_hours":   None,
                "health":      "MISSING",
            })
    return results


# ── 6. Quality alerts (from latest quality report) ────────────────────────

def _check_quality_alerts() -> list[dict[str, Any]]:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(_META_BUCKET)

    latest_blob = None
    latest_time = None
    for blob in bucket.list_blobs(prefix="quality_reports/"):
        if blob.name.endswith(".json") and (
            latest_time is None or blob.updated > latest_time
        ):
            latest_blob = blob
            latest_time = blob.updated

    if not latest_blob:
        return []

    try:
        data = json.loads(latest_blob.download_as_text())
    except Exception:
        return []

    alerts = []
    for finding in data.get("findings", []):
        sev = finding.get("severity", "")
        if sev in ("CRITICAL", "HIGH"):
            alerts.append({
                "severity": sev,
                "code":     finding.get("code", "?"),
                "product":  finding.get("product", "?"),
                "country":  finding.get("country_group", finding.get("country", "?")),
                "message":  finding.get("message", "")[:200],
                "report":   latest_blob.name,
                "report_ts": latest_time.isoformat() if latest_time else None,
            })
    return sorted(alerts, key=lambda a: 0 if a["severity"] == "CRITICAL" else 1)


# ── 7. SLA summary ────────────────────────────────────────────────────────

def _compute_sla(jobs: list[dict], vault: list[dict], meta: list[dict]) -> dict:
    total_jobs   = len(jobs)
    healthy_jobs = sum(1 for j in jobs if j.get("status") == "SUCCEEDED")
    job_sla      = round(healthy_jobs / total_jobs * 100, 1) if total_jobs else 0

    total_vault   = len(vault)
    fresh_vault   = sum(1 for v in vault if v.get("health") == "OK")
    vault_sla     = round(fresh_vault / total_vault * 100, 1) if total_vault else 0

    total_meta    = len(meta)
    fresh_meta    = sum(1 for m in meta if m.get("health") == "OK")
    meta_sla      = round(fresh_meta / total_meta * 100, 1) if total_meta else 0

    overall = "OPERATIONAL"
    if job_sla < 80 or vault_sla < 70:
        overall = "DEGRADED"
    if job_sla < 50:
        overall = "OUTAGE"

    return {
        "overall":    overall,
        "job_sla":    job_sla,
        "vault_sla":  vault_sla,
        "meta_sla":   meta_sla,
        "healthy_jobs":  healthy_jobs,
        "total_jobs":    total_jobs,
        "fresh_vault":   fresh_vault,
        "total_vault":   total_vault,
        "fresh_meta":    fresh_meta,
        "total_meta":    total_meta,
    }


# ── Main ──────────────────────────────────────────────────────────────────

def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    log.info("Collecting platform health status (33-component inventory)...")

    jobs    = _check_jobs()
    scheds  = _check_schedulers()
    funcs   = _check_functions()
    vault   = _check_vault_freshness()
    meta    = _check_metadata_freshness()
    alerts  = _check_quality_alerts()
    sla     = _compute_sla(jobs, vault, meta)

    status: dict[str, Any] = {
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "project":            _PROJECT,
        "region":             _REGION,
        "component_counts":   {
            "cloud_run_jobs":   len(jobs),
            "schedulers":       len(scheds),
            "cloud_functions":  len(funcs),
            "total":            len(jobs) + len(scheds) + len(funcs),
        },
        "sla":                sla,
        "cloud_run_jobs":     jobs,
        "schedulers":         scheds,
        "cloud_functions":    funcs,
        "vault_freshness":    vault,
        "metadata_freshness": meta,
        "quality_alerts":     alerts,
    }

    from google.cloud import storage
    blob = storage.Client().bucket(_META_BUCKET).blob("health/health_status.json")
    blob.upload_from_string(
        json.dumps(status, indent=2, default=str),
        content_type="application/json",
    )
    log.info("Written → gs://%s/health/health_status.json", _META_BUCKET)

    # Console summary
    log.info("Platform status: %s  |  Job SLA: %.0f%%  |  Vault SLA: %.0f%%",
             sla["overall"], sla["job_sla"], sla["vault_sla"])

    failed_jobs  = [j for j in jobs    if j.get("status") == "FAILED"]
    stale_scheds = [s for s in scheds  if s.get("stale")]
    bad_funcs    = [f for f in funcs   if f.get("status") != "OK"]
    stale_vault  = [v for v in vault   if v.get("health") == "STALE"]
    crit_alerts  = [a for a in alerts  if a.get("severity") == "CRITICAL"]

    if failed_jobs or stale_scheds or bad_funcs or stale_vault or crit_alerts:
        for j in failed_jobs:
            log.warning("FAILED JOB:        %s", j["job"])
        for s in stale_scheds:
            log.warning("STALE SCHEDULER:   %s  (%.0fh ago)", s["scheduler"],
                        s.get("age_hours") or 0)
        for f in bad_funcs:
            log.warning("FUNCTION ISSUE:    %s  [%s]", f["function"], f.get("status"))
        for v in stale_vault:
            log.warning("STALE VAULT:       %s/%s  (%d days)",
                        v["product"], v["country"], v["age_days"])
        for a in crit_alerts:
            log.warning("CRITICAL ALERT:    %s/%s  [%s]",
                        a["product"], a["country"], a["code"])
    else:
        log.info("All health checks PASSED ✓")


if __name__ == "__main__":
    run()
