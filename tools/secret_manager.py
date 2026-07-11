"""
tools/secret_manager.py — Lekwankwa Corporation

Enterprise API key management via GCP Secret Manager.

NOTE: This module is deliberately NOT named `secrets.py`. A module named
`secrets` in tools/ shadows the Python standard-library `secrets` module
whenever a script inside tools/ is run directly (e.g. `python tools/foo.py`,
which puts tools/ on sys.path[0]). numpy.random does `from secrets import
randbits`, so the shadow breaks numpy — and therefore pandas — with
"cannot import name randbits". Keep this name distinct from any stdlib module.

In Cloud Run (production):
  - Secrets are loaded from GCP Secret Manager at container startup.
  - The .env file and raw env-var API keys are NOT used.

In local development:
  - Secret Manager call is attempted first.
  - Falls back to .env / env var if Secret Manager is unavailable
    (no credentials, or running offline).

Usage in entry points (run.py / ingest_all.py):
    from tools.secret_manager import load_all_secrets_to_env
    load_all_secrets_to_env()   # call once before any scraper code runs

After that call, all downstream modules can use os.environ.get("BLS_API_KEY")
as before — no changes needed in individual scraper files.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

# Maps env-var name → GCP Secret Manager secret name
_SECRET_MAP: dict[str, str] = {
    "FRED_API_KEY":      "fred-api-key",
    "ALFRED_API_KEY":    "alfred-api-key",
    "BLS_API_KEY":       "bls-api-key",
    "USDA_API_KEY":      "usda-api-key",
    "USDA_ERS_API_KEY":  "usda-ers-api-key",
    "CENSUS_API_KEY":    "census-api-key",
    "EIA_API_KEY":       "eia-api-key",
    "BEA_API_KEY":       "bea-api-key",
}

_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fluted-alloy-498317-u0")
_loaded  = False   # guard: only load once per process


def _get_from_secret_manager(secret_name: str) -> str | None:
    """Return secret value from GCP Secret Manager, or None on any error."""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name   = f"projects/{_PROJECT}/secrets/{secret_name}/versions/latest"
        resp   = client.access_secret_version(request={"name": name})
        return resp.payload.data.decode("utf-8").strip()
    except Exception as exc:
        log.debug("[SECRETS] Secret Manager unavailable for %s: %s", secret_name, exc)
        return None


def load_all_secrets_to_env() -> None:
    """
    Load all API keys from GCP Secret Manager into environment variables.

    Skips any key already present in the environment (so env-var overrides
    work in local dev without touching this function).

    Safe to call multiple times — only runs once per process.
    """
    global _loaded
    if _loaded:
        return
    _loaded = True

    pipeline_env = os.environ.get("PIPELINE_ENV", "development")
    n_loaded = 0
    n_env    = 0
    n_missing = 0

    for env_var, secret_name in _SECRET_MAP.items():
        if os.environ.get(env_var):
            n_env += 1
            continue   # already set — skip (honours local .env overrides)

        val = _get_from_secret_manager(secret_name)
        if val:
            os.environ[env_var] = val
            n_loaded += 1
            log.debug("[SECRETS] Loaded %s from Secret Manager", env_var)
        else:
            if pipeline_env == "production":
                log.warning(
                    "[SECRETS] %s not found in Secret Manager and not set in env. "
                    "Create it with: "
                    "gcloud secrets create %s --data-file=- <<< '<value>' "
                    "--project=%s",
                    env_var, secret_name, _PROJECT,
                )
            n_missing += 1

    log.info(
        "[SECRETS] Loaded %d from Secret Manager, %d already in env, %d missing",
        n_loaded, n_env, n_missing,
    )


def get_api_key(env_var: str) -> str:
    """
    Return a single API key by env-var name.

    Tries Secret Manager if the key is not already in the environment.
    Raises RuntimeError if the key cannot be found anywhere.
    """
    val = os.environ.get(env_var)
    if val:
        return val

    secret_name = _SECRET_MAP.get(env_var)
    if secret_name:
        val = _get_from_secret_manager(secret_name)
        if val:
            os.environ[env_var] = val   # cache for subsequent calls
            return val

    raise RuntimeError(
        f"{env_var} is not set and could not be loaded from Secret Manager. "
        f"Add it with: gcloud secrets create {secret_name or env_var.lower().replace('_', '-')} "
        f"--data-file=- --project={_PROJECT}"
    )
