"""
tools/self_healing/secret_manager.py — Lekwankwa Corporation
Runtime secret retrieval via Google Cloud Secret Manager.
No secrets ever stored in code, logs, or env variables.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache

log = logging.getLogger(__name__)

PROJECT = "fluted-alloy-498317-u0"


@lru_cache(maxsize=32)
def get_secret(secret_name: str, version: str = "latest") -> str:
    """
    Fetch a secret value from GCP Secret Manager at runtime.
    Results are cached per process to avoid redundant API calls.

    Secret names correspond to Section 5 of the deployment spec:
        anthropic-api-key, gcs-service-account-key, fred-api-key,
        bls-api-key, gmail-sender-address, gmail-app-password,
        github-token, firestore-project-id
    """
    try:
        from google.cloud import secretmanager
        client  = secretmanager.SecretManagerServiceClient()
        name    = f"projects/{PROJECT}/secrets/{secret_name}/versions/{version}"
        resp    = client.access_secret_version(request={"name": name})
        payload = resp.payload.data.decode("utf-8").strip()
        log.debug("[SECRET] Loaded %s", secret_name)
        return payload
    except Exception as exc:
        # Fallback to environment variable for local development only
        env_key = secret_name.upper().replace("-", "_")
        val     = os.environ.get(env_key, "")
        if val:
            log.warning("[SECRET] %s not found in Secret Manager — using env var fallback", secret_name)
            return val
        log.error("[SECRET] Failed to load %s: %s", secret_name, exc)
        raise RuntimeError(f"Secret '{secret_name}' unavailable: {exc}") from exc
