#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 2B: Copy repo to Compute Engine
# Run from LOCAL MACHINE (Windows Git Bash or WSL)
# Fill in INSTANCE_NAME and ZONE before running.
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
INSTANCE_NAME="lekwankwa-pipeline-vm"   # ← SET THIS
ZONE="africa-south1-a"                  # ← SET THIS (or confirm with: gcloud compute instances list)
LOCAL_REPO="c:/Users/maaba/Documents/lek_scraper"
REMOTE_PATH="/opt/lekwankwa"

echo "Copying repo to ${INSTANCE_NAME}:${REMOTE_PATH} ..."

# Exclude vault (too large), dev files, cache
gcloud compute scp --recurse \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --exclude="lekwankwa-historical-vault" \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude=".env" \
    --exclude=".git" \
    --exclude="lek_scraper_env" \
    "${LOCAL_REPO}/scrapers" \
    "${LOCAL_REPO}/tools" \
    "${LOCAL_REPO}/validations" \
    "${LOCAL_REPO}/backtesting" \
    "${LOCAL_REPO}/metadata" \
    "${LOCAL_REPO}/configs" \
    "${LOCAL_REPO}/deploy" \
    "${LOCAL_REPO}/requirements.txt" \
    "${INSTANCE_NAME}:${REMOTE_PATH}/" \
    --project="${PROJECT}" \
    --zone="${ZONE}"

echo "Copy complete."

# Copy config files to /opt/lekwankwa/config/
gcloud compute scp \
    "${LOCAL_REPO}/backtesting/backtest_engine/config/catalog_manifest.yaml" \
    "${INSTANCE_NAME}:${REMOTE_PATH}/config/catalog_manifest.yaml" \
    --project="${PROJECT}" \
    --zone="${ZONE}" 2>/dev/null || echo "  (catalog_manifest.yaml not found — generate after deployment)"

echo "Verifying remote structure..."
gcloud compute ssh "${INSTANCE_NAME}" \
    --project="${PROJECT}" \
    --zone="${ZONE}" \
    --command="ls -la /opt/lekwankwa/"

echo "Done."
