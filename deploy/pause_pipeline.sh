#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Pause all Cloud Scheduler pipeline jobs
#
# Pauses every Cloud Scheduler job so no scraper or metadata tool fires
# automatically. Jobs and their Cloud Run definitions are left intact;
# run deploy/resume_pipeline.sh to re-enable them.
#
# Usage (from repo root in Cloud Shell):
#   bash deploy/pause_pipeline.sh
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
SCHED_REGION="europe-west1"

JOBS=(
  sched-food-usa
  sched-wages-usa
  sched-trade-usa
  sched-housing-usa
  sched-imf
  sched-eurostat
  sched-ons
  sched-statcan
  sched-coverage-manifest
  sched-release-calendar
  sched-quality-archive
  sched-quality-live
  sched-pit-disclosure
  sched-health-check
)

echo "========================================================"
echo " Pausing all Lekwankwa Cloud Scheduler jobs"
echo " Project : ${PROJECT}"
echo " Region  : ${SCHED_REGION}"
echo "========================================================"
echo ""

for JOB in "${JOBS[@]}"; do
  echo -n "  Pausing ${JOB} ... "
  gcloud scheduler jobs pause "${JOB}" \
    --location="${SCHED_REGION}" \
    --project="${PROJECT}" \
    --quiet \
    2>/dev/null && echo "✓" || echo "⚠ (already paused or not found)"
done

echo ""
echo "All scheduler jobs paused."
echo "No pipeline runs will fire automatically until you run:"
echo "  bash deploy/resume_pipeline.sh"
