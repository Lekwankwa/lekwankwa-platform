#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Resume all Cloud Scheduler pipeline jobs
#
# Re-enables every Cloud Scheduler job that was paused by
# deploy/pause_pipeline.sh. Each job resumes on its original cron schedule.
#
# Usage (from repo root in Cloud Shell):
#   bash deploy/resume_pipeline.sh
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
echo " Resuming all Lekwankwa Cloud Scheduler jobs"
echo " Project : ${PROJECT}"
echo " Region  : ${SCHED_REGION}"
echo "========================================================"
echo ""

for JOB in "${JOBS[@]}"; do
  echo -n "  Resuming ${JOB} ... "
  gcloud scheduler jobs resume "${JOB}" \
    --location="${SCHED_REGION}" \
    --project="${PROJECT}" \
    --quiet \
    2>/dev/null && echo "✓" || echo "⚠ (already running or not found)"
done

echo ""
echo "All scheduler jobs resumed."
echo "Pipeline will fire again on its normal cron schedule."
