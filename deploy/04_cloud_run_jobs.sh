#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 4: Cloud Run Jobs + Cloud Scheduler
# Builds scraper container, creates 10 Cloud Run Jobs, wires Cloud Scheduler.
#
# Run from repo root:
#   bash deploy/04_cloud_run_jobs.sh
#
# Prerequisites:
#   - gcloud authenticated as info@lekwankwa.com
#   - Cloud Run Jobs API enabled
#   - lekwankwa-vault GCS bucket exists
#   - Secret Manager secrets: FRED_API_KEY, BLS_API_KEY exist
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="us-central1"
SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"
IMAGE="gcr.io/${PROJECT}/lekwankwa-scrapers:latest"
VAULT="gs://lekwankwa-vault"

echo "========================================================"
echo " STEP 1 — Enable required APIs"
echo "========================================================"
gcloud services enable run.googleapis.com \
    cloudscheduler.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    --project="${PROJECT}"
echo "  ✓ APIs enabled"

echo ""
echo "========================================================"
echo " STEP 2 — Build + push container image via Cloud Build"
echo "========================================================"
gcloud builds submit \
    --tag "${IMAGE}" \
    --project="${PROJECT}" \
    .
echo "  ✓ Image pushed: ${IMAGE}"

echo ""
echo "========================================================"
echo " STEP 3 — Create Cloud Run Jobs"
echo "========================================================"

create_job() {
    local NAME="$1"
    shift
    echo "  Creating job: ${NAME}"
    gcloud run jobs create "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="VAULT_ROOT=${VAULT},GOOGLE_CLOUD_PROJECT=${PROJECT}" \
        --set-secrets="FRED_API_KEY=FRED_API_KEY:latest,BLS_API_KEY=BLS_API_KEY:latest" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --project="${PROJECT}" \
        2>/dev/null || \
    gcloud run jobs update "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="VAULT_ROOT=${VAULT},GOOGLE_CLOUD_PROJECT=${PROJECT}" \
        --set-secrets="FRED_API_KEY=FRED_API_KEY:latest,BLS_API_KEY=BLS_API_KEY:latest" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --project="${PROJECT}"
    echo "  ✓ ${NAME}"
}

# USA scrapers (daily — BLS/Census release monthly but daily check is cheap)
create_job "job-food-usa"    "python,-m,scrapers.food_pricing.run,--country,USA"
create_job "job-wages-usa"   "python,-m,scrapers.wages_employment.run,--country,USA"
create_job "job-trade-usa"   "python,-m,scrapers.trade_flows.run,--country,USA"
create_job "job-housing-usa" "python,-m,scrapers.housing.run,--country,USA"

# IMF — runs quarterly (scheduler fires quarterly, job checks internally)
create_job "job-imf" "python,-m,scrapers.imf_global_macro.run,--country,ALL"

# International sources — monthly releases
create_job "job-eurostat" "python,-m,scrapers.eurostat.run_all_ingestion"
create_job "job-ons"      "python,-m,scrapers.ons.ingest_all"
create_job "job-statcan"  "python,-m,scrapers.statcan.ingest_all"
create_job "job-abs"      "python,-m,scrapers.abs.ingest_all"
create_job "job-ssb"      "python,-m,scrapers.ssb.ingest_all"

echo ""
echo "========================================================"
echo " STEP 4 — Grant Cloud Scheduler permission to run jobs"
echo "========================================================"
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/run.invoker" \
    --condition=None \
    --quiet
echo "  ✓ roles/run.invoker granted to ${SA}"

echo ""
echo "========================================================"
echo " STEP 5 — Create Cloud Scheduler triggers"
echo "========================================================"

JOB_BASE="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1"
NAMESPACE="namespaces/${PROJECT}"

schedule_job() {
    local SCHED_NAME="$1"
    local SCHEDULE="$2"
    local JOB_NAME="$3"
    local URI="${JOB_BASE}/${NAMESPACE}/jobs/${JOB_NAME}:run"

    echo "  Scheduling ${SCHED_NAME} → ${JOB_NAME} (${SCHEDULE})"
    gcloud scheduler jobs create http "${SCHED_NAME}" \
        --location="${REGION}" \
        --schedule="${SCHEDULE}" \
        --uri="${URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SA}" \
        --time-zone="UTC" \
        --attempt-deadline="30m" \
        --project="${PROJECT}" \
        2>/dev/null || \
    gcloud scheduler jobs update http "${SCHED_NAME}" \
        --location="${REGION}" \
        --schedule="${SCHEDULE}" \
        --uri="${URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SA}" \
        --project="${PROJECT}"
    echo "  ✓ ${SCHED_NAME}"
}

# USA — daily at 09:00 UTC
schedule_job "sched-food-usa"    "0 9 * * *" "job-food-usa"
schedule_job "sched-wages-usa"   "0 9 * * *" "job-wages-usa"
schedule_job "sched-trade-usa"   "0 9 * * *" "job-trade-usa"
schedule_job "sched-housing-usa" "0 9 * * *" "job-housing-usa"

# IMF — quarterly: 1st of Apr/Jul/Oct/Jan at 08:00 UTC
schedule_job "sched-imf" "0 8 1 1,4,7,10 *" "job-imf"

# International — 1st of each month at 10:00 UTC
schedule_job "sched-eurostat" "0 10 1 * *" "job-eurostat"
schedule_job "sched-ons"      "0 10 1 * *" "job-ons"
schedule_job "sched-statcan"  "0 10 1 * *" "job-statcan"
schedule_job "sched-abs"      "0 10 1 * *" "job-abs"
schedule_job "sched-ssb"      "0 10 1 * *" "job-ssb"

echo ""
echo "========================================================"
echo " STEP 6 — Grant scraper SA access to GCS vault bucket"
echo "========================================================"
gcloud storage buckets add-iam-policy-binding "${VAULT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin" \
    --project="${PROJECT}"
echo "  ✓ ${SA} → roles/storage.objectAdmin on ${VAULT}"

echo ""
echo "========================================================"
echo " DEPLOYMENT SUMMARY"
echo "========================================================"
echo ""
echo "  Image  : ${IMAGE}"
echo "  Vault  : ${VAULT}"
echo "  Region : ${REGION}"
echo ""
echo "  Jobs (10 total):"
echo "    job-food-usa    → daily 09:00 UTC"
echo "    job-wages-usa   → daily 09:00 UTC"
echo "    job-trade-usa   → daily 09:00 UTC"
echo "    job-housing-usa → daily 09:00 UTC"
echo "    job-imf         → quarterly (1 Jan/Apr/Jul/Oct at 08:00 UTC)"
echo "    job-eurostat    → monthly 1st at 10:00 UTC (EU27)"
echo "    job-ons         → monthly 1st at 10:00 UTC (GBR)"
echo "    job-statcan     → monthly 1st at 10:00 UTC (CAN)"
echo "    job-abs         → monthly 1st at 10:00 UTC (AUS)"
echo "    job-ssb         → monthly 1st at 10:00 UTC (NOR)"
echo ""
echo "  To trigger a job manually:"
echo "    gcloud run jobs execute job-food-usa --region=${REGION} --project=${PROJECT}"
echo ""
echo "  To view job logs:"
echo "    gcloud run jobs executions list --job=job-food-usa --region=${REGION}"
echo ""
echo "Section 4 complete."
