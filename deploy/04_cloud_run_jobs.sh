#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 4: Cloud Run Jobs + Cloud Scheduler
# Builds scraper container, creates Cloud Run Jobs for scrapers + metadata tools,
# wires Cloud Scheduler triggers.
#
# Run from repo root in Cloud Shell:
#   bash deploy/04_cloud_run_jobs.sh
#
# Prerequisites:
#   - gcloud authenticated as info@lekwankwa.com
#   - .env file present in repo root (contains all API keys)
#   - lekwankwa-vault and lekwankwa-metadata GCS buckets exist (run 01_gcs_setup.sh)
#
# Schedule summary:
#   USA scrapers:         daily at 09:00, 16:00, 21:00 UTC  (3× daily)
#   International:        1st of each month at 10:00 UTC
#   IMF:                  quarterly, 1st of Jan/Apr/Jul/Oct at 08:00 UTC
#   Coverage manifest:    1st of each month at 02:00 UTC
#   Release calendar:     1st of each month at 04:00 UTC
#   PIT disclosure:       daily at 22:00 UTC (after last 21:00 scraper batch)
#   Quality live:         daily at 21:30 UTC (after last 21:00 scraper batch)
#   Quality archive:      1st of each month at 12:00 UTC
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"
IMAGE="gcr.io/${PROJECT}/lekwankwa-scrapers:latest"
VAULT_BUCKET="lekwankwa-vault"
VAULT="gs://${VAULT_BUCKET}"
METADATA_BUCKET="lekwankwa-metadata"
METADATA="gs://${METADATA_BUCKET}"

echo "========================================================"
echo " STEP 0 — Load API keys from .env"
echo "========================================================"
if [ -f ".env" ]; then
    set -a; source .env; set +a
    echo "  ✓ .env loaded"
else
    echo "  WARNING: .env not found in repo root."
    echo "  Upload .env to Cloud Shell then re-run:"
    echo "    gcloud cloud-shell scp LOCAL:.env cloudshell:~/lekwankwa-platform/.env"
fi

# All env vars passed to every job
ENV_VARS="VAULT_ROOT=${VAULT}"
ENV_VARS="${ENV_VARS},METADATA_BUCKET=${METADATA}"
ENV_VARS="${ENV_VARS},GOOGLE_CLOUD_PROJECT=${PROJECT}"
ENV_VARS="${ENV_VARS},FRED_API_KEY=${FRED_API_KEY:-}"
ENV_VARS="${ENV_VARS},ALFRED_API_KEY=${ALFRED_API_KEY:-}"
ENV_VARS="${ENV_VARS},BLS_API_KEY=${BLS_API_KEY:-}"
ENV_VARS="${ENV_VARS},USDA_API_KEY=${USDA_API_KEY:-}"
ENV_VARS="${ENV_VARS},USDA_ERS_API_KEY=${USDA_ERS_API_KEY:-}"
ENV_VARS="${ENV_VARS},CENSUS_API_KEY=${CENSUS_API_KEY:-}"
ENV_VARS="${ENV_VARS},EIA_API_KEY=${EIA_API_KEY:-}"
ENV_VARS="${ENV_VARS},BEA_API_KEY=${BEA_API_KEY:-}"
ENV_VARS="${ENV_VARS},PIPELINE_ENV=${PIPELINE_ENV:-production}"

echo "  ✓ Env vars built"

echo ""
echo "========================================================"
echo " STEP 1 — Enable required APIs"
echo "========================================================"
gcloud services enable run.googleapis.com \
    cloudscheduler.googleapis.com \
    cloudbuild.googleapis.com \
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

# Standard scraper job: uses VAULT_ROOT env var → gcsfs routes writes to GCS
create_job() {
    local NAME="$1"
    shift
    echo "  Creating: ${NAME}"
    gcloud run jobs create "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="${ENV_VARS}" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --execution-environment=gen2 \
        --project="${PROJECT}" \
        2>/dev/null || \
    gcloud run jobs update "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="${ENV_VARS}" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --execution-environment=gen2 \
        --project="${PROJECT}"
    echo "  ✓ ${NAME}"
}

# Quality report job: mounts vault (read) + metadata bucket (write) via gcsfuse
# Tools use local pathlib on /vault and /metadata — no gcsfs needed
create_quality_job() {
    local NAME="$1"
    shift
    echo "  Creating (dual mount): ${NAME}"
    gcloud run jobs create "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="${ENV_VARS}" \
        --add-volume="name=vault,type=cloud-storage,bucket=${VAULT_BUCKET}" \
        --add-volume-mount="volume=vault,mount-path=/vault" \
        --add-volume="name=metadata,type=cloud-storage,bucket=${METADATA_BUCKET}" \
        --add-volume-mount="volume=metadata,mount-path=/metadata" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --execution-environment=gen2 \
        --project="${PROJECT}" \
        2>/dev/null || \
    gcloud run jobs update "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="${ENV_VARS}" \
        --add-volume="name=vault,type=cloud-storage,bucket=${VAULT_BUCKET}" \
        --add-volume-mount="volume=vault,mount-path=/vault" \
        --add-volume="name=metadata,type=cloud-storage,bucket=${METADATA_BUCKET}" \
        --add-volume-mount="volume=metadata,mount-path=/metadata" \
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --execution-environment=gen2 \
        --project="${PROJECT}"
    echo "  ✓ ${NAME}"
}

# ── Scraper jobs ──────────────────────────────────────────────────────────────
echo ""
echo "  [Scraper jobs]"

# USA — idempotent daily runs; VAULT_ROOT routes writes to GCS via gcsfs
create_job "job-food-usa"    "python,-m,scrapers.food_pricing.run,--country,USA"
create_job "job-wages-usa"   "python,-m,scrapers.wages_employment.run,--country,USA"
create_job "job-trade-usa"   "python,-m,scrapers.trade_flows.run,--country,USA"
create_job "job-housing-usa" "python,-m,scrapers.housing.run,--country,USA"

# IMF — QUAD_VINTAGE, one run per quarter
create_job "job-imf" "python,-m,scrapers.imf_global_macro.run,--country,ALL"

# International source ingestors — monthly, one job per source covers all 5 products
create_job "job-eurostat" "python,-m,scrapers.eurostat.run_all_ingestion"
create_job "job-ons"      "python,-m,scrapers.ons.ingest_all"
create_job "job-statcan"  "python,-m,scrapers.statcan.ingest_all"
create_job "job-abs"      "python,-m,scrapers.abs.ingest_all"
create_job "job-ssb"      "python,-m,scrapers.ssb.ingest_all"

# ── Metadata tool jobs ────────────────────────────────────────────────────────
echo ""
echo "  [Metadata tool jobs]"

# Coverage manifest — reads catalog YAML (in container), uploads JSON to metadata bucket
create_job "job-coverage-manifest" \
    "python,tools/coverage_manifest_generator.py,\
--catalog,backtesting/backtest_engine/config/catalog_manifest.yaml,\
--out-dir,/tmp/coverage_manifest,\
--gcs-bucket,${METADATA}"

# Release calendar — fetches live release dates from source agencies, uploads to metadata
create_job "job-release-calendar" \
    "python,tools/release_calendar_extractor.py,\
--gcs-bucket,${METADATA}"

# PIT disclosure — static generation, runs after last scraper batch each day
create_job "job-pit-disclosure" \
    "python,tools/pit_disclosure_generator.py,\
--out-dir,/tmp/pit_disclosure_out,\
--gcs-bucket,${METADATA}"

# Quality reports — dual gcsfuse mount: vault at /vault (read), metadata at /metadata (write)
create_quality_job "job-quality-live" \
    "python,tools/quality_report_generator.py,\
--vault-root,/vault,\
--out-dir,/metadata/quality_reports,\
--search-root,/vault,\
--series-manifest,backtesting/backtest_engine/config/catalog_expected_series.yaml,\
--mode,live"

create_quality_job "job-quality-archive" \
    "python,tools/quality_report_generator.py,\
--vault-root,/vault,\
--out-dir,/metadata/quality_reports,\
--search-root,/vault,\
--series-manifest,backtesting/backtest_engine/config/catalog_expected_series.yaml,\
--mode,archive"

echo ""
echo "========================================================"
echo " STEP 4 — Grant Cloud Scheduler permission to invoke jobs"
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

JOB_BASE="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1"
NAMESPACE="namespaces/${PROJECT}"

schedule_job() {
    local SCHED_NAME="$1"
    local SCHEDULE="$2"
    local JOB_NAME="$3"
    local URI="${JOB_BASE}/${NAMESPACE}/jobs/${JOB_NAME}:run"

    echo "  ${SCHED_NAME} → ${JOB_NAME} (${SCHEDULE})"
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

echo ""
echo "  [Scraper schedules]"

# USA — 3× daily: 09:00, 16:00, 21:00 UTC (checks for new BLS/Census releases)
schedule_job "sched-food-usa"    "0 9,16,21 * * *"  "job-food-usa"
schedule_job "sched-wages-usa"   "0 9,16,21 * * *"  "job-wages-usa"
schedule_job "sched-trade-usa"   "0 9,16,21 * * *"  "job-trade-usa"
schedule_job "sched-housing-usa" "0 9,16,21 * * *"  "job-housing-usa"

# IMF — quarterly, 1st of Jan/Apr/Jul/Oct at 08:00 UTC (before monthly runs)
schedule_job "sched-imf"         "0 8 1 1,4,7,10 *" "job-imf"

# International — 1st of each month at 10:00 UTC (agencies publish monthly)
schedule_job "sched-eurostat"    "0 10 1 * *"        "job-eurostat"
schedule_job "sched-ons"         "0 10 1 * *"        "job-ons"
schedule_job "sched-statcan"     "0 10 1 * *"        "job-statcan"
schedule_job "sched-abs"         "0 10 1 * *"        "job-abs"
schedule_job "sched-ssb"         "0 10 1 * *"        "job-ssb"

echo ""
echo "  [Metadata tool schedules]"

# Monthly 1st sequence: manifest (02:00) → release calendar (04:00)
# → USA scrapers (09:00) + international (10:00)
# → quality-archive (12:00)
# Then daily: quality-live (21:30, after 21:00 scrapers) → pit-disclosure (22:00)
schedule_job "sched-coverage-manifest"  "0 2 1 * *"   "job-coverage-manifest"
schedule_job "sched-release-calendar"   "0 4 1 * *"   "job-release-calendar"
schedule_job "sched-quality-archive"    "0 12 1 * *"  "job-quality-archive"
schedule_job "sched-quality-live"       "30 21 * * *" "job-quality-live"
schedule_job "sched-pit-disclosure"     "0 22 * * *"  "job-pit-disclosure"

echo ""
echo "========================================================"
echo " STEP 6 — Grant SA access to GCS buckets"
echo "========================================================"
gcloud storage buckets add-iam-policy-binding "${VAULT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin" \
    --project="${PROJECT}"
echo "  ✓ ${SA} → roles/storage.objectAdmin on ${VAULT}"

gcloud storage buckets add-iam-policy-binding "${METADATA}" \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin" \
    --project="${PROJECT}"
echo "  ✓ ${SA} → roles/storage.objectAdmin on ${METADATA}"

echo ""
echo "========================================================"
echo " STEP 7 — Deploy PIT Disclosure Cloud Function"
echo "========================================================"
gcloud functions deploy pit-disclosure-generator \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=deploy/pit_disclosure_function \
    --entry-point=cloud_function_handler \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=${VAULT_BUCKET}" \
    --set-env-vars="VAULT_ROOT=${VAULT},METADATA_BUCKET=${METADATA},GOOGLE_CLOUD_PROJECT=${PROJECT}" \
    --service-account="${SA}" \
    --memory=512Mi \
    --timeout=120s \
    --project="${PROJECT}" \
    2>/dev/null && echo "  ✓ pit-disclosure-generator Cloud Function deployed" \
    || echo "  ⚡ pit-disclosure-generator: update or check logs"

echo ""
echo "========================================================"
echo " DEPLOYMENT SUMMARY"
echo "========================================================"
echo ""
echo "  Image    : ${IMAGE}"
echo "  Vault    : ${VAULT}"
echo "  Metadata : ${METADATA}"
echo "  Region   : ${REGION}"
echo ""
echo "  Scraper jobs (10) — write to gs://lekwankwa-vault via VAULT_ROOT env var:"
echo "    job-food-usa       → 3× daily  09:00 / 16:00 / 21:00 UTC"
echo "    job-wages-usa      → 3× daily  09:00 / 16:00 / 21:00 UTC"
echo "    job-trade-usa      → 3× daily  09:00 / 16:00 / 21:00 UTC"
echo "    job-housing-usa    → 3× daily  09:00 / 16:00 / 21:00 UTC"
echo "    job-imf            → quarterly 08:00 UTC (1 Jan/Apr/Jul/Oct)"
echo "    job-eurostat       → monthly   10:00 UTC (1st)"
echo "    job-ons            → monthly   10:00 UTC (1st)"
echo "    job-statcan        → monthly   10:00 UTC (1st)"
echo "    job-abs            → monthly   10:00 UTC (1st)"
echo "    job-ssb            → monthly   10:00 UTC (1st)"
echo ""
echo "  Metadata tool jobs (5) — write to gs://lekwankwa-metadata:"
echo "    job-coverage-manifest → monthly   02:00 UTC (1st)"
echo "    job-release-calendar  → monthly   04:00 UTC (1st)"
echo "    job-quality-archive   → monthly   12:00 UTC (1st)"
echo "    job-quality-live      → daily     21:30 UTC  [vault+metadata gcsfuse]"
echo "    job-pit-disclosure    → daily     22:00 UTC  (after 21:00 scrapers)"
echo ""
echo "  Monthly 1st sequence (UTC):"
echo "    02:00 coverage-manifest  04:00 release-calendar"
echo "    08:00 IMF (quarterly)    09:00 USA scrapers (1st of 3)"
echo "    10:00 international      12:00 quality-archive"
echo "    16:00 USA scrapers (2nd) 21:00 USA scrapers (3rd)"
echo "    21:30 quality-live       22:00 pit-disclosure"
echo ""
echo "  PIT Disclosure also fires on every scraper completion marker"
echo "  via Cloud Function: pit-disclosure-generator (GCS event trigger)"
echo ""
echo "  To trigger a job manually:"
echo "    gcloud run jobs execute job-food-usa --region=${REGION} --project=${PROJECT}"
echo ""
echo "  To view job logs:"
echo "    gcloud run jobs executions list --job=job-food-usa --region=${REGION}"
echo ""
echo "Section 4 complete."
