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
#   - lekwankwa-vault GCS bucket exists
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"
IMAGE="gcr.io/${PROJECT}/lekwankwa-scrapers:latest"
VAULT="gs://lekwankwa-vault"
VAULT_BUCKET="lekwankwa-vault"

echo "========================================================"
echo " STEP 0 — Load API keys from .env"
echo "========================================================"
if [ -f ".env" ]; then
    set -a; source .env; set +a
    echo "  ✓ .env loaded"
else
    echo "  WARNING: .env not found in repo root."
    echo "  Upload .env to Cloud Shell before running this script."
    echo "  Continuing — keys will be empty strings if missing."
fi

# Build comma-separated env-vars string passed to every job
ENV_VARS="VAULT_ROOT=${VAULT}"
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

echo "  ✓ Env vars built (${#ENV_VARS} chars)"

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
echo " STEP 3 — Create Cloud Run Jobs (scrapers + tools)"
echo "========================================================"

# create_job NAME ARGS...
# Standard job: no volume mount, VAULT_ROOT env var routes writes to GCS via gcsfs
create_job() {
    local NAME="$1"
    shift
    echo "  Creating job: ${NAME}"
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

# create_vault_job NAME ARGS...
# Quality report jobs: mounts GCS bucket at /vault so pathlib reads work transparently
create_vault_job() {
    local NAME="$1"
    shift
    echo "  Creating job (vault mount): ${NAME}"
    gcloud run jobs create "${NAME}" \
        --image="${IMAGE}" \
        --args="$@" \
        --set-env-vars="${ENV_VARS}" \
        --add-volume="name=vault,type=cloud-storage,bucket=${VAULT_BUCKET}" \
        --add-volume-mount="volume=vault,mount-path=/vault" \
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
        --service-account="${SA}" \
        --region="${REGION}" \
        --max-retries=1 \
        --task-timeout=3600 \
        --execution-environment=gen2 \
        --project="${PROJECT}"
    echo "  ✓ ${NAME}"
}

# ── Scraper jobs ──────────────────────────────────────────────────────────────
# USA scrapers — VAULT_ROOT env var routes to GCS via gcsfs (daily check)
create_job "job-food-usa"    "python,-m,scrapers.food_pricing.run,--country,USA"
create_job "job-wages-usa"   "python,-m,scrapers.wages_employment.run,--country,USA"
create_job "job-trade-usa"   "python,-m,scrapers.trade_flows.run,--country,USA"
create_job "job-housing-usa" "python,-m,scrapers.housing.run,--country,USA"

# IMF — QUAD_VINTAGE, runs quarterly
create_job "job-imf" "python,-m,scrapers.imf_global_macro.run,--country,ALL"

# International source ingestors — one job per source, runs all products
create_job "job-eurostat" "python,-m,scrapers.eurostat.run_all_ingestion"
create_job "job-ons"      "python,-m,scrapers.ons.ingest_all"
create_job "job-statcan"  "python,-m,scrapers.statcan.ingest_all"
create_job "job-abs"      "python,-m,scrapers.abs.ingest_all"
create_job "job-ssb"      "python,-m,scrapers.ssb.ingest_all"

# ── Metadata tool jobs ─────────────────────────────────────────────────────────
# Coverage manifest — reads catalog YAML (in container), uploads JSON to GCS
create_job "job-coverage-manifest" \
    "python,tools/coverage_manifest_generator.py,\
--catalog,backtesting/backtest_engine/config/catalog_manifest.yaml,\
--out-dir,/tmp/coverage_manifest,\
--gcs-bucket,${VAULT}"

# Release calendar — fetches from source agencies, uploads to GCS bucket
create_job "job-release-calendar" \
    "python,tools/release_calendar_extractor.py,\
--gcs-bucket,${VAULT}"

# Quality reports — mounts vault at /vault so pathlib reads work on GCS files
# Reads vault parquet files → produces JSON quality reports → writes to /vault/metadata/
create_vault_job "job-quality-live" \
    "python,tools/quality_report_generator.py,\
--vault-root,/vault,\
--out-dir,/vault/metadata/quality_reports,\
--search-root,/vault,\
--series-manifest,backtesting/backtest_engine/config/catalog_expected_series.yaml,\
--mode,live"

create_vault_job "job-quality-archive" \
    "python,tools/quality_report_generator.py,\
--vault-root,/vault,\
--out-dir,/vault/metadata/quality_reports,\
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

# ── Scraper schedules ─────────────────────────────────────────────────────────
# USA — daily at 09:00 UTC (BLS/Census release monthly; daily is cheap idempotent check)
schedule_job "sched-food-usa"    "0 9 * * *"       "job-food-usa"
schedule_job "sched-wages-usa"   "0 9 * * *"       "job-wages-usa"
schedule_job "sched-trade-usa"   "0 9 * * *"       "job-trade-usa"
schedule_job "sched-housing-usa" "0 9 * * *"       "job-housing-usa"

# IMF — quarterly: 1st of Jan/Apr/Jul/Oct at 08:00 UTC (matches WEO publication dates)
schedule_job "sched-imf"         "0 8 1 1,4,7,10 *" "job-imf"

# International — 1st of each month at 10:00 UTC (after monthly agency releases)
schedule_job "sched-eurostat"    "0 10 1 * *"      "job-eurostat"
schedule_job "sched-ons"         "0 10 1 * *"      "job-ons"
schedule_job "sched-statcan"     "0 10 1 * *"      "job-statcan"
schedule_job "sched-abs"         "0 10 1 * *"      "job-abs"
schedule_job "sched-ssb"         "0 10 1 * *"      "job-ssb"

# ── Metadata tool schedules ───────────────────────────────────────────────────
# Monthly sequence on the 1st: manifest → release calendar → then scrapers at 09/10:00
# → quality-live at 11:00 (after scrapers) → quality-archive at 12:00
schedule_job "sched-coverage-manifest"  "0 2 1 * *"  "job-coverage-manifest"
schedule_job "sched-release-calendar"   "0 4 1 * *"  "job-release-calendar"
schedule_job "sched-quality-live"       "0 11 * * *" "job-quality-live"
schedule_job "sched-quality-archive"    "0 12 1 * *" "job-quality-archive"

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
echo "  Image   : ${IMAGE}"
echo "  Vault   : ${VAULT}"
echo "  Region  : ${REGION}"
echo ""
echo "  Scraper jobs (10):"
echo "    job-food-usa       → daily     09:00 UTC"
echo "    job-wages-usa      → daily     09:00 UTC"
echo "    job-trade-usa      → daily     09:00 UTC"
echo "    job-housing-usa    → daily     09:00 UTC"
echo "    job-imf            → quarterly 08:00 UTC (1 Jan/Apr/Jul/Oct)"
echo "    job-eurostat       → monthly   10:00 UTC (1st)"
echo "    job-ons            → monthly   10:00 UTC (1st)"
echo "    job-statcan        → monthly   10:00 UTC (1st)"
echo "    job-abs            → monthly   10:00 UTC (1st)"
echo "    job-ssb            → monthly   10:00 UTC (1st)"
echo ""
echo "  Metadata tool jobs (4):"
echo "    job-coverage-manifest → monthly   02:00 UTC (1st)"
echo "    job-release-calendar  → monthly   04:00 UTC (1st)"
echo "    job-quality-live      → daily     11:00 UTC  [vault mounted at /vault]"
echo "    job-quality-archive   → monthly   12:00 UTC (1st) [vault mounted at /vault]"
echo ""
echo "  Monthly 1st sequence:"
echo "    02:00 coverage-manifest   04:00 release-calendar"
echo "    09:00 USA scrapers        10:00 international scrapers"
echo "    11:00 quality-live        12:00 quality-archive"
echo ""
echo "  To trigger a job manually:"
echo "    gcloud run jobs execute job-food-usa --region=${REGION} --project=${PROJECT}"
echo ""
echo "  To view job logs:"
echo "    gcloud run jobs executions list --job=job-food-usa --region=${REGION}"
echo ""
echo "Section 4 complete."
