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
# Schedule summary (all times UTC, all jobs daily except IMF):
#   02:00  Coverage manifest   (daily)
#   04:00  Release calendar    (daily)
#   08:00  IMF                 (quarterly: 1 Jan/Apr/Jul/Oct only)
#   09:00  USA scrapers        (3× daily: 09:00, 16:00, 21:00)
#   10:00  International       (daily: eurostat/ons/statcan/abs/ssb)
#   12:00  Quality archive     (daily)
#   16:00  USA scrapers        (2nd run)
#   21:00  USA scrapers        (3rd run)
#   21:30  Quality live        (daily, after last USA scraper batch)
#   22:00  PIT disclosure      (daily)
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
SCHED_REGION="europe-west1"   # Cloud Scheduler does not support africa-south1
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

# Non-secret env vars only — API keys are loaded from Secret Manager at runtime
# (tools/secrets.py calls Secret Manager on startup; no raw keys in container env)
ENV_VARS="VAULT_ROOT=${VAULT}"
ENV_VARS="${ENV_VARS},METADATA_BUCKET=${METADATA}"
ENV_VARS="${ENV_VARS},GOOGLE_CLOUD_PROJECT=${PROJECT}"
ENV_VARS="${ENV_VARS},PIPELINE_ENV=production"

echo "  ✓ Env vars built (API keys loaded from Secret Manager at runtime)"

echo ""
echo "========================================================"
echo " STEP 0b — Grant SA access to Secret Manager"
echo "========================================================"
gcloud services enable secretmanager.googleapis.com --project="${PROJECT}"
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None \
    --quiet
echo "  ✓ roles/secretmanager.secretAccessor granted to ${SA}"
echo ""
echo "  IMPORTANT: API keys must be stored as Secret Manager secrets."
echo "  Run once (substitute real values) if not already done:"
echo "    echo -n '<value>' | gcloud secrets create fred-api-key    --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create alfred-api-key  --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create bls-api-key     --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create usda-api-key    --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create usda-ers-api-key --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create census-api-key  --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create eia-api-key     --data-file=- --project=${PROJECT}"
echo "    echo -n '<value>' | gcloud secrets create bea-api-key     --data-file=- --project=${PROJECT}"

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
        --memory=2Gi \
        --cpu=2 \
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
        --memory=2Gi \
        --cpu=2 \
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
        --task-timeout=10800 \
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
        --task-timeout=10800 \
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
# Invoked via -m (not a direct script path) so `import tools.self_healing...`
# resolves correctly — direct-path invocation puts tools/ itself on sys.path[0]
# instead of the repo root, breaking every "tools.*" package import inside
# the script (this is why its self-healing escalation always failed with
# "No module named 'tools'"). --series-manifest also moved from backtesting/
# (local-only test scaffolding, never deployed) to configs/ (actually
# deployed), same fix as SCHEMA_STANDARD.yaml.
create_quality_job "job-quality-live" \
    "python,-m,tools.quality_report_generator,\
--vault-root,/vault,\
--out-dir,/metadata/quality_reports,\
--series-manifest,configs/catalog_expected_series.yaml,\
--mode,live"

create_quality_job "job-quality-archive" \
    "python,-m,tools.quality_report_generator,\
--vault-root,/vault,\
--out-dir,/metadata/quality_reports,\
--series-manifest,configs/catalog_expected_series.yaml,\
--mode,archive"

# Health check — polls Cloud Run + Scheduler + GCS; writes health/health_status.json
create_job "job-health-check" \
    "python,tools/health_check.py"

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
        --location="${SCHED_REGION}" \
        --schedule="${SCHEDULE}" \
        --uri="${URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SA}" \
        --time-zone="UTC" \
        --attempt-deadline="30m" \
        --project="${PROJECT}" \
        2>/dev/null || \
    gcloud scheduler jobs update http "${SCHED_NAME}" \
        --location="${SCHED_REGION}" \
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

# IMF — quarterly only (API publishes 4 times/year; no new data between releases)
schedule_job "sched-imf"         "0 8 1 1,4,7,10 *" "job-imf"

# International — daily at 10:00 UTC (idempotent; skips write if no new data)
schedule_job "sched-eurostat"    "0 10 * * *"        "job-eurostat"
schedule_job "sched-ons"         "0 10 * * *"        "job-ons"
schedule_job "sched-statcan"     "0 10 * * *"        "job-statcan"

echo ""
echo "  [Metadata tool schedules]"

# All metadata tools daily — sequenced around scraper windows:
#   02:00 manifest  04:00 release-cal  (before scrapers start)
#   12:00 quality-archive              (after 09:00 + 10:00 scraper batches)
#   21:30 quality-live                 (after last 21:00 scraper batch)
#   22:00 pit-disclosure               (after quality-live)
schedule_job "sched-coverage-manifest"  "0 2 * * *"   "job-coverage-manifest"
schedule_job "sched-release-calendar"   "0 4 * * *"   "job-release-calendar"
schedule_job "sched-quality-archive"    "0 12 * * *"  "job-quality-archive"
schedule_job "sched-quality-live"       "30 21 * * *" "job-quality-live"
schedule_job "sched-pit-disclosure"     "0 22 * * *"  "job-pit-disclosure"
schedule_job "sched-health-check"       "0 * * * *"   "job-health-check"

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

gcloud storage buckets add-iam-policy-binding "gs://lekwankwa-pipeline-ops" \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin" \
    --project="${PROJECT}"
echo "  ✓ ${SA} → roles/storage.objectAdmin on gs://lekwankwa-pipeline-ops"

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
echo "    job-imf            → quarterly 08:00 UTC (1 Jan/Apr/Jul/Oct only)"
echo "    job-eurostat       → daily     10:00 UTC"
echo "    job-ons            → daily     10:00 UTC"
echo "    job-statcan        → daily     10:00 UTC"
echo ""
echo "  Metadata tool jobs (5) — write to gs://lekwankwa-metadata:"
echo "    job-coverage-manifest → daily 02:00 UTC"
echo "    job-release-calendar  → daily 04:00 UTC"
echo "    job-quality-archive   → daily 12:00 UTC  [vault+metadata gcsfuse]"
echo "    job-quality-live      → daily 21:30 UTC  [vault+metadata gcsfuse]"
echo "    job-pit-disclosure    → daily 22:00 UTC"
echo ""
echo "  Health monitoring job (1) — writes gs://lekwankwa-metadata/health/:"
echo "    job-health-check      → every hour :00 UTC"
echo "    Covers: Cloud Run job crashes / stale schedulers / stale vault data"
echo "    Dashboard: streamlit run tools/health_dashboard.py"
echo ""
echo "  Daily schedule (every day, UTC):"
echo "    02:00 coverage-manifest    04:00 release-calendar"
echo "    09:00 USA scrapers (1/3)   10:00 international scrapers"
echo "    12:00 quality-archive      16:00 USA scrapers (2/3)"
echo "    21:00 USA scrapers (3/3)   21:30 quality-live"
echo "    22:00 pit-disclosure       :00   health-check (every hour)"
echo ""
echo "  IMF exception: quarterly only (1 Jan/Apr/Jul/Oct at 08:00 UTC)"
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
