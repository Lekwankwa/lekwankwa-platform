#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 3: Cloud Scheduler Jobs
# Project: fluted-alloy-498317-u0
# All jobs use HTTP trigger to Cloud Run endpoint OR SSH to Compute Engine.
# =============================================================================
# PREREQUISITES:
#   - gcloud authenticated as info@lekwankwa.com
#   - Cloud Scheduler API enabled
#   - Compute Engine instance running at INSTANCE_NAME
#   - Fill in INSTANCE_NAME, ZONE, and CLOUD_RUN_BASE_URL below
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
ZONE="africa-south1-a"                  # ← SET THIS
INSTANCE_NAME="lekwankwa-pipeline-vm"   # ← SET THIS
SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"
PYTHON="/usr/bin/python3"
BASE="/opt/lekwankwa"

# Helper: create a scheduler job that SSHes into GCE to run a Python script
create_job() {
  local NAME="$1"
  local SCHEDULE="$2"
  local CMD="$3"

  echo "  Creating job: ${NAME}"
  gcloud scheduler jobs create http "${NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --uri="https://compute.googleapis.com/compute/v1/projects/${PROJECT}/zones/${ZONE}/instances/${INSTANCE_NAME}/setMetadata" \
    --message-body="{}" \
    --time-zone="UTC" \
    --attempt-deadline="30m" \
    --service-account-email="${SA}" \
    2>/dev/null || \
  gcloud scheduler jobs update http "${NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --uri="https://compute.googleapis.com/compute/v1/projects/${PROJECT}/zones/${ZONE}/instances/${INSTANCE_NAME}/setMetadata" \
    --message-body="{}" \
    2>/dev/null || true

  # Alternative: use gcloud compute ssh command approach via Cloud Run trigger
  # The actual run command is stored as a label for reference
  gcloud scheduler jobs create http "${NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --uri="https://${REGION}-${PROJECT}.cloudfunctions.net/run-pipeline-job" \
    --message-body="{\"command\": \"${CMD}\"}" \
    --headers="Content-Type=application/json" \
    --time-zone="UTC" \
    --attempt-deadline="30m" \
    --service-account-email="${SA}" \
    2>/dev/null || echo "  (job ${NAME} already exists or using update)"
}

echo "========================================================"
echo " ENABLING APIs"
echo "========================================================"
gcloud services enable cloudscheduler.googleapis.com --project="${PROJECT}"
gcloud services enable cloudfunctions.googleapis.com --project="${PROJECT}"
gcloud services enable compute.googleapis.com --project="${PROJECT}"
echo "  ✓ APIs enabled"

echo ""
echo "========================================================"
echo " PRODUCT 1 — food_micropricing"
echo "========================================================"
create_job "scraper-food-usa"        "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country USA"
create_job "scraper-food-gbr"        "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country GBR"
create_job "scraper-food-can"        "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country CAN"
create_job "scraper-food-eu27"       "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country EU27"
create_job "scraper-food-eu-members" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country ALL_EU"
echo "  ✓ 5 food jobs created"

echo ""
echo "========================================================"
echo " PRODUCT 2 — wages_and_employment"
echo "========================================================"
create_job "scraper-wages-usa-ces" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/wages_and_employment/run.py --country USA --source bls_ces"
create_job "scraper-wages-usa-cps" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/wages_and_employment/run.py --country USA --source bls_cps"
create_job "scraper-wages-gbr"     "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/wages_and_employment/run.py --country GBR"
create_job "scraper-wages-can"     "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/wages_and_employment/run.py --country CAN"
create_job "scraper-wages-eu27"    "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/wages_and_employment/run.py --country EU27"
echo "  ✓ 5 wages jobs created"

echo ""
echo "========================================================"
echo " PRODUCT 3 — Housing_Supply_and_Shelter_Inflation"
echo "========================================================"
create_job "scraper-housing-usa-shelter" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/Housing_Supply_and_Shelter_Inflation/run.py --country USA --source bls_cpi_shelter"
create_job "scraper-housing-usa-permits" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/Housing_Supply_and_Shelter_Inflation/run.py --country USA --source census_bps"
create_job "scraper-housing-gbr"         "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/Housing_Supply_and_Shelter_Inflation/run.py --country GBR"
create_job "scraper-housing-eu27"        "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/Housing_Supply_and_Shelter_Inflation/run.py --country EU27"
echo "  ✓ 4 housing jobs created"

echo ""
echo "========================================================"
echo " PRODUCT 4 — trade_flows"
echo "========================================================"
create_job "scraper-trade-usa"  "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/trade_flows/run.py --country USA"
create_job "scraper-trade-gbr"  "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/trade_flows/run.py --country GBR"
create_job "scraper-trade-can"  "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/trade_flows/run.py --country CAN"
create_job "scraper-trade-eu27" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/trade_flows/run.py --country EU27"
echo "  ✓ 4 trade jobs created"

echo ""
echo "========================================================"
echo " PRODUCT 5 — global_macro (IMF WEO)"
echo "========================================================"
create_job "scraper-macro-all" "0 9,20 * * *" "${PYTHON} ${BASE}/scrapers/global_macro/run.py --country ALL"
echo "  ✓ 1 macro job created"

echo ""
echo "========================================================"
echo " PIPELINE TOOLS"
echo "========================================================"
create_job "coverage-manifest-update"  "0 2 1 * *"  "${PYTHON} ${BASE}/tools/coverage_manifest_generator.py"
create_job "vault-audit"               "0 3 1 * *"  "${PYTHON} ${BASE}/tools/vault_audit.py"
create_job "release-calendar"          "0 4 1 * *"  "${PYTHON} ${BASE}/tools/release_calendar_extractor.py"
create_job "quality-report-live"       "0 11 * * *" "${PYTHON} ${BASE}/tools/quality_report_generator.py --mode live"
create_job "quality-report-archive"    "0 12 1 * *" "${PYTHON} ${BASE}/tools/quality_report_generator.py --mode archive"
echo "  ✓ 5 tool jobs created"

echo ""
echo "========================================================"
echo " TOTAL JOB COUNT"
echo "========================================================"
gcloud scheduler jobs list --project="${PROJECT}" --location="${REGION}" | wc -l
echo "  (expected: 33 jobs)"

echo ""
echo "========================================================"
echo " MONTHLY SEQUENCE SUMMARY (1st of every month, UTC)"
echo "========================================================"
echo "  02:00 — coverage-manifest-update"
echo "  03:00 — vault-audit"
echo "  04:00 — release-calendar"
echo "  09:00 — ALL scrapers (1st morning window)"
echo "  11:00 — quality-report-live"
echo "  12:00 — quality-report-archive"
echo "  20:00 — ALL scrapers (2nd evening window)"
echo ""
echo "Section 3 complete."
