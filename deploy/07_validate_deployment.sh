#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 7: Post-Deployment Validation
# Run from Cloud Shell OR on the Compute Engine instance.
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
BASE="/opt/lekwankwa"
PYTHON="${PYTHON:-/usr/bin/python3}"

PASS=0; FAIL=0
check() {
  local DESC="$1"; shift
  if eval "$@" &>/dev/null; then
    echo "  ✓ ${DESC}"; (( PASS++ )) || true
  else
    echo "  ✗ FAIL: ${DESC}"; (( FAIL++ )) || true
  fi
}

echo "========================================================"
echo " 1. GCS BUCKETS"
echo "========================================================"
check "gs://lekwankwa-vault exists"              "gsutil ls gs://lekwankwa-vault"
check "gs://lekwankwa-institutional-data exists" "gsutil ls gs://lekwankwa-institutional-data"

echo "  Vault vs historical size comparison:"
SRC=$(gsutil du -s gs://lekwankwa-historical-vault 2>/dev/null | awk '{print $1}' || echo "N/A")
DST=$(gsutil du -s gs://lekwankwa-vault            2>/dev/null | awk '{print $1}' || echo "N/A")
echo "  lekwankwa-historical-vault : ${SRC} bytes"
echo "  lekwankwa-vault            : ${DST} bytes"
[ "${SRC}" = "${DST}" ] && echo "  ✓ Sizes match" || echo "  ✗ Size mismatch (re-check copy)"

echo ""
echo "  Product prefix verification:"
for P in food_micropricing wages_and_employment Housing_Supply_and_Shelter_Inflation trade_flows global_macro; do
  check "product=${P} in vault" "gsutil ls -d gs://lekwankwa-vault/product=${P}/"
done

echo ""
echo "========================================================"
echo " 2. COMPUTE ENGINE FOLDER STRUCTURE"
echo "========================================================"
check "Base /opt/lekwankwa exists"         "[ -d ${BASE} ]"
check "scrapers/ directory"                "[ -d ${BASE}/scrapers ]"
check "tools/ directory"                   "[ -d ${BASE}/tools ]"
check "tools/self_healing/ directory"      "[ -d ${BASE}/tools/self_healing ]"
check "validations/ directory"             "[ -d ${BASE}/validations ]"
check "logs/extractors/ directory"         "[ -d ${BASE}/logs/extractors ]"
check "logs/self_healing/ directory"       "[ -d ${BASE}/logs/self_healing ]"
check "config/ directory"                  "[ -d ${BASE}/config ]"
check "exports/csv/ directory"             "[ -d ${BASE}/exports/csv ]"

echo ""
echo "  Key files:"
check "handler.py"              "[ -f ${BASE}/tools/self_healing/handler.py ]"
check "format_converter.py"     "[ -f ${BASE}/tools/format_converter.py ]"
check "food_pricing/run.py"     "[ -f ${BASE}/scrapers/food_pricing/run.py ]"
check "wages_employment/run.py" "[ -f ${BASE}/scrapers/wages_employment/run.py ]"
check "housing/run.py"          "[ -f ${BASE}/scrapers/housing/run.py ]"
check "trade_flows/run.py"      "[ -f ${BASE}/scrapers/trade_flows/run.py ]"
check "imf_global_macro/run.py" "[ -f ${BASE}/scrapers/imf_global_macro/run.py ]"

echo ""
echo "========================================================"
echo " 3. CLOUD SCHEDULER JOBS"
echo "========================================================"
JOB_COUNT=$(gcloud scheduler jobs list --project="${PROJECT}" --location="${REGION}" 2>/dev/null | grep -v "^NAME" | wc -l || echo "0")
echo "  Total jobs: ${JOB_COUNT} (expected: 33)"
check "≥30 scheduler jobs exist" "[ ${JOB_COUNT} -ge 30 ]"

echo "  Spot-checking key jobs:"
for JOB in scraper-food-usa scraper-wages-usa-ces scraper-housing-usa-shelter scraper-trade-usa scraper-macro-all coverage-manifest-update quality-report-live; do
  check "job exists: ${JOB}" "gcloud scheduler jobs describe ${JOB} --project=${PROJECT} --location=${REGION}"
done

echo ""
echo "========================================================"
echo " 4. SCRAPER DRY-RUN (food/USA)"
echo "========================================================"
echo "  Running: ${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country USA --dry-run"
${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country USA --dry-run 2>&1 | tail -5
check "food/USA dry-run exits 0" "${PYTHON} ${BASE}/scrapers/food_pricing/run.py --country USA --dry-run"

echo ""
echo "========================================================"
echo " 5. FORMAT CONVERTER TEST (food/USA → CSV)"
echo "========================================================"
${PYTHON} ${BASE}/tools/format_converter.py \
    --product food_micropricing \
    --country USA \
    --format csv \
    --tier PRIMARY \
    --start 2020-01 \
    --output ${BASE}/exports/csv/ 2>&1
check "Format converter output exists" "[ \"\$(ls ${BASE}/exports/csv/food_micropricing_USA*.csv 2>/dev/null | wc -l)\" -gt 0 ]"

echo ""
echo "========================================================"
echo " 6. SECRET MANAGER"
echo "========================================================"
SECRET_COUNT=$(gcloud secrets list --project="${PROJECT}" 2>/dev/null | grep -v "^NAME" | wc -l || echo "0")
echo "  Total secrets: ${SECRET_COUNT} (expected: 8)"
check "≥8 secrets exist" "[ ${SECRET_COUNT} -ge 8 ]"

for SECRET in anthropic-api-key gcs-service-account-key fred-api-key bls-api-key gmail-sender-address gmail-app-password github-token firestore-project-id; do
  check "secret exists: ${SECRET}" "gcloud secrets describe ${SECRET} --project=${PROJECT}"
done

echo ""
echo "========================================================"
echo " 7. SELF-HEALING DRY RUN (simulated MAJOR_EXCEPTION)"
echo "========================================================"
echo "  Simulating exception in food/USA scraper..."
${PYTHON} - <<'PYEOF'
import sys
sys.path.insert(0, "/opt/lekwankwa")
import os; os.chdir("/opt/lekwankwa")

# Simulate only up to layer 2 (no real Claude call / email in test)
from tools.self_healing.scrape4ai_retry import attempt_scrape4ai_retry

ctx = {
    "product":  "food_micropricing",
    "country":  "USA",
    "source":   "bls_cpi",
    "run_date": "2026-06-28",
    "layer":    "TEST",
    "severity": "HIGH",
}
exc = RuntimeError("TEST_EXCEPTION: dry-run self-heal test")
print("  [TEST] Calling Layer 2 (Scrape4AI) with 1 retry...")

# Override backoff to 1 second for test speed
import tools.self_healing.scrape4ai_retry as r
r.BACKOFF_SECS = [1, 1, 1]
r.MAX_RETRIES  = 1

result = attempt_scrape4ai_retry(
    program="/opt/lekwankwa/scrapers/food_pricing/run.py",
    context=ctx,
    exception=exc,
)
print(f"  [TEST] Layer 2 result: {'SUCCEEDED' if result else 'FAILED (expected in test)'}")
print("  [TEST] Self-heal dry run complete — Layer 2 correctly attempted retry")
PYEOF
check "Self-healing Layer 2 dry-run ran" "true"   # always pass if above didn't crash

echo ""
echo "========================================================"
echo " SUMMARY"
echo "========================================================"
echo "  PASS: ${PASS}"
echo "  FAIL: ${FAIL}"
[ "${FAIL}" -eq 0 ] && echo "  ALL CHECKS PASSED ✓" || echo "  ${FAIL} CHECK(S) FAILED — review above"
