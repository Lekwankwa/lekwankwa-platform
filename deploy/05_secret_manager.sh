#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 5: Secret Manager Setup
# Project: fluted-alloy-498317-u0
# Run from Cloud Shell or any machine with gcloud authenticated.
#
# IMPORTANT: After running this script, fill in REAL values by running:
#   echo -n "REAL_VALUE" | gcloud secrets versions add SECRET_NAME --data-file=-
#
# Never put real values in this script. It only creates placeholder secrets.
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"

echo "========================================================"
echo " Enabling Secret Manager API"
echo "========================================================"
gcloud services enable secretmanager.googleapis.com --project="${PROJECT}"
echo "  ✓ Secret Manager API enabled"

echo ""
echo "========================================================"
echo " Creating secrets (placeholder values)"
echo "========================================================"

create_secret() {
  local NAME="$1"
  local DESC="$2"

  if gcloud secrets describe "${NAME}" --project="${PROJECT}" &>/dev/null; then
    echo "  ⚡ Already exists: ${NAME}"
  else
    echo -n "PLACEHOLDER_FILL_ME_IN" | gcloud secrets create "${NAME}" \
      --project="${PROJECT}" \
      --replication-policy="user-managed" \
      --locations="${REGION}" \
      --data-file=- \
      --labels="purpose=${DESC// /_}"
    echo "  ✓ Created: ${NAME}"
  fi
}

create_secret "anthropic-api-key"         "claude api layer3 diagnosis"
create_secret "gcs-service-account-key"   "gcs vault read write"
create_secret "fred-api-key"              "fred alfred data"
create_secret "bls-api-key"               "bls api"
create_secret "gmail-sender-address"      "self healing emails sender"
create_secret "gmail-app-password"        "gmail smtp app password"
create_secret "github-token"              "auto apply simple fixes"
create_secret "firestore-project-id"      "approval token storage"

echo ""
echo "========================================================"
echo " Grant pipeline SA access to all secrets"
echo "========================================================"

SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"

for SECRET in \
  anthropic-api-key \
  gcs-service-account-key \
  fred-api-key \
  bls-api-key \
  gmail-sender-address \
  gmail-app-password \
  github-token \
  firestore-project-id; do
  gcloud secrets add-iam-policy-binding "${SECRET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/secretmanager.secretAccessor"
  echo "  ✓ ${SA} → ${SECRET}"
done

echo ""
echo "========================================================"
echo " VERIFICATION"
echo "========================================================"
gcloud secrets list --project="${PROJECT}"

echo ""
echo "========================================================"
echo " NEXT STEPS — Fill in real values:"
echo "========================================================"
echo ""
echo "  echo -n 'YOUR_ANTHROPIC_KEY' | gcloud secrets versions add anthropic-api-key --data-file=-"
echo "  echo -n 'YOUR_FRED_KEY'      | gcloud secrets versions add fred-api-key --data-file=-"
echo "  echo -n 'YOUR_BLS_KEY'       | gcloud secrets versions add bls-api-key --data-file=-"
echo "  echo -n 'info@lekwankwa.com' | gcloud secrets versions add gmail-sender-address --data-file=-"
echo "  echo -n 'YOUR_APP_PASSWORD'  | gcloud secrets versions add gmail-app-password --data-file=-"
echo "  echo -n 'YOUR_GITHUB_TOKEN'  | gcloud secrets versions add github-token --data-file=-"
echo "  echo -n '${PROJECT}'         | gcloud secrets versions add firestore-project-id --data-file=-"
echo ""
echo "  # For gcs-service-account-key: upload the JSON key file"
echo "  gcloud secrets versions add gcs-service-account-key --data-file=path/to/sa-key.json"
echo ""
echo "Section 5 complete."
