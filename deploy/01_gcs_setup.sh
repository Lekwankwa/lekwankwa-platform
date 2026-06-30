#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 1: GCS Bucket Setup
# Project: fluted-alloy-498317-u0
# Run from any machine with gsutil authenticated as info@lekwankwa.com
# =============================================================================

set -euo pipefail

PROJECT="fluted-alloy-498317-u0"
REGION="africa-south1"
PIPELINE_SA="lekwankwa-pipeline@${PROJECT}.iam.gserviceaccount.com"
DELIVERY_SA="lekwankwa-delivery@${PROJECT}.iam.gserviceaccount.com"

echo "========================================================"
echo " SECTION 1A — lekwankwa-vault (internal vault bucket)"
echo "========================================================"

# GCS buckets cannot be renamed — create new bucket and copy
gsutil mb -l ${REGION} -p ${PROJECT} gs://lekwankwa-vault 2>/dev/null \
  && echo "  Created gs://lekwankwa-vault" \
  || echo "  gs://lekwankwa-vault already exists"

echo "  Copying vault contents (parallel -m)..."
gsutil -m cp -r gs://lekwankwa-historical-vault/* gs://lekwankwa-vault/

echo "  Verifying copy..."
SRC_COUNT=$(gsutil ls -r gs://lekwankwa-historical-vault/ | grep -v '/$' | wc -l)
DST_COUNT=$(gsutil ls -r gs://lekwankwa-vault/           | grep -v '/$' | wc -l)
echo "  Source objects : ${SRC_COUNT}"
echo "  Dest objects   : ${DST_COUNT}"
if [ "${SRC_COUNT}" -eq "${DST_COUNT}" ]; then
  echo "  ✓ Object counts match"
else
  echo "  ✗ MISMATCH — investigate before proceeding"
  exit 1
fi

echo "  Verifying 5 product= prefixes..."
for PRODUCT in food_micropricing wages_and_employment Housing_Supply_and_Shelter_Inflation trade_flows global_macro; do
  COUNT=$(gsutil ls -d gs://lekwankwa-vault/product=${PRODUCT}/ 2>/dev/null | wc -l)
  if [ "${COUNT}" -gt 0 ]; then
    echo "  ✓ product=${PRODUCT}"
  else
    echo "  ✗ MISSING product=${PRODUCT}"
  fi
done

echo ""
echo "  Setting IAM — lekwankwa-vault (internal pipeline SA only)"
gsutil iam ch serviceAccount:${PIPELINE_SA}:objectAdmin gs://lekwankwa-vault
gsutil iam ch -d allUsers gs://lekwankwa-vault 2>/dev/null || true
gsutil iam ch -d allAuthenticatedUsers gs://lekwankwa-vault 2>/dev/null || true
echo "  ✓ lekwankwa-vault IAM set (pipeline SA only)"

echo ""
echo "========================================================"
echo " SECTION 1B — lekwankwa-institutional-data (delivery)"
echo "========================================================"

gsutil mb -l ${REGION} -p ${PROJECT} gs://lekwankwa-institutional-data 2>/dev/null \
  && echo "  Created gs://lekwankwa-institutional-data" \
  || echo "  gs://lekwankwa-institutional-data already exists"

# Create placeholder structure
echo ""  | gsutil cp - gs://lekwankwa-institutional-data/clients/.keep
echo "  Created clients/ prefix"

echo "  Setting IAM — lekwankwa-institutional-data"
gsutil iam ch serviceAccount:${DELIVERY_SA}:objectCreator gs://lekwankwa-institutional-data
# No allUsers access — clients use signed URLs only
gsutil iam ch -d allUsers gs://lekwankwa-institutional-data 2>/dev/null || true
gsutil iam ch -d allAuthenticatedUsers gs://lekwankwa-institutional-data 2>/dev/null || true
echo "  ✓ Delivery bucket IAM set (signed URLs only for clients)"

echo ""
echo "========================================================"
echo " SECTION 1C — lekwankwa-metadata (metadata tool outputs)"
echo "========================================================"

gsutil mb -l ${REGION} -p ${PROJECT} gs://lekwankwa-metadata 2>/dev/null \
  && echo "  Created gs://lekwankwa-metadata" \
  || echo "  gs://lekwankwa-metadata already exists"

echo "  Setting IAM — lekwankwa-metadata (pipeline SA read/write)"
gsutil iam ch serviceAccount:${PIPELINE_SA}:objectAdmin gs://lekwankwa-metadata
gsutil iam ch serviceAccount:${DELIVERY_SA}:objectViewer gs://lekwankwa-metadata
gsutil iam ch -d allUsers gs://lekwankwa-metadata 2>/dev/null || true
gsutil iam ch -d allAuthenticatedUsers gs://lekwankwa-metadata 2>/dev/null || true
echo "  ✓ lekwankwa-metadata IAM set"

# Migrate existing metadata/ content from vault → dedicated metadata bucket
VAULT_META_COUNT=$(gsutil ls -r gs://lekwankwa-vault/metadata/ 2>/dev/null | grep -v '/$' | wc -l)
if [ "${VAULT_META_COUNT}" -gt 0 ]; then
    echo ""
    echo "  Migrating ${VAULT_META_COUNT} metadata files from vault → lekwankwa-metadata ..."
    gsutil -m cp -r gs://lekwankwa-vault/metadata/* gs://lekwankwa-metadata/
    echo "  Verifying migration..."
    META_COUNT=$(gsutil ls -r gs://lekwankwa-metadata/ 2>/dev/null | grep -v '/$' | wc -l)
    echo "  Source (vault/metadata/): ${VAULT_META_COUNT} files"
    echo "  Dest (lekwankwa-metadata/): ${META_COUNT} files"
    if [ "${META_COUNT}" -ge "${VAULT_META_COUNT}" ]; then
        echo "  ✓ Migration complete"
        echo "  NOTE: vault/metadata/ originals kept until you confirm and manually delete:"
        echo "    gsutil -m rm -r gs://lekwankwa-vault/metadata/"
    else
        echo "  ✗ MISMATCH — verify before deleting vault/metadata/"
    fi
else
    echo "  (No existing vault/metadata/ content to migrate)"
fi

echo ""
echo "  Placeholder folder structure:"
for PREFIX in coverage_manifest pit_disclosure quality_reports release_calendar; do
    echo "" | gsutil cp - "gs://lekwankwa-metadata/${PREFIX}/.keep" 2>/dev/null || true
done
echo "  ✓ Placeholder prefixes created in lekwankwa-metadata"

echo ""
echo "========================================================"
echo " VERIFICATION"
echo "========================================================"
gsutil ls gs://lekwankwa-vault              && echo "  ✓ lekwankwa-vault accessible"
gsutil ls gs://lekwankwa-institutional-data && echo "  ✓ lekwankwa-institutional-data accessible"
gsutil ls gs://lekwankwa-metadata           && echo "  ✓ lekwankwa-metadata accessible"

SRC_SIZE=$(gsutil du -s gs://lekwankwa-historical-vault | awk '{print $1}')
DST_SIZE=$(gsutil du -s gs://lekwankwa-vault           | awk '{print $1}')
echo "  Source size : ${SRC_SIZE} bytes"
echo "  Dest size   : ${DST_SIZE} bytes"

echo ""
echo "Section 1 complete."
