#!/usr/bin/env bash
# =============================================================================
# Lekwankwa Corporation — Section 2: Compute Engine Setup
# Run ON the Compute Engine instance as root or sudo
# =============================================================================

set -euo pipefail

REPO_ROOT="/opt/lekwankwa"
USER="lekwankwa-pipeline"
GROUP="lekwankwa-pipeline"

echo "========================================================"
echo " SECTION 2A — Create folder structure"
echo "========================================================"

# Tools
mkdir -p ${REPO_ROOT}/tools/self_healing

# Scrapers
mkdir -p ${REPO_ROOT}/scrapers/food_pricing
mkdir -p ${REPO_ROOT}/scrapers/wages_employment
mkdir -p ${REPO_ROOT}/scrapers/trade_flows
mkdir -p ${REPO_ROOT}/scrapers/housing
mkdir -p ${REPO_ROOT}/scrapers/imf_global_macro
mkdir -p ${REPO_ROOT}/scrapers/utilities
mkdir -p ${REPO_ROOT}/scrapers/alfred_pit

# Validations — product-organised subfolders (mirrors repo structure)
mkdir -p ${REPO_ROOT}/validations/food_micropricing
mkdir -p ${REPO_ROOT}/validations/wages_and_employment
mkdir -p ${REPO_ROOT}/validations/housing
mkdir -p ${REPO_ROOT}/validations/trade_flows
mkdir -p ${REPO_ROOT}/validations/imf_global_macro
mkdir -p ${REPO_ROOT}/validations/eurostat
mkdir -p ${REPO_ROOT}/validations/non_eu
mkdir -p ${REPO_ROOT}/validations/gx_universal       # GX Universal validator
mkdir -p ${REPO_ROOT}/validations/temporal_coverage   # temporal coverage validator

# Exports
mkdir -p ${REPO_ROOT}/exports/csv
mkdir -p ${REPO_ROOT}/exports/json
mkdir -p ${REPO_ROOT}/exports/parquet

# Logs
mkdir -p ${REPO_ROOT}/logs/extractors
mkdir -p ${REPO_ROOT}/logs/quality_report
mkdir -p ${REPO_ROOT}/logs/live_feed_audit
mkdir -p ${REPO_ROOT}/logs/self_healing
mkdir -p ${REPO_ROOT}/logs/vault_audit
mkdir -p ${REPO_ROOT}/logs/release_calendar
mkdir -p ${REPO_ROOT}/logs/coverage_manifest

# Config
mkdir -p ${REPO_ROOT}/config

echo "  ✓ Folder structure created"

echo ""
echo "========================================================"
echo " SECTION 2B — Set permissions"
echo "========================================================"

# Create service user if it doesn't exist
id -u ${USER} &>/dev/null || useradd -r -s /usr/sbin/nologin ${USER}

chmod -R 755 ${REPO_ROOT}/
chown -R ${USER}:${GROUP} ${REPO_ROOT}/
echo "  ✓ Permissions set (755, owned by ${USER}:${GROUP})"

echo ""
echo "========================================================"
echo " SECTION 2C — Install Python dependencies"
echo "========================================================"

pip install --break-system-packages \
  pandas>=2.0.0 \
  pyarrow>=14.0.0 \
  requests>=2.31.0 \
  anthropic>=0.28.0 \
  google-cloud-secret-manager>=2.18.0 \
  google-cloud-firestore>=2.14.0 \
  python-dotenv>=1.0.0

echo "  ✓ Python dependencies installed"

echo ""
echo "========================================================"
echo " VERIFICATION"
echo "========================================================"

echo "  Folder structure:"
ls -la ${REPO_ROOT}/
echo ""
echo "  Scrapers:"
ls -la ${REPO_ROOT}/scrapers/
echo ""
echo "  Tools:"
ls -la ${REPO_ROOT}/tools/
echo ""
echo "  Logs:"
ls -la ${REPO_ROOT}/logs/

echo ""
echo "Section 2 complete."
echo "Next: run 03_copy_repo.sh to copy code from local machine."
