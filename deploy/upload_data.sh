#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Upload Raw Data to Azure Storage Landing Zone
# ══════════════════════════════════════════════════════════════
# Uploads the 4 raw source data files to the ADLS Gen2 landing
# container for Bronze ingestion.
#
# Usage: ./deploy/upload_data.sh <environment> <storage_account>
# Example: ./deploy/upload_data.sh dev telcochurnsadev
# ══════════════════════════════════════════════════════════════

set -euo pipefail

TARGET_ENV="${1:?Usage: $0 <environment> <storage_account>}"
STORAGE_ACCOUNT="${2:?Usage: $0 <environment> <storage_account>}"
CONTAINER="landing"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"

# Files to upload (relative to repo root)
FILES=(
    "customer_info.parquet"
    "usage.parquet"
    "calls.csv"
    "cease.csv"
)

echo "══════════════════════════════════════════════════════════════"
echo " Upload Raw Data → Azure Storage Landing Zone"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo " Environment:     ${TARGET_ENV}"
echo " Storage Account: ${STORAGE_ACCOUNT}"
echo " Container:       ${CONTAINER}"
echo ""

PASS=0
FAIL=0
TOTAL=${#FILES[@]}

for file in "${FILES[@]}"; do
    LOCAL_PATH="${REPO_ROOT}/${file}"
    STEP=$((PASS + FAIL + 1))

    if [ ! -f "${LOCAL_PATH}" ]; then
        echo "[${STEP}/${TOTAL}] SKIP — ${file} not found at ${LOCAL_PATH}"
        FAIL=$((FAIL + 1))
        continue
    fi

    FILE_SIZE=$(du -h "${LOCAL_PATH}" | cut -f1)
    echo -n "[${STEP}/${TOTAL}] Uploading ${file} (${FILE_SIZE})..."

    if az storage blob upload \
        --account-name "${STORAGE_ACCOUNT}" \
        --container-name "${CONTAINER}" \
        --file "${LOCAL_PATH}" \
        --name "${file}" \
        --overwrite \
        --timeout 600 \
        --output none 2>&1; then
        echo " OK"
        PASS=$((PASS + 1))
    else
        echo " FAILED"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "══════════════════════════════════════════════════════════════"
echo " Results: ${PASS}/${TOTAL} uploaded, ${FAIL} failed"
echo "══════════════════════════════════════════════════════════════"

if [ "${FAIL}" -gt 0 ]; then
    echo ""
    echo " WARNING: ${FAIL} file(s) failed to upload."
    exit 1
fi

echo ""
echo " NEXT STEP: Run the DLT pipeline in Databricks to ingest into Bronze."
exit 0
