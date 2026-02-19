#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Upload Raw Data to Shared Landing Storage
# ══════════════════════════════════════════════════════════════
# Uploads the 4 raw source data files to the shared ADLS Gen2
# landing storage account. Data is uploaded once and read by
# all environments (dev/staging/prod).
#
# Usage: ./deploy/upload_data.sh <label> <storage_account> [data_path]
# Example: ./deploy/upload_data.sh shared telcochurnsalanding /path/to/data
# ══════════════════════════════════════════════════════════════

set -euo pipefail

TARGET_ENV="${1:?Usage: $0 <environment> <storage_account> [data_path]}"
STORAGE_ACCOUNT="${2:?Usage: $0 <environment> <storage_account> [data_path]}"
CONTAINER="landing"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"

# Optional 3rd arg: path to data files (defaults to repo root)
DATA_PATH="${3:-${REPO_ROOT}}"

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
    LOCAL_PATH="${DATA_PATH}/${file}"
    STEP=$((PASS + FAIL + 1))

    if [ ! -f "${LOCAL_PATH}" ]; then
        echo "[${STEP}/${TOTAL}] SKIP — ${file} not found at ${LOCAL_PATH}"
        FAIL=$((FAIL + 1))
        continue
    fi

    FILE_SIZE=$(du -h "${LOCAL_PATH}" | cut -f1)
    LOCAL_BYTES=$(wc -c < "${LOCAL_PATH}" | tr -d ' ')

    # Check if blob already exists with same size
    REMOTE_BYTES=$(az storage blob show \
        --account-name "${STORAGE_ACCOUNT}" \
        --container-name "${CONTAINER}" \
        --name "${file}" \
        --query "properties.contentLength" \
        --output tsv 2>/dev/null || echo "0")

    if [ "${REMOTE_BYTES}" = "${LOCAL_BYTES}" ]; then
        echo "[${STEP}/${TOTAL}] SKIP — ${file} (${FILE_SIZE}) already uploaded"
        PASS=$((PASS + 1))
        continue
    fi

    echo -n "[${STEP}/${TOTAL}] Uploading ${file} (${FILE_SIZE})..."

    if az storage blob upload \
        --account-name "${STORAGE_ACCOUNT}" \
        --container-name "${CONTAINER}" \
        --file "${LOCAL_PATH}" \
        --name "${file}" \
        --overwrite \
        --max-connections 1 \
        --timeout 1800 \
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
