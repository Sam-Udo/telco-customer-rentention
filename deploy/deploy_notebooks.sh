#!/usr/bin/env bash
# Deploy Databricks notebooks
# Requires: DATABRICKS_HOST, DATABRICKS_TOKEN, TARGET_ENV, CATALOG_NAME

set -euo pipefail

echo "Deploying to ${TARGET_ENV}"
echo "Workspace: ${DATABRICKS_HOST}"
echo "Catalog: ${CATALOG_NAME}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_PATH="/Workspace/telco-churn-${TARGET_ENV}"

export DATABRICKS_HOST="${DATABRICKS_HOST}"
export DATABRICKS_TOKEN="${DATABRICKS_TOKEN}"

# Create workspace directories
echo "[1/4] Creating workspace directories..."
if ! databricks workspace mkdirs "${WORKSPACE_PATH}/notebooks" 2>&1; then
    echo "ERROR: Failed to create ${WORKSPACE_PATH}/notebooks"
    exit 1
fi

if ! databricks workspace mkdirs "${WORKSPACE_PATH}/dlt" 2>&1; then
    echo "ERROR: Failed to create ${WORKSPACE_PATH}/dlt"
    exit 1
fi

if ! databricks workspace mkdirs "${WORKSPACE_PATH}/src" 2>&1; then
    echo "ERROR: Failed to create ${WORKSPACE_PATH}/src"
    exit 1
fi

# Patch catalog references for environment
echo "[2/4] Patching catalog references..."
TEMP_DIR=$(mktemp -d)
cp -r "${REPO_ROOT}/notebooks" "${TEMP_DIR}/"
cp -r "${REPO_ROOT}/dlt" "${TEMP_DIR}/"
cp -r "${REPO_ROOT}/src" "${TEMP_DIR}/"

if [ "${TARGET_ENV}" != "prod" ]; then
    find "${TEMP_DIR}" -name "*.py" -exec sed -i.bak \
        "s/uk_telecoms\./uk_telecoms_${TARGET_ENV}./g" {} \;
    find "${TEMP_DIR}" -name "*.py" -exec sed -i.bak \
        "s/uk_telecoms\.ml/uk_telecoms_${TARGET_ENV}.ml/g" {} \;
    find "${TEMP_DIR}" -name "*.bak" -delete
fi

# Upload notebooks
echo "[3/4] Uploading notebooks..."
UPLOAD_ERRORS=0
UPLOAD_COUNT=0

for notebook in "${TEMP_DIR}/notebooks"/*.py; do
    nb_name=$(basename "$notebook" .py)
    echo "  Uploading: ${nb_name}"

    if ! databricks workspace import \
        --language PYTHON \
        --overwrite \
        "${notebook}" \
        "${WORKSPACE_PATH}/notebooks/${nb_name}" 2>&1; then
        echo "  ERROR: Failed to upload ${nb_name}"
        UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
    else
        UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
    fi
done

# Upload DLT pipeline
echo "  Uploading: dlt_pipeline"
if ! databricks workspace import \
    --language PYTHON \
    --overwrite \
    "${TEMP_DIR}/dlt/dlt_pipeline.py" \
    "${WORKSPACE_PATH}/dlt/dlt_pipeline" 2>&1; then
    echo "  ERROR: Failed to upload dlt_pipeline"
    UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
else
    UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
fi

# Upload src modules
for srcfile in "${TEMP_DIR}/src"/*.py; do
    src_name=$(basename "$srcfile" .py)
    echo "  Uploading: src/${src_name}"

    if ! databricks workspace import \
        --language PYTHON \
        --overwrite \
        "${srcfile}" \
        "${WORKSPACE_PATH}/src/${src_name}" 2>&1; then
        echo "  ERROR: Failed to upload src/${src_name}"
        UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
    else
        UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
    fi
done

rm -rf "${TEMP_DIR}"

echo "[4/4] Deployment summary:"
echo "  Success: ${UPLOAD_COUNT} files"
echo "  Failed:  ${UPLOAD_ERRORS} files"

if [ "${UPLOAD_ERRORS}" -gt 0 ]; then
    echo "ERROR: ${UPLOAD_ERRORS} file(s) failed to upload"
    exit 1
else
    echo "Deployment complete"
    exit 0
fi
