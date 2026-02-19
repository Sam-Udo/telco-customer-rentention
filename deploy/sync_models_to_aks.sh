#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Sync Models from Databricks DBFS → Azure File Share → AKS
# ══════════════════════════════════════════════════════════════
# Called by: aks-pipeline.yml and cd-pipeline.yml
#
# This script bridges Databricks (where models are trained) and
# AKS (where models are served) by:
#   1. Downloading model .txt files from Databricks DBFS
#   2. Uploading to Azure File Share (backing the K8s PVC)
#   3. Uploading scores CSV to the scores file share
#   4. Rolling restart of API pods to reload models
#   5. Verifying /ready endpoint returns 200
#
# Required env vars:
#   DATABRICKS_HOST    — Workspace URL (e.g. https://adb-xxx.azuredatabricks.net)
#   DATABRICKS_TOKEN   — PAT or SP token
#   AKS_RESOURCE_GROUP — AKS resource group
#   AKS_CLUSTER_NAME   — AKS cluster name
#   TARGET_ENV         — dev | staging | prod
#
# Optional env vars:
#   API_URL            — API base URL for readiness check (default: auto-detect from ingress)
#   NAMESPACE          — K8s namespace (default: telco-churn)
#   DBFS_MODEL_DIR     — DBFS path for models (default: dbfs:/tmp/aks_models)

set -euo pipefail

echo "═══════════════════════════════════════════════════"
echo "  Syncing Models to AKS (${TARGET_ENV})"
echo "═══════════════════════════════════════════════════"

NAMESPACE="${NAMESPACE:-telco-churn}"
DBFS_MODEL_DIR="${DBFS_MODEL_DIR:-/tmp/aks_models}"
LOCAL_DIR=$(mktemp -d)
ENV_PREFIX="${TARGET_ENV}"
[ "${TARGET_ENV}" = "prod" ] && ENV_PREFIX="prod"

MODEL_FILES=("churn_model_30d.txt" "churn_model_60d.txt" "churn_model_90d.txt")
SCORES_FILE="churn_scores.csv"

# ── Step 1: Download models from Databricks DBFS ──
echo "[1/5] Downloading models from DBFS..."

for file in "${MODEL_FILES[@]}" "${SCORES_FILE}"; do
    echo "  Downloading ${file}..."
    python3 -c "
import requests, base64, os
headers = {'Authorization': 'Bearer ${DATABRICKS_TOKEN}'}
host = '${DATABRICKS_HOST}'.rstrip('/')
path = '${DBFS_MODEL_DIR}/${file}'
offset = 0
output = '${LOCAL_DIR}/${file}'
with open(output, 'wb') as f:
    while True:
        resp = requests.get(f'{host}/api/2.0/dbfs/read',
            headers=headers,
            params={'path': path, 'offset': offset, 'length': 1048576})
        data = resp.json()
        if 'data' not in data:
            raise RuntimeError(f'DBFS read failed for {path}: {data}')
        chunk = base64.b64decode(data['data'])
        f.write(chunk)
        offset += data.get('bytes_read', 0)
        if data.get('bytes_read', 0) < 1048576:
            break
size = os.path.getsize(output)
print(f'    {output} ({size:,} bytes)')
"
done

echo "  All files downloaded to ${LOCAL_DIR}"
ls -lh "${LOCAL_DIR}/"

# ── Step 2: Get Azure File Share details from K8s PVCs ──
echo "[2/5] Resolving Azure File Share from K8s PVCs..."

# Get AKS credentials
az aks get-credentials --resource-group "${AKS_RESOURCE_GROUP}" \
    --name "${AKS_CLUSTER_NAME}" --overwrite-existing 2>/dev/null

# Model PVC → PV → Azure File Share
MODEL_PVC="${ENV_PREFIX}-model-pvc"
[ "${TARGET_ENV}" = "prod" ] && MODEL_PVC="prod-model-pvc"
MODEL_PV=$(kubectl get pvc "${MODEL_PVC}" -n "${NAMESPACE}" -o jsonpath='{.spec.volumeName}')
MODEL_SHARE=$(kubectl get pv "${MODEL_PV}" -o jsonpath='{.spec.csi.volumeHandle}' | cut -d'#' -f3)

# Scores PVC → PV → Azure File Share
SCORES_PVC="${ENV_PREFIX}-scores-pvc"
[ "${TARGET_ENV}" = "prod" ] && SCORES_PVC="prod-scores-pvc"
SCORES_PV=$(kubectl get pvc "${SCORES_PVC}" -n "${NAMESPACE}" -o jsonpath='{.spec.volumeName}')
SCORES_SHARE=$(kubectl get pv "${SCORES_PV}" -o jsonpath='{.spec.csi.volumeHandle}' | cut -d'#' -f3)

# Storage account from the K8s secret (auto-created by CSI driver)
SA_SECRET=$(kubectl get secret -n "${NAMESPACE}" -o name | grep azure-storage-account | head -1)
SA_NAME=$(kubectl get "${SA_SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.azurestorageaccountname}' | base64 -d)
SA_KEY=$(kubectl get "${SA_SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.azurestorageaccountkey}' | base64 -d)

echo "  Storage account: ${SA_NAME}"
echo "  Model share: ${MODEL_SHARE}"
echo "  Scores share: ${SCORES_SHARE}"

# ── Step 3: Upload models to Azure File Share ──
echo "[3/5] Uploading models to Azure File Share..."

for file in "${MODEL_FILES[@]}"; do
    echo "  Uploading ${file}..."
    az storage file upload \
        --account-name "${SA_NAME}" \
        --account-key "${SA_KEY}" \
        --share-name "${MODEL_SHARE}" \
        --source "${LOCAL_DIR}/${file}" \
        --path "${file}" \
        --output none
done

echo "  Uploading ${SCORES_FILE}..."
az storage file upload \
    --account-name "${SA_NAME}" \
    --account-key "${SA_KEY}" \
    --share-name "${SCORES_SHARE}" \
    --source "${LOCAL_DIR}/${SCORES_FILE}" \
    --path "${SCORES_FILE}" \
    --output none

echo "  Upload complete"

# ── Step 4: Rolling restart API pods ──
echo "[4/5] Restarting API pods..."

API_DEPLOYMENT="${ENV_PREFIX}-churn-api"
[ "${TARGET_ENV}" = "prod" ] && API_DEPLOYMENT="prod-churn-api"
DASHBOARD_DEPLOYMENT="${ENV_PREFIX}-churn-dashboard"
[ "${TARGET_ENV}" = "prod" ] && DASHBOARD_DEPLOYMENT="prod-churn-dashboard"

kubectl rollout restart deployment "${API_DEPLOYMENT}" -n "${NAMESPACE}"
kubectl rollout restart deployment "${DASHBOARD_DEPLOYMENT}" -n "${NAMESPACE}"
echo "  Waiting for rollout..."
kubectl rollout status deployment "${API_DEPLOYMENT}" -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment "${DASHBOARD_DEPLOYMENT}" -n "${NAMESPACE}" --timeout=300s

echo "  Pods:"
kubectl get pods -n "${NAMESPACE}" -o wide

# ── Step 5: Verify readiness ──
echo "[5/5] Verifying API readiness..."

API_URL="${API_URL:-}"
if [ -z "${API_URL}" ]; then
    # Auto-detect from ingress
    INGRESS_HOST=$(kubectl get ingress -n "${NAMESPACE}" -o jsonpath='{.items[0].spec.rules[0].host}' 2>/dev/null || echo "")
    if [ -n "${INGRESS_HOST}" ]; then
        API_URL="https://${INGRESS_HOST}/api"
    fi
fi

if [ -n "${API_URL}" ]; then
    # Wait up to 60s for readiness
    for i in $(seq 1 12); do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/ready" 2>/dev/null || echo "000")
        if [ "${HTTP_CODE}" = "200" ]; then
            echo "  API ready (HTTP 200)"
            curl -s "${API_URL}/model/info" | python3 -m json.tool 2>/dev/null || true
            break
        fi
        echo "  Waiting for readiness... (attempt ${i}/12, HTTP ${HTTP_CODE})"
        sleep 5
    done

    if [ "${HTTP_CODE}" != "200" ]; then
        echo "  WARNING: API not ready after 60s (HTTP ${HTTP_CODE})"
        echo "  Pods may still be starting. Check: kubectl get pods -n ${NAMESPACE}"
        exit 1
    fi
else
    echo "  No API URL detected — skipping readiness check"
fi

# ── Cleanup ──
rm -rf "${LOCAL_DIR}"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Model sync complete for $(echo "${TARGET_ENV}" | tr '[:lower:]' '[:upper:]')"
echo "═══════════════════════════════════════════════════"
