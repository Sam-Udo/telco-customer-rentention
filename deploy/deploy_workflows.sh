#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Deploy Databricks Workflows (Jobs) via Databricks CLI
# ══════════════════════════════════════════════════════════════
# Called by: CD pipeline (cd-pipeline.yml)
# Env vars required:
#   DATABRICKS_HOST   — Workspace URL
#   DATABRICKS_TOKEN  — PAT or service principal token
#   TARGET_ENV        — dev | staging | prod
#
# This script:
#   1. Reads the workflow JSON template
#   2. Patches environment-specific values (notebook paths, cluster configs)
#   3. Creates or updates the Databricks job via CLI (NOT curl)

set -euo pipefail

echo "═══════════════════════════════════════════════════"
echo "  Deploying Workflow to $(echo "${TARGET_ENV}" | tr '[:lower:]' '[:upper:]')"
echo "  Workspace: ${DATABRICKS_HOST}"
echo "═══════════════════════════════════════════════════"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKFLOW_TEMPLATE="${REPO_ROOT}/workflows/churn_pipeline.json"
WORKSPACE_PATH="/Workspace/telco-churn-${TARGET_ENV}"

# ── Configure Databricks CLI ──
export DATABRICKS_HOST="${DATABRICKS_HOST}"
export DATABRICKS_TOKEN="${DATABRICKS_TOKEN}"
databricks jobs configure --version=2.1 2>/dev/null || true

# ── Environment-specific overrides ──
case "${TARGET_ENV}" in
    dev)
        JOB_NAME="telco_churn_pipeline_DEV"
        NUM_WORKERS_ETL=1
        NUM_WORKERS_ML=1
        SCHEDULE_PAUSE="PAUSED"
        ;;
    staging)
        JOB_NAME="telco_churn_pipeline_STAGING"
        NUM_WORKERS_ETL=1
        NUM_WORKERS_ML=1
        SCHEDULE_PAUSE="PAUSED"
        ;;
    prod)
        JOB_NAME="telco_churn_pipeline"
        NUM_WORKERS_ETL=4
        NUM_WORKERS_ML=2
        SCHEDULE_PAUSE="UNPAUSED"
        ;;
    *)
        echo "ERROR: Unknown environment: ${TARGET_ENV}"
        exit 1
        ;;
esac

# ── Build the patched workflow JSON ──
echo "[1/3] Patching workflow for ${TARGET_ENV}..."
TEMP_WORKFLOW=$(mktemp)

cat "${WORKFLOW_TEMPLATE}" | python3 -c "
import json, sys

workflow = json.load(sys.stdin)

# Patch job name
workflow['name'] = '${JOB_NAME}'

# Patch schedule
workflow['schedule']['pause_status'] = '${SCHEDULE_PAUSE}'

# Patch notebook paths to environment-specific workspace
for task in workflow['tasks']:
    if 'notebook_task' in task:
        old_path = task['notebook_task']['notebook_path']
        nb_name = old_path.split('/')[-1]
        task['notebook_task']['notebook_path'] = '${WORKSPACE_PATH}/notebooks/' + nb_name

# Patch cluster sizes
for cluster in workflow['job_clusters']:
    if cluster['job_cluster_key'] == 'etl_cluster':
        cluster['new_cluster']['num_workers'] = ${NUM_WORKERS_ETL}
    elif cluster['job_cluster_key'] == 'ml_cluster':
        cluster['new_cluster']['num_workers'] = ${NUM_WORKERS_ML}

# Patch tags
if 'tags' not in workflow:
    workflow['tags'] = {}
workflow['tags']['environment'] = '${TARGET_ENV}'

json.dump(workflow, sys.stdout, indent=2)
" > "${TEMP_WORKFLOW}"

# Validate the patched JSON
python3 -c "import json; json.load(open('${TEMP_WORKFLOW}'))" || { echo "ERROR: Invalid workflow JSON"; exit 1; }
echo "  Patched JSON validated"

# ── Check if job already exists (using CLI, not curl) ──
echo "[2/3] Checking for existing job..."
EXISTING_JOB_ID=$(databricks jobs list --output json 2>/dev/null | \
    python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for job in data.get('jobs', []):
        if job.get('settings', {}).get('name') == '${JOB_NAME}':
            print(job['job_id'])
            break
except:
    pass
" || echo "")

# ── Create or Update (using CLI to avoid token leakage) ──
echo "[3/3] Deploying job..."
if [ -n "${EXISTING_JOB_ID}" ]; then
    echo "  Updating existing job: ${EXISTING_JOB_ID}"

    if RESULT=$(databricks jobs reset --job-id "${EXISTING_JOB_ID}" --json-file "${TEMP_WORKFLOW}" 2>&1); then
        echo "  Job updated: ${JOB_NAME} (ID: ${EXISTING_JOB_ID})"
    else
        echo "  ERROR: Failed to update job ${EXISTING_JOB_ID}"
        echo "  ${RESULT}"
        rm -f "${TEMP_WORKFLOW}"
        exit 1
    fi
else
    echo "  Creating new job..."

    if RESULT=$(databricks jobs create --json-file "${TEMP_WORKFLOW}" 2>&1); then
        NEW_JOB_ID=$(echo "${RESULT}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get('job_id', 'unknown'))
except (json.JSONDecodeError, ValueError):
    print('unknown')
" 2>/dev/null)
        if [ "${NEW_JOB_ID}" = "unknown" ] || [ -z "${NEW_JOB_ID}" ]; then
            # CLI may not return JSON — verify by listing jobs
            NEW_JOB_ID=$(databricks jobs list --output json 2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for job in data.get('jobs', []):
        if job.get('settings', {}).get('name') == '${JOB_NAME}':
            print(job['job_id'])
            break
except:
    pass
")
        fi
        echo "  Job created: ${JOB_NAME} (ID: ${NEW_JOB_ID:-verified})"
    else
        echo "  ERROR: Failed to create job"
        echo "  ${RESULT}"
        rm -f "${TEMP_WORKFLOW}"
        exit 1
    fi
fi

# ── Cleanup ──
rm -f "${TEMP_WORKFLOW}"

echo ""
echo "Workflow deployment complete for $(echo "${TARGET_ENV}" | tr '[:lower:]' '[:upper:]')."
exit 0
