#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Configure Azure DevOps — Project, Variables, Pipelines
# ══════════════════════════════════════════════════════════════
# Creates the complete ADO project configuration for telco-churn.
#
# Prerequisites:
#   - az CLI with devops extension: az extension add --name azure-devops
#   - Logged in: az login
#
# Env vars required:
#   AZURE_DEVOPS_ORG  — e.g. https://dev.azure.com/myorg
#   AZURE_DEVOPS_PAT  — Personal access token (Full access scope)
#
# Usage: ./deploy/configure_ado.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

AZURE_DEVOPS_ORG="${AZURE_DEVOPS_ORG:?Set AZURE_DEVOPS_ORG (e.g. https://dev.azure.com/myorg)}"
AZURE_DEVOPS_PAT="${AZURE_DEVOPS_PAT:?Set AZURE_DEVOPS_PAT (Personal Access Token)}"
PROJECT_NAME="telco-churn"

export AZURE_DEVOPS_EXT_PAT="${AZURE_DEVOPS_PAT}"

echo "══════════════════════════════════════════════════════════════"
echo " Azure DevOps Project Configuration"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo " Organization: ${AZURE_DEVOPS_ORG}"
echo " Project:      ${PROJECT_NAME}"
echo ""

# ── Step 1: Configure defaults ──
echo "[1/5] Configuring az devops defaults..."
az devops configure --defaults organization="${AZURE_DEVOPS_ORG}"

# ── Step 2: Create project ──
echo "[2/5] Creating project: ${PROJECT_NAME}..."
if az devops project show --project "${PROJECT_NAME}" &> /dev/null 2>&1; then
    echo "       Project already exists — skipping."
else
    az devops project create \
        --name "${PROJECT_NAME}" \
        --visibility private \
        --process Agile \
        --output none
    echo "       Created."
fi

az devops configure --defaults project="${PROJECT_NAME}"

# ── Step 3: Create variable groups ──
echo "[3/5] Creating variable groups..."

create_var_group() {
    local group_name="$1"
    shift
    echo "       Creating: ${group_name}..."

    # Build --variables argument
    local vars=""
    while [ $# -gt 0 ]; do
        vars="${vars} --variables $1"
        shift
    done

    eval az pipelines variable-group create \
        --name "\"${group_name}\"" \
        --authorize true \
        ${vars} \
        --output none 2>/dev/null || echo "       (may already exist)"
}

# Databricks groups
create_var_group "databricks-common" \
    "pythonVersion=3.11"

create_var_group "databricks-dev" \
    "DATABRICKS_HOST=https://adb-REPLACE.azuredatabricks.net" \
    "TARGET_ENV=dev" \
    "CATALOG_NAME=uk_telecoms_dev" \
    "LANDING_PATH=/mnt/landing-dev"

create_var_group "databricks-staging" \
    "DATABRICKS_HOST=https://adb-REPLACE.azuredatabricks.net" \
    "TARGET_ENV=staging" \
    "CATALOG_NAME=uk_telecoms_staging" \
    "LANDING_PATH=/mnt/landing-staging"

create_var_group "databricks-prod" \
    "DATABRICKS_HOST=https://adb-REPLACE.azuredatabricks.net" \
    "TARGET_ENV=prod" \
    "CATALOG_NAME=uk_telecoms" \
    "LANDING_PATH=/mnt/landing"

# AKS groups
create_var_group "aks-dev" \
    "AKS_SERVICE_CONNECTION=aks-dev-connection" \
    "ACR_SERVICE_CONNECTION=acr-dev-connection" \
    "DEV_API_URL=http://dev-churn-api.telco-churn.svc.cluster.local:8000"

create_var_group "aks-staging" \
    "AKS_SERVICE_CONNECTION=aks-staging-connection" \
    "ACR_SERVICE_CONNECTION=acr-staging-connection"

create_var_group "aks-prod" \
    "AKS_SERVICE_CONNECTION=aks-prod-connection" \
    "ACR_SERVICE_CONNECTION=acr-prod-connection"

# Infrastructure groups
create_var_group "azure-infra-dev" \
    "AZURE_SUBSCRIPTION_ID=REPLACE-WITH-SUBSCRIPTION-ID"

create_var_group "azure-infra-staging" \
    "AZURE_SUBSCRIPTION_ID=REPLACE-WITH-SUBSCRIPTION-ID"

create_var_group "azure-infra-prod" \
    "AZURE_SUBSCRIPTION_ID=REPLACE-WITH-SUBSCRIPTION-ID"

echo ""

# ── Step 4: Create pipelines ──
echo "[4/5] Creating pipeline definitions..."

PIPELINES=(
    "telco-churn-ci:.azure-devops/ci-pipeline.yml"
    "telco-churn-cd:.azure-devops/cd-pipeline.yml"
    "telco-churn-infra:.azure-devops/infra-pipeline.yml"
    "telco-churn-aks:.azure-devops/aks-pipeline.yml"
)

for entry in "${PIPELINES[@]}"; do
    NAME="${entry%%:*}"
    YAML_PATH="${entry##*:}"
    echo "       Creating pipeline: ${NAME} → ${YAML_PATH}"
    az pipelines create \
        --name "${NAME}" \
        --yml-path "${YAML_PATH}" \
        --repository-type tfsgit \
        --skip-first-run true \
        --output none 2>/dev/null || echo "       (may already exist)"
done
echo ""

# ── Step 5: Create environments ──
echo "[5/5] Creating environments with approval gates..."

ENVIRONMENTS=(
    "telco-churn-dev"
    "telco-churn-staging"
    "telco-churn-prod"
    "telco-churn-infra-dev"
    "telco-churn-infra-staging"
    "telco-churn-infra-prod"
    "telco-churn-aks-dev"
    "telco-churn-aks-staging"
    "telco-churn-aks-prod"
)

for env in "${ENVIRONMENTS[@]}"; do
    echo "       Creating environment: ${env}"
    az devops invoke \
        --area distributedtask \
        --resource environments \
        --route-parameters project="${PROJECT_NAME}" \
        --http-method POST \
        --api-version 7.1 \
        --in-file <(echo "{\"name\": \"${env}\"}") \
        --output none 2>/dev/null || echo "       (may already exist)"
done

echo ""
echo "══════════════════════════════════════════════════════════════"
echo " ADO Configuration Complete"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo " MANUAL STEPS REQUIRED:"
echo ""
echo " 1. Add secret variables to variable groups:"
echo "    - databricks-{dev,staging,prod}: DATABRICKS_TOKEN"
echo "    - azure-infra-{dev,staging,prod}: TF_STATE_ACCESS_KEY"
echo ""
echo " 2. Update placeholder values:"
echo "    - DATABRICKS_HOST in each databricks-* group"
echo "    - AZURE_SUBSCRIPTION_ID in each azure-infra-* group"
echo "    - Service connection names in each aks-* group"
echo ""
echo " 3. Add approval gates to production environments:"
echo "    - telco-churn-prod"
echo "    - telco-churn-infra-prod"
echo "    - telco-churn-aks-prod"
echo "    (Azure DevOps > Environments > telco-churn-prod > Approvals)"
echo ""
echo " 4. Create service connections:"
echo "    - Azure Resource Manager (for Terraform)"
echo "    - AKS (per environment)"
echo "    - ACR (per environment)"
echo "══════════════════════════════════════════════════════════════"
