#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Setup Terraform Remote State Backend (one-time bootstrap)
# ══════════════════════════════════════════════════════════════
# Run this ONCE before any terraform init / pipeline execution.
# Creates the Azure Storage Account that holds Terraform remote state.
#
# Creates:
#   - Resource Group:    telco-churn-tfstate
#   - Storage Account:   telcochurntfstate
#   - Blob Container:    tfstate
#   - Versioning + 14-day soft-delete enabled
#
# Prerequisites: az CLI installed and logged in (az login)
# Usage: ./deploy/setup_terraform_backend.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

RESOURCE_GROUP="telco-churn-tfstate"
STORAGE_ACCOUNT="telcochurntfstate"
CONTAINER_NAME="tfstate"
LOCATION="uksouth"

echo "══════════════════════════════════════════════════════════════"
echo " Terraform State Backend Bootstrap"
echo "══════════════════════════════════════════════════════════════"
echo ""

# ── Step 0: Verify Azure CLI login ──
echo "[0/6] Verifying Azure CLI login..."
if ! az account show --query "name" -o tsv > /dev/null 2>&1; then
    echo "ERROR: Not logged in to Azure. Run 'az login' first."
    exit 1
fi
SUBSCRIPTION=$(az account show --query "name" -o tsv)
echo "       Active subscription: ${SUBSCRIPTION}"
echo ""

# ── Step 1: Create resource group ──
echo "[1/6] Creating resource group: ${RESOURCE_GROUP}..."
az group create \
    --name "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --output none
echo "       Done."

# ── Step 2: Create storage account ──
echo "[2/6] Creating storage account: ${STORAGE_ACCOUNT}..."
az storage account create \
    --name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --output none
echo "       Done."

# ── Step 3: Create blob container ──
echo "[3/6] Creating blob container: ${CONTAINER_NAME}..."
az storage container create \
    --name "${CONTAINER_NAME}" \
    --account-name "${STORAGE_ACCOUNT}" \
    --output none
echo "       Done."

# ── Step 4: Enable blob versioning ──
echo "[4/6] Enabling blob versioning..."
az storage account blob-service-properties update \
    --account-name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --enable-versioning true \
    --output none
echo "       Done."

# ── Step 5: Enable soft-delete (14 days) ──
echo "[5/6] Enabling soft-delete (14-day retention)..."
az storage account blob-service-properties update \
    --account-name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --enable-delete-retention true \
    --delete-retention-days 14 \
    --output none
echo "       Done."

# ── Step 6: Retrieve access key ──
echo "[6/6] Retrieving storage access key..."
ACCESS_KEY=$(az storage account keys list \
    --account-name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --query '[0].value' \
    -o tsv)

echo ""
echo "══════════════════════════════════════════════════════════════"
echo " Bootstrap Complete"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo " Resource Group:    ${RESOURCE_GROUP}"
echo " Storage Account:   ${STORAGE_ACCOUNT}"
echo " Container:         ${CONTAINER_NAME}"
echo " Location:          ${LOCATION}"
echo ""
echo " Access Key:"
echo "   ${ACCESS_KEY}"
echo ""
echo " NEXT STEPS:"
echo "   1. Store this key as TF_STATE_ACCESS_KEY in these ADO variable groups:"
echo "      - azure-infra-dev"
echo "      - azure-infra-staging"
echo "      - azure-infra-prod"
echo ""
echo "   2. Mark TF_STATE_ACCESS_KEY as a secret variable in each group."
echo ""
echo "   3. You can now run: terraform init -backend-config=environments/dev/backend.tfvars"
echo "══════════════════════════════════════════════════════════════"
