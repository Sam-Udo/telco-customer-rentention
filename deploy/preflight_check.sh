#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Pre-Deployment Preflight Check
# ══════════════════════════════════════════════════════════════
# Validates that all required tools, credentials, and resources
# are in place before running any deployment pipeline.
#
# Usage: ./deploy/preflight_check.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

PASS=0
FAIL=0
WARN=0

echo "══════════════════════════════════════════════════════════════"
echo " Pre-Deployment Preflight Check"
echo "══════════════════════════════════════════════════════════════"
echo ""

check_pass() { echo "  [PASS] $1"; PASS=$((PASS + 1)); }
check_fail() { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }
check_warn() { echo "  [WARN] $1"; WARN=$((WARN + 1)); }

# ── Section 1: CLI Tools ──
echo "── CLI Tools ──"

REQUIRED_TOOLS=("az" "terraform" "kubectl" "docker" "python3" "jq")
OPTIONAL_TOOLS=("kustomize" "databricks" "helm")

for tool in "${REQUIRED_TOOLS[@]}"; do
    if command -v "$tool" &> /dev/null; then
        check_pass "$tool installed ($(command -v "$tool"))"
    else
        check_fail "$tool NOT FOUND — required for deployment"
    fi
done

for tool in "${OPTIONAL_TOOLS[@]}"; do
    if command -v "$tool" &> /dev/null; then
        check_pass "$tool installed"
    else
        check_warn "$tool not found — optional but recommended"
    fi
done
echo ""

# ── Section 2: Azure Authentication ──
echo "── Azure Authentication ──"

if az account show --query "id" -o tsv &> /dev/null; then
    SUB_NAME=$(az account show --query "name" -o tsv)
    SUB_ID=$(az account show --query "id" -o tsv)
    check_pass "Azure CLI logged in (subscription: ${SUB_NAME})"
    check_pass "Subscription ID: ${SUB_ID}"
else
    check_fail "Azure CLI not logged in — run 'az login'"
fi
echo ""

# ── Section 3: Terraform State Backend ──
echo "── Terraform State Backend ──"

if az storage container show \
    --name tfstate \
    --account-name telcochurntfstate \
    --query "name" -o tsv &> /dev/null 2>&1; then
    check_pass "Terraform state storage accessible (telcochurntfstate/tfstate)"
else
    check_fail "Terraform state storage NOT accessible — run deploy/setup_terraform_backend.sh"
fi
echo ""

# ── Section 4: Terraform Version ──
echo "── Terraform Version ──"

if command -v terraform &> /dev/null; then
    TF_VERSION=$(terraform version -json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['terraform_version'])" 2>/dev/null || echo "unknown")
    TF_MAJOR=$(echo "$TF_VERSION" | cut -d. -f1)
    TF_MINOR=$(echo "$TF_VERSION" | cut -d. -f2)
    if [ "$TF_MAJOR" -ge 1 ] && [ "$TF_MINOR" -ge 5 ] 2>/dev/null; then
        check_pass "Terraform version ${TF_VERSION} (>= 1.5 required)"
    else
        check_fail "Terraform version ${TF_VERSION} — need >= 1.5"
    fi
fi
echo ""

# ── Section 5: Docker Daemon ──
echo "── Docker Daemon ──"

if docker info &> /dev/null; then
    check_pass "Docker daemon running"
else
    check_warn "Docker daemon not running — needed for container builds"
fi
echo ""

# ── Section 6: Resource Quotas (if logged in) ──
echo "── Azure Resource Quotas (uksouth) ──"

if az account show &> /dev/null 2>&1; then
    DSV2_USAGE=$(az vm list-usage --location uksouth \
        --query "[?contains(localName, 'Standard DSv2 Family')].{current:currentValue, limit:limit}" \
        -o tsv 2>/dev/null || echo "")
    if [ -n "$DSV2_USAGE" ]; then
        CURRENT=$(echo "$DSV2_USAGE" | awk '{print $1}')
        LIMIT=$(echo "$DSV2_USAGE" | awk '{print $2}')
        AVAILABLE=$((LIMIT - CURRENT))
        if [ "$AVAILABLE" -ge 20 ]; then
            check_pass "DSv2 vCPUs: ${CURRENT}/${LIMIT} used (${AVAILABLE} available)"
        else
            check_warn "DSv2 vCPUs: ${CURRENT}/${LIMIT} used — only ${AVAILABLE} available (20+ recommended)"
        fi
    else
        check_warn "Could not query VM quotas"
    fi
else
    check_warn "Skipping quota check — not logged in"
fi
echo ""

# ── Section 7: Service Principal (if configured) ──
echo "── Service Principal ──"

if [ -n "${ARM_CLIENT_ID:-}" ]; then
    if az ad sp show --id "$ARM_CLIENT_ID" &> /dev/null 2>&1; then
        check_pass "Service principal ${ARM_CLIENT_ID} is valid"
    else
        check_fail "Service principal ${ARM_CLIENT_ID} not found or expired"
    fi
else
    check_warn "ARM_CLIENT_ID not set — service principal not configured (OK for manual deployment)"
fi
echo ""

# ── Summary ──
echo "══════════════════════════════════════════════════════════════"
echo " Results: ${PASS} passed, ${FAIL} failed, ${WARN} warnings"
echo "══════════════════════════════════════════════════════════════"

if [ "${FAIL}" -gt 0 ]; then
    echo ""
    echo " ACTION REQUIRED: Fix the ${FAIL} failed check(s) above before deploying."
    exit 1
else
    echo ""
    echo " All critical checks passed. Ready to deploy."
    exit 0
fi
