#!/usr/bin/env bash
# Pre-deployment validation checklist
# Run this before deploying to production

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

check_pass() {
    echo -e "${GREEN}✓${NC} $1"
    PASS=$((PASS + 1))
    return 0
}

check_fail() {
    echo -e "${RED}✗${NC} $1"
    FAIL=$((FAIL + 1))
    return 0
}

check_warn() {
    echo -e "${YELLOW}!${NC} $1"
    WARN=$((WARN + 1))
    return 0
}

echo "Pre-Deployment Checklist"
echo "========================"
echo ""

echo "1. Tool Prerequisites"
echo "---------------------"
command -v az &>/dev/null && check_pass "Azure CLI installed" || check_fail "Azure CLI not found"
command -v kubectl &>/dev/null && check_pass "kubectl installed" || check_fail "kubectl not found"
command -v terraform &>/dev/null && check_pass "Terraform installed" || check_fail "Terraform not found"
command -v helm &>/dev/null && check_pass "Helm installed" || check_fail "Helm not found"
command -v docker &>/dev/null && check_pass "Docker installed" || check_fail "Docker not found"
echo ""

echo "2. Azure Authentication"
echo "----------------------"
if az account show &>/dev/null; then
    SUBSCRIPTION=$(az account show --query name -o tsv)
    check_pass "Logged into Azure: $SUBSCRIPTION"
else
    check_fail "Not logged into Azure (run: az login)"
fi
echo ""

echo "3. Domain Configuration"
echo "----------------------"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if grep -r "DOMAIN_PLACEHOLDER" "$BASE_DIR/k8s" &>/dev/null; then
    check_fail "Domain placeholders not configured (run: ./deploy/configure-domain.sh)"
else
    DOMAIN=$(grep -r "host:" "$BASE_DIR/k8s/base/ingress.yaml" | head -1 | awk '{print $3}')
    check_pass "Domain configured: $DOMAIN"
fi

if grep -r "LETSENCRYPT_EMAIL_PLACEHOLDER" "$BASE_DIR/k8s" &>/dev/null; then
    check_fail "Let's Encrypt email not configured"
else
    EMAIL=$(grep -r "email:" "$BASE_DIR/k8s/base/cert-manager.yaml" | head -1 | awk '{print $2}')
    check_pass "Let's Encrypt email: $EMAIL"
fi
echo ""

echo "4. Infrastructure State"
echo "----------------------"
if [[ -f "$BASE_DIR/infrastructure/terraform.tfstate" ]] || \
   az storage account list --query "[?contains(name, 'tfstate')]" -o tsv &>/dev/null; then
    check_pass "Terraform state exists"
else
    check_warn "Terraform state not found (first deployment)"
fi
echo ""

echo "5. Kubernetes Configuration"
echo "--------------------------"
if grep -r "MANAGED_IDENTITY_CLIENT_ID_PLACEHOLDER" "$BASE_DIR/k8s" &>/dev/null; then
    check_warn "Managed identity client IDs are placeholders (will be updated by Terraform)"
fi

ACR=$(grep "newName:" "$BASE_DIR/k8s/overlays/prod/kustomization.yaml" | head -1 | awk '{print $2}' | cut -d'/' -f1 2>/dev/null || echo "")
if [[ -n "$ACR" && "$ACR" != "ACR_PLACEHOLDER" ]]; then
    check_pass "ACR configured: $ACR"
else
    check_fail "ACR not configured in prod overlay"
fi
echo ""

echo "6. Security Validation"
echo "---------------------"
if grep -q "runAsNonRoot: true" "$BASE_DIR/k8s/base/"*.yaml; then
    check_pass "Pod security contexts configured"
else
    check_fail "Missing pod security contexts"
fi

if [[ -f "$BASE_DIR/k8s/base/network-policies.yaml" ]]; then
    check_pass "Network policies defined"
else
    check_fail "Network policies missing"
fi

if [[ -f "$BASE_DIR/k8s/base/pdb.yaml" ]]; then
    check_pass "Pod disruption budgets configured"
else
    check_warn "No pod disruption budgets (optional for demo)"
fi
echo ""

echo "7. Observability"
echo "---------------"
if grep -q "prometheus.io/scrape" "$BASE_DIR/k8s/base/"*.yaml; then
    check_pass "Prometheus metrics annotations present"
else
    check_warn "No Prometheus annotations"
fi
echo ""

echo "8. Data Readiness"
echo "----------------"
if [[ -f "$BASE_DIR/customer_info.parquet" ]]; then
    SIZE=$(du -h "$BASE_DIR/customer_info.parquet" | cut -f1)
    check_pass "customer_info.parquet exists ($SIZE)"
else
    check_fail "customer_info.parquet not found"
fi

if [[ -f "$BASE_DIR/usage.parquet" ]]; then
    SIZE=$(du -h "$BASE_DIR/usage.parquet" | cut -f1)
    check_pass "usage.parquet exists ($SIZE)"
else
    check_fail "usage.parquet not found"
fi

if [[ -f "$BASE_DIR/calls.csv" ]] && [[ -f "$BASE_DIR/cease.csv" ]]; then
    check_pass "calls.csv and cease.csv exist"
else
    check_fail "CSV files missing"
fi
echo ""

echo "Summary"
echo "======="
echo -e "${GREEN}PASS:${NC} $PASS"
[[ $WARN -gt 0 ]] && echo -e "${YELLOW}WARN:${NC} $WARN"
[[ $FAIL -gt 0 ]] && echo -e "${RED}FAIL:${NC} $FAIL"
echo ""

if [[ $FAIL -eq 0 ]]; then
    echo -e "${GREEN}System ready for deployment!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Register your domain"
    echo "2. Run: ./deploy/configure-domain.sh <domain> <email>"
    echo "3. Run: cd infrastructure && terraform init && terraform apply"
    echo "4. Configure DNS A record"
    echo "5. Deploy AKS: kubectl apply -k k8s/overlays/prod"
    exit 0
else
    echo -e "${RED}Fix the failures above before deploying${NC}"
    exit 1
fi
