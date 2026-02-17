#!/usr/bin/env bash
# Production deployment orchestrator
# Complete deployment from infrastructure to live application

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

log_info() { echo -e "${BLUE}==>${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }
log_warn() { echo -e "${YELLOW}!${NC} $1"; }

DOMAIN="${1:-}"
EMAIL="${2:-}"
ENVIRONMENT="${3:-prod}"

if [[ -z "$DOMAIN" || -z "$EMAIL" ]]; then
    cat <<EOF
Production Deployment Script
============================

This script deploys the complete churn prediction platform with custom domain.

Prerequisites:
  - Domain registered (e.g., churn-demo.xyz from Namecheap)
  - Azure subscription with Owner/Contributor access
  - Tools: az, kubectl, terraform, helm, docker

Usage:
  ./deploy-production.sh <domain> <email> [environment]

Example:
  ./deploy-production.sh churn-demo.xyz admin@churn-demo.xyz prod

Steps performed:
  1. Pre-deployment validation
  2. Configure domain in k8s manifests
  3. Deploy Azure infrastructure (Terraform)
  4. Configure DNS nameservers (manual step)
  5. Get AKS credentials
  6. Install cert-manager
  7. Install NGINX ingress
  8. Create DNS A record
  9. Build and push Docker images
  10. Deploy k8s manifests
  11. Verify deployment

EOF
    exit 1
fi

log_info "Starting production deployment for $DOMAIN"
echo ""

# ═══════════════════════════════════════════
# STEP 1: Pre-deployment validation
# ═══════════════════════════════════════════
log_info "STEP 1/11: Pre-deployment validation"
if ! "$SCRIPT_DIR/pre-deployment-checklist.sh"; then
    log_error "Pre-deployment checks failed"
    exit 1
fi
log_success "Pre-deployment checks passed"
echo ""

# ═══════════════════════════════════════════
# STEP 2: Configure domain
# ═══════════════════════════════════════════
log_info "STEP 2/11: Configuring domain"
"$SCRIPT_DIR/configure-domain.sh" "$DOMAIN" "$EMAIL"
log_success "Domain configured: $DOMAIN"
echo ""

# ═══════════════════════════════════════════
# STEP 3: Deploy infrastructure
# ═══════════════════════════════════════════
log_info "STEP 3/11: Deploying Azure infrastructure"
cd "$PROJECT_DIR/infrastructure"

log_info "Initializing Terraform..."
terraform init -reconfigure -backend-config="environments/$ENVIRONMENT/backend.tfvars"

log_info "Planning infrastructure..."
terraform plan -var-file="environments/$ENVIRONMENT/terraform.tfvars" -out=tfplan

log_warn "Review the plan above. Press ENTER to apply or Ctrl+C to cancel."
read -r

log_info "Applying infrastructure (this takes ~15-20 minutes)..."
terraform apply tfplan

DNS_NAMESERVERS=$(terraform output -json dns_nameservers | jq -r '.[]')
ACR_NAME=$(terraform output -raw acr_name)
AKS_NAME=$(terraform output -raw aks_cluster_name)
AKS_RG=$(terraform output -raw aks_resource_group_name)

log_success "Infrastructure deployed"
echo ""

# ═══════════════════════════════════════════
# STEP 4: Configure nameservers (manual)
# ═══════════════════════════════════════════
log_info "STEP 4/11: Configure DNS nameservers"
echo ""
log_warn "MANUAL ACTION REQUIRED:"
echo ""
echo "Go to your domain registrar (e.g., Namecheap) and set custom nameservers to:"
echo ""
echo "$DNS_NAMESERVERS" | while read -r ns; do
    echo "  - $ns"
done
echo ""
log_warn "Press ENTER after updating nameservers (propagation takes 5-60 mins)"
read -r

log_info "Waiting for DNS propagation..."
RETRIES=0
MAX_RETRIES=12
while [[ $RETRIES -lt $MAX_RETRIES ]]; do
    if dig NS "$DOMAIN" +short | grep -q "azure-dns"; then
        log_success "DNS propagated successfully"
        break
    fi
    ((RETRIES++))
    log_info "Waiting for DNS propagation... ($RETRIES/$MAX_RETRIES)"
    sleep 30
done

if [[ $RETRIES -eq $MAX_RETRIES ]]; then
    log_warn "DNS not fully propagated yet, continuing anyway..."
fi
echo ""

# ═══════════════════════════════════════════
# STEP 5: Get AKS credentials
# ═══════════════════════════════════════════
log_info "STEP 5/11: Getting AKS credentials"
az aks get-credentials \
    --resource-group "$AKS_RG" \
    --name "$AKS_NAME" \
    --overwrite-existing
log_success "AKS credentials configured"
echo ""

# ═══════════════════════════════════════════
# STEP 6: Install cert-manager
# ═══════════════════════════════════════════
log_info "STEP 6/11: Installing cert-manager"
if ! kubectl get namespace cert-manager &>/dev/null; then
    helm repo add jetstack https://charts.jetstack.io
    helm repo update
    helm install cert-manager jetstack/cert-manager \
        --namespace cert-manager \
        --create-namespace \
        --version v1.14.0 \
        --set installCRDs=true
    log_info "Waiting for cert-manager pods..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s
else
    log_info "cert-manager already installed"
fi
log_success "cert-manager ready"
echo ""

# ═══════════════════════════════════════════
# STEP 7: Install NGINX ingress
# ═══════════════════════════════════════════
log_info "STEP 7/11: Installing NGINX ingress"
if ! kubectl get namespace ingress-nginx &>/dev/null; then
    helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
    helm repo update
    helm install ingress-nginx ingress-nginx/ingress-nginx \
        --namespace ingress-nginx \
        --create-namespace \
        --set controller.service.annotations."service\.beta\.kubernetes\.io/azure-load-balancer-health-probe-request-path"=/healthz
    log_info "Waiting for ingress external IP..."
    kubectl wait --for=jsonpath='{.status.loadBalancer.ingress[0].ip}' \
        service/ingress-nginx-controller -n ingress-nginx --timeout=300s
else
    log_info "NGINX ingress already installed"
fi

INGRESS_IP=$(kubectl get svc -n ingress-nginx ingress-nginx-controller \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
log_success "NGINX ingress ready at $INGRESS_IP"
echo ""

# ═══════════════════════════════════════════
# STEP 8: Create DNS A record
# ═══════════════════════════════════════════
log_info "STEP 8/11: Creating DNS A record"
DNS_RG="rg-telco-churn-networking-$ENVIRONMENT"

if ! az network dns record-set a show \
    --resource-group "$DNS_RG" \
    --zone-name "$DOMAIN" \
    --name "@" &>/dev/null; then

    az network dns record-set a add-record \
        --resource-group "$DNS_RG" \
        --zone-name "$DOMAIN" \
        --record-set-name "@" \
        --ipv4-address "$INGRESS_IP"
    log_success "DNS A record created: $DOMAIN -> $INGRESS_IP"
else
    log_info "DNS A record already exists"
fi

log_info "Verifying DNS resolution..."
sleep 10
if dig "$DOMAIN" +short | grep -q "$INGRESS_IP"; then
    log_success "DNS resolves correctly"
else
    log_warn "DNS not resolving yet (may take a few minutes)"
fi
echo ""

# ═══════════════════════════════════════════
# STEP 9: Build and push Docker images
# ═══════════════════════════════════════════
log_info "STEP 9/11: Building and pushing Docker images"
cd "$PROJECT_DIR/serving"

log_info "Logging into ACR..."
az acr login --name "$ACR_NAME"

log_info "Building API image..."
docker build -t "${ACR_NAME}.azurecr.io/churn-api:latest" -f api/Dockerfile .

log_info "Pushing API image..."
docker push "${ACR_NAME}.azurecr.io/churn-api:latest"

log_info "Building dashboard image..."
docker build -t "${ACR_NAME}.azurecr.io/churn-dashboard:latest" -f dashboard/Dockerfile .

log_info "Pushing dashboard image..."
docker push "${ACR_NAME}.azurecr.io/churn-dashboard:latest"

log_success "Docker images built and pushed"
echo ""

# ═══════════════════════════════════════════
# STEP 10: Deploy k8s manifests
# ═══════════════════════════════════════════
log_info "STEP 10/11: Deploying Kubernetes manifests"
cd "$PROJECT_DIR/k8s"

log_info "Applying production overlay..."
kubectl apply -k "overlays/$ENVIRONMENT/"

log_info "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod \
    -l app.kubernetes.io/part-of=telco-churn \
    -n telco-churn \
    --timeout=300s || log_warn "Some pods not ready yet"

log_success "Kubernetes manifests deployed"
echo ""

# ═══════════════════════════════════════════
# STEP 11: Verify deployment
# ═══════════════════════════════════════════
log_info "STEP 11/11: Verifying deployment"

log_info "Pod status:"
kubectl get pods -n telco-churn

log_info "Service status:"
kubectl get svc -n telco-churn

log_info "Ingress status:"
kubectl get ingress -n telco-churn

log_info "Certificate status:"
kubectl get certificate -n telco-churn

log_info "HPA status:"
kubectl get hpa -n telco-churn

echo ""
log_info "Waiting for TLS certificate (this can take 5-10 minutes)..."
CERT_RETRIES=0
CERT_MAX_RETRIES=20
while [[ $CERT_RETRIES -lt $CERT_MAX_RETRIES ]]; do
    CERT_READY=$(kubectl get certificate churn-tls-secret -n telco-churn -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "False")
    if [[ "$CERT_READY" == "True" ]]; then
        log_success "TLS certificate issued successfully"
        break
    fi
    ((CERT_RETRIES++))
    log_info "Waiting for certificate... ($CERT_RETRIES/$CERT_MAX_RETRIES)"
    sleep 30
done

if [[ $CERT_RETRIES -eq $CERT_MAX_RETRIES ]]; then
    log_warn "Certificate not issued yet. Check: kubectl describe certificate churn-tls-secret -n telco-churn"
fi

echo ""
log_info "Testing endpoints..."

# Test API health
if curl -sSf "https://$DOMAIN/api/health" &>/dev/null; then
    log_success "API health check passed"
else
    log_warn "API not responding yet (may need a few more minutes)"
fi

# Test dashboard
if curl -sSf "https://$DOMAIN" &>/dev/null; then
    log_success "Dashboard accessible"
else
    log_warn "Dashboard not responding yet (may need a few more minutes)"
fi

echo ""
log_success "═══════════════════════════════════════════"
log_success "  DEPLOYMENT COMPLETE!"
log_success "═══════════════════════════════════════════"
echo ""
echo "Access your application:"
echo ""
echo "  API Documentation:  https://$DOMAIN/api/docs"
echo "  Dashboard:          https://$DOMAIN"
echo "  Health Check:       https://$DOMAIN/api/health"
echo ""
echo "Next steps:"
echo ""
echo "  1. Upload data:     ./deploy/upload_data.sh $ENVIRONMENT"
echo "  2. Run DLT pipeline in Databricks"
echo "  3. Train models:    Run notebook 04_model_training.py"
echo "  4. Batch scoring:   Run notebook 05_batch_scoring.py"
echo "  5. Export models:   ./deploy/export_models_for_aks.py"
echo "  6. Test prediction: curl -X POST https://$DOMAIN/api/predict -H 'Content-Type: application/json' -d '{...}'"
echo ""
echo "Troubleshooting:"
echo ""
echo "  View logs:          kubectl logs -n telco-churn deployment/prod-churn-api"
echo "  Check certificate:  kubectl describe certificate churn-tls-secret -n telco-churn"
echo "  Check ingress:      kubectl describe ingress churn-ingress -n telco-churn"
echo ""
