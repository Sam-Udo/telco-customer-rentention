#!/usr/bin/env bash
# Configure custom domain for AKS deployment
# Usage: ./configure-domain.sh <domain> <email>

set -euo pipefail

DOMAIN="${1:-}"
EMAIL="${2:-}"

if [[ -z "$DOMAIN" || -z "$EMAIL" ]]; then
    echo "Usage: ./configure-domain.sh <domain> <email>"
    echo "Example: ./configure-domain.sh churn-demo.xyz admin@churn-demo.xyz"
    exit 1
fi

echo "Configuring domain: $DOMAIN"
echo "Let's Encrypt email: $EMAIL"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_DIR="$BASE_DIR/k8s"

echo "Updating k8s manifests..."
find "$K8S_DIR" -type f \( -name "*.yaml" -o -name "*.yml" \) -exec sed -i.bak \
    -e "s|DOMAIN_PLACEHOLDER|$DOMAIN|g" \
    -e "s|PROD_DOMAIN_PLACEHOLDER|$DOMAIN|g" \
    -e "s|LETSENCRYPT_EMAIL_PLACEHOLDER|$EMAIL|g" \
    {} \;

find "$K8S_DIR" -type f -name "*.bak" -delete

echo "Domain configuration complete!"
echo ""
echo "Next steps:"
echo "1. Register domain: $DOMAIN (e.g., from Namecheap ~\$1.50/year for .xyz)"
echo "2. Deploy Azure infrastructure: cd infrastructure && terraform apply"
echo "3. Get AKS ingress IP: kubectl get svc -n ingress-nginx ingress-nginx-controller"
echo "4. Create Azure DNS A record pointing $DOMAIN to ingress IP"
echo "5. Update domain nameservers to Azure DNS nameservers"
echo "6. Deploy k8s manifests: kubectl apply -k k8s/overlays/prod"
echo "7. Verify TLS certificate: kubectl get certificate -n telco-churn"
echo ""
echo "Wait 5-10 minutes for Let's Encrypt to issue certificate, then access:"
echo "  API: https://$DOMAIN/api/docs"
echo "  Dashboard: https://$DOMAIN"
