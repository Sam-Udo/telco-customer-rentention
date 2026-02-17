#!/usr/bin/env bash
# ============================================================
# Kubernetes Manifest Validation — kustomize build + placeholder check
# Requires: kustomize (or kubectl with kustomize support)
# Usage: bash tests/validate_k8s.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../k8s"
PASS=0
FAIL=0

check() {
    local description="$1"
    shift
    echo -n "  [CHECK] ${description} ... "
    if "$@" > /dev/null 2>&1; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================"
echo " Kubernetes Manifest Validation"
echo "============================================"

# Determine kustomize command
KUSTOMIZE_CMD=""
if command -v kustomize &> /dev/null; then
    KUSTOMIZE_CMD="kustomize build"
    echo ""
    echo "Using: kustomize $(kustomize version 2>/dev/null | head -1)"
elif command -v kubectl &> /dev/null && kubectl kustomize --help &> /dev/null; then
    KUSTOMIZE_CMD="kubectl kustomize"
    echo ""
    echo "Using: kubectl kustomize"
else
    echo "SKIP: neither kustomize nor kubectl found on PATH"
    exit 0
fi

echo ""

# 1. Build base manifests
check "kustomize build k8s/base" \
    ${KUSTOMIZE_CMD} "${K8S_DIR}/base"

# 2-4. Build overlay manifests
for env in dev staging prod; do
    check "kustomize build k8s/overlays/${env}" \
        ${KUSTOMIZE_CMD} "${K8S_DIR}/overlays/${env}"
done

# 5. Verify no ACR_PLACEHOLDER remains after prod overlay build
check "no ACR_PLACEHOLDER in prod overlay" \
    bash -c "! ${KUSTOMIZE_CMD} '${K8S_DIR}/overlays/prod' 2>/dev/null | grep -q 'ACR_PLACEHOLDER'"

echo ""
echo "============================================"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "============================================"

[ "${FAIL}" -eq 0 ] && exit 0 || exit 1
