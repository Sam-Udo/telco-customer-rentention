#!/usr/bin/env bash
# ============================================================
# Terraform Validation — init, validate, fmt check
# Requires: terraform >= 1.5
# Usage: bash tests/validate_terraform.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/../infrastructure"
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
echo " Terraform Validation"
echo "============================================"

if ! command -v terraform &> /dev/null; then
    echo "SKIP: terraform CLI not found on PATH"
    exit 0
fi

echo ""
echo "Terraform version: $(terraform version -json 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin)["terraform_version"])' 2>/dev/null || terraform version | head -1)"
echo ""

# 1. terraform init (no backend)
check "terraform init -backend=false" \
    terraform -chdir="${INFRA_DIR}" init -backend=false -input=false

# 2. terraform validate
check "terraform validate" \
    terraform -chdir="${INFRA_DIR}" validate

# 3. terraform fmt -check -recursive
check "terraform fmt -check -recursive" \
    terraform -chdir="${INFRA_DIR}" fmt -check -recursive

echo ""
echo "============================================"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "============================================"

[ "${FAIL}" -eq 0 ] && exit 0 || exit 1
