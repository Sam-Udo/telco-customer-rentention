#!/usr/bin/env bash
# ============================================================
# Azure DevOps Pipeline YAML Validation — parse check + structure
# Requires: python3 + pyyaml
# Usage: bash tests/validate_pipelines.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="${SCRIPT_DIR}/../.azure-devops"
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
echo " Pipeline YAML Validation"
echo "============================================"

if ! python3 -c "import yaml" &> /dev/null; then
    echo "SKIP: python3 pyyaml not available"
    exit 0
fi

echo ""

# 1-4. All pipeline YAMLs parse correctly
PIPELINES=("ci-pipeline.yml" "cd-pipeline.yml" "infra-pipeline.yml" "aks-pipeline.yml")
for pipeline in "${PIPELINES[@]}"; do
    check "${pipeline} parses as valid YAML" \
        python3 -c "import yaml; yaml.safe_load(open('${PIPELINE_DIR}/${pipeline}'))"
done

# 5. ci-pipeline.yml has 'stages' key
check "ci-pipeline.yml has 'stages' key" \
    python3 -c "
import yaml, sys
with open('${PIPELINE_DIR}/ci-pipeline.yml') as f:
    data = yaml.safe_load(f)
assert 'stages' in data, 'missing stages key'
"

echo ""
echo "============================================"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "============================================"

[ "${FAIL}" -eq 0 ] && exit 0 || exit 1
