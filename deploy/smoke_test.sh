#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# Smoke Test — API Endpoint Validation
# ══════════════════════════════════════════════════════════════
# Standalone smoke test for the Churn Prediction API.
# Extracted from aks-pipeline.yml for manual / local use.
#
# Usage: ./deploy/smoke_test.sh <API_URL>
# Example: ./deploy/smoke_test.sh http://localhost:8000
#          ./deploy/smoke_test.sh https://churn.telco.internal/api
# ══════════════════════════════════════════════════════════════

set -euo pipefail

API_URL="${1:?Usage: $0 <API_URL>}"
# Strip trailing slash
API_URL="${API_URL%/}"

PASS=0
FAIL=0
TOTAL=6

echo "══════════════════════════════════════════════════════════════"
echo " Smoke Test — Churn Prediction API"
echo "══════════════════════════════════════════════════════════════"
echo " Target: ${API_URL}"
echo ""

test_pass() { echo "  [PASS] $1"; PASS=$((PASS + 1)); }
test_fail() { echo "  [FAIL] $1 — $2"; FAIL=$((FAIL + 1)); }

# ── Test 1: Health endpoint ──
echo "── Test 1/6: GET /health ──"
HTTP_CODE=$(curl -s -o /tmp/smoke_health.json -w "%{http_code}" "${API_URL}/health" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    test_pass "Health check returned 200"
else
    test_fail "Health check" "HTTP ${HTTP_CODE}"
fi

# ── Test 2: Readiness endpoint ──
echo "── Test 2/6: GET /ready ──"
HTTP_CODE=$(curl -s -o /tmp/smoke_ready.json -w "%{http_code}" "${API_URL}/ready" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    test_pass "Readiness check returned 200"
else
    test_fail "Readiness check" "HTTP ${HTTP_CODE} (models may not be loaded)"
fi

# ── Test 3: Model info ──
echo "── Test 3/6: GET /model/info ──"
HTTP_CODE=$(curl -s -o /tmp/smoke_modelinfo.json -w "%{http_code}" "${API_URL}/model/info" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    if python3 -c "
import json, sys
data = json.load(open('/tmp/smoke_modelinfo.json'))
assert 'model_count' in data, 'missing model_count'
assert 'horizons' in data, 'missing horizons'
assert 'thresholds' in data, 'missing thresholds'
" 2>/dev/null; then
        test_pass "Model info — valid structure"
    else
        test_fail "Model info" "invalid JSON structure"
    fi
else
    test_fail "Model info" "HTTP ${HTTP_CODE}"
fi

# ── Test 4: Single prediction ──
echo "── Test 4/6: POST /predict ──"
PREDICT_PAYLOAD='{
  "customer_id": "SMOKE-TEST-001",
  "contract_status_ord": 3,
  "ooc_days": 30,
  "tenure_days": 365,
  "calls_30d": 2,
  "calls_90d": 5,
  "loyalty_calls_90d": 1,
  "speed": 80,
  "line_speed": 65,
  "monthly_total_mb": 50000,
  "technology": "FTTP",
  "sales_channel": "Online",
  "tenure_bucket": "90d-1y"
}'
HTTP_CODE=$(curl -s -o /tmp/smoke_predict.json -w "%{http_code}" \
    -X POST "${API_URL}/predict" \
    -H "Content-Type: application/json" \
    -d "$PREDICT_PAYLOAD" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    if python3 -c "
import json, sys
data = json.load(open('/tmp/smoke_predict.json'))
assert 'risk_tier' in data, 'missing risk_tier'
assert 'predictions' in data, 'missing predictions'
assert len(data['predictions']) == 3, f'expected 3 horizons, got {len(data[\"predictions\"])}'
assert data['risk_tier'] in ('RED', 'AMBER', 'YELLOW', 'GREEN'), f'invalid tier: {data[\"risk_tier\"]}'
for p in data['predictions']:
    assert 'horizon_days' in p and 'churn_probability' in p
" 2>/dev/null; then
        TIER=$(python3 -c "import json; print(json.load(open('/tmp/smoke_predict.json'))['risk_tier'])")
        test_pass "Single prediction — tier=${TIER}, 3 horizons"
    else
        test_fail "Single prediction" "invalid response structure"
    fi
else
    test_fail "Single prediction" "HTTP ${HTTP_CODE}"
fi

# ── Test 5: Batch prediction ──
echo "── Test 5/6: POST /predict/batch ──"
BATCH_PAYLOAD="{\"customers\": [${PREDICT_PAYLOAD}, ${PREDICT_PAYLOAD}]}"
# Update second customer_id
BATCH_PAYLOAD=$(echo "$BATCH_PAYLOAD" | python3 -c "
import json, sys
data = json.load(sys.stdin)
data['customers'][1]['customer_id'] = 'SMOKE-TEST-002'
print(json.dumps(data))
" 2>/dev/null || echo "$BATCH_PAYLOAD")

HTTP_CODE=$(curl -s -o /tmp/smoke_batch.json -w "%{http_code}" \
    -X POST "${API_URL}/predict/batch" \
    -H "Content-Type: application/json" \
    -d "$BATCH_PAYLOAD" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    if python3 -c "
import json, sys
data = json.load(open('/tmp/smoke_batch.json'))
assert data['batch_size'] == 2, f'expected batch_size=2, got {data[\"batch_size\"]}'
assert len(data['predictions']) == 2, f'expected 2 predictions'
" 2>/dev/null; then
        test_pass "Batch prediction — 2 customers scored"
    else
        test_fail "Batch prediction" "invalid response"
    fi
else
    test_fail "Batch prediction" "HTTP ${HTTP_CODE}"
fi

# ── Test 6: Metrics endpoint ──
echo "── Test 6/6: GET /metrics ──"
HTTP_CODE=$(curl -s -o /tmp/smoke_metrics.txt -w "%{http_code}" "${API_URL}/metrics" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    if grep -q "churn_api_requests_total" /tmp/smoke_metrics.txt 2>/dev/null; then
        test_pass "Prometheus metrics — format valid"
    else
        test_fail "Metrics" "missing churn_api_requests_total in output"
    fi
else
    test_fail "Metrics" "HTTP ${HTTP_CODE}"
fi

# ── Summary ──
echo ""
echo "══════════════════════════════════════════════════════════════"
echo " Results: ${PASS}/${TOTAL} passed, ${FAIL} failed"
echo "══════════════════════════════════════════════════════════════"

# Cleanup
rm -f /tmp/smoke_health.json /tmp/smoke_ready.json /tmp/smoke_modelinfo.json \
      /tmp/smoke_predict.json /tmp/smoke_batch.json /tmp/smoke_metrics.txt

[ "${FAIL}" -eq 0 ] && exit 0 || exit 1
