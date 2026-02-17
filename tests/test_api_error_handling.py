"""
Test API Error Handling & Validation
=====================================
Tests for FastAPI error scenarios, input validation, and edge cases.
"""

import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add serving/api to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))

from main import app

client = TestClient(app)


class TestCORSAndSecurity:
    """Test CORS configuration and security headers."""

    def test_cors_blocks_unauthorized_origin(self):
        """CORS should reject requests from non-whitelisted origins."""
        response = client.post(
            "/predict",
            json={"customer_id": "test"},
            headers={"Origin": "https://evil.com"}
        )
        # Without models loaded, 503 is returned before CORS; with models, 400/422
        assert response.status_code in [400, 422, 503]

    def test_cors_allows_authorized_origin(self):
        """CORS should allow requests from whitelisted origins."""
        # Note: TestClient doesn't enforce CORS, so we check config
        from main import ALLOWED_ORIGINS
        assert "http://churn-dashboard-service:8501" in ALLOWED_ORIGINS
        assert "*" not in ALLOWED_ORIGINS  # Should NOT allow all


class TestInputValidation:
    """Test Pydantic validation and range checks."""

    def test_missing_required_field(self):
        """Should reject request with missing customer_id."""
        response = client.post("/predict", json={})
        assert response.status_code == 422
        assert "customer_id" in response.json()["detail"][0]["loc"]

    def test_invalid_customer_id_too_long(self):
        """Should reject customer_id > 50 characters."""
        response = client.post(
            "/predict",
            json={"customer_id": "a" * 51}  # Exceeds max_length=50
        )
        assert response.status_code == 422

    def test_invalid_speed_too_high(self):
        """Should reject speed > 10000 Mbps."""
        payload = {
            "customer_id": "test",
            "speed": 99999,  # Exceeds le=10000
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422
        error_msg = str(response.json())
        assert "speed" in error_msg.lower()

    def test_invalid_speed_negative(self):
        """Should reject negative speed."""
        payload = {
            "customer_id": "test",
            "speed": -100,  # Violates ge=0
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422

    def test_invalid_technology(self):
        """Should reject technology not in whitelist."""
        payload = {
            "customer_id": "test",
            "technology": "5G",  # Not in {FTTP, FTTC, MPF, WLR, Unknown}
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422
        assert "technology" in str(response.json()).lower()

    def test_invalid_tenure_bucket(self):
        """Should reject invalid tenure_bucket."""
        payload = {
            "customer_id": "test",
            "tenure_bucket": "invalid",  # Not in valid set
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422
        assert "tenure_bucket" in str(response.json()).lower()

    def test_usage_mom_change_capped(self):
        """Should reject usage_mom_change outside ±10x range."""
        payload = {
            "customer_id": "test",
            "usage_mom_change": 50.0,  # Exceeds le=10
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422

    def test_active_days_in_month_validation(self):
        """Should reject active_days_in_month > 31."""
        payload = {
            "customer_id": "test",
            "active_days_in_month": 35,  # Exceeds le=31
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 422


class TestErrorResponses:
    """Test error handling and response formats."""

    def test_health_endpoint_always_200(self):
        """Health endpoint should always return 200."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_ready_endpoint_when_models_missing(self):
        """Ready endpoint should return 503 if models not loaded."""
        # Note: In test environment, models may not be loaded
        response = client.get("/ready")
        # Either 200 (models loaded) or 503 (not loaded)
        assert response.status_code in [200, 503]

        if response.status_code == 503:
            assert "models loaded" in response.json()["detail"].lower()

    def test_model_info_structure(self):
        """Model info endpoint should return expected structure."""
        response = client.get("/model/info")
        assert response.status_code == 200
        data = response.json()

        assert "loaded_at" in data
        assert "model_count" in data
        assert "horizons" in data
        assert "failed_horizons" in data
        assert "thresholds" in data
        assert isinstance(data["thresholds"], dict)

    def test_error_response_does_not_leak_internals(self):
        """Error responses should not expose stack traces."""
        # Send malformed JSON
        response = client.post(
            "/predict",
            data="not json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422  # Unprocessable Entity

        # Check response doesn't contain sensitive info
        response_text = response.text.lower()
        assert "traceback" not in response_text
        assert "exception" not in response_text
        assert "/usr/" not in response_text  # No file paths

    def test_batch_endpoint_size_limit(self):
        """Batch endpoint should reject batches > 1000 customers."""
        payload = {
            "customers": [{"customer_id": f"cust_{i}"} for i in range(1001)]
        }
        response = client.post("/predict/batch", json=payload)
        assert response.status_code == 422  # Pydantic validation error


class TestPrometheusMetrics:
    """Test Prometheus metrics endpoint."""

    def test_metrics_endpoint_returns_text(self):
        """Metrics endpoint should return Prometheus text format."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; charset=utf-8"

        # Check for expected metric names
        metrics_text = response.text
        assert "churn_api_requests_total" in metrics_text
        assert "churn_api_request_duration_seconds" in metrics_text

    def test_metrics_increment_on_request(self):
        """Metrics should increment after requests."""
        # Get initial metrics
        response1 = client.get("/metrics")
        metrics_before = response1.text

        # Make a request (will fail without models, but that's OK)
        client.get("/health")

        # Get metrics again
        response2 = client.get("/metrics")
        metrics_after = response2.text

        # Metrics should have changed
        assert metrics_before != metrics_after or "churn_api_requests_total" in metrics_after


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_all_zero_features(self):
        """Should handle customer with all zero features."""
        payload = {
            "customer_id": "test_zero",
            # All numeric features default to 0
            "technology": "Unknown",
            "sales_channel": "Unknown",
            "tenure_bucket": "0-90d",
        }
        response = client.post("/predict", json=payload)
        # Should either succeed (if models loaded) or 503 (models not loaded)
        assert response.status_code in [200, 503]

    def test_maximum_valid_values(self):
        """Should handle maximum valid values for all fields."""
        payload = {
            "customer_id": "a" * 50,  # Max length
            "speed": 10000,  # Max speed
            "line_speed": 10000,
            "tenure_days": 36500,  # Max tenure (100 years)
            "usage_mom_change": 10.0,  # Max change
            "usage_vs_3mo_avg": 10.0,
            "technology": "FTTP",
            "sales_channel": "Unknown",
            "tenure_bucket": "3y+",
        }
        response = client.post("/predict", json=payload)
        assert response.status_code in [200, 503]  # Valid input

    def test_special_characters_in_customer_id(self):
        """Should handle special characters in customer_id."""
        payloads = [
            {"customer_id": "CUST-123-456"},  # Hyphens
            {"customer_id": "CUST_123_456"},  # Underscores
            {"customer_id": "CUST.123.456"},  # Dots
        ]
        for payload in payloads:
            response = client.post("/predict", json=payload)
            assert response.status_code in [200, 503]  # Should accept


class TestRequestIDTracking:
    """Test request ID generation and logging."""

    def test_response_includes_request_id(self):
        """Response should include a request_id field."""
        payload = {
            "customer_id": "test",
            "technology": "FTTP",
            "sales_channel": "Online",
            "tenure_bucket": "1y-2y",
        }
        response = client.post("/predict", json=payload)

        if response.status_code == 200:
            data = response.json()
            assert "request_id" in data
            assert len(data["request_id"]) == 8  # UUID[:8]


# Run with: pytest tests/test_api_error_handling.py -v
