"""
FastAPI endpoint integration tests using TestClient.
All LightGBM models are mocked — no real model files needed.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))

from main import app, models, model_metadata
from starlette.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _setup_models():
    """Pre-populate models dict for each test, clean up after."""
    mock_booster = MagicMock()
    mock_booster.predict.return_value = [0.65]

    models[30] = mock_booster
    models[60] = mock_booster
    models[90] = mock_booster
    model_metadata["loaded_at"] = "2024-01-01T00:00:00Z"
    model_metadata["model_count"] = "3"
    model_metadata["horizons"] = "[30, 60, 90]"
    yield
    models.clear()
    model_metadata.clear()


class TestHealthEndpoint:
    def test_health_returns_200(self):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_response_body(self):
        resp = client.get("/health")
        assert resp.json()["status"] == "healthy"


class TestReadyEndpoint:
    def test_ready_returns_200_with_all_models(self):
        resp = client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["models_loaded"] == 3

    def test_ready_returns_503_with_no_models(self):
        models.clear()
        resp = client.get("/ready")
        assert resp.status_code == 503

    def test_ready_returns_503_with_partial_models(self):
        del models[90]
        resp = client.get("/ready")
        assert resp.status_code == 503


class TestModelInfoEndpoint:
    def test_model_info_returns_200(self):
        resp = client.get("/model/info")
        assert resp.status_code == 200

    def test_model_info_structure(self):
        resp = client.get("/model/info")
        data = resp.json()
        assert "model_count" in data
        assert "horizons" in data
        assert "thresholds" in data
        assert data["model_count"] == 3


class TestPredictEndpoint:
    def test_predict_returns_valid_response(self, sample_customer_payload):
        resp = client.post("/predict", json=sample_customer_payload)
        assert resp.status_code == 200

    def test_predict_has_three_horizons(self, sample_customer_payload):
        resp = client.post("/predict", json=sample_customer_payload)
        data = resp.json()
        assert len(data["predictions"]) == 3

    def test_predict_includes_risk_tier(self, sample_customer_payload):
        resp = client.post("/predict", json=sample_customer_payload)
        data = resp.json()
        assert data["risk_tier"] in ["RED", "AMBER", "YELLOW", "GREEN"]

    def test_predict_includes_action_fields(self, sample_customer_payload):
        resp = client.post("/predict", json=sample_customer_payload)
        data = resp.json()
        assert "recommended_action" in data
        assert "action_owner" in data
        assert "action_sla" in data

    def test_predict_returns_503_when_model_missing(self, sample_customer_payload):
        models.clear()
        resp = client.post("/predict", json=sample_customer_payload)
        assert resp.status_code == 503


class TestBatchPredictEndpoint:
    def test_batch_predict_valid(self, sample_customer_payload):
        batch = {"customers": [sample_customer_payload] * 3}
        resp = client.post("/predict/batch", json=batch)
        assert resp.status_code == 200
        data = resp.json()
        assert data["batch_size"] == 3
        assert len(data["predictions"]) == 3

    def test_batch_predict_exceeds_limit(self, sample_customer_payload):
        batch = {"customers": [sample_customer_payload] * 1001}
        resp = client.post("/predict/batch", json=batch)
        # Should fail at Pydantic validation (max_length=1000) or endpoint check
        assert resp.status_code in [400, 422]
