"""
Tests for Pydantic request/response models in serving/api/main.py.
"""

import os
import sys

import pytest
from pydantic import ValidationError

# Ensure API directory is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))

from main import (
    BatchRequest,
    BatchResponse,
    CustomerFeatures,
    HorizonPrediction,
    ModelInfo,
    PredictionResponse,
)


class TestCustomerFeatures:
    """Tests for the CustomerFeatures Pydantic model."""

    def test_valid_minimal_input(self):
        """Only customer_id is truly required; rest have defaults."""
        cf = CustomerFeatures(customer_id="CUST-001")
        assert cf.customer_id == "CUST-001"

    def test_customer_id_required(self):
        """Missing customer_id should raise ValidationError."""
        with pytest.raises(ValidationError):
            CustomerFeatures()

    def test_ooc_days_default_sentinel(self):
        """ooc_days defaults to -9999 (in-contract sentinel)."""
        cf = CustomerFeatures(customer_id="X")
        assert cf.ooc_days == -9999

    def test_technology_default(self):
        cf = CustomerFeatures(customer_id="X")
        assert cf.technology == "Unknown"

    def test_sales_channel_default(self):
        cf = CustomerFeatures(customer_id="X")
        assert cf.sales_channel == "Unknown"

    def test_tenure_bucket_default(self):
        cf = CustomerFeatures(customer_id="X")
        assert cf.tenure_bucket == "0-90d"

    def test_all_numeric_defaults_are_zero_or_sentinel(self):
        """Most numeric fields default to 0, ooc_days to -9999."""
        cf = CustomerFeatures(customer_id="X")
        assert cf.speed == 0
        assert cf.tenure_days == 0
        assert cf.calls_30d == 0

    def test_custom_values_accepted(self):
        cf = CustomerFeatures(
            customer_id="CUST-002",
            speed=100.0,
            line_speed=80.0,
            tenure_days=730.0,
            technology="FTTP",
        )
        assert cf.speed == 100.0
        assert cf.technology == "FTTP"


class TestHorizonPrediction:
    def test_valid_horizon_prediction(self):
        hp = HorizonPrediction(horizon_days=30, churn_probability=0.65, risk_decile=7)
        assert hp.horizon_days == 30
        assert hp.churn_probability == 0.65
        assert hp.risk_decile == 7


class TestPredictionResponse:
    def test_valid_prediction_response(self):
        resp = PredictionResponse(
            customer_id="C-001",
            predictions=[
                HorizonPrediction(horizon_days=30, churn_probability=0.7, risk_decile=8),
            ],
            risk_tier="RED",
            recommended_action="Call now",
            action_owner="Loyalty",
            action_sla="24h",
            scored_at="2024-01-01T00:00:00Z",
            request_id="abc12345",
        )
        assert resp.risk_tier == "RED"
        assert len(resp.predictions) == 1


class TestBatchRequest:
    def test_valid_batch(self):
        customers = [CustomerFeatures(customer_id=f"C-{i}") for i in range(5)]
        batch = BatchRequest(customers=customers)
        assert len(batch.customers) == 5

    def test_batch_max_length_enforced(self):
        """Pydantic max_length=1000 should reject larger batches."""
        customers = [CustomerFeatures(customer_id=f"C-{i}") for i in range(1001)]
        with pytest.raises(ValidationError):
            BatchRequest(customers=customers)


class TestModelInfo:
    def test_valid_model_info(self):
        info = ModelInfo(
            loaded_at="2024-01-01T00:00:00Z",
            model_count=3,
            horizons=[30, 60, 90],
            failed_horizons=[],
            thresholds={"red_30d": 0.5, "amber_60d": 0.5, "yellow_90d": 0.5},
        )
        assert info.model_count == 3
        assert len(info.horizons) == 3
