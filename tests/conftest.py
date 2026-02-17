"""
Shared test fixtures for the Telco Churn Prediction test suite.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

# ── Project paths ──
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
API_DIR = os.path.join(PROJECT_ROOT, "serving", "api")


def _ensure_path(path):
    if path not in sys.path:
        sys.path.insert(0, path)


# Ensure API and src dirs are on path so test-module-level imports work
_ensure_path(API_DIR)
_ensure_path(SRC_DIR)


@pytest.fixture
def sample_customer_payload():
    """Minimal valid customer JSON payload for API tests."""
    return {
        "customer_id": "TEST-001",
        "contract_status_ord": 3.0,
        "contract_dd_cancels": 0.0,
        "dd_cancel_60_day": 0.0,
        "ooc_days": 30.0,
        "speed": 80.0,
        "line_speed": 65.0,
        "speed_gap_pct": 18.75,
        "tenure_days": 365.0,
        "calls_30d": 2.0,
        "calls_90d": 5.0,
        "calls_180d": 8.0,
        "loyalty_calls_90d": 1.0,
        "complaint_calls_90d": 0.0,
        "tech_calls_90d": 1.0,
        "avg_talk_time": 120.0,
        "avg_hold_time": 30.0,
        "max_hold_time": 60.0,
        "days_since_last_call": 15.0,
        "monthly_download_mb": 42500.0,
        "monthly_upload_mb": 7500.0,
        "monthly_total_mb": 50000.0,
        "avg_daily_download_mb": 1416.0,
        "std_daily_total_mb": 500.0,
        "active_days_in_month": 30.0,
        "peak_daily_total_mb": 3000.0,
        "usage_mom_change": -0.05,
        "usage_vs_3mo_avg": -0.02,
        "prior_cease_count": 0.0,
        "days_since_last_cease": 0.0,
        "technology": "FTTP",
        "sales_channel": "Online",
        "tenure_bucket": "90d-1y",
    }


@pytest.fixture
def mock_booster():
    """A single mock LightGBM Booster that returns prob=0.65."""
    booster = MagicMock()
    booster.predict.return_value = [0.65]
    return booster


@pytest.fixture
def mock_loaded_models(mock_booster):
    """Pre-populated models dict with mock boosters for all 3 horizons."""
    return {30: mock_booster, 60: mock_booster, 90: mock_booster}
