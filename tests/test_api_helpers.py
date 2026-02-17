"""
Tests for helper functions in serving/api/main.py:
- features_to_dataframe()
- Decile calculation logic
- load_models()
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))

from main import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    CustomerFeatures,
    features_to_dataframe,
)


class TestFeaturesToDataframe:
    """Tests for features_to_dataframe()."""

    def test_returns_single_row_dataframe(self):
        cf = CustomerFeatures(customer_id="X")
        df = features_to_dataframe(cf)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_all_numeric_features_present(self):
        cf = CustomerFeatures(customer_id="X")
        df = features_to_dataframe(cf)
        for feat in NUMERIC_FEATURES:
            assert feat in df.columns, f"Missing numeric feature: {feat}"

    def test_all_categorical_features_present(self):
        cf = CustomerFeatures(customer_id="X")
        df = features_to_dataframe(cf)
        for feat in CATEGORICAL_FEATURES:
            assert feat in df.columns, f"Missing categorical feature: {feat}"

    def test_categorical_features_are_categorical_dtype(self):
        cf = CustomerFeatures(customer_id="X")
        df = features_to_dataframe(cf)
        for feat in CATEGORICAL_FEATURES:
            assert isinstance(df[feat].dtype, pd.CategoricalDtype), \
                f"{feat} should be pd.Categorical, got {df[feat].dtype}"

    def test_feature_values_match_input(self):
        cf = CustomerFeatures(customer_id="X", speed=100.0, tenure_days=730.0)
        df = features_to_dataframe(cf)
        assert df["speed"].iloc[0] == 100.0
        assert df["tenure_days"].iloc[0] == 730.0

    def test_total_feature_count(self):
        """Should have 30 numeric + 3 categorical = 33 columns."""
        cf = CustomerFeatures(customer_id="X")
        df = features_to_dataframe(cf)
        assert len(df.columns) == len(ALL_FEATURES)


class TestDecileCalculation:
    """Tests for the decile formula: min(int(prob * 10) + 1, 10)."""

    @staticmethod
    def _calc_decile(prob: float) -> int:
        return min(int(prob * 10) + 1, 10)

    def test_low_probability(self):
        assert self._calc_decile(0.05) == 1

    def test_mid_probability(self):
        assert self._calc_decile(0.55) == 6

    def test_high_probability(self):
        assert self._calc_decile(0.95) == 10

    def test_max_capped_at_10(self):
        """prob=1.0 gives int(10)+1=11, capped to 10."""
        assert self._calc_decile(1.0) == 10

    def test_zero_probability(self):
        assert self._calc_decile(0.0) == 1

    def test_boundary_0_1(self):
        """prob=0.1 -> int(1)+1=2."""
        assert self._calc_decile(0.1) == 2


class TestLoadModels:
    """Tests for load_models() with mocked filesystem."""

    def test_load_models_with_existing_files(self):
        from main import load_models, models, model_metadata

        models.clear()
        model_metadata.clear()

        mock_booster = MagicMock()
        mock_booster.predict.return_value = [0.5]
        with patch("main.os.path.exists", return_value=True), \
             patch("main.os.path.getsize", return_value=50000), \
             patch("main.lgb.Booster", return_value=mock_booster):
            load_models()

        assert len(models) == 3
        assert 30 in models
        assert 60 in models
        assert 90 in models
        assert model_metadata["model_count"] == "3"

    def test_load_models_with_missing_files(self):
        from main import load_models, models, model_metadata

        models.clear()
        model_metadata.clear()

        with patch("main.os.path.exists", return_value=False):
            load_models()

        assert len(models) == 0
        assert model_metadata["model_count"] == "0"
