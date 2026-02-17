"""
Tests to validate all YAML configuration files parse correctly
and contain required keys. Cross-references feature lists with API.
"""

import os
import sys

import pytest
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
ENV_CONFIG_DIR = os.path.join(CONFIG_DIR, "environments")

# Import API feature lists for cross-validation
sys.path.insert(0, os.path.join(PROJECT_ROOT, "serving", "api"))
from main import CATEGORICAL_FEATURES, NUMERIC_FEATURES


def _load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


class TestFeatureConfig:
    """Tests for config/feature_config.yaml."""

    @pytest.fixture
    def config(self):
        return _load_yaml(os.path.join(CONFIG_DIR, "feature_config.yaml"))

    def test_parseable(self, config):
        assert config is not None
        assert isinstance(config, dict)

    def test_has_horizons(self, config):
        assert config["horizons"] == [30, 60, 90]

    def test_has_numeric_features(self, config):
        features = config["features"]["numeric"]
        assert isinstance(features, list)
        assert len(features) > 0

    def test_has_categorical_features(self, config):
        features = config["features"]["categorical"]
        assert isinstance(features, list)
        assert len(features) > 0

    def test_numeric_features_match_api(self, config):
        """Feature config numeric list must match API NUMERIC_FEATURES."""
        config_features = set(config["features"]["numeric"])
        api_features = set(NUMERIC_FEATURES)
        assert config_features == api_features, \
            f"Mismatch: config_only={config_features - api_features}, api_only={api_features - config_features}"

    def test_categorical_features_match_api(self, config):
        """Feature config categorical list must match API CATEGORICAL_FEATURES."""
        config_features = set(config["features"]["categorical"])
        api_features = set(CATEGORICAL_FEATURES)
        assert config_features == api_features, \
            f"Mismatch: config_only={config_features - api_features}, api_only={api_features - config_features}"

    def test_has_risk_tier_thresholds(self, config):
        thresholds = config["risk_tiers"]
        assert "red_threshold" in thresholds
        assert "amber_threshold" in thresholds
        assert "yellow_threshold" in thresholds

    def test_has_lgbm_params(self, config):
        params = config["lgbm_params"]
        assert "objective" in params
        assert params["objective"] == "binary"

    def test_has_splits(self, config):
        splits = config["splits"]
        assert "train_end" in splits
        assert "validation_end" in splits


class TestEnvironmentConfigs:
    """Tests for config/environments/{dev,staging,prod}.yaml."""

    @pytest.mark.parametrize("env", ["dev", "staging", "prod"])
    def test_parseable(self, env):
        config = _load_yaml(os.path.join(ENV_CONFIG_DIR, f"{env}.yaml"))
        assert config is not None
        assert isinstance(config, dict)

    @pytest.mark.parametrize("env", ["dev", "staging", "prod"])
    def test_has_catalog_key(self, env):
        config = _load_yaml(os.path.join(ENV_CONFIG_DIR, f"{env}.yaml"))
        assert "catalog" in config, f"{env}.yaml missing 'catalog' key"

    def test_prod_catalog_is_base_name(self):
        config = _load_yaml(os.path.join(ENV_CONFIG_DIR, "prod.yaml"))
        assert config["catalog"] == "uk_telecoms"

    def test_dev_catalog_is_suffixed(self):
        config = _load_yaml(os.path.join(ENV_CONFIG_DIR, "dev.yaml"))
        assert config["catalog"] == "uk_telecoms_dev"
