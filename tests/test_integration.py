"""
Integration Tests — End-to-End Pipeline Validation
===================================================
Tests that validate data flows through the entire pipeline correctly.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import sys
import os

# Add src/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from risk_tiers import RiskThresholds, assign_risk_tier, TIER_ACTIONS


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .appName("TelcoChurnIntegrationTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestRiskTierLogic:
    """Test risk_tiers.py logic (canonical source of truth)."""

    def test_red_tier_assignment(self):
        """High 30d probability should assign RED tier."""
        tier = assign_risk_tier(0.8, 0.3, 0.2, RiskThresholds())
        assert tier == "RED"

    def test_amber_tier_assignment(self):
        """High 60d probability (low 30d) should assign AMBER."""
        tier = assign_risk_tier(0.3, 0.7, 0.2, RiskThresholds())
        assert tier == "AMBER"

    def test_yellow_tier_assignment(self):
        """High 90d probability (low 30d, 60d) should assign YELLOW."""
        tier = assign_risk_tier(0.2, 0.3, 0.6, RiskThresholds())
        assert tier == "YELLOW"

    def test_green_tier_assignment(self):
        """All probabilities below threshold should assign GREEN."""
        tier = assign_risk_tier(0.2, 0.3, 0.4, RiskThresholds())
        assert tier == "GREEN"

    def test_exact_threshold_not_triggered(self):
        """Exactly at threshold (0.5) should NOT trigger (> not >=)."""
        thresholds = RiskThresholds(red=0.5, amber=0.5, yellow=0.5)
        tier = assign_risk_tier(0.5, 0.5, 0.5, thresholds)
        assert tier == "GREEN"  # Should be GREEN (not RED)

    def test_just_above_threshold_triggers(self):
        """Just above threshold should trigger."""
        thresholds = RiskThresholds(red=0.5, amber=0.5, yellow=0.5)
        tier = assign_risk_tier(0.51, 0.4, 0.3, thresholds)
        assert tier == "RED"

    def test_cascading_priority(self):
        """RED has priority over AMBER, AMBER over YELLOW."""
        # All high
        assert assign_risk_tier(0.8, 0.8, 0.8, RiskThresholds()) == "RED"

        # 60d and 90d high, 30d low
        assert assign_risk_tier(0.2, 0.8, 0.8, RiskThresholds()) == "AMBER"

        # Only 90d high
        assert assign_risk_tier(0.2, 0.3, 0.8, RiskThresholds()) == "YELLOW"

    def test_custom_thresholds(self):
        """Should respect custom thresholds."""
        custom = RiskThresholds(red=0.7, amber=0.6, yellow=0.5)

        # Just below RED threshold
        assert assign_risk_tier(0.69, 0.5, 0.4, custom) == "GREEN"

        # Just above RED threshold
        assert assign_risk_tier(0.71, 0.5, 0.4, custom) == "RED"

    def test_tier_actions_defined(self):
        """All tiers should have action definitions."""
        for tier in ["RED", "AMBER", "YELLOW", "GREEN"]:
            assert tier in TIER_ACTIONS
            assert "action" in TIER_ACTIONS[tier]
            assert "owner" in TIER_ACTIONS[tier]
            assert "sla" in TIER_ACTIONS[tier]


class TestDataQualityConstraints:
    """Test data quality rules that should hold in production."""

    def test_feature_schema_consistency(self):
        """Feature names should be consistent across modules."""
        # Load feature list from API
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving", "api"))
        from main import NUMERIC_FEATURES, CATEGORICAL_FEATURES, ALL_FEATURES

        # Validate counts
        assert len(NUMERIC_FEATURES) == 29
        assert len(CATEGORICAL_FEATURES) == 3
        assert len(ALL_FEATURES) == 32

        # Validate no overlap
        assert set(NUMERIC_FEATURES).isdisjoint(set(CATEGORICAL_FEATURES))

        # Validate specific critical features exist
        critical_features = [
            "ooc_days",
            "dd_cancel_60_day",
            "loyalty_calls_90d",
            "usage_mom_change",
            "technology",
            "tenure_bucket",
        ]
        for feat in critical_features:
            assert feat in ALL_FEATURES

    def test_horizon_consistency(self):
        """HORIZONS should be [30, 60, 90] everywhere."""
        from main import HORIZONS as API_HORIZONS

        assert API_HORIZONS == [30, 60, 90]


class TestPointInTimeCorrectness:
    """Test that temporal features don't leak future data."""

    def test_cease_history_excludes_future(self, spark):
        """Cease history features should only count PRIOR ceases."""
        # Simulate data
        customer_snapshots = spark.createDataFrame([
            ("C001", "2024-01-01"),
            ("C001", "2024-02-01"),
            ("C001", "2024-03-01"),
        ], ["customer_id", "observation_date"])

        cease_events = spark.createDataFrame([
            ("C001", "2023-12-15"),  # Prior cease (should count)
            ("C001", "2024-01-15"),  # Between snapshots
            ("C001", "2024-02-20"),  # Future from Jan, prior from Mar
        ], ["customer_id", "cease_date"])

        # Join with point-in-time correctness
        result = (
            customer_snapshots.alias("s")
            .join(
                cease_events.alias("c"),
                (F.col("s.customer_id") == F.col("c.customer_id"))
                & (F.col("c.cease_date") < F.col("s.observation_date")),  # STRICT <
                "left"
            )
            .groupBy("s.customer_id", "s.observation_date")
            .agg(F.count("c.cease_date").alias("prior_cease_count"))
            .orderBy("observation_date")
        )

        rows = result.collect()

        # 2024-01-01: Only 2023-12-15 cease is prior
        assert rows[0]["prior_cease_count"] == 1

        # 2024-02-01: Both 2023-12-15 and 2024-01-15 are prior
        assert rows[1]["prior_cease_count"] == 2

        # 2024-03-01: All 3 ceases are prior
        assert rows[2]["prior_cease_count"] == 3

    def test_usage_trend_uses_lagged_values(self, spark):
        """Usage MoM change should use PRIOR month, not current."""
        # Simulate monthly usage
        usage = spark.createDataFrame([
            ("C001", "2024-01-01", 10000),  # No prior month
            ("C001", "2024-02-01", 12000),  # +20% from Jan
            ("C001", "2024-03-01", 9000),   # -25% from Feb
        ], ["customer_id", "month_key", "monthly_total_mb"])

        # Calculate MoM change
        from pyspark.sql.window import Window

        w = Window.partitionBy("customer_id").orderBy("month_key")

        result = (
            usage
            .withColumn("prev_month_total", F.lag("monthly_total_mb", 1).over(w))
            .withColumn(
                "usage_mom_change",
                F.when(
                    F.col("prev_month_total") > 0,
                    (F.col("monthly_total_mb") - F.col("prev_month_total")) / F.col("prev_month_total")
                ).otherwise(None)
            )
            .orderBy("month_key")
        )

        rows = result.collect()

        # Jan: No prior month
        assert rows[0]["usage_mom_change"] is None

        # Feb: +20%
        assert abs(rows[1]["usage_mom_change"] - 0.2) < 0.001

        # Mar: -25%
        assert abs(rows[2]["usage_mom_change"] - (-0.25)) < 0.001


class TestConfigurationManagement:
    """Test configuration loading and validation."""

    def test_environment_variable_defaults(self):
        """API should have sensible defaults when env vars not set."""
        from main import RED_THRESHOLD, AMBER_THRESHOLD, YELLOW_THRESHOLD, MAX_BATCH_SIZE

        # Defaults should be defined
        assert isinstance(RED_THRESHOLD, float)
        assert isinstance(AMBER_THRESHOLD, float)
        assert isinstance(YELLOW_THRESHOLD, float)
        assert isinstance(MAX_BATCH_SIZE, int)

        # Defaults should be reasonable
        assert 0 <= RED_THRESHOLD <= 1
        assert 0 <= AMBER_THRESHOLD <= 1
        assert 0 <= YELLOW_THRESHOLD <= 1
        assert MAX_BATCH_SIZE == 1000


class TestNullHandling:
    """Test how nulls are handled in features."""

    def test_ooc_days_null_sentinel(self):
        """ooc_days nulls should be filled with -9999 sentinel."""
        # This is the Silver layer logic
        # In production, null ooc_days means customer is in-contract
        # We use -9999 as a sentinel value (LightGBM handles this fine)

        # Simulate Silver cleaning
        from pyspark.sql.types import IntegerType

        df = spark.createDataFrame([
            ("C001", None),
            ("C002", 30),
            ("C003", 0),
        ], ["customer_id", "ooc_days_raw"])

        df_clean = df.withColumn(
            "ooc_days",
            F.when(F.col("ooc_days_raw").isNull(), -9999)
             .otherwise(F.col("ooc_days_raw"))
        )

        rows = df_clean.collect()
        assert rows[0]["ooc_days"] == -9999  # Null → -9999
        assert rows[1]["ooc_days"] == 30      # Preserved
        assert rows[2]["ooc_days"] == 0       # Preserved


# Run with: pytest tests/test_integration.py -v
