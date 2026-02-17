"""
Unit tests for Gold layer feature engineering.
Validates point-in-time correctness and multi-horizon target creation.
"""

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed — skipping Spark tests")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType
)
from datetime import date


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("feature_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestTargetLabels:
    """Tests for multi-horizon target variable creation."""

    def test_30d_churn_label(self, spark):
        """Customer who ceases 15 days after observation should be labeled churned_in_30d = 1."""
        spine = spark.createDataFrame(
            [("cust_1", date(2024, 1, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        cease = spark.createDataFrame(
            [("cust_1", date(2024, 1, 16))],  # 15 days later
            ["unique_customer_identifier", "cease_placed_date"]
        )

        labels = (
            spine.alias("s")
            .join(
                cease.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
                & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), 30)),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
            .agg(F.max(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)).alias("churned_in_30d"))
        )

        result = labels.collect()[0]["churned_in_30d"]
        assert result == 1

    def test_30d_no_churn_outside_window(self, spark):
        """Customer who ceases 45 days after observation should NOT be labeled churned_in_30d."""
        spine = spark.createDataFrame(
            [("cust_1", date(2024, 1, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        cease = spark.createDataFrame(
            [("cust_1", date(2024, 2, 15))],  # 45 days later — outside 30d window
            ["unique_customer_identifier", "cease_placed_date"]
        )

        labels = (
            spine.alias("s")
            .join(
                cease.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
                & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), 30)),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
            .agg(F.max(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)).alias("churned_in_30d"))
        )

        result = labels.collect()[0]["churned_in_30d"]
        assert result == 0

    def test_90d_captures_wider_window(self, spark):
        """Customer ceasing at 45 days should be caught by 90d but not 30d."""
        spine = spark.createDataFrame(
            [("cust_1", date(2024, 1, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        cease = spark.createDataFrame(
            [("cust_1", date(2024, 2, 15))],  # 45 days later
            ["unique_customer_identifier", "cease_placed_date"]
        )

        results = {}
        for days in [30, 90]:
            labels = (
                spine.alias("s")
                .join(
                    cease.alias("c"),
                    (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                    & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
                    & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), days)),
                    "left"
                )
                .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
                .agg(
                    F.max(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0))
                    .alias(f"churned_in_{days}d")
                )
            )
            results[days] = labels.collect()[0][f"churned_in_{days}d"]

        assert results[30] == 0   # Outside 30d window
        assert results[90] == 1   # Inside 90d window

    def test_no_churn_customer(self, spark):
        """Customer with no cease record should have 0 for all horizons."""
        spine = spark.createDataFrame(
            [("cust_no_churn", date(2024, 1, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        cease = spark.createDataFrame(
            [("other_cust", date(2024, 1, 10))],  # Different customer
            ["unique_customer_identifier", "cease_placed_date"]
        )

        labels = (
            spine.alias("s")
            .join(
                cease.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
                & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), 30)),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
            .agg(F.max(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)).alias("churned_in_30d"))
        )

        result = labels.collect()[0]["churned_in_30d"]
        assert result == 0


class TestPointInTimeCorrectness:
    """Tests to verify no data leakage in feature engineering."""

    def test_call_features_use_only_prior_data(self, spark):
        """Call features should only count calls BEFORE the observation date."""
        spine = spark.createDataFrame(
            [("cust_1", date(2024, 3, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        calls = spark.createDataFrame([
            ("cust_1", date(2024, 2, 15)),  # Before obs → should count
            ("cust_1", date(2024, 2, 20)),  # Before obs → should count
            ("cust_1", date(2024, 3, 1)),   # ON obs date → should NOT count (not strictly before)
            ("cust_1", date(2024, 3, 15)),  # After obs → should NOT count
        ], ["unique_customer_identifier", "event_date"])

        result = (
            spine.alias("s")
            .join(
                calls.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.event_date") < F.col("s.datevalue"))
                & (F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 180)),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
            .agg(F.count("c.event_date").alias("calls_180d"))
        )

        call_count = result.collect()[0]["calls_180d"]
        assert call_count == 2  # Only the 2 calls before March 1st

    def test_cease_history_excludes_future(self, spark):
        """Prior cease count should only include ceases BEFORE observation date."""
        spine = spark.createDataFrame(
            [("cust_1", date(2024, 6, 1))],
            ["unique_customer_identifier", "datevalue"]
        )
        cease = spark.createDataFrame([
            ("cust_1", date(2024, 1, 10)),  # Before → count
            ("cust_1", date(2024, 4, 20)),  # Before → count
            ("cust_1", date(2024, 6, 15)),  # After → do NOT count
        ], ["unique_customer_identifier", "cease_placed_date"])

        result = (
            spine.alias("s")
            .join(
                cease.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.cease_placed_date") < F.col("s.datevalue")),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"), F.col("s.datevalue"))
            .agg(F.count("c.cease_placed_date").alias("prior_cease_count"))
        )

        count = result.collect()[0]["prior_cease_count"]
        assert count == 2  # Only the 2 ceases before June 1st


class TestTemporalSplit:
    """Tests for the temporal train/val/test split."""

    def test_split_assignment(self, spark):
        """Verify split boundaries are correctly applied."""
        data = [
            (date(2023, 6, 1),),   # train
            (date(2024, 3, 1),),   # train (before 2024-04-01)
            (date(2024, 5, 1),),   # validation
            (date(2024, 6, 1),),   # validation (before 2024-07-01)
            (date(2024, 8, 1),),   # test
        ]
        df = spark.createDataFrame(data, ["observation_date"])

        result = df.withColumn("split",
            F.when(F.col("observation_date") < "2024-04-01", "train")
             .when(F.col("observation_date") < "2024-07-01", "validation")
             .otherwise("test")
        )

        splits = [row["split"] for row in result.orderBy("observation_date").collect()]
        assert splits == ["train", "train", "validation", "validation", "test"]
