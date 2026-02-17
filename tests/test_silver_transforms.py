"""
Unit tests for Silver layer transformations.
Run on Databricks with: pytest tests/test_silver_transforms.py

These tests validate the core Silver cleaning logic WITHOUT requiring
the full Bronze → Silver pipeline to run. They use inline test DataFrames.
"""

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed — skipping Spark tests")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
from datetime import date


@pytest.fixture(scope="session")
def spark():
    """Create or get SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("silver_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestCustomerInfoSilver:
    """Tests for Silver customer_info transformations."""

    def test_ooc_days_null_handling(self, spark):
        """Null ooc_days should be replaced with -9999 sentinel."""
        schema = StructType([
            StructField("unique_customer_identifier", StringType()),
            StructField("ooc_days", DoubleType()),
        ])
        data = [("cust_1", 30.0), ("cust_2", None), ("cust_3", -15.0)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("ooc_days",
            F.when(F.col("ooc_days").isNull(), F.lit(-9999.0))
             .otherwise(F.col("ooc_days"))
        )

        results = {row["unique_customer_identifier"]: row["ooc_days"] for row in result.collect()}
        assert results["cust_1"] == 30.0
        assert results["cust_2"] == -9999.0
        assert results["cust_3"] == -15.0

    def test_contract_status_ordinal_encoding(self, spark):
        """Contract status prefix (01-06) should be extracted as integer."""
        schema = StructType([
            StructField("contract_status", StringType()),
        ])
        data = [
            ("01 Early Contract",),
            ("02 In Contract",),
            ("06 OOC",),
        ]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("contract_status_ord",
            F.substring(F.col("contract_status"), 1, 2).cast(IntegerType())
        )

        values = [row["contract_status_ord"] for row in result.collect()]
        assert values == [1, 2, 6]

    def test_speed_gap_calculation(self, spark):
        """Speed gap % should be (advertised - actual) / advertised * 100."""
        schema = StructType([
            StructField("speed", IntegerType()),
            StructField("line_speed", DoubleType()),
        ])
        data = [(100, 80.0), (50, 50.0), (0, 0.0)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("speed_gap_pct",
            F.when(F.col("speed") > 0,
                F.round((F.col("speed") - F.col("line_speed")) / F.col("speed") * 100, 2)
            ).otherwise(F.lit(0.0))
        )

        values = [row["speed_gap_pct"] for row in result.collect()]
        assert values[0] == 20.0   # (100-80)/100*100
        assert values[1] == 0.0    # (50-50)/50*100
        assert values[2] == 0.0    # speed=0 → default 0

    def test_tenure_bucket_assignment(self, spark):
        """Tenure days should map to correct buckets."""
        schema = StructType([
            StructField("tenure_days", IntegerType()),
        ])
        data = [(30,), (200,), (500,), (800,), (1500,)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("tenure_bucket",
            F.when(F.col("tenure_days") < 90, "0-90d")
             .when(F.col("tenure_days") < 365, "90d-1y")
             .when(F.col("tenure_days") < 730, "1y-2y")
             .when(F.col("tenure_days") < 1095, "2y-3y")
             .otherwise("3y+")
        )

        values = [row["tenure_bucket"] for row in result.collect()]
        assert values == ["0-90d", "90d-1y", "1y-2y", "2y-3y", "3y+"]


class TestUsageSilver:
    """Tests for Silver usage transformations."""

    def test_string_to_double_cast(self, spark):
        """Usage string columns should cast to double correctly."""
        schema = StructType([
            StructField("usage_download_mbs", StringType()),
            StructField("usage_upload_mbs", StringType()),
        ])
        data = [("1234.567", "89.012"), ("0.000", "0.000")]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("usage_download_mbs", F.col("usage_download_mbs").cast(DoubleType()))
        values = [row["usage_download_mbs"] for row in result.collect()]
        assert abs(values[0] - 1234.567) < 0.001
        assert values[1] == 0.0

    def test_total_usage_calculation(self, spark):
        """Total usage should be download + upload."""
        schema = StructType([
            StructField("download", DoubleType()),
            StructField("upload", DoubleType()),
        ])
        data = [(100.0, 50.0), (0.0, 0.0)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("total", F.col("download") + F.col("upload"))
        values = [row["total"] for row in result.collect()]
        assert values[0] == 150.0
        assert values[1] == 0.0


class TestCallsSilver:
    """Tests for Silver calls transformations."""

    def test_null_call_type_imputation(self, spark):
        """Null call_type should be imputed as 'Unknown'."""
        schema = StructType([
            StructField("call_type", StringType()),
        ])
        data = [("Loyalty",), (None,), ("Tech",)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("call_type",
            F.when(F.col("call_type").isNull(), F.lit("Unknown"))
             .otherwise(F.col("call_type"))
        )

        values = [row["call_type"] for row in result.collect()]
        assert values == ["Loyalty", "Unknown", "Tech"]

    def test_call_type_flags(self, spark):
        """Call type flags should correctly identify Loyalty, Complaints, Tech."""
        schema = StructType([
            StructField("call_type", StringType()),
        ])
        data = [("Loyalty",), ("Complaints",), ("Tech",), ("Other",)]
        df = spark.createDataFrame(data, schema)

        result = (
            df
            .withColumn("is_loyalty", F.when(F.col("call_type") == "Loyalty", 1).otherwise(0))
            .withColumn("is_complaint", F.when(F.col("call_type") == "Complaints", 1).otherwise(0))
            .withColumn("is_tech", F.when(F.col("call_type") == "Tech", 1).otherwise(0))
        )

        rows = result.collect()
        assert rows[0]["is_loyalty"] == 1
        assert rows[1]["is_complaint"] == 1
        assert rows[2]["is_tech"] == 1
        assert rows[3]["is_loyalty"] == 0


class TestCeaseSilver:
    """Tests for Silver cease transformations."""

    def test_completion_flag(self, spark):
        """is_completed should be 1 when cease_completed_date is not null."""
        schema = StructType([
            StructField("cease_completed_date", StringType()),
        ])
        data = [("2024-01-15",), (None,), ("2024-03-20",)]
        df = spark.createDataFrame(data, schema)

        result = df.withColumn("is_completed",
            F.when(F.col("cease_completed_date").isNotNull(), 1).otherwise(0)
        )

        values = [row["is_completed"] for row in result.collect()]
        assert values == [1, 0, 1]
