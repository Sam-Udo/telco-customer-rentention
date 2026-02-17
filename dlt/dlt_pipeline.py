# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables Pipeline — Production Version
# MAGIC
# MAGIC This is the DLT-native version of notebooks 01 (Bronze) and 02 (Silver).
# MAGIC Use this in production for automated, incremental, quality-gated ingestion.
# MAGIC
# MAGIC **Benefits over standard notebooks:**
# MAGIC - Declarative (define WHAT, not HOW)
# MAGIC - Auto-scaling compute
# MAGIC - Built-in quality expectations (fail/drop/warn)
# MAGIC - Automatic lineage in Unity Catalog
# MAGIC - Incremental processing with Auto Loader

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType, IntegerType

LANDING_PATH = "/mnt/landing"

# ═══════════════════════════════════════════════════════════
# BRONZE LAYER — Raw Ingestion (Auto Loader for incremental)
# ═══════════════════════════════════════════════════════════

@dlt.table(
    name="raw_customer_info",
    comment="Raw monthly customer snapshots — no transformations",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "unique_customer_identifier"}
)
def raw_customer_info():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/customer_info_schema")
        .load(f"{LANDING_PATH}/customer_info/")
    )


@dlt.table(
    name="raw_usage",
    comment="Raw daily broadband usage — download/upload as strings from source",
    table_properties={"quality": "bronze"}
)
def raw_usage():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/usage_schema")
        .load(f"{LANDING_PATH}/usage/")
    )


@dlt.table(
    name="raw_calls",
    comment="Raw contact center call records",
    table_properties={"quality": "bronze"}
)
def raw_calls():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/calls_schema")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{LANDING_PATH}/calls/")
    )


@dlt.table(
    name="raw_cease",
    comment="Raw churn events — cease placed/completed dates",
    table_properties={"quality": "bronze"}
)
def raw_cease():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/cease_schema")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{LANDING_PATH}/cease/")
    )


# ═══════════════════════════════════════════════════════════
# SILVER LAYER — Cleaned, Typed, Validated
# ═══════════════════════════════════════════════════════════

@dlt.table(
    name="customer_info",
    comment="Cleaned customer snapshots — typed, nulls handled, derived columns",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_customer_id", "unique_customer_identifier IS NOT NULL")
@dlt.expect("valid_datevalue", "datevalue IS NOT NULL")
@dlt.expect_or_drop("valid_speed", "speed > 0")
@dlt.expect("valid_tenure", "tenure_days >= 0")
def silver_customer_info():
    return (
        dlt.read("raw_customer_info")
        .withColumn("datevalue", F.col("datevalue").cast(DateType()))
        .withColumn("ooc_days",
            F.when(F.col("ooc_days").isNull(), F.lit(-9999.0))
             .otherwise(F.col("ooc_days"))
        )
        .withColumn("contract_status_ord",
            F.substring(F.col("contract_status"), 1, 2).cast(IntegerType())
        )
        .withColumn("speed_gap_pct",
            F.when(F.col("speed") > 0,
                F.round((F.col("speed") - F.col("line_speed")) / F.col("speed") * 100, 2)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("tenure_bucket",
            F.when(F.col("tenure_days") < 90, "0-90d")
             .when(F.col("tenure_days") < 365, "90d-1y")
             .when(F.col("tenure_days") < 730, "1y-2y")
             .when(F.col("tenure_days") < 1095, "2y-3y")
             .otherwise("3y+")
        )
    )


@dlt.table(
    name="usage_daily",
    comment="Daily usage with numeric types — string-to-double cast applied",
    table_properties={"quality": "silver"},
    partition_cols=["month_key"]
)
@dlt.expect_or_fail("valid_customer_id", "unique_customer_identifier IS NOT NULL")
@dlt.expect("non_negative_download", "usage_download_mbs >= 0 OR usage_download_mbs IS NULL")
@dlt.expect("non_negative_upload", "usage_upload_mbs >= 0 OR usage_upload_mbs IS NULL")
def silver_usage_daily():
    return (
        dlt.read("raw_usage")
        .withColumn("usage_download_mbs", F.col("usage_download_mbs").cast(DoubleType()))
        .withColumn("usage_upload_mbs", F.col("usage_upload_mbs").cast(DoubleType()))
        .withColumn("calendar_date", F.col("calendar_date").cast(DateType()))
        .withColumn("usage_total_mbs",
            F.col("usage_download_mbs") + F.col("usage_upload_mbs")
        )
        .withColumn("month_key", F.date_trunc("month", F.col("calendar_date")))
    )


@dlt.table(
    name="usage_monthly",
    comment="Monthly aggregated usage per customer — primary feature grain",
    table_properties={"quality": "silver"}
)
def silver_usage_monthly():
    return (
        dlt.read("usage_daily")
        .groupBy("unique_customer_identifier", "month_key")
        .agg(
            F.sum("usage_download_mbs").alias("monthly_download_mb"),
            F.sum("usage_upload_mbs").alias("monthly_upload_mb"),
            F.sum("usage_total_mbs").alias("monthly_total_mb"),
            F.mean("usage_download_mbs").alias("avg_daily_download_mb"),
            F.mean("usage_upload_mbs").alias("avg_daily_upload_mb"),
            F.stddev("usage_total_mbs").alias("std_daily_total_mb"),
            F.count("*").alias("active_days_in_month"),
            F.max("usage_total_mbs").alias("peak_daily_total_mb"),
        )
    )


@dlt.table(
    name="calls",
    comment="Cleaned call records — null types imputed, flags derived",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_customer_id", "unique_customer_identifier IS NOT NULL")
@dlt.expect("valid_event_date", "event_date IS NOT NULL")
def silver_calls():
    return (
        dlt.read("raw_calls")
        .withColumn("event_date", F.col("event_date").cast(DateType()))
        .withColumn("call_type",
            F.when(F.col("call_type").isNull(), F.lit("Unknown"))
             .otherwise(F.col("call_type"))
        )
        .withColumn("is_loyalty_call",
            F.when(F.col("call_type") == "Loyalty", 1).otherwise(0)
        )
        .withColumn("is_complaint_call",
            F.when(F.col("call_type") == "Complaints", 1).otherwise(0)
        )
        .withColumn("is_tech_call",
            F.when(F.col("call_type") == "Tech", 1).otherwise(0)
        )
    )


@dlt.table(
    name="cease",
    comment="Cleaned cease events — dates parsed, completion flag derived",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("valid_customer_id", "unique_customer_identifier IS NOT NULL")
@dlt.expect_or_fail("valid_placed_date", "cease_placed_date IS NOT NULL")
def silver_cease():
    return (
        dlt.read("raw_cease")
        .withColumn("cease_placed_date", F.col("cease_placed_date").cast(DateType()))
        .withColumn("cease_completed_date", F.col("cease_completed_date").cast(DateType()))
        .withColumn("is_completed",
            F.when(F.col("cease_completed_date").isNotNull(), 1).otherwise(0)
        )
        .withColumn("cease_month", F.date_trunc("month", F.col("cease_placed_date")))
    )
