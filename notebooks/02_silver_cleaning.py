# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Cleaning, Typing & Validation
# MAGIC
# MAGIC **Purpose:** Transform Bronze raw data into clean, typed, validated Silver tables. Fix known data quality issues. Add derived columns needed downstream.
# MAGIC
# MAGIC **Design Pattern:** DLT Expectations for quality gates (shown in comments). Standard Spark transforms for assessment.
# MAGIC
# MAGIC **Output:** `uk_telecoms.silver.{customer_info, usage_daily, usage_monthly, calls, cease}`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType, IntegerType
from pyspark.sql import Window

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Silver: Customer Info
# MAGIC
# MAGIC **Transforms:**
# MAGIC - Cast `datevalue` to DateType
# MAGIC - Handle null `ooc_days` (sentinel value -9999)
# MAGIC - Derive `contract_status_ord` (ordinal encoding from prefix)
# MAGIC - Derive `speed_gap_pct` (advertised vs actual — key churn signal)
# MAGIC - Derive `tenure_bucket` (categorical grouping)

# COMMAND ----------

df_customer = spark.table("uk_telecoms.bronze.raw_customer_info")

silver_customer = (
    df_customer
    # Type corrections
    .withColumn("datevalue", F.col("datevalue").cast(DateType()))

    # Null handling: ooc_days
    # Null ooc_days = early contract customers (no OOC date yet)
    # Using -9999 as sentinel so the model can distinguish "no OOC info" from "0 days OOC"
    .withColumn("ooc_days",
        F.when(F.col("ooc_days").isNull(), F.lit(-9999.0))
         .otherwise(F.col("ooc_days"))
    )

    # Ordinal encode contract_status (already prefixed 01-06)
    # "01 Early Contract" → 1, "06 OOC" → 6
    .withColumn("contract_status_ord",
        F.substring(F.col("contract_status"), 1, 2).cast(IntegerType())
    )

    # Speed gap: (advertised - actual) / advertised * 100
    # Higher = customer getting less than promised = dissatisfaction signal
    .withColumn("speed_gap_pct",
        F.when(F.col("speed") > 0,
            F.round((F.col("speed") - F.col("line_speed")) / F.col("speed") * 100, 2)
        ).otherwise(F.lit(0.0))
    )

    # Tenure buckets for categorical feature
    .withColumn("tenure_bucket",
        F.when(F.col("tenure_days") < 90, "0-90d")
         .when(F.col("tenure_days") < 365, "90d-1y")
         .when(F.col("tenure_days") < 730, "1y-2y")
         .when(F.col("tenure_days") < 1095, "2y-3y")
         .otherwise("3y+")
    )

    # Drop ingestion metadata columns (not needed in Silver)
    .drop("_ingested_at", "_source_file")
)

# COMMAND ----------

# Data quality checks (equivalent to DLT @expect)
total = silver_customer.count()
null_id = silver_customer.filter(F.col("unique_customer_identifier").isNull()).count()
null_date = silver_customer.filter(F.col("datevalue").isNull()).count()
negative_speed = silver_customer.filter(F.col("speed") <= 0).count()
negative_tenure = silver_customer.filter(F.col("tenure_days") < 0).count()

print("Silver Customer Info — Quality Checks:")
print(f"  Total rows:           {total:>12,}")
print(f"  Null customer_id:     {null_id:>12,} {'PASS' if null_id == 0 else 'FAIL'}")
print(f"  Null datevalue:       {null_date:>12,} {'PASS' if null_date == 0 else 'FAIL'}")
print(f"  Negative speed:       {negative_speed:>12,} {'PASS' if negative_speed == 0 else 'WARN'}")
print(f"  Negative tenure:      {negative_tenure:>12,} {'PASS' if negative_tenure == 0 else 'WARN'}")

# COMMAND ----------

silver_customer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.silver.customer_info")

print(f"silver.customer_info: {spark.table('uk_telecoms.silver.customer_info').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Silver: Usage Daily (83M rows)
# MAGIC
# MAGIC **Critical fix:** `usage_download_mbs` and `usage_upload_mbs` are stored as STRING. Cast to DOUBLE.
# MAGIC
# MAGIC **Transforms:**
# MAGIC - Cast string usage columns to DOUBLE
# MAGIC - Cast `calendar_date` to DateType
# MAGIC - Derive `usage_total_mbs` and `month_key`
# MAGIC - Partition by `month_key` for query performance

# COMMAND ----------

df_usage = spark.table("uk_telecoms.bronze.raw_usage")

silver_usage_daily = (
    df_usage
    # Critical: cast string → double
    .withColumn("usage_download_mbs", F.col("usage_download_mbs").cast(DoubleType()))
    .withColumn("usage_upload_mbs", F.col("usage_upload_mbs").cast(DoubleType()))
    .withColumn("calendar_date", F.col("calendar_date").cast(DateType()))

    # Derived columns
    .withColumn("usage_total_mbs",
        F.col("usage_download_mbs") + F.col("usage_upload_mbs")
    )
    .withColumn("month_key", F.date_trunc("month", F.col("calendar_date")))

    .drop("_ingested_at", "_source_file")
)

# COMMAND ----------

# Quality checks
total_usage = silver_usage_daily.count()
null_download = silver_usage_daily.filter(F.col("usage_download_mbs").isNull()).count()
null_upload = silver_usage_daily.filter(F.col("usage_upload_mbs").isNull()).count()
negative_dl = silver_usage_daily.filter(F.col("usage_download_mbs") < 0).count()

print("Silver Usage Daily — Quality Checks:")
print(f"  Total rows:           {total_usage:>12,}")
print(f"  Null download (cast): {null_download:>12,} {'PASS' if null_download == 0 else 'WARN — some strings failed cast'}")
print(f"  Null upload (cast):   {null_upload:>12,} {'PASS' if null_upload == 0 else 'WARN'}")
print(f"  Negative download:    {negative_dl:>12,} {'PASS' if negative_dl == 0 else 'WARN'}")

# COMMAND ----------

silver_usage_daily.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("month_key") \
    .saveAsTable("uk_telecoms.silver.usage_daily")

print(f"silver.usage_daily: {spark.table('uk_telecoms.silver.usage_daily').count():,} rows (partitioned by month_key)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Silver: Usage Monthly (Pre-Aggregated)
# MAGIC
# MAGIC Aggregate daily → monthly to match the `customer_info` snapshot grain.
# MAGIC This reduces 83M rows → ~3.5M rows and is the primary usage table for feature engineering.

# COMMAND ----------

silver_usage_monthly = (
    spark.table("uk_telecoms.silver.usage_daily")
    .groupBy("unique_customer_identifier", "month_key")
    .agg(
        # Volume metrics
        F.sum("usage_download_mbs").alias("monthly_download_mb"),
        F.sum("usage_upload_mbs").alias("monthly_upload_mb"),
        F.sum("usage_total_mbs").alias("monthly_total_mb"),

        # Daily averages
        F.mean("usage_download_mbs").alias("avg_daily_download_mb"),
        F.mean("usage_upload_mbs").alias("avg_daily_upload_mb"),
        F.mean("usage_total_mbs").alias("avg_daily_total_mb"),

        # Volatility (standard deviation of daily usage)
        F.stddev("usage_total_mbs").alias("std_daily_total_mb"),

        # Activity metrics
        F.count("*").alias("active_days_in_month"),

        # Peak usage
        F.max("usage_total_mbs").alias("peak_daily_total_mb"),
        F.min("usage_total_mbs").alias("min_daily_total_mb"),
    )
)

silver_usage_monthly.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.silver.usage_monthly")

print(f"silver.usage_monthly: {spark.table('uk_telecoms.silver.usage_monthly').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Silver: Calls
# MAGIC
# MAGIC **Transforms:**
# MAGIC - Cast `event_date` to DateType
# MAGIC - Impute null `call_type` as "Unknown"
# MAGIC - Derive call type flag columns (Loyalty, Complaints, Tech)

# COMMAND ----------

df_calls = spark.table("uk_telecoms.bronze.raw_calls")

silver_calls = (
    df_calls
    .withColumn("event_date", F.col("event_date").cast(DateType()))

    # Impute null call_type
    .withColumn("call_type",
        F.when(F.col("call_type").isNull(), F.lit("Unknown"))
         .otherwise(F.col("call_type"))
    )

    # High-signal call type flags (used in feature engineering)
    .withColumn("is_loyalty_call",
        F.when(F.col("call_type") == "Loyalty", 1).otherwise(0)
    )
    .withColumn("is_complaint_call",
        F.when(F.col("call_type") == "Complaints", 1).otherwise(0)
    )
    .withColumn("is_tech_call",
        F.when(F.col("call_type") == "Tech", 1).otherwise(0)
    )

    .drop("_ingested_at", "_source_file")
)

# COMMAND ----------

# Quality checks
total_calls = silver_calls.count()
null_type_after = silver_calls.filter(F.col("call_type").isNull()).count()
null_date = silver_calls.filter(F.col("event_date").isNull()).count()

print("Silver Calls — Quality Checks:")
print(f"  Total rows:            {total_calls:>12,}")
print(f"  Null call_type (post): {null_type_after:>12,} {'PASS' if null_type_after == 0 else 'FAIL'}")
print(f"  Null event_date:       {null_date:>12,} {'PASS' if null_date == 0 else 'FAIL'}")

# COMMAND ----------

silver_calls.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.silver.calls")

print(f"silver.calls: {spark.table('uk_telecoms.silver.calls').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Silver: Cease (Target Source)
# MAGIC
# MAGIC **Transforms:**
# MAGIC - Cast dates to DateType
# MAGIC - Derive `is_completed` flag
# MAGIC - Derive `cease_month` for temporal analysis

# COMMAND ----------

df_cease = spark.table("uk_telecoms.bronze.raw_cease")

silver_cease = (
    df_cease
    .withColumn("cease_placed_date", F.col("cease_placed_date").cast(DateType()))
    .withColumn("cease_completed_date", F.col("cease_completed_date").cast(DateType()))

    # Was this cease actually completed, or pending/retracted?
    .withColumn("is_completed",
        F.when(F.col("cease_completed_date").isNotNull(), 1).otherwise(0)
    )

    # Month-level grouping for trend analysis
    .withColumn("cease_month", F.date_trunc("month", F.col("cease_placed_date")))

    .drop("_ingested_at", "_source_file")
)

# COMMAND ----------

# Quality checks
total_cease = silver_cease.count()
null_placed = silver_cease.filter(F.col("cease_placed_date").isNull()).count()
completed = silver_cease.filter("is_completed = 1").count()
pending = silver_cease.filter("is_completed = 0").count()

print("Silver Cease — Quality Checks:")
print(f"  Total rows:          {total_cease:>12,}")
print(f"  Null placed_date:    {null_placed:>12,} {'PASS' if null_placed == 0 else 'FAIL'}")
print(f"  Completed ceases:    {completed:>12,}")
print(f"  Pending/retracted:   {pending:>12,}")

# COMMAND ----------

silver_cease.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.silver.cease")

print(f"silver.cease: {spark.table('uk_telecoms.silver.cease').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Optimize Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER for fast lookups on primary query patterns
# MAGIC OPTIMIZE uk_telecoms.silver.customer_info ZORDER BY (unique_customer_identifier, datevalue);
# MAGIC OPTIMIZE uk_telecoms.silver.usage_monthly ZORDER BY (unique_customer_identifier, month_key);
# MAGIC OPTIMIZE uk_telecoms.silver.calls ZORDER BY (unique_customer_identifier, event_date);
# MAGIC OPTIMIZE uk_telecoms.silver.cease ZORDER BY (unique_customer_identifier, cease_placed_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Silver Layer Summary

# COMMAND ----------

silver_tables = [
    "uk_telecoms.silver.customer_info",
    "uk_telecoms.silver.usage_daily",
    "uk_telecoms.silver.usage_monthly",
    "uk_telecoms.silver.calls",
    "uk_telecoms.silver.cease",
]

print("Silver Layer — Final Row Counts:")
print("-" * 55)
for table in silver_tables:
    count = spark.table(table).count()
    print(f"  {table:<45} {count:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [03_gold_features]($./03_gold_features) — Feature engineering & Feature Store registration
