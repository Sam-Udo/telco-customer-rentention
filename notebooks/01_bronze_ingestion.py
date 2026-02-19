# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion to Delta
# MAGIC
# MAGIC **Purpose:** Load raw source files into Delta tables with zero transformations. Schema-on-read. This is the immutable audit layer.
# MAGIC
# MAGIC **Design Pattern:** Direct bulk load (assessment). Production would use Auto Loader via DLT for incremental ingestion.
# MAGIC
# MAGIC **Output:** `uk_telecoms.bronze.{raw_customer_info, raw_usage, raw_calls, raw_cease}`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Setup — Catalog & Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Catalog already exists (created via Terraform with managed storage location)
# MAGIC -- Skip CREATE CATALOG to avoid metastore root storage error
# MAGIC USE CATALOG uk_telecoms;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw data — no transformations';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned, typed, validated data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Feature-engineered, model-ready data';
# MAGIC CREATE SCHEMA IF NOT EXISTS ml     COMMENT 'Models, experiments, scoring outputs';

# COMMAND ----------

# Source data location — shared landing storage (same for all environments)
LANDING_STORAGE = spark.conf.get("spark.telco.landing_storage", "telcochurnsalanding")
LANDING_PATH = f"abfss://landing@{LANDING_STORAGE}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Ingest: Customer Info (Parquet → Delta)

# COMMAND ----------

df_customer_raw = (
    spark.read
    .format("parquet")
    .load(f"{LANDING_PATH}/customer_info.parquet")
)

# Add ingestion metadata
from pyspark.sql import functions as F

df_customer_raw = (
    df_customer_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("customer_info.parquet"))
)

df_customer_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.bronze.raw_customer_info")

row_count = spark.table("uk_telecoms.bronze.raw_customer_info").count()
print(f"bronze.raw_customer_info: {row_count:,} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Ingest: Usage (Parquet → Delta) — 83M rows, 3.1 GB
# MAGIC
# MAGIC Spark handles this natively. No chunking, no memory management. This is why we use Spark.

# COMMAND ----------

df_usage_raw = (
    spark.read
    .format("parquet")
    .load(f"{LANDING_PATH}/usage.parquet")
)

df_usage_raw = (
    df_usage_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("usage.parquet"))
)

df_usage_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.bronze.raw_usage")

row_count = spark.table("uk_telecoms.bronze.raw_usage").count()
print(f"bronze.raw_usage: {row_count:,} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Ingest: Calls (CSV → Delta)

# COMMAND ----------

df_calls_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{LANDING_PATH}/calls.csv")
)

df_calls_raw = (
    df_calls_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("calls.csv"))
)

df_calls_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.bronze.raw_calls")

row_count = spark.table("uk_telecoms.bronze.raw_calls").count()
print(f"bronze.raw_calls: {row_count:,} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Ingest: Cease (CSV → Delta)

# COMMAND ----------

df_cease_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{LANDING_PATH}/cease.csv")
)

df_cease_raw = (
    df_cease_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("cease.csv"))
)

df_cease_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.bronze.raw_cease")

row_count = spark.table("uk_telecoms.bronze.raw_cease").count()
print(f"bronze.raw_cease: {row_count:,} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Validation — Row Count Checks

# COMMAND ----------

expected = {
    "uk_telecoms.bronze.raw_customer_info": 3_545_538,
    "uk_telecoms.bronze.raw_usage": 83_185_050,
    "uk_telecoms.bronze.raw_calls": 628_437,
    "uk_telecoms.bronze.raw_cease": 146_363,
}

print("Bronze Layer Validation:")
print("-" * 60)
for table, expected_count in expected.items():
    actual = spark.table(table).count()
    status = "PASS" if actual == expected_count else "WARN"
    print(f"  [{status}] {table}: {actual:>12,} rows (expected {expected_count:>12,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Delta Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add table comments for documentation / Unity Catalog lineage
# MAGIC ALTER TABLE uk_telecoms.bronze.raw_customer_info SET TBLPROPERTIES (
# MAGIC   'quality' = 'bronze',
# MAGIC   'source' = 'customer_info.parquet',
# MAGIC   'grain' = 'customer x month',
# MAGIC   'description' = 'Raw monthly customer snapshots — 26 months of contract, product, and tenure data'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE uk_telecoms.bronze.raw_usage SET TBLPROPERTIES (
# MAGIC   'quality' = 'bronze',
# MAGIC   'source' = 'usage.parquet',
# MAGIC   'grain' = 'customer x day',
# MAGIC   'description' = 'Raw daily broadband usage — download/upload MBs (stored as strings in source)'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE uk_telecoms.bronze.raw_calls SET TBLPROPERTIES (
# MAGIC   'quality' = 'bronze',
# MAGIC   'source' = 'calls.csv',
# MAGIC   'grain' = 'call event',
# MAGIC   'description' = 'Raw contact center call records — type, talk time, hold time'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE uk_telecoms.bronze.raw_cease SET TBLPROPERTIES (
# MAGIC   'quality' = 'bronze',
# MAGIC   'source' = 'cease.csv',
# MAGIC   'grain' = 'cease event',
# MAGIC   'description' = 'Raw churn events — cease placed/completed dates with reason codes'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [02_silver_cleaning]($./02_silver_cleaning) — Clean, type, and validate data
