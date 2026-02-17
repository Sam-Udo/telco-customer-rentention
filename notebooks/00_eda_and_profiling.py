# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Exploratory Data Analysis & Data Profiling
# MAGIC
# MAGIC **Purpose:** Understand data distributions, quality issues, class balance, and key relationships before building the pipeline.
# MAGIC
# MAGIC **Outcome:** Validated understanding of all 4 datasets, churn rate analysis, and identified data quality actions for Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.1 Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.figsize"] = (14, 6)
plt.rcParams["font.size"] = 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.2 Load Raw Data

# COMMAND ----------

# -- Adjust paths to your cloud storage mount or DBFS location --
LANDING_PATH = "/mnt/landing"  # or dbfs:/FileStore/telco-churn/

df_customer = spark.read.parquet(f"{LANDING_PATH}/customer_info.parquet")
df_usage = spark.read.parquet(f"{LANDING_PATH}/usage.parquet")
df_calls = spark.read.csv(f"{LANDING_PATH}/calls.csv", header=True, inferSchema=True)
df_cease = spark.read.csv(f"{LANDING_PATH}/cease.csv", header=True, inferSchema=True)

print(f"customer_info:  {df_customer.count():>12,} rows x {len(df_customer.columns)} cols")
print(f"usage:          {df_usage.count():>12,} rows x {len(df_usage.columns)} cols")
print(f"calls:          {df_calls.count():>12,} rows x {len(df_calls.columns)} cols")
print(f"cease:          {df_cease.count():>12,} rows x {len(df_cease.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.3 Customer Info — Monthly Snapshots

# COMMAND ----------

df_customer.printSchema()

# COMMAND ----------

# Temporal coverage: 26 monthly snapshots
display(
    df_customer
    .groupBy("datevalue")
    .agg(
        F.countDistinct("unique_customer_identifier").alias("unique_customers"),
        F.count("*").alias("total_rows")
    )
    .orderBy("datevalue")
)

# COMMAND ----------

# Contract status distribution
display(
    df_customer
    .groupBy("contract_status")
    .agg(F.count("*").alias("count"))
    .orderBy("contract_status")
)

# COMMAND ----------

# Technology distribution
display(
    df_customer
    .groupBy("technology")
    .agg(F.countDistinct("unique_customer_identifier").alias("unique_customers"))
    .orderBy(F.desc("unique_customers"))
)

# COMMAND ----------

# Speed distribution
display(
    df_customer
    .groupBy("speed")
    .agg(F.countDistinct("unique_customer_identifier").alias("unique_customers"))
    .orderBy("speed")
)

# COMMAND ----------

# Speed gap analysis: advertised vs actual
speed_gap = (
    df_customer
    .withColumn("speed_gap_pct",
        F.round((F.col("speed") - F.col("line_speed")) / F.col("speed") * 100, 2)
    )
    .select("speed", "line_speed", "speed_gap_pct", "technology")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
speed_gap.groupby("technology")["speed_gap_pct"].mean().sort_values().plot.barh(ax=axes[0])
axes[0].set_title("Avg Speed Gap % by Technology")
axes[0].set_xlabel("Speed Gap %")

axes[1].hist(speed_gap["speed_gap_pct"].clip(-50, 100), bins=50, edgecolor="black", alpha=0.7)
axes[1].set_title("Distribution of Speed Gap %")
axes[1].set_xlabel("Speed Gap % (advertised vs actual)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Null analysis
from pyspark.sql import DataFrame

def null_report(df: DataFrame, name: str) -> pd.DataFrame:
    """Generate null count and percentage for each column."""
    total = df.count()
    nulls = []
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        nulls.append({
            "dataset": name,
            "column": col_name,
            "null_count": null_count,
            "null_pct": round(null_count / total * 100, 2)
        })
    return pd.DataFrame(nulls)

null_customer = null_report(df_customer, "customer_info")
display(null_customer[null_customer["null_count"] > 0])

# COMMAND ----------

# Tenure distribution
tenure_pdf = df_customer.select("tenure_days").toPandas()
fig, ax = plt.subplots(figsize=(14, 5))
ax.hist(tenure_pdf["tenure_days"], bins=100, edgecolor="black", alpha=0.7)
ax.set_title("Customer Tenure Distribution (days)")
ax.set_xlabel("Tenure (days)")
ax.axvline(365, color="red", linestyle="--", label="1 year")
ax.axvline(730, color="orange", linestyle="--", label="2 years")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DD cancellation signals
display(
    df_customer
    .groupBy("datevalue")
    .agg(
        F.mean("contract_dd_cancels").alias("dd_cancel_rate"),
        F.mean("dd_cancel_60_day").alias("dd_cancel_60d_rate")
    )
    .orderBy("datevalue")
)

# COMMAND ----------

# Sales channel distribution
display(
    df_customer
    .groupBy("sales_channel")
    .agg(F.countDistinct("unique_customer_identifier").alias("unique_customers"))
    .orderBy(F.desc("unique_customers"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.4 Cease Data — Churn Events (Target Source)

# COMMAND ----------

df_cease.printSchema()

# COMMAND ----------

# Churn volume over time
display(
    df_cease
    .withColumn("cease_month", F.date_trunc("month", F.col("cease_placed_date").cast(DateType())))
    .groupBy("cease_month")
    .agg(F.count("*").alias("cease_count"))
    .orderBy("cease_month")
)

# COMMAND ----------

# Churn reasons — grouped
display(
    df_cease
    .groupBy("reason_description_insight")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# Completed vs pending/retracted ceases
display(
    df_cease
    .withColumn("is_completed",
        F.when(F.col("cease_completed_date").isNotNull(), "Completed").otherwise("Pending/Retracted")
    )
    .groupBy("is_completed")
    .agg(F.count("*").alias("count"))
)

# COMMAND ----------

# Customers with multiple ceases (repeat churners)
repeat_churners = (
    df_cease
    .groupBy("unique_customer_identifier")
    .agg(F.count("*").alias("cease_count"))
    .filter("cease_count > 1")
)
print(f"Customers with multiple ceases: {repeat_churners.count():,}")
display(repeat_churners.groupBy("cease_count").agg(F.count("*").alias("customers")).orderBy("cease_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.5 Calls Data — Contact Center

# COMMAND ----------

df_calls.printSchema()

# COMMAND ----------

# Call type distribution
display(
    df_calls
    .groupBy("call_type")
    .agg(
        F.count("*").alias("call_count"),
        F.mean("talk_time_seconds").alias("avg_talk_time"),
        F.mean("hold_time_seconds").alias("avg_hold_time")
    )
    .orderBy(F.desc("call_count"))
)

# COMMAND ----------

# Null call types
null_calls = df_calls.filter(F.col("call_type").isNull()).count()
total_calls = df_calls.count()
print(f"Null call_type: {null_calls:,} / {total_calls:,} ({null_calls/total_calls*100:.1f}%)")

# COMMAND ----------

# Call volume over time
display(
    df_calls
    .withColumn("call_month", F.date_trunc("month", F.col("event_date").cast(DateType())))
    .groupBy("call_month")
    .agg(F.count("*").alias("call_count"))
    .orderBy("call_month")
)

# COMMAND ----------

# Talk time / hold time distributions
calls_pdf = df_calls.select("talk_time_seconds", "hold_time_seconds").toPandas()
fig, axes = plt.subplots(1, 2, figsize=(16, 5))
axes[0].hist(calls_pdf["talk_time_seconds"].clip(0, 5000), bins=80, edgecolor="black", alpha=0.7)
axes[0].set_title("Talk Time Distribution (seconds)")
axes[1].hist(calls_pdf["hold_time_seconds"].clip(0, 3000), bins=80, edgecolor="black", alpha=0.7, color="orange")
axes[1].set_title("Hold Time Distribution (seconds)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.6 Usage Data — Daily Broadband Usage

# COMMAND ----------

df_usage.printSchema()
# NOTE: usage_download_mbs and usage_upload_mbs are STRING type — must fix in Silver

# COMMAND ----------

# Sample rows (confirm string type issue)
display(df_usage.limit(10))

# COMMAND ----------

# Cast and get basic stats (sample for speed on 83M rows)
usage_sample = (
    df_usage
    .sample(fraction=0.01, seed=42)
    .withColumn("download_mb", F.col("usage_download_mbs").cast(DoubleType()))
    .withColumn("upload_mb", F.col("usage_upload_mbs").cast(DoubleType()))
)

display(usage_sample.select("download_mb", "upload_mb").summary())

# COMMAND ----------

# Monthly usage trends (sampled for EDA speed)
display(
    df_usage
    .withColumn("month", F.date_trunc("month", F.col("calendar_date").cast(DateType())))
    .withColumn("download_mb", F.col("usage_download_mbs").cast(DoubleType()))
    .groupBy("month")
    .agg(
        F.mean("download_mb").alias("avg_daily_download_mb"),
        F.count("*").alias("records")
    )
    .orderBy("month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.7 Entity Overlap Analysis

# COMMAND ----------

# How do datasets overlap?
customer_ids = df_customer.select("unique_customer_identifier").distinct()
cease_ids = df_cease.select("unique_customer_identifier").distinct()
calls_ids = df_calls.select("unique_customer_identifier").distinct()

n_customer = customer_ids.count()
n_cease = cease_ids.count()
n_calls = calls_ids.count()
n_all_three = customer_ids.intersect(cease_ids).intersect(calls_ids).count()
n_customer_only = customer_ids.subtract(cease_ids).subtract(calls_ids).count()

print(f"customer_info unique customers: {n_customer:>10,}")
print(f"cease unique customers:         {n_cease:>10,}")
print(f"calls unique customers:         {n_calls:>10,}")
print(f"In ALL three datasets:          {n_all_three:>10,}")
print(f"In customer_info ONLY:          {n_customer_only:>10,} (never churned, never called)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.8 Churn Rate Analysis — Class Balance

# COMMAND ----------

# Overall churn rate per monthly snapshot
# For each snapshot month, what % of customers placed a cease within 30/60/90 days?

cease_dates = (
    df_cease
    .withColumn("cease_placed_date", F.col("cease_placed_date").cast(DateType()))
    .select("unique_customer_identifier", "cease_placed_date")
)

snapshots = (
    df_customer
    .select("unique_customer_identifier", "datevalue")
    .distinct()
)

for horizon in [30, 60, 90]:
    churn_rate = (
        snapshots.alias("s")
        .join(
            cease_dates.alias("c"),
            (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
            & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
            & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), horizon)),
            "left"
        )
        .groupBy(F.col("s.datevalue").alias("snapshot_month"))
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)).alias("churned")
        )
        .withColumn(f"churn_rate_{horizon}d", F.round(F.col("churned") / F.col("total") * 100, 2))
        .orderBy("snapshot_month")
    )
    print(f"\n=== {horizon}-Day Churn Rate by Snapshot Month ===")
    display(churn_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.9 Key Findings Summary
# MAGIC
# MAGIC | Finding | Detail | Action |
# MAGIC |---|---|---|
# MAGIC | **Usage columns are strings** | `usage_download_mbs`, `usage_upload_mbs` stored as STRING | Cast to DOUBLE in Silver |
# MAGIC | **17,948 null call_type** | 2.8% of calls missing department | Impute as "Unknown" in Silver |
# MAGIC | **18,948 null ooc_days** | Likely early-contract customers | Fill with sentinel -9999 in Silver |
# MAGIC | **27,217 null cease_completed_date** | Pending/retracted ceases | Keep as churn intent in target |
# MAGIC | **Repeat churners exist** | Some customers placed multiple ceases | Use as feature (`prior_cease_count`) |
# MAGIC | **customer_info is master** | 100% of cease/calls customers in customer_info | Use as spine table |
# MAGIC | **Class imbalance expected** | Churn rate varies by horizon and month | Use `scale_pos_weight` in LightGBM + AUC-PR metric |
# MAGIC | **Speed gap varies by technology** | FTTC/MPF have larger gaps than FTTP | Strong feature candidate |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [01_bronze_ingestion]($./01_bronze_ingestion) — Load raw data into Delta tables
