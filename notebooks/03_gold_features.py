# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Feature Engineering & Feature Store
# MAGIC
# MAGIC **Purpose:** Build the ML-ready feature table. Every feature is **point-in-time correct** — no data leakage.
# MAGIC Multi-horizon target labels (30/60/90 day churn windows).
# MAGIC
# MAGIC **Design Pattern:** Databricks Feature Store with Unity Catalog. Spine-based joins.
# MAGIC
# MAGIC **Output:**
# MAGIC - `uk_telecoms.gold.churn_features` (Feature Store table)
# MAGIC - `uk_telecoms.gold.churn_labels` (multi-horizon targets)
# MAGIC - `uk_telecoms.gold.churn_training_set` (features + labels + temporal split)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from databricks.feature_engineering import FeatureEngineeringClient

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

fe = FeatureEngineeringClient()

# Horizons for multi-horizon prediction
HORIZONS = [30, 60, 90]

# Temporal split boundaries
TRAIN_END = "2024-04-01"
VALIDATION_END = "2024-07-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Observation Spine
# MAGIC
# MAGIC One row per (customer, month). This is what we predict on: "At this month, will this customer churn within the next N days?"

# COMMAND ----------

spine = (
    spark.table("uk_telecoms.silver.customer_info")
    .select("unique_customer_identifier", "datevalue")
    .distinct()
)

print(f"Observation spine: {spine.count():,} rows (customer x month combinations)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Multi-Horizon Target Labels
# MAGIC
# MAGIC For each (customer, observation_date), compute:
# MAGIC - `churned_in_30d`: Did this customer place a cease within 30 days?
# MAGIC - `churned_in_60d`: Did this customer place a cease within 60 days?
# MAGIC - `churned_in_90d`: Did this customer place a cease within 90 days?

# COMMAND ----------

cease = spark.table("uk_telecoms.silver.cease")

# Start with the spine
labels = spine.select(
    F.col("unique_customer_identifier"),
    F.col("datevalue").alias("observation_date")
)

# Compute each horizon label
for days in HORIZONS:
    horizon_labels = (
        spine.alias("s")
        .join(
            cease.alias("c"),
            (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
            & (F.col("c.cease_placed_date") >= F.col("s.datevalue"))
            & (F.col("c.cease_placed_date") < F.date_add(F.col("s.datevalue"), days)),
            "left"
        )
        .groupBy(
            F.col("s.unique_customer_identifier").alias("unique_customer_identifier"),
            F.col("s.datevalue").alias("observation_date")
        )
        .agg(
            F.max(
                F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)
            ).alias(f"churned_in_{days}d")
        )
    )
    labels = labels.join(
        horizon_labels,
        ["unique_customer_identifier", "observation_date"],
        "left"
    )

labels.cache()

# COMMAND ----------

# Churn rate by horizon
for days in HORIZONS:
    col = f"churned_in_{days}d"
    rate = labels.agg(F.mean(col)).collect()[0][0]
    print(f"  {col}: {rate*100:.2f}% churn rate")

labels.write.format("delta").mode("overwrite").saveAsTable("uk_telecoms.gold.churn_labels")
print(f"\ngold.churn_labels: {labels.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Customer Snapshot Features
# MAGIC
# MAGIC Direct features from the Silver `customer_info` table for each observation point.

# COMMAND ----------

customer = spark.table("uk_telecoms.silver.customer_info")

customer_features = (
    customer
    .select(
        "unique_customer_identifier",
        F.col("datevalue").alias("observation_date"),

        # Contract features
        "contract_status_ord",
        "contract_dd_cancels",
        "dd_cancel_60_day",
        "ooc_days",

        # Product features
        "technology",
        "speed",
        "line_speed",
        "speed_gap_pct",

        # Channel
        "sales_channel",
        "crm_package_name",

        # Tenure
        "tenure_days",
        "tenure_bucket",
    )
)

print(f"Customer features: {customer_features.count():,} rows, {len(customer_features.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Call Behavior Features (Windowed Lookbacks)
# MAGIC
# MAGIC For each (customer, observation_date), compute call statistics from the **prior** 30/90/180 days.
# MAGIC
# MAGIC **Key:** All lookbacks use `event_date < observation_date` (strictly before) to prevent leakage.

# COMMAND ----------

calls = spark.table("uk_telecoms.silver.calls")

call_features = (
    spine.alias("s")
    .join(
        calls.alias("c"),
        (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
        & (F.col("c.event_date") < F.col("s.datevalue"))             # STRICTLY before observation
        & (F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 180)),  # within 180-day lookback
        "left"
    )
    .groupBy(
        F.col("s.unique_customer_identifier").alias("unique_customer_identifier"),
        F.col("s.datevalue").alias("observation_date")
    )
    .agg(
        # ── Call volume by window ──
        F.sum(F.when(
            F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 30), 1
        ).otherwise(0)).alias("calls_30d"),

        F.sum(F.when(
            F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 90), 1
        ).otherwise(0)).alias("calls_90d"),

        F.count("c.event_date").alias("calls_180d"),

        # ── Call type signals (90-day window) ──
        F.sum(F.when(
            (F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 90))
            & (F.col("c.is_loyalty_call") == 1), 1
        ).otherwise(0)).alias("loyalty_calls_90d"),

        F.sum(F.when(
            (F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 90))
            & (F.col("c.is_complaint_call") == 1), 1
        ).otherwise(0)).alias("complaint_calls_90d"),

        F.sum(F.when(
            (F.col("c.event_date") >= F.date_sub(F.col("s.datevalue"), 90))
            & (F.col("c.is_tech_call") == 1), 1
        ).otherwise(0)).alias("tech_calls_90d"),

        # ── Duration statistics ──
        F.mean("c.talk_time_seconds").alias("avg_talk_time"),
        F.mean("c.hold_time_seconds").alias("avg_hold_time"),
        F.max("c.hold_time_seconds").alias("max_hold_time"),

        # ── Recency: days since last call ──
        F.datediff(
            F.col("s.datevalue"),
            F.max("c.event_date")
        ).alias("days_since_last_call"),
    )
)

call_features.cache()
print(f"Call features: {call_features.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Usage Features (Monthly Aggregates + Trends)
# MAGIC
# MAGIC Current month usage + month-over-month trend + 3-month average comparison.

# COMMAND ----------

usage = spark.table("uk_telecoms.silver.usage_monthly")

# ── Current month usage (join on matching month) ──
usage_current = (
    spine.alias("s")
    .join(
        usage.alias("u"),
        (F.col("s.unique_customer_identifier") == F.col("u.unique_customer_identifier"))
        & (F.col("u.month_key") == F.col("s.datevalue")),
        "left"
    )
    .select(
        F.col("s.unique_customer_identifier").alias("unique_customer_identifier"),
        F.col("s.datevalue").alias("observation_date"),
        F.col("u.monthly_download_mb"),
        F.col("u.monthly_upload_mb"),
        F.col("u.monthly_total_mb"),
        F.col("u.avg_daily_download_mb"),
        F.col("u.std_daily_total_mb"),
        F.col("u.active_days_in_month"),
        F.col("u.peak_daily_total_mb"),
    )
)

# COMMAND ----------

# ── Usage trends (MoM change + 3-month average comparison) ──
w_customer = Window.partitionBy("unique_customer_identifier").orderBy("month_key")

usage_with_trends = (
    usage
    # Previous month total
    .withColumn("prev_month_total",
        F.lag("monthly_total_mb", 1).over(w_customer)
    )
    # Rolling 3-month average (prior 3 months, excluding current)
    .withColumn("prev_3mo_avg_total",
        F.avg("monthly_total_mb").over(
            w_customer.rowsBetween(-3, -1)
        )
    )
    # Month-over-month change
    .withColumn("usage_mom_change",
        F.when(F.col("prev_month_total") > 0,
            (F.col("monthly_total_mb") - F.col("prev_month_total")) / F.col("prev_month_total")
        ).otherwise(F.lit(None))
    )
    # Comparison vs 3-month average (declining = disengagement signal)
    .withColumn("usage_vs_3mo_avg",
        F.when(F.col("prev_3mo_avg_total") > 0,
            (F.col("monthly_total_mb") - F.col("prev_3mo_avg_total")) / F.col("prev_3mo_avg_total")
        ).otherwise(F.lit(None))
    )
    .select(
        "unique_customer_identifier",
        F.col("month_key").alias("observation_date"),
        "usage_mom_change",
        "usage_vs_3mo_avg",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Cease History Features (Prior Churn Signals)
# MAGIC
# MAGIC Customers who have previously placed a cease are structurally more likely to churn again.

# COMMAND ----------

cease_history = (
    spine.alias("s")
    .join(
        cease.alias("c"),
        (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
        & (F.col("c.cease_placed_date") < F.col("s.datevalue")),  # only PRIOR ceases
        "left"
    )
    .groupBy(
        F.col("s.unique_customer_identifier").alias("unique_customer_identifier"),
        F.col("s.datevalue").alias("observation_date")
    )
    .agg(
        F.count("c.cease_placed_date").alias("prior_cease_count"),
        F.datediff(
            F.col("s.datevalue"),
            F.max("c.cease_placed_date")
        ).alias("days_since_last_cease"),
    )
)

print(f"Cease history features: {cease_history.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 Assemble Full Feature Table

# COMMAND ----------

feature_table = (
    customer_features
    .join(call_features, ["unique_customer_identifier", "observation_date"], "left")
    .join(usage_current, ["unique_customer_identifier", "observation_date"], "left")
    .join(usage_with_trends, ["unique_customer_identifier", "observation_date"], "left")
    .join(cease_history, ["unique_customer_identifier", "observation_date"], "left")
    .dropDuplicates(["unique_customer_identifier", "observation_date"])
)

feature_table.cache()
total_features = len(feature_table.columns) - 2  # exclude ID + observation_date
print(f"Feature table: {feature_table.count():,} rows x {total_features} features")
print(f"Columns: {feature_table.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.9 Register in Feature Store (Unity Catalog)

# COMMAND ----------

# Create or overwrite the Feature Store table
try:
    fe.create_table(
        name="uk_telecoms.gold.churn_features",
        primary_keys=["unique_customer_identifier", "observation_date"],
        df=feature_table,
        description="Telco churn prediction features — point-in-time correct, multi-horizon compatible"
    )
    print("Feature Store table CREATED: uk_telecoms.gold.churn_features")
except Exception as e:
    if "already exists" in str(e):
        fe.write_table(
            name="uk_telecoms.gold.churn_features",
            df=feature_table,
            mode="overwrite"
        )
        print("Feature Store table UPDATED: uk_telecoms.gold.churn_features")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.10 Build Training Set (Features + Labels + Temporal Split)

# COMMAND ----------

labels = spark.table("uk_telecoms.gold.churn_labels")

training_set = (
    feature_table
    .join(labels, ["unique_customer_identifier", "observation_date"], "inner")
    .withColumn("split",
        F.when(F.col("observation_date") < TRAIN_END, "train")
         .when(F.col("observation_date") < VALIDATION_END, "validation")
         .otherwise("test")
    )
)

training_set.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.gold.churn_training_set")

# COMMAND ----------

# Split distribution
display(
    training_set
    .groupBy("split")
    .agg(
        F.count("*").alias("rows"),
        F.mean("churned_in_30d").alias("churn_rate_30d"),
        F.mean("churned_in_60d").alias("churn_rate_60d"),
        F.mean("churned_in_90d").alias("churn_rate_90d"),
    )
    .orderBy("split")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.11 Optimize Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE uk_telecoms.gold.churn_features ZORDER BY (unique_customer_identifier, observation_date);
# MAGIC OPTIMIZE uk_telecoms.gold.churn_training_set ZORDER BY (observation_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.12 Gold Layer Summary

# COMMAND ----------

gold_tables = [
    "uk_telecoms.gold.churn_features",
    "uk_telecoms.gold.churn_labels",
    "uk_telecoms.gold.churn_training_set",
]

print("Gold Layer — Final Summary:")
print("-" * 55)
for table in gold_tables:
    count = spark.table(table).count()
    cols = len(spark.table(table).columns)
    print(f"  {table:<45} {count:>10,} rows x {cols} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [04_model_training]($./04_model_training) — Train multi-horizon LightGBM models with MLflow
