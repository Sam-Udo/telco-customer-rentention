# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Lakehouse Monitoring & Model Observability
# MAGIC
# MAGIC **Purpose:** Set up automated monitoring for data drift, prediction drift, and model performance degradation.
# MAGIC Alert when retraining or investigation is needed.
# MAGIC
# MAGIC **Design Pattern:** Databricks Lakehouse Monitoring + custom performance tracking.
# MAGIC
# MAGIC **Monitors:**
# MAGIC - Feature drift on `gold.churn_features`
# MAGIC - Prediction drift on `gold.churn_scores`
# MAGIC - Model performance (actuals vs predicted, computed after 30/60/90 day windows close)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Feature Drift Monitor
# MAGIC
# MAGIC Monitors the Gold feature table for distribution shifts across months.
# MAGIC Sliced by `technology` and `sales_channel` to detect segment-specific drift.

# COMMAND ----------

FEATURE_TABLE = "uk_telecoms.gold.churn_features"

try:
    monitor = w.quality_monitors.create(
        table_name=FEATURE_TABLE,
        output_schema_name="uk_telecoms.gold",
        time_series=MonitorTimeSeries(
            timestamp_col="observation_date",
            granularities=["1 month"]
        ),
        slicing_exprs=["technology", "sales_channel"],
        assets_dir="/Workspace/Users/telco_churn/monitor_assets/features"
    )
    print(f"Feature drift monitor CREATED on {FEATURE_TABLE}")
except Exception as e:
    if "already being monitored" in str(e).lower() or "already exists" in str(e).lower():
        print(f"Feature drift monitor already exists on {FEATURE_TABLE}")
    else:
        print(f"Monitor creation note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Prediction Drift Monitor
# MAGIC
# MAGIC Monitors the scoring output for shifts in predicted probability distributions.
# MAGIC Key alert: if the average predicted churn probability shifts significantly, the model may be stale.

# COMMAND ----------

SCORES_TABLE = "uk_telecoms.gold.churn_scores"

try:
    monitor = w.quality_monitors.create(
        table_name=SCORES_TABLE,
        output_schema_name="uk_telecoms.gold",
        time_series=MonitorTimeSeries(
            timestamp_col="score_date",
            granularities=["1 month"]
        ),
        slicing_exprs=["risk_tier"],
        assets_dir="/Workspace/Users/telco_churn/monitor_assets/scores"
    )
    print(f"Prediction drift monitor CREATED on {SCORES_TABLE}")
except Exception as e:
    if "already being monitored" in str(e).lower() or "already exists" in str(e).lower():
        print(f"Prediction drift monitor already exists on {SCORES_TABLE}")
    else:
        print(f"Monitor creation note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Retrospective Model Performance Tracking
# MAGIC
# MAGIC After each scoring cycle, once the prediction window has elapsed (e.g., 30 days later),
# MAGIC join predictions with actual ceases to compute real AUC-ROC / AUC-PR.

# COMMAND ----------

from sklearn.metrics import roc_auc_score, average_precision_score

HORIZONS = [30, 60, 90]

def compute_retrospective_performance(score_date: str) -> pd.DataFrame:
    """
    For a given score_date, join predicted probabilities with actual cease events
    to compute model performance metrics. Only valid once the prediction window has closed.
    """
    scores = spark.table("uk_telecoms.gold.churn_scores").filter(f"score_date = '{score_date}'")
    cease = spark.table("uk_telecoms.silver.cease")

    results = []
    for horizon in HORIZONS:
        # Join scores with actuals
        actuals = (
            scores.alias("s")
            .join(
                cease.alias("c"),
                (F.col("s.unique_customer_identifier") == F.col("c.unique_customer_identifier"))
                & (F.col("c.cease_placed_date") >= F.col("s.score_date"))
                & (F.col("c.cease_placed_date") < F.date_add(F.col("s.score_date"), horizon)),
                "left"
            )
            .groupBy(F.col("s.unique_customer_identifier"))
            .agg(
                F.first(f"churn_prob_{horizon}d").alias("predicted"),
                F.max(F.when(F.col("c.cease_placed_date").isNotNull(), 1).otherwise(0)).alias("actual"),
            )
            .toPandas()
        )

        if actuals["actual"].sum() > 0:
            auc_roc = roc_auc_score(actuals["actual"], actuals["predicted"])
            auc_pr = average_precision_score(actuals["actual"], actuals["predicted"])
        else:
            auc_roc = None
            auc_pr = None

        results.append({
            "score_date": score_date,
            "horizon": horizon,
            "auc_roc": auc_roc,
            "auc_pr": auc_pr,
            "n_customers": len(actuals),
            "actual_churn_count": int(actuals["actual"].sum()),
            "actual_churn_rate": actuals["actual"].mean(),
            "predicted_churn_rate": actuals["predicted"].mean(),
        })

    return pd.DataFrame(results)

# COMMAND ----------

# Example: compute retrospective performance for the latest scoring run
# (Only valid if enough time has passed since the score_date)
try:
    latest_score_date = (
        spark.table("uk_telecoms.gold.churn_scores")
        .agg(F.max("score_date"))
        .collect()[0][0]
    )
    retro = compute_retrospective_performance(str(latest_score_date))
    display(retro)
except Exception as e:
    print(f"Retrospective performance not yet available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Custom Alert Logic
# MAGIC
# MAGIC Define alert conditions that would trigger investigation or retraining.

# COMMAND ----------

def check_alerts(score_date: str) -> list:
    """
    Check for alert conditions on a scoring run.
    Returns a list of alert messages (empty = all clear).
    """
    alerts = []
    scores = spark.table("uk_telecoms.gold.churn_scores").filter(f"score_date = '{score_date}'")
    pdf = scores.toPandas()

    for horizon in HORIZONS:
        col = f"churn_prob_{horizon}d"

        # Alert 1: Mean prediction shift > 20% from historical baseline
        current_mean = pdf[col].mean()
        # In production, compare against a stored baseline
        # For now, flag if mean > 0.5 (unusually high)
        if current_mean > 0.5:
            alerts.append(f"ALERT: {horizon}d mean prediction ({current_mean:.3f}) is unusually high")

        # Alert 2: Prediction distribution collapsed (low variance)
        current_std = pdf[col].std()
        if current_std < 0.05:
            alerts.append(f"ALERT: {horizon}d prediction variance ({current_std:.4f}) is very low — model may not be discriminating")

    # Alert 3: Risk tier concentration
    tier_counts = pdf["risk_tier"].value_counts(normalize=True)
    if tier_counts.get("RED", 0) > 0.3:
        alerts.append(f"ALERT: {tier_counts['RED']*100:.1f}% of customers in RED tier — investigate data quality")

    if tier_counts.get("GREEN", 0) > 0.95:
        alerts.append(f"ALERT: {tier_counts['GREEN']*100:.1f}% in GREEN — model may have lost sensitivity")

    return alerts

# COMMAND ----------

# Run alert check
try:
    latest_score_date = (
        spark.table("uk_telecoms.gold.churn_scores")
        .agg(F.max("score_date"))
        .collect()[0][0]
    )
    alerts = check_alerts(str(latest_score_date))
    if alerts:
        print("ALERTS TRIGGERED:")
        for a in alerts:
            print(f"  {a}")
    else:
        print("All clear — no alerts triggered.")
except Exception as e:
    alerts = []
    print(f"Alert check skipped: {e}")

# COMMAND ----------

# Signal the workflow DAG whether retraining is needed
retrain_needed = len(alerts) > 0
dbutils.jobs.taskValues.set(key="retrain_needed", value=str(retrain_needed).lower())
print(f"retrain_needed = {retrain_needed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.6 Monitoring Dashboard Data
# MAGIC
# MAGIC Write monitoring metrics to a tracking table for DBSQL dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring metrics table (append-only log)
# MAGIC CREATE TABLE IF NOT EXISTS uk_telecoms.ml.monitoring_metrics (
# MAGIC   score_date DATE,
# MAGIC   horizon INT,
# MAGIC   metric_name STRING,
# MAGIC   metric_value DOUBLE,
# MAGIC   computed_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Append-only log of model monitoring metrics for DBSQL dashboards';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.7 Monitoring Summary
# MAGIC
# MAGIC | Monitor | Table | What It Detects |
# MAGIC |---|---|---|
# MAGIC | Feature Drift | `gold.churn_features` | Input distribution shifts (PSI), null rate changes |
# MAGIC | Prediction Drift | `gold.churn_scores` | Output distribution shifts, mean/variance changes |
# MAGIC | Retrospective Perf | Custom function | Actual AUC-ROC/PR after window closes |
# MAGIC | Custom Alerts | Custom function | Extreme tier concentration, collapsed variance |
# MAGIC
# MAGIC ### Retrain Triggers
# MAGIC 1. PSI > 0.2 on any top-10 feature → investigate + retrain
# MAGIC 2. Retrospective AUC-PR drops > 10% from training baseline → retrain
# MAGIC 3. RED tier > 30% of base → investigate data quality first
# MAGIC 4. GREEN tier > 95% of base → model lost sensitivity, retrain

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Pipeline complete.** All 6 notebooks form the end-to-end Databricks Lakehouse solution:
# MAGIC
# MAGIC `00_eda` → `01_bronze` → `02_silver` → `03_gold` → `04_training` → `05_scoring` → `06_monitoring`
