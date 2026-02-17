# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Batch Scoring: Multi-Horizon Risk Matrix
# MAGIC
# MAGIC **Purpose:** Score all active customers with the 3 Champion models (30d, 60d, 90d).
# MAGIC Produce a composite risk tier (RED/AMBER/YELLOW/GREEN) and tiered action lists for each team.
# MAGIC
# MAGIC **Design Pattern:** Load Champion models from UC → Score → Composite risk tier → Tiered output tables.
# MAGIC
# MAGIC **Output:**
# MAGIC - `uk_telecoms.gold.churn_scores` — Full scoring output with all 3 probabilities
# MAGIC - `uk_telecoms.gold.action_list_loyalty` — RED tier (immediate calls)
# MAGIC - `uk_telecoms.gold.action_list_marketing` — AMBER tier (digital campaign)
# MAGIC - `uk_telecoms.gold.action_list_customer_success` — YELLOW tier (proactive check-in)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Setup

# COMMAND ----------

import mlflow
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

mlflow.autolog(disable=True)
from dataclasses import dataclass

# Risk tier logic (inlined for Databricks workspace compatibility)
@dataclass
class RiskThresholds:
    red: float = 0.5
    amber: float = 0.5
    yellow: float = 0.5

def assign_risk_tier(prob_30d, prob_60d, prob_90d, thresholds=RiskThresholds()):
    if prob_30d > thresholds.red:
        return "RED"
    elif prob_60d > thresholds.amber:
        return "AMBER"
    elif prob_90d > thresholds.yellow:
        return "YELLOW"
    else:
        return "GREEN"

TIER_ACTIONS = {
    "RED": {"action": "Emergency outbound call", "owner": "Loyalty", "sla": "24h"},
    "AMBER": {"action": "Digital retention campaign", "owner": "CRM/Marketing", "sla": "7d"},
    "YELLOW": {"action": "Customer success check-in", "owner": "Customer Success", "sla": "30d"},
    "GREEN": {"action": "Monitor only", "owner": "Automated", "sla": "N/A"},
}

mlflow.set_registry_uri("databricks-uc")

HORIZONS = [30, 60, 90]

# Must match exactly the features used in training (04_model_training)
NUMERIC_FEATURES = [
    "contract_status_ord", "contract_dd_cancels", "dd_cancel_60_day", "ooc_days",
    "speed", "line_speed", "speed_gap_pct",
    "tenure_days",
    "calls_30d", "calls_90d", "calls_180d",
    "loyalty_calls_90d", "complaint_calls_90d", "tech_calls_90d",
    "avg_talk_time", "avg_hold_time", "max_hold_time", "days_since_last_call",
    "monthly_download_mb", "monthly_upload_mb", "monthly_total_mb",
    "avg_daily_download_mb", "std_daily_total_mb", "active_days_in_month",
    "peak_daily_total_mb", "usage_mom_change", "usage_vs_3mo_avg",
    "prior_cease_count", "days_since_last_cease",
]
CATEGORICAL_FEATURES = ["technology", "sales_channel", "tenure_bucket"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# OK Configure thresholds (supports environment-specific overrides)
# These can be overridden via Databricks widgets or job parameters
try:
    widget_names = [w.name for w in dbutils.widgets.getAll()]
    RED_THRESHOLD = float(dbutils.widgets.get("red_threshold")) if "red_threshold" in widget_names else 0.5
    AMBER_THRESHOLD = float(dbutils.widgets.get("amber_threshold")) if "amber_threshold" in widget_names else 0.5
    YELLOW_THRESHOLD = float(dbutils.widgets.get("yellow_threshold")) if "yellow_threshold" in widget_names else 0.5
except:
    # Fallback if widgets not initialized
    RED_THRESHOLD = 0.5
    AMBER_THRESHOLD = 0.5
    YELLOW_THRESHOLD = 0.5

thresholds = RiskThresholds(red=RED_THRESHOLD, amber=AMBER_THRESHOLD, yellow=YELLOW_THRESHOLD)

print(f"Risk thresholds: RED={RED_THRESHOLD}, AMBER={AMBER_THRESHOLD}, YELLOW={YELLOW_THRESHOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Load Latest Features

# COMMAND ----------

# Get the most recent observation date (latest monthly snapshot)
latest_date = (
    spark.table("uk_telecoms.gold.churn_features")
    .agg(F.max("observation_date"))
    .collect()[0][0]
)
print(f"Scoring on observation date: {latest_date}")

features_latest = (
    spark.table("uk_telecoms.gold.churn_features")
    .filter(F.col("observation_date") == latest_date)
)

n_customers = features_latest.count()
print(f"Active customers to score: {n_customers:,}")

# COMMAND ----------

# Convert to pandas for LightGBM scoring
pdf = features_latest.toPandas()

# Use exact same feature columns as training
feature_cols = ALL_FEATURES

# Set categorical types
for col in CATEGORICAL_FEATURES:
    if col in pdf.columns:
        pdf[col] = pdf[col].astype("category")

print(f"Feature columns: {len(feature_cols)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Score with All 3 Champion Models

# COMMAND ----------

for horizon in HORIZONS:
    model_uri = f"models:/uk_telecoms.ml.churn_model_{horizon}d@Champion"
    print(f"Loading: {model_uri}")

    model = mlflow.lightgbm.load_model(model_uri)
    pdf[f"churn_prob_{horizon}d"] = model.predict_proba(pdf[feature_cols])[:, 1]

    # Risk decile (1=lowest risk, 10=highest risk)
    pdf[f"risk_decile_{horizon}d"] = pd.qcut(
        pdf[f"churn_prob_{horizon}d"], 10,
        labels=False, duplicates="drop"
    ) + 1

    mean_prob = pdf[f"churn_prob_{horizon}d"].mean()
    top_decile_prob = pdf[pdf[f"risk_decile_{horizon}d"] == pdf[f"risk_decile_{horizon}d"].max()][f"churn_prob_{horizon}d"].mean()
    print(f"  {horizon}d — Mean prob: {mean_prob:.4f} | Top decile avg: {top_decile_prob:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Assign Composite Risk Tiers
# MAGIC
# MAGIC **Using canonical logic from** `src/risk_tiers.py` — ensures consistency across batch scoring and API.

# COMMAND ----------

# OK Apply risk tier assignment using imported function
pdf["risk_tier"] = pdf.apply(
    lambda row: assign_risk_tier(
        row["churn_prob_30d"],
        row["churn_prob_60d"],
        row["churn_prob_90d"],
        thresholds
    ),
    axis=1
)

pdf["score_date"] = latest_date

# Risk tier distribution
tier_counts = pdf["risk_tier"].value_counts()
print("\n" + "="*50)
print("  RISK TIER DISTRIBUTION")
print("="*50)
for tier in ["RED", "AMBER", "YELLOW", "GREEN"]:
    count = tier_counts.get(tier, 0)
    pct = count / len(pdf) * 100
    action = TIER_ACTIONS[tier]["action"]  # OK Also from imported module
    owner = TIER_ACTIONS[tier]["owner"]
    print(f"  {tier:<8} {count:>8,} customers ({pct:>5.1f}%) → {owner}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Write Scores to Delta

# COMMAND ----------

# Select scoring output columns
score_cols = (
    ["unique_customer_identifier", "observation_date", "score_date", "risk_tier"]
    + [f"churn_prob_{h}d" for h in HORIZONS]
    + [f"risk_decile_{h}d" for h in HORIZONS]
)

scores_df = spark.createDataFrame(pdf[score_cols])

scores_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uk_telecoms.gold.churn_scores")

print(f"gold.churn_scores: {scores_df.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Generate Tiered Action Lists

# COMMAND ----------

# ── RED → Loyalty Call Center ──
# Highest urgency: these customers are likely to churn within 30 days
red_list = (
    scores_df
    .filter("risk_tier = 'RED'")
    .orderBy(F.desc("churn_prob_30d"))
)
red_list.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_telecoms.gold.action_list_loyalty")
print(f"action_list_loyalty (RED):             {red_list.count():>8,} customers")

# ── AMBER → CRM/Marketing Digital Campaign ──
# Medium urgency: drifting customers, 60-day horizon
amber_list = (
    scores_df
    .filter("risk_tier = 'AMBER'")
    .orderBy(F.desc("churn_prob_60d"))
)
amber_list.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_telecoms.gold.action_list_marketing")
print(f"action_list_marketing (AMBER):         {amber_list.count():>8,} customers")

# ── YELLOW → Customer Success Proactive Outreach ──
# Low urgency: early warning, 90-day horizon
yellow_list = (
    scores_df
    .filter("risk_tier = 'YELLOW'")
    .orderBy(F.desc("churn_prob_90d"))
)
yellow_list.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_telecoms.gold.action_list_customer_success")
print(f"action_list_customer_success (YELLOW): {yellow_list.count():>8,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 Scoring Summary & Visualisation

# COMMAND ----------

# Probability distribution per horizon
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
colors = ["#E53935", "#FF9800", "#4CAF50"]

for i, horizon in enumerate(HORIZONS):
    col = f"churn_prob_{horizon}d"
    axes[i].hist(pdf[col], bins=50, color=colors[i], edgecolor="black", alpha=0.8)
    axes[i].axvline(x=0.5, color="black", linestyle="--", label="Threshold (0.5)")
    axes[i].set_title(f"{horizon}-Day Churn Probability", fontsize=13, fontweight="bold")
    axes[i].set_xlabel("Predicted Probability")
    axes[i].set_ylabel("Customer Count")
    axes[i].legend()

plt.suptitle("Churn Probability Distributions by Horizon", fontsize=15, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig("/tmp/scoring_distributions.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------

# Risk tier breakdown as a donut chart
import matplotlib.pyplot as plt

tier_order = ["RED", "AMBER", "YELLOW", "GREEN"]
tier_colors = ["#E53935", "#FF9800", "#FFEB3B", "#4CAF50"]
tier_sizes = [tier_counts.get(t, 0) for t in tier_order]

fig, ax = plt.subplots(figsize=(8, 8))
wedges, texts, autotexts = ax.pie(
    tier_sizes, labels=tier_order, colors=tier_colors,
    autopct="%1.1f%%", startangle=90, pctdistance=0.8,
    wedgeprops=dict(width=0.4, edgecolor="white", linewidth=2)
)
for text in autotexts:
    text.set_fontsize(12)
    text.set_fontweight("bold")
ax.set_title("Customer Risk Tier Distribution", fontsize=15, fontweight="bold")
plt.tight_layout()
plt.savefig("/tmp/risk_tier_donut.png", dpi=150)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.8 Sample Output: Retention Action Cards

# COMMAND ----------

# Show top 10 highest-risk customers across all tiers
display(
    scores_df
    .orderBy(F.desc("churn_prob_30d"))
    .select(
        "unique_customer_identifier",
        "risk_tier",
        F.round("churn_prob_30d", 3).alias("prob_30d"),
        F.round("churn_prob_60d", 3).alias("prob_60d"),
        F.round("churn_prob_90d", 3).alias("prob_90d"),
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [06_monitoring]($./06_monitoring) — Set up Lakehouse Monitoring for drift detection
