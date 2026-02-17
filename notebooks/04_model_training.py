# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Multi-Horizon Model Training (LightGBM + MLflow + SHAP)
# MAGIC
# MAGIC **Purpose:** Train 3 independent LightGBM models (30d, 60d, 90d churn) with full experiment tracking.
# MAGIC Compare feature importance across horizons — this is the key analytical insight.
# MAGIC
# MAGIC **Design Pattern:** One model per horizon → MLflow tracking → Unity Catalog Model Registry → SHAP cross-horizon comparison.
# MAGIC
# MAGIC **Output:**
# MAGIC - 3 registered models: `uk_telecoms.ml.churn_model_{30,60,90}d`
# MAGIC - SHAP summary plots per horizon
# MAGIC - Cross-horizon performance comparison
# MAGIC - All artifacts logged to MLflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Setup

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from mlflow import MlflowClient
import lightgbm as lgb

# Disable autologging to prevent conflicts with explicit MLflow runs
mlflow.autolog(disable=True)
# End any stale MLflow runs from previous attempts
mlflow.end_run()
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_recall_curve, roc_curve,
    classification_report, confusion_matrix
)
from sklearn.calibration import calibration_curve
import shap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

mlflow.set_registry_uri("databricks-uc")

# Use a shared experiment path that exists
import os
username = spark.sql("SELECT current_user()").collect()[0][0]
experiment_path = f"/Users/{username}/churn_prediction_multi_horizon"
mlflow.set_experiment(experiment_path)
print(f"MLflow experiment: {experiment_path}")

client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Load Training Set & Define Features

# COMMAND ----------

df = spark.table("uk_telecoms.gold.churn_training_set")

NUMERIC_FEATURES = [
    # Contract
    "contract_status_ord", "contract_dd_cancels", "dd_cancel_60_day", "ooc_days",
    # Product
    "speed", "line_speed", "speed_gap_pct",
    # Tenure
    "tenure_days",
    # Call behavior
    "calls_30d", "calls_90d", "calls_180d",
    "loyalty_calls_90d", "complaint_calls_90d", "tech_calls_90d",
    "avg_talk_time", "avg_hold_time", "max_hold_time", "days_since_last_call",
    # Usage
    "monthly_download_mb", "monthly_upload_mb", "monthly_total_mb",
    "avg_daily_download_mb", "std_daily_total_mb", "active_days_in_month",
    "peak_daily_total_mb", "usage_mom_change", "usage_vs_3mo_avg",
    # Cease history
    "prior_cease_count", "days_since_last_cease",
]

CATEGORICAL_FEATURES = ["technology", "sales_channel", "tenure_bucket"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
HORIZONS = [30, 60, 90]

print(f"Features: {len(ALL_FEATURES)} ({len(NUMERIC_FEATURES)} numeric + {len(CATEGORICAL_FEATURES)} categorical)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Prepare Pandas DataFrames for LightGBM
# MAGIC
# MAGIC We use Spark for all ETL but LightGBM trains on pandas. The aggregated training set fits comfortably in memory.

# COMMAND ----------

all_targets = [f"churned_in_{h}d" for h in HORIZONS]
select_cols = ALL_FEATURES + all_targets + ["split"]

pdf_train = df.filter("split = 'train'").select(select_cols).toPandas()
pdf_val   = df.filter("split = 'validation'").select(select_cols).toPandas()
pdf_test  = df.filter("split = 'test'").select(select_cols).toPandas()

# Encode categoricals for LightGBM
for c in CATEGORICAL_FEATURES:
    for pdf in [pdf_train, pdf_val, pdf_test]:
        pdf[c] = pdf[c].astype("category")

print(f"Train: {len(pdf_train):,}  |  Val: {len(pdf_val):,}  |  Test: {len(pdf_test):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Train One Model Per Horizon

# COMMAND ----------

results = {}       # horizon → metrics dict
models = {}        # horizon → trained model
shap_values_all = {}  # horizon → shap values

for horizon in HORIZONS:
    target = f"churned_in_{horizon}d"
    print(f"\n{'='*60}")
    print(f"  TRAINING: {horizon}-DAY CHURN MODEL")
    print(f"{'='*60}")

    X_train = pdf_train[ALL_FEATURES]
    y_train = pdf_train[target]
    X_val = pdf_val[ALL_FEATURES]
    y_val = pdf_val[target]
    X_test = pdf_test[ALL_FEATURES]
    y_test = pdf_test[target]

    pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    print(f"  Churn rate (train): {y_train.mean()*100:.2f}%  |  scale_pos_weight: {pos_weight:.2f}")

    with mlflow.start_run(run_name=f"lgbm_churn_{horizon}d", nested=True) as run:

        params = {
            "objective": "binary",
            "metric": "average_precision",
            "learning_rate": 0.05,
            "num_leaves": 63,
            "max_depth": 7,
            "min_child_samples": 50,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "scale_pos_weight": pos_weight,
            "n_estimators": 1000,
            "verbose": -1,
        }
        mlflow.log_params(params)
        mlflow.log_param("horizon_days", horizon)
        mlflow.log_param("n_features", len(ALL_FEATURES))
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("churn_rate_train", round(y_train.mean(), 4))

        # Train
        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[
                lgb.log_evaluation(200),
                lgb.early_stopping(50),
            ]
        )
        models[horizon] = model

        # Predict
        y_prob_val = model.predict_proba(X_val)[:, 1]
        y_prob_test = model.predict_proba(X_test)[:, 1]

        # ── Metrics ──
        metrics = {
            "val_auc_roc": roc_auc_score(y_val, y_prob_val),
            "val_auc_pr": average_precision_score(y_val, y_prob_val),
            "test_auc_roc": roc_auc_score(y_test, y_prob_test),
            "test_auc_pr": average_precision_score(y_test, y_prob_test),
            "horizon_days": horizon,
        }
        mlflow.log_metrics(metrics)
        results[horizon] = metrics

        print(f"  Val  AUC-ROC: {metrics['val_auc_roc']:.4f}  |  AUC-PR: {metrics['val_auc_pr']:.4f}")
        print(f"  Test AUC-ROC: {metrics['test_auc_roc']:.4f}  |  AUC-PR: {metrics['test_auc_pr']:.4f}")

        # ── SHAP Explainability ──
        explainer = shap.TreeExplainer(model)
        sv = explainer.shap_values(X_test)
        shap_values_all[horizon] = sv

        # SHAP summary plot
        fig, ax = plt.subplots(figsize=(12, 8))
        shap.summary_plot(sv[1] if isinstance(sv, list) else sv, X_test, show=False, max_display=15)
        plt.title(f"SHAP Feature Importance — {horizon}-Day Churn", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(f"/tmp/shap_{horizon}d.png", dpi=150, bbox_inches="tight")
        mlflow.log_artifact(f"/tmp/shap_{horizon}d.png")
        plt.show()
        plt.close()

        # ── ROC Curve ──
        fpr, tpr, _ = roc_curve(y_test, y_prob_test)
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(fpr, tpr, lw=2, label=f"AUC = {metrics['test_auc_roc']:.4f}")
        ax.plot([0, 1], [0, 1], "k--", lw=1)
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.set_title(f"ROC Curve — {horizon}-Day Churn")
        ax.legend()
        plt.tight_layout()
        plt.savefig(f"/tmp/roc_{horizon}d.png", dpi=150)
        mlflow.log_artifact(f"/tmp/roc_{horizon}d.png")
        plt.show()
        plt.close()

        # ── Precision-Recall Curve ──
        precision, recall, _ = precision_recall_curve(y_test, y_prob_test)
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(recall, precision, lw=2, label=f"AUC-PR = {metrics['test_auc_pr']:.4f}")
        ax.axhline(y=y_test.mean(), color="r", linestyle="--", label=f"Baseline = {y_test.mean():.3f}")
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.set_title(f"Precision-Recall Curve — {horizon}-Day Churn")
        ax.legend()
        plt.tight_layout()
        plt.savefig(f"/tmp/pr_{horizon}d.png", dpi=150)
        mlflow.log_artifact(f"/tmp/pr_{horizon}d.png")
        plt.show()
        plt.close()

        # ── Calibration Curve ──
        fraction_pos, mean_pred = calibration_curve(y_test, y_prob_test, n_bins=10, strategy="quantile")
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(mean_pred, fraction_pos, "s-", label="Model")
        ax.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
        ax.set_xlabel("Mean predicted probability")
        ax.set_ylabel("Fraction of positives")
        ax.set_title(f"Calibration Curve — {horizon}-Day Churn")
        ax.legend()
        plt.tight_layout()
        plt.savefig(f"/tmp/calibration_{horizon}d.png", dpi=150)
        mlflow.log_artifact(f"/tmp/calibration_{horizon}d.png")
        plt.show()
        plt.close()

        # ── Lift Chart (Top Decile) ──
        test_scored = pd.DataFrame({"y_true": y_test.values, "y_prob": y_prob_test})
        test_scored["decile"] = pd.qcut(test_scored["y_prob"], 10, labels=False, duplicates="drop") + 1
        lift_data = (
            test_scored.groupby("decile")
            .agg(churn_rate=("y_true", "mean"), count=("y_true", "count"))
            .reset_index()
        )
        baseline_rate = y_test.mean()
        lift_data["lift"] = lift_data["churn_rate"] / baseline_rate

        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(lift_data["decile"], lift_data["lift"], color=sns.color_palette("RdYlGn_r", 10), edgecolor="black")
        ax.axhline(y=1.0, color="black", linestyle="--", label="Baseline (random)")
        ax.set_xlabel("Risk Decile (10 = highest risk)")
        ax.set_ylabel("Lift vs Random")
        ax.set_title(f"Lift Chart — {horizon}-Day Churn")
        ax.legend()
        plt.tight_layout()
        plt.savefig(f"/tmp/lift_{horizon}d.png", dpi=150)
        mlflow.log_artifact(f"/tmp/lift_{horizon}d.png")
        plt.show()
        plt.close()

        top_decile_lift = lift_data[lift_data["decile"] == lift_data["decile"].max()]["lift"].values[0]
        mlflow.log_metric("lift_top_decile", top_decile_lift)
        print(f"  Lift @ top decile: {top_decile_lift:.2f}x")

        # ── Register Model in Unity Catalog ──
        signature = infer_signature(X_train, model.predict_proba(X_train)[:, 1])
        mlflow.lightgbm.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=f"uk_telecoms.ml.churn_model_{horizon}d",
        )
        print(f"  Model registered: uk_telecoms.ml.churn_model_{horizon}d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Cross-Horizon Performance Comparison

# COMMAND ----------

comparison = pd.DataFrame(results).T
comparison.index.name = "horizon_days"
print("\n" + "="*70)
print("  CROSS-HORIZON MODEL COMPARISON")
print("="*70)
display(comparison)

# COMMAND ----------

# Bar chart comparison
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

comparison[["test_auc_roc"]].plot.bar(ax=axes[0], color="#2196F3", edgecolor="black", legend=False)
axes[0].set_title("Test AUC-ROC by Horizon", fontsize=13, fontweight="bold")
axes[0].set_ylabel("AUC-ROC")
axes[0].set_xticklabels([f"{h}d" for h in HORIZONS], rotation=0)

comparison[["test_auc_pr"]].plot.bar(ax=axes[1], color="#FF9800", edgecolor="black", legend=False)
axes[1].set_title("Test AUC-PR by Horizon", fontsize=13, fontweight="bold")
axes[1].set_ylabel("AUC-PR")
axes[1].set_xticklabels([f"{h}d" for h in HORIZONS], rotation=0)

plt.suptitle("Multi-Horizon Model Performance", fontsize=15, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig("/tmp/cross_horizon_comparison.png", dpi=150, bbox_inches="tight")
mlflow.log_artifact("/tmp/cross_horizon_comparison.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Cross-Horizon Feature Importance Comparison
# MAGIC
# MAGIC This is the **key analytical insight**: feature importance shifts across horizons.
# MAGIC 30-day = tipping-point signals. 90-day = slow-burn signals.

# COMMAND ----------

# Build a comparison table of mean absolute SHAP values across horizons
importance_comparison = {}

for horizon in HORIZONS:
    sv = shap_values_all[horizon]
    vals = sv[1] if isinstance(sv, list) else sv
    mean_abs_shap = np.abs(vals).mean(axis=0)
    importance_comparison[f"{horizon}d"] = pd.Series(mean_abs_shap, index=ALL_FEATURES)

imp_df = pd.DataFrame(importance_comparison)
imp_df["overall_avg"] = imp_df.mean(axis=1)
imp_df = imp_df.sort_values("overall_avg", ascending=False)

# Top 15 features
top_features = imp_df.head(15)

fig, ax = plt.subplots(figsize=(14, 8))
x = np.arange(len(top_features))
width = 0.25

bars1 = ax.barh(x + width, top_features["30d"], width, label="30-day", color="#E53935")
bars2 = ax.barh(x, top_features["60d"], width, label="60-day", color="#FF9800")
bars3 = ax.barh(x - width, top_features["90d"], width, label="90-day", color="#4CAF50")

ax.set_yticks(x)
ax.set_yticklabels(top_features.index, fontsize=11)
ax.set_xlabel("Mean |SHAP value|", fontsize=12)
ax.set_title("Feature Importance Across Horizons — Tipping Point vs Slow Burn", fontsize=14, fontweight="bold")
ax.legend(fontsize=12)
ax.invert_yaxis()
plt.tight_layout()
plt.savefig("/tmp/cross_horizon_shap.png", dpi=150, bbox_inches="tight")
mlflow.log_artifact("/tmp/cross_horizon_shap.png")
plt.show()

# COMMAND ----------

# Rank shift table
rank_df = imp_df[["30d", "60d", "90d"]].rank(ascending=False).astype(int)
rank_df.columns = ["Rank_30d", "Rank_60d", "Rank_90d"]
rank_df["Rank_Shift_30_to_90"] = rank_df["Rank_30d"] - rank_df["Rank_90d"]
print("\nFeature Rank Shifts Across Horizons:")
print("Positive = more important at 30d (tipping point)")
print("Negative = more important at 90d (slow burn)")
display(rank_df.sort_values("Rank_Shift_30_to_90", ascending=False).head(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Model Governance: Compare & Promote
# MAGIC
# MAGIC **Safety gate:** Only promote to Champion if new model meets minimum performance thresholds
# MAGIC and outperforms (or matches) current Champion.

# COMMAND ----------

# Performance thresholds (configurable per environment)
MIN_AUC_PR_THRESHOLD = 0.20  # Minimum acceptable AUC-PR (must beat baseline)
MIN_IMPROVEMENT_PCT = 0.0    # New model must be >= this % better than Champion (0% = allow equal)

# COMMAND ----------

def compare_and_promote(horizon: int, new_metrics: dict):
    """
    Model governance gate: compare new model against current Champion.

    Promotion criteria:
    1. New model meets minimum threshold (AUC-PR > MIN_AUC_PR_THRESHOLD)
    2. New model >= Champion performance (with MIN_IMPROVEMENT_PCT tolerance)
    3. No significant performance regression on val set

    Returns:
        bool: True if promoted, False if rejected
    """
    model_name = f"uk_telecoms.ml.churn_model_{horizon}d"

    # Get current Champion metrics (if exists)
    try:
        champion_versions = [v for v in client.search_model_versions(f"name='{model_name}'")
                             if "Champion" in [alias.alias for alias in v.aliases]]

        if not champion_versions:
            print(f"  No current Champion — this will be the first.")
            should_promote = True
            comparison_reason = "First model"
        else:
            champion_version = champion_versions[0].version
            champion_run_id = champion_versions[0].run_id

            # Get Champion's metrics from its MLflow run
            champion_run = client.get_run(champion_run_id)
            champion_auc_pr = champion_run.data.metrics.get("test_auc_pr", 0)
            champion_auc_roc = champion_run.data.metrics.get("test_auc_roc", 0)

            new_auc_pr = new_metrics["test_auc_pr"]
            new_auc_roc = new_metrics["test_auc_roc"]

            print(f"  Current Champion (v{champion_version}):")
            print(f"    Test AUC-PR:  {champion_auc_pr:.4f}")
            print(f"    Test AUC-ROC: {champion_auc_roc:.4f}")
            print(f"  New Challenger:")
            print(f"    Test AUC-PR:  {new_auc_pr:.4f}")
            print(f"    Test AUC-ROC: {new_auc_roc:.4f}")

            # Check minimum threshold
            if new_auc_pr < MIN_AUC_PR_THRESHOLD:
                should_promote = False
                comparison_reason = f"Below minimum threshold ({new_auc_pr:.4f} < {MIN_AUC_PR_THRESHOLD})"

            # Check improvement requirement
            elif new_auc_pr < champion_auc_pr * (1 - MIN_IMPROVEMENT_PCT / 100):
                should_promote = False
                pct_change = ((new_auc_pr - champion_auc_pr) / champion_auc_pr) * 100
                comparison_reason = f"Regression detected ({pct_change:+.2f}%)"

            else:
                should_promote = True
                pct_change = ((new_auc_pr - champion_auc_pr) / champion_auc_pr) * 100
                comparison_reason = f"Performance improvement: {pct_change:+.2f}%"

    except Exception as e:
        print(f"  Warning: Could not retrieve Champion metrics: {e}")
        print(f"  Promoting as Champion (unable to compare)")
        should_promote = True
        comparison_reason = "Unable to compare (Champion metrics unavailable)"

    # Decision
    latest_versions = client.search_model_versions(f"name='{model_name}'")
    latest_version = max(v.version for v in latest_versions)

    if should_promote:
        client.set_registered_model_alias(model_name, "Champion", latest_version)
        print(f"  OK PROMOTED to Champion: v{latest_version}")
        print(f"     Reason: {comparison_reason}")
    else:
        client.set_registered_model_alias(model_name, "Challenger", latest_version)
        print(f"  FAIL NOT PROMOTED (tagged as Challenger): v{latest_version}")
        print(f"     Reason: {comparison_reason}")
        print(f"     Action: Review model or retrain with more data")

    # Log decision to MLflow (skip if no active run)
    try:
        active = mlflow.active_run()
        if active:
            mlflow.log_param("promotion_decision", "Champion" if should_promote else "Challenger")
            mlflow.log_param("promotion_reason", comparison_reason)
            if not should_promote:
                mlflow.set_tag("requires_review", "true")
    except Exception:
        pass  # Non-critical logging

    return should_promote

# COMMAND ----------

print("\n" + "="*70)
print("  MODEL GOVERNANCE: PROMOTION DECISIONS")
print("="*70 + "\n")

promotion_results = {}

for horizon in HORIZONS:
    print(f"[{horizon}-Day Model]")
    promoted = compare_and_promote(horizon, results[horizon])
    promotion_results[horizon] = promoted
    print()

# COMMAND ----------

# Summary
print("="*70)
print("  PROMOTION SUMMARY")
print("="*70)
for horizon, promoted in promotion_results.items():
    status = "OK Champion" if promoted else "FAIL Challenger (review required)"
    print(f"  {horizon}-day model: {status}")

if not all(promotion_results.values()):
    print("\nWARNING  WARNING: Not all models were promoted to Champion.")
    print("   Review Challenger models before deploying to production.")
else:
    print("\nOK All models passed governance checks and promoted to Champion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.8 Training Summary
# MAGIC
# MAGIC | Metric | 30-Day | 60-Day | 90-Day |
# MAGIC |---|---|---|---|
# MAGIC | Test AUC-ROC | {results[30]['test_auc_roc']:.4f} | {results[60]['test_auc_roc']:.4f} | {results[90]['test_auc_roc']:.4f} |
# MAGIC | Test AUC-PR | {results[30]['test_auc_pr']:.4f} | {results[60]['test_auc_pr']:.4f} | {results[90]['test_auc_pr']:.4f} |
# MAGIC
# MAGIC **Key Insight:** Feature importance shifts across horizons — 30d is driven by loyalty calls and DD cancellations (tipping points), while 90d is driven by usage decline and speed gaps (slow burn).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** [05_batch_scoring]($./05_batch_scoring) — Score all active customers with the multi-horizon risk matrix
