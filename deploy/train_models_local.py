#!/usr/bin/env python3
"""
Train production LightGBM models locally using the actual telco churn data.
Full pipeline: Bronze -> Silver -> Gold -> Model Training -> Batch Scoring.

Outputs:
    - 3 LightGBM model files (churn_model_{30,60,90}d.txt)
    - Batch scores CSV (churn_scores.csv)

Usage:
    python deploy/train_models_local.py [MODEL_DIR] [SCORES_DIR]
    python deploy/train_models_local.py /tmp/trained_models /tmp/batch_scores
"""
import gc
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, average_precision_score
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ══════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════
DATA_DIR = Path(".")
MODEL_OUTPUT_DIR = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/trained_models")
SCORES_OUTPUT_DIR = Path(sys.argv[2] if len(sys.argv) > 2 else "/tmp/batch_scores")
HORIZONS = [30, 60, 90]

# Feature schema (matches CLAUDE.md and serving/api/main.py)
NUMERIC_FEATURES = [
    "contract_status_ord", "contract_dd_cancels", "dd_cancel_60_day", "ooc_days",
    "speed", "line_speed", "speed_gap_pct", "tenure_days",
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

# Risk tier thresholds (same as configmap.yaml / risk_engine.py)
RED_THRESHOLD = 0.5
AMBER_THRESHOLD = 0.5
YELLOW_THRESHOLD = 0.5

print("=" * 80)
print("Telco Churn — Full Production Pipeline (Local)")
print("=" * 80)
print(f"  Models output: {MODEL_OUTPUT_DIR}")
print(f"  Scores output: {SCORES_OUTPUT_DIR}")

# ══════════════════════════════════════════
# BRONZE: Load raw data
# ══════════════════════════════════════════
print("\n[1/7] BRONZE: Loading raw data...")

customer = pd.read_parquet(DATA_DIR / "customer_info.parquet")
calls = pd.read_csv(DATA_DIR / "calls.csv")
cease = pd.read_csv(DATA_DIR / "cease.csv")

print(f"  customer_info: {len(customer):,} rows")
print(f"  calls: {len(calls):,} rows")
print(f"  cease: {len(cease):,} rows")

# Load usage separately (83M rows — memory sensitive)
print("  Loading usage.parquet (83M rows)...")
usage = pd.read_parquet(DATA_DIR / "usage.parquet")
print(f"  usage: {len(usage):,} rows")

# ══════════════════════════════════════════
# SILVER: Clean and type data
# ══════════════════════════════════════════
print("\n[2/7] SILVER: Cleaning and typing data...")

# Customer info
customer['datevalue'] = pd.to_datetime(customer['datevalue'])
customer['ooc_days'] = customer['ooc_days'].fillna(-9999)
customer['contract_status_ord'] = customer['contract_status'].str[:2].astype(int)
customer['speed_gap_pct'] = ((customer['speed'] - customer['line_speed']) / customer['speed'] * 100).round(2)
customer['tenure_bucket'] = pd.cut(
    customer['tenure_days'],
    bins=[0, 90, 365, 730, 1095, np.inf],
    labels=['0-90d', '90d-1y', '1y-2y', '2y-3y', '3y+'],
    right=False
)

# Usage: cast string -> numeric, compute monthly aggregates, then free raw data
print("  Aggregating usage to monthly (this takes ~60s)...")
usage['calendar_date'] = pd.to_datetime(usage['calendar_date'])
usage['usage_download_mbs'] = pd.to_numeric(usage['usage_download_mbs'], errors='coerce')
usage['usage_upload_mbs'] = pd.to_numeric(usage['usage_upload_mbs'], errors='coerce')
usage['usage_total_mbs'] = usage['usage_download_mbs'] + usage['usage_upload_mbs']
usage['month_key'] = usage['calendar_date'].dt.to_period('M')

usage_monthly = usage.groupby(['unique_customer_identifier', 'month_key']).agg({
    'usage_download_mbs': ['sum', 'mean'],
    'usage_upload_mbs': ['sum', 'mean'],
    'usage_total_mbs': ['sum', 'mean', 'std', 'max'],
    'calendar_date': 'count'
}).reset_index()
usage_monthly.columns = ['unique_customer_identifier', 'month_key',
                          'monthly_download_mb', 'avg_daily_download_mb',
                          'monthly_upload_mb', 'avg_daily_upload_mb',
                          'monthly_total_mb', 'avg_daily_total_mb', 'std_daily_total_mb',
                          'peak_daily_total_mb', 'active_days_in_month']

# Free raw usage memory (83M rows)
del usage
gc.collect()
print("  Freed raw usage data from memory")

# Usage trends
usage_monthly = usage_monthly.sort_values(['unique_customer_identifier', 'month_key'])
usage_monthly['prev_month_total'] = usage_monthly.groupby('unique_customer_identifier')['monthly_total_mb'].shift(1)
usage_monthly['usage_mom_change'] = (
    (usage_monthly['monthly_total_mb'] - usage_monthly['prev_month_total']) /
    usage_monthly['prev_month_total'].replace(0, np.nan)
).fillna(0)

# 3-month rolling average
usage_monthly['prev_3mo_avg'] = (
    usage_monthly.groupby('unique_customer_identifier')['monthly_total_mb']
    .rolling(3, min_periods=1).mean().shift(1).reset_index(0, drop=True)
)
usage_monthly['usage_vs_3mo_avg'] = (
    (usage_monthly['monthly_total_mb'] - usage_monthly['prev_3mo_avg']) /
    usage_monthly['prev_3mo_avg'].replace(0, np.nan)
).fillna(0)

# Calls
calls['event_date'] = pd.to_datetime(calls['event_date'])
calls['call_type'] = calls['call_type'].fillna('Unknown')
calls['is_loyalty_call'] = (calls['call_type'] == 'Loyalty').astype(int)
calls['is_complaint_call'] = (calls['call_type'] == 'Complaints').astype(int)
calls['is_tech_call'] = (calls['call_type'] == 'Tech').astype(int)

# Cease
cease['cease_placed_date'] = pd.to_datetime(cease['cease_placed_date'])

print("  Data cleaning complete")

# ══════════════════════════════════════════
# GOLD: Feature Engineering (vectorized)
# ══════════════════════════════════════════
print("\n[3/7] GOLD: Feature engineering (vectorized)...")

# Create observation spine (customer x month)
spine = customer[['unique_customer_identifier', 'datevalue']].drop_duplicates()
print(f"  {len(spine):,} observation points")

# Initialize feature DataFrame from customer attributes
features = customer[['unique_customer_identifier', 'datevalue',
                     'contract_status_ord', 'contract_dd_cancels', 'dd_cancel_60_day',
                     'ooc_days', 'technology', 'speed', 'line_speed', 'speed_gap_pct',
                     'sales_channel', 'tenure_days', 'tenure_bucket']].copy()

# ── Merge usage features ──
print("  Merging usage features...")
features['month_key'] = features['datevalue'].dt.to_period('M')
features = features.merge(
    usage_monthly[['unique_customer_identifier', 'month_key', 'monthly_download_mb',
                   'monthly_upload_mb', 'monthly_total_mb', 'avg_daily_download_mb',
                   'std_daily_total_mb', 'active_days_in_month', 'peak_daily_total_mb',
                   'usage_mom_change', 'usage_vs_3mo_avg']],
    on=['unique_customer_identifier', 'month_key'],
    how='left'
)
del usage_monthly
gc.collect()

# ── Call features (vectorized merge + groupby) ──
print("  Computing call features (vectorized)...")
calls_subset = calls[['unique_customer_identifier', 'event_date',
                       'is_loyalty_call', 'is_complaint_call', 'is_tech_call',
                       'talk_time_seconds', 'hold_time_seconds']].copy()

# Merge spine with calls on customer_id
merged_calls = spine.merge(calls_subset, on='unique_customer_identifier', how='inner')

# Filter: only calls BEFORE observation date and within 180 days
merged_calls['days_before'] = (merged_calls['datevalue'] - merged_calls['event_date']).dt.days
merged_calls = merged_calls[(merged_calls['days_before'] > 0) & (merged_calls['days_before'] <= 180)]

# Window flags
merged_calls['in_30d'] = (merged_calls['days_before'] <= 30).astype(int)
merged_calls['in_90d'] = (merged_calls['days_before'] <= 90).astype(int)

# Conditional aggregation for 90d-window call types
merged_calls['loyalty_90d'] = merged_calls['in_90d'] * merged_calls['is_loyalty_call']
merged_calls['complaint_90d'] = merged_calls['in_90d'] * merged_calls['is_complaint_call']
merged_calls['tech_90d'] = merged_calls['in_90d'] * merged_calls['is_tech_call']

call_agg = merged_calls.groupby(['unique_customer_identifier', 'datevalue']).agg(
    calls_30d=('in_30d', 'sum'),
    calls_90d=('in_90d', 'sum'),
    calls_180d=('event_date', 'count'),
    loyalty_calls_90d=('loyalty_90d', 'sum'),
    complaint_calls_90d=('complaint_90d', 'sum'),
    tech_calls_90d=('tech_90d', 'sum'),
    avg_talk_time=('talk_time_seconds', 'mean'),
    avg_hold_time=('hold_time_seconds', 'mean'),
    max_hold_time=('hold_time_seconds', 'max'),
    last_call_date=('event_date', 'max')
).reset_index()

call_agg['days_since_last_call'] = (call_agg['datevalue'] - call_agg['last_call_date']).dt.days
call_agg = call_agg.drop(columns=['last_call_date'])

# Merge call features (left join — customers with no calls get NaN → filled to 0 later)
features = features.merge(call_agg, on=['unique_customer_identifier', 'datevalue'], how='left')
del merged_calls, call_agg, calls_subset
gc.collect()
print(f"    {len(features):,} rows after call features merge")

# ── Cease history features (vectorized merge + groupby) ──
print("  Computing cease history features (vectorized)...")
cease_subset = cease[['unique_customer_identifier', 'cease_placed_date']].copy()

merged_cease = spine.merge(cease_subset, on='unique_customer_identifier', how='inner')
# Only prior ceases (strictly before observation date)
merged_cease = merged_cease[merged_cease['cease_placed_date'] < merged_cease['datevalue']]

cease_agg = merged_cease.groupby(['unique_customer_identifier', 'datevalue']).agg(
    prior_cease_count=('cease_placed_date', 'count'),
    last_cease_date=('cease_placed_date', 'max')
).reset_index()

cease_agg['days_since_last_cease'] = (cease_agg['datevalue'] - cease_agg['last_cease_date']).dt.days
cease_agg = cease_agg.drop(columns=['last_cease_date'])

features = features.merge(cease_agg, on=['unique_customer_identifier', 'datevalue'], how='left')
del merged_cease, cease_agg
gc.collect()

# Fill missing values (customers with no calls/ceases get 0)
for col in NUMERIC_FEATURES:
    if col in features.columns:
        if col == 'days_since_last_call' or col == 'days_since_last_cease':
            features[col] = features[col].fillna(9999)
        else:
            features[col] = features[col].fillna(0)

print(f"  Feature engineering complete: {len(features):,} rows x {len(ALL_FEATURES)} features")

# ── Multi-horizon targets (vectorized merge + groupby) ──
print("\n[4/7] GOLD: Creating multi-horizon targets (vectorized)...")
labels = spine.copy()

for horizon in HORIZONS:
    label_col = f'churned_in_{horizon}d'

    # Merge spine with cease events
    merged_future = spine.merge(cease_subset, on='unique_customer_identifier', how='inner')

    # Filter: cease within future window [observation_date, observation_date + horizon)
    merged_future = merged_future[
        (merged_future['cease_placed_date'] >= merged_future['datevalue']) &
        (merged_future['cease_placed_date'] < merged_future['datevalue'] + pd.Timedelta(days=horizon))
    ]

    # Any customer with at least one future cease in window = churned
    churners = merged_future.groupby(['unique_customer_identifier', 'datevalue']).size().reset_index(name='_count')
    churners[label_col] = 1

    labels = labels.merge(
        churners[['unique_customer_identifier', 'datevalue', label_col]],
        on=['unique_customer_identifier', 'datevalue'],
        how='left'
    )
    labels[label_col] = labels[label_col].fillna(0).astype(int)
    print(f"  {label_col}: {labels[label_col].sum():,} positive ({labels[label_col].mean()*100:.2f}%)")

del cease_subset
gc.collect()

# Merge features + labels
training_data = features.merge(labels, on=['unique_customer_identifier', 'datevalue'], how='inner')

# Temporal split (same as CLAUDE.md: train < 2024-04, val < 2024-07, test >= 2024-07)
training_data['split'] = 'train'
training_data.loc[training_data['datevalue'] >= '2024-04-01', 'split'] = 'validation'
training_data.loc[training_data['datevalue'] >= '2024-07-01', 'split'] = 'test'

print(f"  Train: {(training_data['split'] == 'train').sum():,} rows")
print(f"  Validation: {(training_data['split'] == 'validation').sum():,} rows")
print(f"  Test: {(training_data['split'] == 'test').sum():,} rows")

# ══════════════════════════════════════════
# MODEL TRAINING: One model per horizon
# ══════════════════════════════════════════
print(f"\n[5/7] MODEL TRAINING: Training {len(HORIZONS)} production models...")

MODEL_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
trained_models = {}

for horizon in HORIZONS:
    print(f"\n  Training {horizon}-day churn model...")

    target = f'churned_in_{horizon}d'

    # Prepare data
    train = training_data[training_data['split'] == 'train']
    val = training_data[training_data['split'] == 'validation']
    test = training_data[training_data['split'] == 'test']

    X_train = train[ALL_FEATURES].copy()
    y_train = train[target]
    X_val = val[ALL_FEATURES].copy()
    y_val = val[target]
    X_test = test[ALL_FEATURES].copy()
    y_test = test[target]

    # Convert categorical features
    for cat_col in CATEGORICAL_FEATURES:
        X_train[cat_col] = X_train[cat_col].astype('category')
        X_val[cat_col] = X_val[cat_col].astype('category')
        X_test[cat_col] = X_test[cat_col].astype('category')

    # Calculate class weight
    n_pos = (y_train == 1).sum()
    n_neg = (y_train == 0).sum()
    pos_weight = n_neg / max(n_pos, 1)
    print(f"    Class balance: {n_pos:,} positive / {n_neg:,} negative (weight: {pos_weight:.1f})")

    # Training parameters
    params = {
        'objective': 'binary',
        'metric': 'average_precision',
        'learning_rate': 0.05,
        'num_leaves': 63,
        'max_depth': 7,
        'min_child_samples': 50,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'scale_pos_weight': pos_weight,
        'verbose': -1,
        'seed': 42 + horizon
    }

    # Create LightGBM datasets
    train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_FEATURES)
    val_data = lgb.Dataset(X_val, label=y_val, categorical_feature=CATEGORICAL_FEATURES, reference=train_data)

    # Train model
    model = lgb.train(
        params,
        train_data,
        num_boost_round=1000,
        valid_sets=[train_data, val_data],
        valid_names=['train', 'val'],
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(100)]
    )

    # Evaluate
    y_pred_val = model.predict(X_val)
    y_pred_test = model.predict(X_test)

    val_auc_roc = roc_auc_score(y_val, y_pred_val)
    val_auc_pr = average_precision_score(y_val, y_pred_val)
    test_auc_roc = roc_auc_score(y_test, y_pred_test)
    test_auc_pr = average_precision_score(y_test, y_pred_test)

    print(f"    Validation -- AUC-ROC: {val_auc_roc:.4f}, AUC-PR: {val_auc_pr:.4f}")
    print(f"    Test       -- AUC-ROC: {test_auc_roc:.4f}, AUC-PR: {test_auc_pr:.4f}")

    # Save model
    model_path = MODEL_OUTPUT_DIR / f"churn_model_{horizon}d.txt"
    model.save_model(str(model_path))
    file_size = model_path.stat().st_size
    trained_models[horizon] = model

    print(f"    Saved {model_path.name} ({file_size:,} bytes)")

# ══════════════════════════════════════════
# VALIDATION: Test model loading from disk
# ══════════════════════════════════════════
print(f"\n[6/7] VALIDATION: Testing model loading from disk...")

for horizon in HORIZONS:
    model_path = MODEL_OUTPUT_DIR / f"churn_model_{horizon}d.txt"
    loaded_model = lgb.Booster(model_file=str(model_path))

    # Test prediction with a single row
    test_row = training_data[ALL_FEATURES].head(1).copy()
    for cat_col in CATEGORICAL_FEATURES:
        test_row[cat_col] = test_row[cat_col].astype('category')
    pred = loaded_model.predict(test_row)[0]

    print(f"  {horizon}d model: loads correctly, test prediction = {pred:.4f}")

# ══════════════════════════════════════════
# BATCH SCORING: Score latest snapshot
# ══════════════════════════════════════════
print(f"\n[7/7] BATCH SCORING: Scoring latest customer snapshot...")

SCORES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Get latest observation date
latest_date = features['datevalue'].max()
print(f"  Latest observation date: {latest_date.strftime('%Y-%m-%d')}")

# Filter features to latest snapshot
latest_features = features[features['datevalue'] == latest_date].copy()
print(f"  Customers to score: {len(latest_features):,}")

# Prepare feature matrix
X_score = latest_features[ALL_FEATURES].copy()
for cat_col in CATEGORICAL_FEATURES:
    X_score[cat_col] = X_score[cat_col].astype('category')

# Score with all 3 horizon models
for horizon in HORIZONS:
    prob_col = f'churn_prob_{horizon}d'
    latest_features[prob_col] = trained_models[horizon].predict(X_score)

    # Compute deciles (1 = lowest risk, 10 = highest risk)
    latest_features[f'risk_decile_{horizon}d'] = pd.qcut(
        latest_features[prob_col], 10, labels=False, duplicates='drop'
    ) + 1

# Assign risk tiers
def assign_risk_tier(row):
    if row['churn_prob_30d'] > RED_THRESHOLD:
        return 'RED'
    elif row['churn_prob_60d'] > AMBER_THRESHOLD:
        return 'AMBER'
    elif row['churn_prob_90d'] > YELLOW_THRESHOLD:
        return 'YELLOW'
    else:
        return 'GREEN'

latest_features['risk_tier'] = latest_features.apply(assign_risk_tier, axis=1)
latest_features['score_date'] = latest_date.strftime('%Y-%m-%d')
latest_features['observation_date'] = latest_date.strftime('%Y-%m-%d')

# Select output columns
score_cols = (
    ['unique_customer_identifier', 'observation_date', 'score_date', 'risk_tier']
    + [f'churn_prob_{h}d' for h in HORIZONS]
    + [f'risk_decile_{h}d' for h in HORIZONS]
)
scores_df = latest_features[score_cols]

# Write CSV
scores_path = SCORES_OUTPUT_DIR / "churn_scores.csv"
scores_df.to_csv(str(scores_path), index=False)

# Print tier distribution
tier_dist = scores_df['risk_tier'].value_counts()
print(f"\n  Risk Tier Distribution:")
for tier in ['RED', 'AMBER', 'YELLOW', 'GREEN']:
    count = tier_dist.get(tier, 0)
    pct = count / len(scores_df) * 100
    print(f"    {tier:8s}: {count:>8,} ({pct:5.1f}%)")

print(f"\n  Saved {len(scores_df):,} scores to {scores_path}")

# ══════════════════════════════════════════
# DONE
# ══════════════════════════════════════════
print("\n" + "=" * 80)
print(f"PIPELINE COMPLETE")
print(f"  Models: {MODEL_OUTPUT_DIR}")
for h in HORIZONS:
    p = MODEL_OUTPUT_DIR / f"churn_model_{h}d.txt"
    print(f"    {p.name} ({p.stat().st_size:,} bytes)")
print(f"  Scores: {scores_path} ({len(scores_df):,} rows)")
print("=" * 80)
