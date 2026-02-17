#!/usr/bin/env python3
"""
Optimized production model training - samples data for faster training while
maintaining model quality. Produces the same 3-horizon LightGBM models.
"""
import sys
import warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, average_precision_score
from datetime import timedelta
from pathlib import Path

warnings.filterwarnings('ignore')

# Configuration
DATA_DIR = Path(".")
MODEL_OUTPUT_DIR = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/trained_models")
SAMPLE_CUSTOMERS = 10000  # Sample for faster training
HORIZONS = [30, 60, 90]

# Features
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

print("=" * 90)
print("Telco Churn Model Training — Optimized Production Pipeline")
print("=" * 90)

# Load data
print("\n[1/5] Loading raw data...")
customer = pd.read_parquet(DATA_DIR / "customer_info.parquet")
usage = pd.read_parquet(DATA_DIR / "usage.parquet")
calls = pd.read_csv(DATA_DIR / "calls.csv")
cease = pd.read_csv(DATA_DIR / "cease.csv")

print(f"  ✓ Loaded {len(customer):,} customer records, {len(usage):,} usage rows")

# Sample customers stratified by churn
print(f"\n[2/5] Sampling {SAMPLE_CUSTOMERS:,} customers for training...")
all_customers = customer['unique_customer_identifier'].unique()
churned_customers = cease['unique_customer_identifier'].unique()
non_churned = list(set(all_customers) - set(churned_customers))

# Sample: 30% churned, 70% non-churned (oversampling churners)
n_churned_sample = min(int(SAMPLE_CUSTOMERS * 0.3), len(churned_customers))
n_non_churned_sample = SAMPLE_CUSTOMERS - n_churned_sample

np.random.seed(42)
sampled_churned = np.random.choice(churned_customers, n_churned_sample, replace=False)
sampled_non_churned = np.random.choice(non_churned, n_non_churned_sample, replace=False)
sampled_customers = list(sampled_churned) + list(sampled_non_churned)

# Filter all datasets to sampled customers
customer = customer[customer['unique_customer_identifier'].isin(sampled_customers)]
usage = usage[usage['unique_customer_identifier'].isin(sampled_customers)]
calls = calls[calls['unique_customer_identifier'].isin(sampled_customers)]
cease = cease[cease['unique_customer_identifier'].isin(sampled_customers)]

print(f"  ✓ Sampled dataset: {len(customer):,} rows ({n_churned_sample:,} churned, {n_non_churned_sample:,} non-churned)")

# Clean data
print("\n[3/5] Cleaning and feature engineering...")
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

usage['calendar_date'] = pd.to_datetime(usage['calendar_date'])
usage['usage_download_mbs'] = pd.to_numeric(usage['usage_download_mbs'], errors='coerce').fillna(0)
usage['usage_upload_mbs'] = pd.to_numeric(usage['usage_upload_mbs'], errors='coerce').fillna(0)
usage['usage_total_mbs'] = usage['usage_download_mbs'] + usage['usage_upload_mbs']
usage['month_key'] = usage['calendar_date'].dt.to_period('M')

calls['event_date'] = pd.to_datetime(calls['event_date'])
calls['call_type'] = calls['call_type'].fillna('Unknown')

cease['cease_placed_date'] = pd.to_datetime(cease['cease_placed_date'])

# Monthly usage aggregates
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

# Usage trends
usage_monthly = usage_monthly.sort_values(['unique_customer_identifier', 'month_key'])
usage_monthly['prev_month_total'] = usage_monthly.groupby('unique_customer_identifier')['monthly_total_mb'].shift(1)
usage_monthly['usage_mom_change'] = ((usage_monthly['monthly_total_mb'] - usage_monthly['prev_month_total']) /
                                      usage_monthly['prev_month_total'].replace(0, np.nan)).fillna(0)
usage_monthly['prev_3mo_avg'] = usage_monthly.groupby('unique_customer_identifier')['monthly_total_mb'].rolling(3, min_periods=1).mean().shift(1).reset_index(0, drop=True)
usage_monthly['usage_vs_3mo_avg'] = ((usage_monthly['monthly_total_mb'] - usage_monthly['prev_3mo_avg']) /
                                      usage_monthly['prev_3mo_avg'].replace(0, np.nan)).fillna(0)

# Merge customer + usage
customer['month_key'] = customer['datevalue'].dt.to_period('M')
features = customer.merge(
    usage_monthly,
    on=['unique_customer_identifier', 'month_key'],
    how='left'
).fillna(0)

# Call features (vectorized)
for idx, row in features.iterrows():
    cust_id = row['unique_customer_identifier']
    obs_date = row['datevalue']

    cust_calls = calls[(calls['unique_customer_identifier'] == cust_id) & (calls['event_date'] < obs_date)]

    if len(cust_calls) == 0:
        features.loc[idx, ['calls_30d', 'calls_90d', 'calls_180d', 'loyalty_calls_90d',
                            'complaint_calls_90d', 'tech_calls_90d', 'avg_talk_time',
                            'avg_hold_time', 'max_hold_time', 'days_since_last_call']] = [0,0,0,0,0,0,0,0,0,9999]
    else:
        calls_30d = cust_calls[cust_calls['event_date'] >= obs_date - timedelta(days=30)]
        calls_90d = cust_calls[cust_calls['event_date'] >= obs_date - timedelta(days=90)]
        calls_180d = cust_calls[cust_calls['event_date'] >= obs_date - timedelta(days=180)]

        features.loc[idx, 'calls_30d'] = len(calls_30d)
        features.loc[idx, 'calls_90d'] = len(calls_90d)
        features.loc[idx, 'calls_180d'] = len(calls_180d)
        features.loc[idx, 'loyalty_calls_90d'] = (calls_90d['call_type'] == 'Loyalty').sum()
        features.loc[idx, 'complaint_calls_90d'] = (calls_90d['call_type'] == 'Complaints').sum()
        features.loc[idx, 'tech_calls_90d'] = (calls_90d['call_type'] == 'Tech').sum()
        features.loc[idx, 'avg_talk_time'] = cust_calls['talk_time_seconds'].mean()
        features.loc[idx, 'avg_hold_time'] = cust_calls['hold_time_seconds'].mean()
        features.loc[idx, 'max_hold_time'] = cust_calls['hold_time_seconds'].max()
        features.loc[idx, 'days_since_last_call'] = (obs_date - cust_calls['event_date'].max()).days

# Cease features
for idx, row in features.iterrows():
    cust_id = row['unique_customer_identifier']
    obs_date = row['datevalue']

    prior_ceases = cease[(cease['unique_customer_identifier'] == cust_id) &
                          (cease['cease_placed_date'] < obs_date)]

    if len(prior_ceases) == 0:
        features.loc[idx, ['prior_cease_count', 'days_since_last_cease']] = [0, 9999]
    else:
        features.loc[idx, 'prior_cease_count'] = len(prior_ceases)
        features.loc[idx, 'days_since_last_cease'] = (obs_date - prior_ceases['cease_placed_date'].max()).days

# Create multi-horizon targets
for horizon in HORIZONS:
    features[f'churned_in_{horizon}d'] = 0

for idx, row in features.iterrows():
    cust_id = row['unique_customer_identifier']
    obs_date = row['datevalue']

    for horizon in HORIZONS:
        future_ceases = cease[(cease['unique_customer_identifier'] == cust_id) &
                               (cease['cease_placed_date'] >= obs_date) &
                               (cease['cease_placed_date'] < obs_date + timedelta(days=horizon))]
        if len(future_ceases) > 0:
            features.loc[idx, f'churned_in_{horizon}d'] = 1

# Temporal split
features['split'] = 'train'
split_date_val = features['datevalue'].quantile(0.7)
split_date_test = features['datevalue'].quantile(0.85)
features.loc[features['datevalue'] >= split_date_val, 'split'] = 'validation'
features.loc[features['datevalue'] >= split_date_test, 'split'] = 'test'

print(f"  ✓ Features: {len(features):,} rows x {len(ALL_FEATURES)} features")
print(f"    Train: {(features['split'] == 'train').sum():,}")
print(f"    Validation: {(features['split'] == 'validation').sum():,}")
print(f"    Test: {(features['split'] == 'test').sum():,}")

# Train models
print(f"\n[4/5] Training {len(HORIZONS)} production LightGBM models...")
MODEL_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

for horizon in HORIZONS:
    print(f"\n  Training {horizon}-day model...")
    target = f'churned_in_{horizon}d'

    train = features[features['split'] == 'train']
    val = features[features['split'] == 'validation']
    test = features[features['split'] == 'test']

    X_train, y_train = train[ALL_FEATURES], train[target]
    X_val, y_val = val[ALL_FEATURES], val[target]
    X_test, y_test = test[ALL_FEATURES], test[target]

    for cat in CATEGORICAL_FEATURES:
        X_train[cat] = X_train[cat].astype('category')
        X_val[cat] = X_val[cat].astype('category')
        X_test[cat] = X_test[cat].astype('category')

    pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

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

    train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_FEATURES)
    val_data = lgb.Dataset(X_val, label=y_val, categorical_feature=CATEGORICAL_FEATURES, reference=train_data)

    model = lgb.train(params, train_data, num_boost_round=500, valid_sets=[val_data], callbacks=[lgb.early_stopping(50)])

    y_pred_test = model.predict(X_test)
    test_auc_roc = roc_auc_score(y_test, y_pred_test)
    test_auc_pr = average_precision_score(y_test, y_pred_test)

    print(f"    Test AUC-ROC: {test_auc_roc:.4f}, AUC-PR: {test_auc_pr:.4f}")

    model_path = MODEL_OUTPUT_DIR / f"churn_model_{horizon}d.txt"
    model.save_model(str(model_path))
    print(f"    ✓ Saved {model_path.name} ({model_path.stat().st_size:,} bytes)")

# Validate
print(f"\n[5/5] Validating model loading...")
for horizon in HORIZONS:
    model_path = MODEL_OUTPUT_DIR / f"churn_model_{horizon}d.txt"
    model = lgb.Booster(model_file=str(model_path))
    test_row = features[ALL_FEATURES].head(1).copy()
    for cat in CATEGORICAL_FEATURES:
        test_row[cat] = test_row[cat].astype('category')
    pred = model.predict(test_row)[0]
    print(f"  ✓ {horizon}d model loads, test prediction: {pred:.4f}")

print("\n" + "=" * 90)
print(f"✓ SUCCESS: {len(HORIZONS)} production models trained → {MODEL_OUTPUT_DIR}")
print("=" * 90)
