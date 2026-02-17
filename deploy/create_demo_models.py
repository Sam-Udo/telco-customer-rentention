#!/usr/bin/env python3
"""
Create demo LightGBM models for AKS deployment testing.
These are minimal but valid models matching the API's feature schema.
"""
import os
import sys
import numpy as np
import pandas as pd
import lightgbm as lgb

# Feature schema from API (must match exactly)
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

HORIZONS = [30, 60, 90]

def create_demo_models(output_dir: str):
    """Create minimal valid LightGBM models for each horizon."""
    os.makedirs(output_dir, exist_ok=True)

    print(f"Creating demo models in {output_dir}...")

    # Generate synthetic training data
    np.random.seed(42)
    n_samples = 500

    # Numeric features
    df = pd.DataFrame(
        np.random.randn(n_samples, len(NUMERIC_FEATURES)),
        columns=NUMERIC_FEATURES
    )

    # Categorical features
    df["technology"] = pd.Categorical(
        np.random.choice(["FTTP", "FTTC", "MPF", "WLR"], n_samples)
    )
    df["sales_channel"] = pd.Categorical(
        np.random.choice(["Online", "Phone", "Retail"], n_samples)
    )
    df["tenure_bucket"] = pd.Categorical(
        np.random.choice(["0-90d", "90d-1y", "1y-2y", "2y-3y", "3y+"], n_samples)
    )

    # Binary target (10% churn rate)
    y = np.random.binomial(1, 0.1, n_samples)

    # Train minimal model for each horizon
    for horizon in HORIZONS:
        print(f"\nTraining {horizon}-day model...")

        # Create LightGBM dataset
        train_data = lgb.Dataset(
            df,
            label=y,
            categorical_feature=CATEGORICAL_FEATURES,
            free_raw_data=False
        )

        # Train parameters (minimal but valid)
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "num_leaves": 7,
            "max_depth": 3,
            "learning_rate": 0.1,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": -1,
            "seed": 42 + horizon
        }

        # Train model
        model = lgb.train(
            params,
            train_data,
            num_boost_round=20,
            valid_sets=[train_data],
            valid_names=["train"]
        )

        # Save model
        model_path = os.path.join(output_dir, f"churn_model_{horizon}d.txt")
        model.save_model(model_path)

        # Verify model file
        file_size = os.path.getsize(model_path)
        print(f"  ✓ Saved {model_path} ({file_size:,} bytes)")

        # Validate model can load and predict
        loaded = lgb.Booster(model_file=model_path)
        test_pred = loaded.predict(df.head(1))
        print(f"  ✓ Validated (test prediction: {test_pred[0]:.4f})")

    print(f"\n✓ Successfully created {len(HORIZONS)} demo models")
    return True

if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/demo_models"
    create_demo_models(output_dir)
