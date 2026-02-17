"""
Export MLflow Champion models to LightGBM native format for AKS serving.
=======================================================================
Bridges Databricks MLflow → AKS by exporting LightGBM .txt model files
to Azure File Share (mounted as PVC in AKS pods).

Run this script on Databricks or locally with:
    python deploy/export_models_for_aks.py --output-dir /mnt/models --env prod

What it does:
    1. Loads Champion model from Unity Catalog Model Registry
    2. Saves as LightGBM native .txt format (no MLflow dependency needed at serving time)
    3. Uploads to Azure File Share (mounted as PVC in AKS pods)
"""

import argparse
import logging
import os
import tempfile

import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("model-export")

HORIZONS = [30, 60, 90]


def export_models(catalog: str, output_dir: str):
    """Export all 3 horizon Champion models to LightGBM native format."""
    mlflow.set_registry_uri("databricks-uc")

    os.makedirs(output_dir, exist_ok=True)

    for horizon in HORIZONS:
        model_name = f"{catalog}.ml.churn_model_{horizon}d"
        model_uri = f"models:/{model_name}@Champion"

        logger.info(f"Loading {model_uri}...")
        model = mlflow.lightgbm.load_model(model_uri)

        output_path = os.path.join(output_dir, f"churn_model_{horizon}d.txt")
        model.save_model(output_path)
        logger.info(f"Exported: {output_path}")

    logger.info(f"All {len(HORIZONS)} models exported to {output_dir}")


def export_scores(catalog: str, output_dir: str):
    """Export latest batch scores as CSV for the dashboard."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    scores_df = spark.table(f"{catalog}.gold.churn_scores")
    output_path = os.path.join(output_dir, "churn_scores.csv")
    scores_df.toPandas().to_csv(output_path, index=False)
    logger.info(f"Exported scores: {output_path} ({scores_df.count()} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export MLflow models for AKS serving")
    parser.add_argument("--output-dir", required=True, help="Output directory for model files")
    parser.add_argument("--env", default="prod", choices=["dev", "staging", "prod"])
    parser.add_argument("--export-scores", action="store_true", help="Also export batch scores CSV")
    args = parser.parse_args()

    catalog = "uk_telecoms" if args.env == "prod" else f"uk_telecoms_{args.env}"
    export_models(catalog, args.output_dir)

    if args.export_scores:
        scores_dir = os.path.join(os.path.dirname(args.output_dir), "data")
        export_scores(catalog, scores_dir)
