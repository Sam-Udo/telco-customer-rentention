"""
Export MLflow Champion models to LightGBM native format for AKS serving.
=======================================================================
Bridges Databricks MLflow → AKS by exporting LightGBM .txt model files
and optionally uploading them to Azure File Share (mounted as PVC in AKS).

Usage:
    # On Databricks (export to DBFS — used by workflow notebook 07):
    python deploy/export_models_for_aks.py --output-dir /tmp/models --env dev

    # From CI/CD agent (download from DBFS + upload to Azure File Share):
    python deploy/export_models_for_aks.py \
        --output-dir /tmp/models --env dev \
        --upload-to-afs \
        --storage-account f66e629f0f9144baeb089fc \
        --storage-key $STORAGE_KEY \
        --model-share pvc-xxxx --scores-share pvc-yyyy

What it does:
    1. Loads Champion models from Unity Catalog Model Registry
    2. Saves as LightGBM native .txt format (no MLflow dependency at serving time)
    3. Optionally uploads to Azure File Share (AKS PVC backing store)
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
    exported = []

    for horizon in HORIZONS:
        model_name = f"{catalog}.ml.churn_model_{horizon}d"
        model_uri = f"models:/{model_name}@Champion"

        logger.info(f"Loading {model_uri}...")
        model = mlflow.lightgbm.load_model(model_uri)

        output_path = os.path.join(output_dir, f"churn_model_{horizon}d.txt")
        model.booster_.save_model(output_path)

        size = os.path.getsize(output_path)
        assert size >= 1000, f"Model {horizon}d suspiciously small: {size} bytes"
        exported.append(output_path)
        logger.info(f"Exported: {output_path} ({size:,} bytes)")

    logger.info(f"All {len(HORIZONS)} models exported to {output_dir}")
    return exported


def export_scores(catalog: str, output_dir: str):
    """Export latest batch scores as CSV for the dashboard."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    os.makedirs(output_dir, exist_ok=True)
    scores_df = spark.table(f"{catalog}.gold.churn_scores")
    output_path = os.path.join(output_dir, "churn_scores.csv")
    scores_df.toPandas().to_csv(output_path, index=False)
    logger.info(f"Exported scores: {output_path} ({scores_df.count()} rows)")
    return output_path


def upload_to_azure_file_share(local_files, storage_account, storage_key, share_name):
    """Upload files to Azure File Share (the backing store for AKS PVCs)."""
    from azure.storage.fileshare import ShareFileClient

    for local_path in local_files:
        file_name = os.path.basename(local_path)
        file_client = ShareFileClient(
            account_url=f"https://{storage_account}.file.core.windows.net",
            share_name=share_name,
            file_path=file_name,
            credential=storage_key,
        )
        with open(local_path, "rb") as f:
            file_client.upload_file(f)
        logger.info(f"Uploaded to AFS: {share_name}/{file_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export MLflow models for AKS serving")
    parser.add_argument("--output-dir", required=True, help="Output directory for model files")
    parser.add_argument("--env", default="prod", choices=["dev", "staging", "prod"])
    parser.add_argument("--export-scores", action="store_true", help="Also export batch scores CSV")
    parser.add_argument("--upload-to-afs", action="store_true", help="Upload to Azure File Share")
    parser.add_argument("--storage-account", help="Azure Storage account name (for AFS upload)")
    parser.add_argument("--storage-key", help="Azure Storage account key (for AFS upload)")
    parser.add_argument("--model-share", help="Azure File Share name for models PVC")
    parser.add_argument("--scores-share", help="Azure File Share name for scores PVC")
    args = parser.parse_args()

    catalog = "uk_telecoms" if args.env == "prod" else f"uk_telecoms_{args.env}"
    model_files = export_models(catalog, args.output_dir)

    scores_file = None
    if args.export_scores:
        scores_dir = os.path.join(args.output_dir, "scores")
        scores_file = export_scores(catalog, scores_dir)

    if args.upload_to_afs:
        if not all([args.storage_account, args.storage_key, args.model_share]):
            parser.error("--upload-to-afs requires --storage-account, --storage-key, --model-share")

        upload_to_azure_file_share(model_files, args.storage_account, args.storage_key, args.model_share)

        if scores_file and args.scores_share:
            upload_to_azure_file_share([scores_file], args.storage_account, args.storage_key, args.scores_share)

    logger.info("Export complete.")
