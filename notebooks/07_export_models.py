# Databricks notebook source
# ══════════════════════════════════════════════════════════════
# 07 — Export Champion Models to DBFS for AKS Serving
# ══════════════════════════════════════════════════════════════
# This notebook runs as part of the churn_pipeline workflow
# AFTER batch_scoring, BEFORE monitoring.
#
# What it does:
#   1. Loads 3 Champion models (30d/60d/90d) from Unity Catalog
#   2. Saves as LightGBM native .txt format to DBFS
#   3. Exports batch scores as CSV to DBFS
#
# The pipeline's sync_models_to_aks.sh script then picks up
# these files from DBFS and uploads them to the Azure File
# Share backing the AKS model PVC.

# COMMAND ----------

import os
import mlflow

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# Resolve catalog from environment
catalog = spark.conf.get("spark.databricks.unityCatalog.defaultCatalog", "uk_telecoms")
# Fallback: check for widget
try:
    catalog = dbutils.widgets.get("catalog")
except:
    pass

print(f"Using catalog: {catalog}")

HORIZONS = [30, 60, 90]
LOCAL_DIR = "/tmp/aks_models"
DBFS_DIR = "dbfs:/tmp/aks_models"

os.makedirs(LOCAL_DIR, exist_ok=True)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════
# STEP 1: Export Champion models to LightGBM native .txt
# ══════════════════════════════════════════════════════════════
# DBFS FUSE mount (/dbfs/) doesn't support append mode that
# LightGBM's save_model uses, so we save to local /tmp first
# then copy to DBFS.

exported = []

for horizon in HORIZONS:
    model_name = f"{catalog}.ml.churn_model_{horizon}d"
    model_uri = f"models:/{model_name}@Champion"

    print(f"Loading {model_uri}...")
    model = mlflow.lightgbm.load_model(model_uri)

    local_path = os.path.join(LOCAL_DIR, f"churn_model_{horizon}d.txt")
    model.booster_.save_model(local_path)
    size = os.path.getsize(local_path)

    dbfs_path = f"{DBFS_DIR}/churn_model_{horizon}d.txt"
    dbutils.fs.cp(f"file:{local_path}", dbfs_path)

    exported.append({"horizon": horizon, "path": dbfs_path, "size_bytes": size})
    print(f"  Exported: {dbfs_path} ({size:,} bytes)")

print(f"\nAll {len(HORIZONS)} models exported to {DBFS_DIR}")

# COMMAND ----------

# ══════════════════════════════════════════════════════════════
# STEP 2: Export batch scores as CSV
# ══════════════════════════════════════════════════════════════

scores_df = spark.table(f"{catalog}.gold.churn_scores")
row_count = scores_df.count()

local_scores = os.path.join(LOCAL_DIR, "churn_scores.csv")
scores_df.toPandas().to_csv(local_scores, index=False)
dbutils.fs.cp(f"file:{local_scores}", f"{DBFS_DIR}/churn_scores.csv")

scores_size = os.path.getsize(local_scores)
print(f"Exported scores: {DBFS_DIR}/churn_scores.csv ({row_count:,} rows, {scores_size:,} bytes)")

# COMMAND ----------

# ══════════════════════════════════════════════════════════════
# STEP 3: Verify exports
# ══════════════════════════════════════════════════════════════

files = dbutils.fs.ls(DBFS_DIR)
print("DBFS export directory contents:")
for f in files:
    print(f"  {f.name:30s} {f.size:>12,} bytes")

total_size = sum(f.size for f in files)
print(f"\nTotal: {len(files)} files, {total_size:,} bytes")

# Sanity check: models must be >= 1KB
for item in exported:
    assert item["size_bytes"] >= 1000, f"Model {item['horizon']}d suspiciously small: {item['size_bytes']} bytes"

print("\nAll exports verified. Ready for AKS sync.")
