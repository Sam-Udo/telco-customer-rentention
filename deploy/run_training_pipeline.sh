#!/bin/bash
#
# Run the full training pipeline with local Spark
# Produces production LightGBM models for AKS deployment
#

set -e  # Exit on error

MODEL_OUTPUT_DIR=${1:-"/tmp/trained_models"}

echo "================================================================================================"
echo "Telco Churn — Full Training Pipeline (Local Spark)"
echo "================================================================================================"
echo "Output: $MODEL_OUTPUT_DIR"
echo ""

# Check Java
if ! command -v java &> /dev/null; then
    echo "ERROR: Java is required but not installed."
    echo "Install Java:"
    echo "  macOS:  brew install openjdk@11"
    echo "  Ubuntu: sudo apt-get install openjdk-11-jdk"
    exit 1
fi

echo "✓ Java version: $(java -version 2>&1 | head -n 1)"
echo ""

# Run pipeline
echo "[1/3] Bronze ingestion..."
python3 notebooks/01_bronze_ingestion_local.py
echo ""

echo "[2/3] Silver cleaning..."
python3 notebooks/02_silver_cleaning_local.py
echo ""

echo "[3/3] Model training..."
python3 notebooks/04_model_training_local.py "$MODEL_OUTPUT_DIR"
echo ""

echo "================================================================================================"
echo "✓ PIPELINE COMPLETE"
echo "Models saved to: $MODEL_OUTPUT_DIR"
ls -lh "$MODEL_OUTPUT_DIR"
echo "================================================================================================"
