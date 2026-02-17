"""Telco Churn Prediction — REST API serving multi-horizon (30/60/90 day) churn predictions."""
from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List

import lightgbm as lgb
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel, Field, field_validator

from risk_engine import RiskThresholds, assign_risk_tier, TIER_ACTIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("churn-api")

MODEL_DIR = os.getenv("MODEL_DIR", "/models")
HORIZONS = [30, 60, 90]
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "1000"))
RED_THRESHOLD = float(os.getenv("RED_THRESHOLD", "0.5"))
AMBER_THRESHOLD = float(os.getenv("AMBER_THRESHOLD", "0.5"))
YELLOW_THRESHOLD = float(os.getenv("YELLOW_THRESHOLD", "0.5"))

ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "ALLOWED_ORIGINS",
        "http://churn-dashboard-service:8501,http://localhost:8501",
    ).split(",")
]

REQUEST_COUNT = Counter(
    "churn_api_requests_total", "Total prediction requests", ["endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "churn_api_request_duration_seconds",
    "Request latency",
    ["endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)
PREDICTION_COUNT = Counter(
    "churn_api_predictions_total", "Total predictions made", ["horizon"]
)
TIER_COUNT = Counter(
    "churn_api_risk_tier_total", "Risk tier distribution", ["tier"]
)
MODELS_LOADED = Gauge(
    "churn_api_models_loaded", "Number of models successfully loaded"
)
MODEL_LOAD_DURATION = Histogram(
    "churn_api_model_load_duration_seconds",
    "Model loading duration",
    ["horizon"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0),
)

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

VALID_TECHNOLOGIES = {"FTTP", "FTTC", "MPF", "WLR", "Unknown"}
VALID_TENURE_BUCKETS = {"0-90d", "90d-1y", "1y-2y", "2y-3y", "3y+"}


models: Dict[int, lgb.Booster] = {}
model_metadata: Dict[str, str] = {}
failed_horizons: List[int] = []


def load_models():
    """Load all 3 horizon models from disk with validation."""
    global models, model_metadata, failed_horizons
    failed_horizons = []

    for horizon in HORIZONS:
        model_path = os.path.join(MODEL_DIR, f"churn_model_{horizon}d.txt")
        load_start = time.time()

        try:
            if not os.path.exists(model_path):
                logger.warning(f"Model file not found: {model_path}")
                failed_horizons.append(horizon)
                continue

            file_size = os.path.getsize(model_path)
            if file_size < 1000:
                logger.error(
                    f"Model {horizon}d suspiciously small: {file_size} bytes"
                )
                failed_horizons.append(horizon)
                continue

            model = lgb.Booster(model_file=model_path)

            dummy = pd.DataFrame(
                [[0.0] * len(NUMERIC_FEATURES)],
                columns=NUMERIC_FEATURES,
            )
            for cat in CATEGORICAL_FEATURES:
                dummy[cat] = pd.Categorical(["Unknown"])
            _ = model.predict(dummy)

            models[horizon] = model
            load_duration = time.time() - load_start
            MODEL_LOAD_DURATION.labels(horizon=str(horizon)).observe(load_duration)
            logger.info(
                f"Loaded and validated: churn_model_{horizon}d "
                f"({file_size / 1024:.0f} KB, {load_duration:.2f}s)"
            )
        except Exception as e:
            logger.error(f"Failed to load {horizon}d model: {e}")
            failed_horizons.append(horizon)

    MODELS_LOADED.set(len(models))
    model_metadata["loaded_at"] = datetime.now(timezone.utc).isoformat()
    model_metadata["model_count"] = str(len(models))
    model_metadata["horizons"] = str(list(models.keys()))
    model_metadata["failed_horizons"] = str(failed_horizons)
    logger.info(f"Model registry ready: {len(models)}/{len(HORIZONS)} models loaded")


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_models()
    yield
    models.clear()
    logger.info("Models unloaded")


app = FastAPI(
    title="Telco Churn Prediction API",
    description="Multi-horizon churn prediction (30/60/90 day) with risk tier assignment",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type", "Accept"],
)


class CustomerFeatures(BaseModel):
    """Input features for a single customer prediction."""
    customer_id: str = Field(
        ..., min_length=1, max_length=50, description="Unique customer identifier"
    )
    contract_status_ord: float = Field(
        0, ge=0, le=6, description="Ordinal-encoded contract status (1-6)"
    )
    contract_dd_cancels: float = Field(0, ge=0)
    dd_cancel_60_day: float = Field(0, ge=0)
    ooc_days: float = Field(-9999, description="Out-of-contract days (-9999 = in contract)")
    speed: float = Field(0, ge=0, le=10000, description="Advertised speed (Mbps)")
    line_speed: float = Field(0, ge=0, le=10000, description="Actual line speed (Mbps)")
    speed_gap_pct: float = Field(
        0, ge=-100, le=100, description="Gap between advertised and actual speed (%)"
    )
    tenure_days: float = Field(0, ge=0, le=36500, description="Customer tenure in days")
    calls_30d: float = Field(0, ge=0, description="Calls in last 30 days")
    calls_90d: float = Field(0, ge=0, description="Calls in last 90 days")
    calls_180d: float = Field(0, ge=0, description="Calls in last 180 days")
    loyalty_calls_90d: float = Field(0, ge=0, description="Loyalty calls in last 90 days")
    complaint_calls_90d: float = Field(0, ge=0, description="Complaint calls in last 90 days")
    tech_calls_90d: float = Field(0, ge=0, description="Tech support calls in last 90 days")
    avg_talk_time: float = Field(0, ge=0)
    avg_hold_time: float = Field(0, ge=0)
    max_hold_time: float = Field(0, ge=0)
    days_since_last_call: float = Field(0, ge=0)
    monthly_download_mb: float = Field(0, ge=0)
    monthly_upload_mb: float = Field(0, ge=0)
    monthly_total_mb: float = Field(0, ge=0)
    avg_daily_download_mb: float = Field(0, ge=0)
    std_daily_total_mb: float = Field(0, ge=0)
    active_days_in_month: float = Field(0, ge=0, le=31)
    peak_daily_total_mb: float = Field(0, ge=0)
    usage_mom_change: float = Field(
        0, ge=-10, le=10, description="Month-over-month usage change ratio (capped +/-10x)"
    )
    usage_vs_3mo_avg: float = Field(
        0, ge=-10, le=10, description="Usage vs 3-month rolling average ratio (capped +/-10x)"
    )
    prior_cease_count: float = Field(0, ge=0)
    days_since_last_cease: float = Field(0, ge=0)
    technology: str = Field(
        "Unknown", description="Broadband technology (FTTP, FTTC, MPF, WLR, Unknown)"
    )
    sales_channel: str = Field("Unknown")
    tenure_bucket: str = Field(
        "0-90d", description="Tenure bucket (0-90d, 90d-1y, 1y-2y, 2y-3y, 3y+)"
    )

    @field_validator("technology")
    @classmethod
    def validate_technology(cls, v: str) -> str:
        if v not in VALID_TECHNOLOGIES:
            raise ValueError(
                f"Invalid technology: {v}. Must be one of {VALID_TECHNOLOGIES}"
            )
        return v

    @field_validator("tenure_bucket")
    @classmethod
    def validate_tenure_bucket(cls, v: str) -> str:
        if v not in VALID_TENURE_BUCKETS:
            raise ValueError(
                f"Invalid tenure bucket: {v}. Must be one of {VALID_TENURE_BUCKETS}"
            )
        return v


class HorizonPrediction(BaseModel):
    horizon_days: int
    churn_probability: float
    risk_decile: int


class PredictionResponse(BaseModel):
    customer_id: str
    predictions: List[HorizonPrediction]
    risk_tier: str
    recommended_action: str
    action_owner: str
    action_sla: str
    scored_at: str
    request_id: str


class BatchRequest(BaseModel):
    customers: List[CustomerFeatures] = Field(..., max_length=1000)


class BatchResponse(BaseModel):
    predictions: List[PredictionResponse]
    batch_size: int
    scored_at: str
    request_id: str


class ModelInfo(BaseModel):
    loaded_at: str
    model_count: int
    horizons: List[int]
    failed_horizons: List[int]
    thresholds: Dict[str, float]


def features_to_dataframe(customer: CustomerFeatures) -> pd.DataFrame:
    """Convert a CustomerFeatures pydantic model to a single-row DataFrame."""
    data = {feat: [getattr(customer, feat)] for feat in NUMERIC_FEATURES}
    for feat in CATEGORICAL_FEATURES:
        data[feat] = pd.Categorical([getattr(customer, feat)])
    return pd.DataFrame(data)


def predict_single(customer: CustomerFeatures, request_id: str) -> PredictionResponse:
    """Run all 3 horizon models on a single customer."""
    df = features_to_dataframe(customer)
    thresholds = RiskThresholds(
        red=RED_THRESHOLD, amber=AMBER_THRESHOLD, yellow=YELLOW_THRESHOLD
    )

    horizon_results = []
    probs = {}

    for horizon in HORIZONS:
        if horizon not in models:
            raise HTTPException(
                status_code=503,
                detail=f"Model for {horizon}d horizon not loaded",
            )
        prob = float(models[horizon].predict(df)[0])
        probs[horizon] = prob
        decile = min(int(prob * 10) + 1, 10)
        horizon_results.append(
            HorizonPrediction(
                horizon_days=horizon,
                churn_probability=round(prob, 6),
                risk_decile=decile,
            )
        )
        PREDICTION_COUNT.labels(horizon=str(horizon)).inc()

    tier = assign_risk_tier(probs[30], probs[60], probs[90], thresholds)
    action = TIER_ACTIONS[tier]
    TIER_COUNT.labels(tier=tier).inc()

    return PredictionResponse(
        customer_id=customer.customer_id,
        predictions=horizon_results,
        risk_tier=tier,
        recommended_action=action["action"],
        action_owner=action["owner"],
        action_sla=action["sla"],
        scored_at=datetime.now(timezone.utc).isoformat(),
        request_id=request_id,
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict(customer: CustomerFeatures, request: Request):
    """Score a single customer across all 3 horizons (30/60/90 day)."""
    request_id = str(uuid.uuid4())
    start = time.time()
    try:
        result = predict_single(customer, request_id)
        REQUEST_COUNT.labels(endpoint="/predict", status="success").inc()
        logger.info(
            f"prediction_success request_id={request_id} "
            f"customer_id={customer.customer_id} "
            f"tier={result.risk_tier} "
            f"duration_ms={int((time.time() - start) * 1000)}"
        )
        return result
    except HTTPException:
        REQUEST_COUNT.labels(endpoint="/predict", status="error").inc()
        raise
    except Exception as e:
        REQUEST_COUNT.labels(endpoint="/predict", status="error").inc()
        logger.error(
            f"prediction_failed request_id={request_id} "
            f"customer_id={customer.customer_id} "
            f"error={type(e).__name__}"
        )
        raise HTTPException(status_code=500, detail="Prediction failed")
    finally:
        REQUEST_LATENCY.labels(endpoint="/predict").observe(time.time() - start)


@app.post("/predict/batch", response_model=BatchResponse)
async def predict_batch(batch: BatchRequest):
    """Score multiple customers in a single request (max 1000)."""
    request_id = str(uuid.uuid4())
    start = time.time()
    try:
        results = [predict_single(c, request_id) for c in batch.customers]
        REQUEST_COUNT.labels(endpoint="/predict/batch", status="success").inc()
        logger.info(
            f"batch_success request_id={request_id} "
            f"batch_size={len(results)} "
            f"duration_ms={int((time.time() - start) * 1000)}"
        )
        return BatchResponse(
            predictions=results,
            batch_size=len(results),
            scored_at=datetime.now(timezone.utc).isoformat(),
            request_id=request_id,
        )
    except HTTPException:
        REQUEST_COUNT.labels(endpoint="/predict/batch", status="error").inc()
        raise
    except Exception as e:
        REQUEST_COUNT.labels(endpoint="/predict/batch", status="error").inc()
        logger.error(
            f"batch_failed request_id={request_id} "
            f"batch_size={len(batch.customers)} "
            f"error={type(e).__name__}"
        )
        raise HTTPException(status_code=500, detail="Batch prediction failed")
    finally:
        REQUEST_LATENCY.labels(endpoint="/predict/batch").observe(time.time() - start)


@app.get("/health")
async def health():
    """Liveness probe — always returns OK if the process is running."""
    return {"status": "healthy"}


@app.get("/ready")
async def ready():
    """Readiness probe — returns OK only if all 3 models are loaded."""
    if len(models) < len(HORIZONS):
        raise HTTPException(
            status_code=503,
            detail=f"Only {len(models)}/{len(HORIZONS)} models loaded. "
            f"Failed: {failed_horizons}",
        )
    return {"status": "ready", "models_loaded": len(models)}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint for HPA and Grafana."""
    return JSONResponse(
        content=generate_latest().decode("utf-8"),
        media_type="text/plain",
    )


@app.get("/model/info", response_model=ModelInfo)
async def model_info():
    """Model metadata: versions, load time, thresholds."""
    return ModelInfo(
        loaded_at=model_metadata.get("loaded_at", "not loaded"),
        model_count=len(models),
        horizons=list(models.keys()),
        failed_horizons=failed_horizons,
        thresholds={
            "red_30d": RED_THRESHOLD,
            "amber_60d": AMBER_THRESHOLD,
            "yellow_90d": YELLOW_THRESHOLD,
        },
    )
