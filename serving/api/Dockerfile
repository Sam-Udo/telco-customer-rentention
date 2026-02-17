# ══════════════════════════════════════════════════════════════
# Telco Churn Prediction API — Production Docker Image
# ══════════════════════════════════════════════════════════════
# Multi-stage build for minimal attack surface and image size.
# Final image: ~250MB (python-slim + lightgbm + fastapi)

# ── Stage 1: Build dependencies ──
FROM python:3.11-slim AS builder

WORKDIR /build
COPY api/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Production runtime ──
FROM python:3.11-slim

# Install curl for health checks and libgomp1 for LightGBM
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl libgomp1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Security: non-root user
RUN groupadd -r churnapi --gid=1000 && \
    useradd -r -g churnapi --uid=1000 -d /app -s /sbin/nologin churnapi

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY api/main.py api/risk_engine.py ./

# Create model directory with restricted permissions
RUN mkdir -p /models && \
    chown churnapi:churnapi /models && \
    chmod 550 /models

# Create tmp directory for uvicorn (required for writable filesystem)
RUN mkdir -p /app/tmp && \
    chown churnapi:churnapi /app/tmp && \
    chmod 750 /app/tmp

# Switch to non-root user
USER churnapi

# Expose port
EXPOSE 8000

# Health check using curl
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run with uvicorn (single worker per pod — HPA handles scaling)
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--access-log", \
     "--log-level", "info"]
