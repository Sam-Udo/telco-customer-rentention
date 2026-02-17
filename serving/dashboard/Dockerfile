# ══════════════════════════════════════════════════════════════
# Telco Churn Dashboard — Production Docker Image
# ══════════════════════════════════════════════════════════════
# Streamlit dashboard with Plotly visualisations.
# Final image: ~400MB (python-slim + streamlit + plotly)

# ── Stage 1: Build dependencies ──
FROM python:3.11-slim AS builder

WORKDIR /build
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Production runtime ──
FROM python:3.11-slim

# Install curl for health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Security: non-root user
RUN groupadd -r dashboard --gid=1000 && \
    useradd -r -g dashboard --uid=1000 -d /app -s /sbin/nologin dashboard

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY dashboard/app.py ./

# Create data directory (for mounted scores CSV)
RUN mkdir -p /data && \
    chown dashboard:dashboard /data && \
    chmod 550 /data

# Streamlit config directory
RUN mkdir -p /app/.streamlit && \
    chown dashboard:dashboard /app/.streamlit

# Copy Streamlit config
COPY <<'EOF' /app/.streamlit/config.toml
[server]
headless = true
port = 8501
enableCORS = true
enableXsrfProtection = true
maxUploadSize = 200

[logger]
level = "info"

[client]
showErrorDetails = false
toolbarMode = "minimal"

[browser]
gatherUsageStats = false

[theme]
primaryColor = "#DC3545"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F8F9FA"
textColor = "#212529"
EOF

RUN chown dashboard:dashboard /app/.streamlit/config.toml

# Switch to non-root user
USER dashboard

# Expose port
EXPOSE 8501

# Health check using curl
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
CMD ["streamlit", "run", "app.py", \
     "--server.address", "0.0.0.0", \
     "--server.port", "8501"]
