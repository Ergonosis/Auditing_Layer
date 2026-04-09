# Stage 1: install dependencies
FROM python:3.13-slim AS builder
WORKDIR /build

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
    google-cloud-secret-manager>=2.16 \
    databricks-sql-connector>=4.0

# Stage 2: runtime image
FROM python:3.13-slim AS runtime
WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code and config
COPY src/ ./src/
COPY tests/fixtures/ ./tests/fixtures/
COPY unification_config.yaml ./

# Flush stdout/stderr immediately so Cloud Run captures all log lines
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default: incremental run. Override CMD args in the Cloud Run Job spec for full_refresh.
ENTRYPOINT ["python", "-m", "src.pipeline"]
CMD ["--type", "incremental"]
