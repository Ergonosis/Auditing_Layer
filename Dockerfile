# Stage 1: install dependencies
FROM python:3.13-slim AS builder
WORKDIR /build

# Unification repo is copied in by cloudbuild.yaml before docker build
COPY ergonosis_unification/ /build/ergonosis_unification/
COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e /build/ergonosis_unification[databricks,gcp] \
 && pip install --no-cache-dir -r requirements.txt \
    google-cloud-secret-manager>=2.16

# Stage 2: runtime
FROM python:3.13-slim AS runtime
WORKDIR /app

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /build/ergonosis_unification /app/ergonosis_unification

# Re-register the editable install path
RUN pip install --no-cache-dir --no-deps -e /app/ergonosis_unification

COPY src/ ./src/
COPY config/ ./config/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV UNIFICATION_SRC_PATH=/app/ergonosis_unification/src

ENTRYPOINT ["python", "-m", "src.main"]
