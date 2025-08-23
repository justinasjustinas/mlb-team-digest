# syntax=docker/dockerfile:1

# --- Base runtime image ---
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps (add gcc/libpq/etc. only if you need them)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create app dir
WORKDIR /app

# Install dependencies first (better cache)
# If you don't have requirements.txt yet, create one; otherwise remove this block.
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy code
COPY . /app

# Default command runs the ingest (override in Cloud Run Job args)
# Example args get passed after the image, e.g.
#   gcloud run jobs create ... --args="--team","112","--date","2025-08-23"
ENTRYPOINT ["python", "/app/mlb_ingest.py"]
