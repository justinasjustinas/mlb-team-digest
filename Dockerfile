# syntax=docker/dockerfile:1
FROM python@sha256:6d09f05f9bebe88e6100f8feaf8b7eaacf7d5005c45e26dfec66f64fdf75f1a4

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps: tzdata gives zoneinfo the timezone DB
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for better layer cache
COPY requirements.txt .
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

# Copy code (this brings in both mlb_ingest.py and game_digest.py)
COPY . .

# Default: run the ingest
ENTRYPOINT ["python", "/app/mlb_ingest.py"]
