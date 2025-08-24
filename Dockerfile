# syntax=docker/dockerfile:1
FROM python:3.12-slim

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
