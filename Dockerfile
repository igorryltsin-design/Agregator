# Multi-stage build: build frontend with Node, then run Flask with Python

FROM node:20-alpine AS web
WORKDIR /build

# Build main frontend
COPY frontend/package*.json frontend/
RUN cd frontend && npm ci
COPY frontend frontend
RUN cd frontend && npm run build

# Build AiWord frontend (served from /aiword)
COPY AiWord/package*.json AiWord/
RUN cd AiWord && npm ci
COPY AiWord AiWord
RUN cd AiWord && npm run build

FROM python:3.11-slim AS app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

# System deps: tesseract for OCR, ffmpeg for audio duration/transcription helpers
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-rus ffmpeg libmagic1 curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# App sources
COPY . ./
# Copy built frontends from the Node stage
COPY --from=web /build/frontend/dist ./frontend/dist
COPY --from=web /build/AiWord/dist ./AiWord/dist

# Default env (override in compose)
ENV FLASK_ENV=production \
    SCAN_ROOT=/data \
    OCR_LANGS=rus+eng \
    PDF_OCR_PAGES=5 \
    OCR_DISS_FIRST_PAGE=1

EXPOSE 5050
CMD ["python", "app.py"]
