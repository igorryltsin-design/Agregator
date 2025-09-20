# Многоэтапная сборка: сначала frontend на Node, затем запуск Flask на Python

FROM node:20-alpine AS web
WORKDIR /build

# Сборка основного фронтенда
COPY frontend/package*.json frontend/
RUN cd frontend && npm ci
COPY frontend frontend
RUN cd frontend && npm run build

# Сборка фронтенда AiWord (отдаётся из /aiword)
COPY AiWord/package*.json AiWord/
RUN cd AiWord && npm ci
COPY AiWord AiWord
RUN cd AiWord && npm run build

FROM python:3.11-slim AS app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

# Системные зависимости: tesseract для OCR, ffmpeg для длительности/транскрипции аудио
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-rus ffmpeg libmagic1 curl \
    && rm -rf /var/lib/apt/lists/*

# Python-зависимости
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Исходники приложения
COPY . ./
# Копируем собранные фронтенды из этапа Node
COPY --from=web /build/frontend/dist ./frontend/dist
COPY --from=web /build/AiWord/dist ./AiWord/dist

# Переменные окружения по умолчанию (переопределяются в compose)
ENV FLASK_ENV=production \
    SCAN_ROOT=/data \
    OCR_LANGS=rus+eng \
    PDF_OCR_PAGES=5 \
    OCR_DISS_FIRST_PAGE=1

EXPOSE 5050
CMD ["python", "app.py"]
