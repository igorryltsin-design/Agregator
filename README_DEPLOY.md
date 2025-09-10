# Развёртывание без установки зависимостей

Ниже краткая инструкция, как перенести приложение на другой компьютер без установки пакетов из интернета.

## Вариант A: перенос conda‑окружения (рекомендуется)

1. Установите зависимости в текущем окружении (один раз):
   - `conda install -c conda-forge faster-whisper huggingface_hub ffmpeg`
2. Скачайте оффлайн‑модель faster‑whisper (например, large‑v3) в локальный кэш проекта:
   - `python3 scripts/test_transcribe_fw.py --model large-v3 --lang ru --cache ./models/faster-whisper sample_library/Audio_23857.wav`
3. Запакуйте окружение:
   - `conda install -c conda-forge conda-pack`
   - `conda pack -n <ИМЯ_ОКРУЖЕНИЯ> -o dist/flask_catalog_env.tar.gz`
4. Убедитесь, что в `.env` указан путь к модели (локальный каталог):
   - `TRANSCRIBE_MODEL_PATH=./models/faster-whisper/Systran__faster-whisper-large-v3`

Перенос и запуск на другом ПК (той же ОС/арх):

```bash
mkdir env && tar -xzf dist/flask_catalog_env.tar.gz -C env
env/bin/conda-unpack
env/bin/python app.py
# открыть http://localhost:5050
```

## Вариант B: Docker (опционально)

Если нужен полностью самодостаточный запуск — можно собрать Docker‑образ.
Примерный `Dockerfile` (упрощённый) можно добавить по запросу.

## Диагностика на целевой машине

- Проверка ffmpeg и “тишины” аудио:
  - `python3 scripts/test_ffmpeg.py sample_library/Audio_23857.wav`
- Проверка распознавания faster‑whisper без Flask:
  - `python3 scripts/test_transcribe_fw.py --model ./models/faster-whisper/Systran__faster-whisper-large-v3 --lang ru sample_library/Audio_23857.wav`
- В самом приложении — Настройки → «Диагностика транскрибации».

## Замечания

- Для переноса без интернета модель должна быть заранее положена в каталог проекта и указана в `.env`.
- Переносимое conda‑окружение совместимо в рамках той же ОС/архитектуры (например, macOS ARM → macOS ARM).
  Для другой платформы соберите пакет на целевой платформе (или используйте Docker).

