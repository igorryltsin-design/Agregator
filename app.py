from logging.handlers import RotatingFileHandler
# REST API и маршруты импорта/экспорта перенесены в `routes.py` как Blueprint.
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
import itertools

from flask import Flask, request, redirect, url_for, jsonify, send_from_directory, send_file, Response, make_response, session, g, abort, stream_with_context
from functools import wraps
from werkzeug.utils import secure_filename
import sqlite3
import threading
from queue import Queue

try:
    from flask import copy_current_app_context
except ImportError:  # запасной вариант для старых версий Flask
    def copy_current_app_context(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from flask import current_app
            with current_app.app_context():
                return func(*args, **kwargs)
        return wrapper
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import aliased
from models import (
    db,
    File,
    Tag,
    TagSchema,
    ChangeLog,
    upsert_tag,
    file_to_dict,
    Collection,
    User,
    CollectionMember,
    UserActionLog,
    TaskRecord,
    LlmEndpoint,
    AiWordAccess,
)

import fitz  # библиотека PyMuPDF
import requests
from dotenv import load_dotenv
try:
    import docx
except ImportError:
    docx = None
try:
    from striprtf.striprtf import rtf_to_text
except ImportError:
    rtf_to_text = None
try:
    from ebooklib import epub
except ImportError:
    epub = None
try:
    import djvu.decode
except ImportError:
    djvu = None
try:
    import pytesseract  # необязательный OCR для PDF, состоящих из изображений
except ImportError:
    pytesseract = None
try:
    from PIL import Image as PILImage
except Exception:
    PILImage = None
try:
    from faster_whisper import WhisperModel as FasterWhisperModel
except Exception:
    FasterWhisperModel = None
try:
    from huggingface_hub import snapshot_download as hf_snapshot_download
except Exception:
    hf_snapshot_download = None
import subprocess, shutil, wave
import time
from html import escape

# ------------------- RU morphology (Natasha/pymorphy2) -------------------
try:
    from razdel import tokenize as _ru_tokenize
except Exception:
    _ru_tokenize = None
try:
    import pymorphy2
    _morph = pymorphy2.MorphAnalyzer()
except Exception:
    _morph = None

logger = logging.getLogger(__name__)

_RU_SYNONYMS = {
    'машина': ['автомобиль', 'авто', 'тачка', 'car'],
    'изображение': ['картинка', 'фото', 'фотография', 'image', 'picture', 'снимок', 'скриншот', 'screenshot'],
    'вуз': ['университет', 'институт', 'универ'],
    'статья': ['публикация', 'paper', 'publication', 'пейпер'],
    'диссертация': ['дисс', 'thesis', 'dissertation'],
    'журнал': ['journal', 'magazine', 'сборник'],
    'номер': ['выпуск', 'issue'],
    'страница': ['стр', 'страницы', 'pages', 'page'],
    'год': ['лет', 'г'],
}

def _ru_tokens(text: str) -> list[str]:
    s = (text or '').lower()
    if _ru_tokenize:
        try:
            return [t.text for t in _ru_tokenize(s)]
        except Exception:
            pass
    import re as _re
    return _re.sub(r"[^\w\d]+", " ", s).strip().split()

def _lemma(word: str) -> str:
    if _morph:
        try:
            p = _morph.parse(word)
            if p:
                return p[0].normal_form
        except Exception:
            pass
    return word.lower()

def _lemmas(text: str) -> list[str]:
    return [_lemma(w) for w in _ru_tokens(text)]

def _expand_synonyms(lemmas: list[str]) -> set[str]:
    out = set(lemmas)
    for l in list(lemmas):
        for s in _RU_SYNONYMS.get(l, []):
            out.add(_lemma(s))
    return out

LLM_ROUND_ROBIN: dict[str, itertools.cycle] = {}
LLM_ENDPOINT_SIGNATURE: dict[str, tuple[tuple[int, float, tuple[str, ...]], ...]] = {}
LLM_ENDPOINT_POOLS: dict[str, list[dict[str, str]]] = {}
LLM_ENDPOINT_UNIQUE: dict[str, list[dict[str, str]]] = {}
LLM_BUSY_HTTP_CODES = {409, 423, 429, 503}
LLM_BUSY_STATUS_VALUES = {'busy', 'processing', 'in_progress', 'queued', 'pending', 'rate_limited'}
LLM_PURPOSES = [
    {'id': 'summary', 'label': 'Резюме стенограмм'},
    {'id': 'keywords', 'label': 'Ключевые слова'},
    {'id': 'compose', 'label': 'Генерация ответов'},
    {'id': 'metadata', 'label': 'Извлечение метаданных'},
    {'id': 'vision', 'label': 'Анализ изображений'},
    {'id': 'rerank', 'label': 'Реранжирование поиска'},
    {'id': 'default', 'label': 'По умолчанию'},
]
TASK_RETENTION_WINDOW = timedelta(days=1)
TASK_FINAL_STATUSES = ('completed', 'error', 'cancelled')

def _row_text_for_search(f: File) -> str:
    parts = [
        f.title or '', f.author or '', f.keywords or '', f.filename or '',
        getattr(f, 'text_excerpt', '') or '', getattr(f, 'abstract', '') or ''
    ]
    try:
        parts.extend([f"{t.key}={t.value}" for t in (f.tags or []) if t and t.key and t.value])
    except Exception:
        pass
    return ' '.join(parts)

# Простое кэширующее хранилище в памяти для расширения ключевых слов ИИ
AI_EXPAND_CACHE: dict[str, tuple[float, list[str]]] = {}

# Весовые коэффициенты и параметры оценки (можно менять через переменные окружения)
def _getf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

AI_SCORE_TITLE = _getf('AI_SCORE_TITLE', 2.5)
AI_SCORE_AUTHOR = _getf('AI_SCORE_AUTHOR', 1.5)
AI_SCORE_KEYWORDS = _getf('AI_SCORE_KEYWORDS', 1.2)
AI_SCORE_EXCERPT = _getf('AI_SCORE_EXCERPT', 1.0)
AI_SCORE_ABSTRACT = _getf('AI_SCORE_ABSTRACT', 1.0)
AI_SCORE_TAG = _getf('AI_SCORE_TAG', 1.0)
AI_BOOST_PHRASE = _getf('AI_BOOST_PHRASE', 3.0)
AI_BOOST_MULTI = _getf('AI_BOOST_MULTI', 0.6)  # дополнительный бонус за каждое уникальное слово
AI_BOOST_SNIPPET_COOCCUR = _getf('AI_BOOST_SNIPPET_COOCCUR', 0.8)

def _now() -> float:
    return time.time()

def _sha256(s: str) -> str:
    try:
        return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()
    except Exception:
        return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

# ------------------- Конфигурация -------------------

BASE_DIR = Path(__file__).parent
# Подхватить .env, если есть рядом
load_dotenv(BASE_DIR / ".env") if (BASE_DIR / ".env").exists() else None

def getenv_bool(name, default=False):
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

SCAN_ROOT = Path(os.getenv("SCAN_ROOT", str(BASE_DIR / "sample_library")))
EXTRACT_TEXT = getenv_bool("EXTRACT_TEXT", True)
OCR_LANGS_CFG = os.getenv("OCR_LANGS", "rus+eng")
PDF_OCR_PAGES_CFG = int(os.getenv("PDF_OCR_PAGES", "5"))
# Всегда запускать OCR для первой страницы диссертации (по умолчанию включено)
ALWAYS_OCR_FIRST_PAGE_DISSERTATION = getenv_bool("OCR_DISS_FIRST_PAGE", True)
DEFAULT_USE_LLM = getenv_bool("DEFAULT_USE_LLM", False)
DEFAULT_PRUNE = getenv_bool("DEFAULT_PRUNE", True)
COLLECTIONS_IN_SEPARATE_DIRS = getenv_bool("COLLECTIONS_IN_SEPARATE_DIRS", False)
COLLECTION_TYPE_SUBDIRS = getenv_bool("COLLECTION_TYPE_SUBDIRS", False)

LMSTUDIO_API_BASE = os.getenv("LMSTUDIO_API_BASE", "http://localhost:1234/v1")
# Модель LLM по умолчанию
LMSTUDIO_MODEL = os.getenv("LMSTUDIO_MODEL", "google/gemma-3n-e4b")
LMSTUDIO_API_KEY = os.getenv("LMSTUDIO_API_KEY", "")
TRANSCRIBE_ENABLED = getenv_bool("TRANSCRIBE_ENABLED", True)
TRANSCRIBE_BACKEND = os.getenv("TRANSCRIBE_BACKEND", "faster-whisper")
TRANSCRIBE_MODEL_PATH = os.getenv("TRANSCRIBE_MODEL_PATH", os.getenv('FASTER_WHISPER_DEFAULT_MODEL', 'small'))
TRANSCRIBE_LANGUAGE = os.getenv("TRANSCRIBE_LANGUAGE", "ru")
IMAGES_VISION_ENABLED = getenv_bool("IMAGES_VISION_ENABLED", False)
KEYWORDS_TO_TAGS_ENABLED = getenv_bool("KEYWORDS_TO_TAGS_ENABLED", True)
# Порядок шагов определения типа до применения LLM
TYPE_DETECT_FLOW = os.getenv("TYPE_DETECT_FLOW", "extension,filename,heuristics,llm")
TYPE_LLM_OVERRIDE = getenv_bool("TYPE_LLM_OVERRIDE", True)
RENAME_PATTERNS = {
    # Плейсхолдеры: {abbr} {degree} {title} {author_last} {year} {filename}
    'dissertation': '{abbr}.{degree}.{title}.{author_last}',
    'dissertation_abstract': '{abbr}.{degree}.{title}.{author_last}',
    'article': 'СТ.{title}.{author_last}',
    'textbook': 'УЧ.{title}.{author_last}',
    'monograph': 'МОНО.{title}.{author_last}',
    'image': 'ИЗО.{title}',
    'audio': 'АУД.{title}',
    'default': '{abbr}.{title}.{author_last}'
}
# Реранжирование на основе ИИ (расположено после определения getenv_bool)
AI_RERANK_LLM = getenv_bool('AI_RERANK_LLM', False)

# Куда сохранять загруженные файлы: подпапка внутри SCAN_ROOT
# По умолчанию используем 'import' (можно поменять в Настройках)
IMPORT_SUBDIR = os.getenv("IMPORT_SUBDIR", "import").strip().strip("/\\")

# Перемещать ли файл в подпапку по типу при переименовании
MOVE_ON_RENAME = getenv_bool("MOVE_ON_RENAME", True)

# Карта подпапок по типам материалов (относительно SCAN_ROOT)
TYPE_DIRS = {
    "dissertation": "dissertations",
    "dissertation_abstract": "dissertation_abstract",
    "article": "articles",
    "textbook": "textbooks",
    "monograph": "monographs",
    "report": "reports",
    "patent": "patents",
    "presentation": "presentations",
    "proceedings": "proceedings",
    "standard": "standards",
    "note": "notes",
    "document": "documents",
    "audio": "audio",
    "image": "images",
    "other": "other",
}

# Промпты LLM (можно переопределить в настройках)
PROMPTS = {
    'metadata_system': (
        "Ты помощник по каталогизации научных материалов. "
        "Твоя задача: определить тип материала из набора: dissertation, dissertation_abstract, article, textbook, "
        "monograph, report, patent, presentation, proceedings, standard, note, document. "
        "Если подходит несколько — выбери наиболее вероятный. Верни ТОЛЬКО валидный JSON без пояснений. "
        "Ключи: material_type, title, author, year, advisor, keywords (array), novelty (string), "
        "literature (array), organizations (array), classification (array). Если данных нет — пустые строки/массивы."
    ),
    'summarize_audio_system': (
        "Ты помощник. Суммаризируй стенограмму аудио в 3–6 предложениях на русском, "
        "выделив тему, основные тезисы и вывод."
    ),
    'keywords_system': (
        "Ты извлекаешь ключевые слова из стенограммы аудио. Верни только JSON-массив строк на русском: "
        "[\"ключ1\", \"ключ2\", ...]. Без пояснений, не более 12 слов/фраз."
    ),
    'vision_system': (
        "Ты помощник по анализу изображений. Опиши изображение 2–4 предложениями на русском и верни 5–12 ключевых слов. "
        "Верни строго JSON: {\"description\":\"...\",\"keywords\":[\"...\"]}."
    )
}
SUMMARIZE_AUDIO = getenv_bool("SUMMARIZE_AUDIO", False)
# Упрощённое извлечение ключевых слов из аудиостенограмм через LLM (лёгкий промпт)
AUDIO_KEYWORDS_LLM = getenv_bool("AUDIO_KEYWORDS_LLM", True)

# Каталог кэша для моделей faster-whisper (когда автозагрузка по псевдониму)
FW_CACHE_DIR = Path(os.getenv("FASTER_WHISPER_CACHE_DIR", str((Path(__file__).parent / "models" / "faster-whisper").resolve())))

# ------------------- Сохранение настроек во время работы -------------------

SETTINGS_STORE_PATH = BASE_DIR / 'runtime_settings.json'


def _runtime_settings_snapshot() -> dict:
    return {
        'SCAN_ROOT': str(SCAN_ROOT),
        'EXTRACT_TEXT': bool(EXTRACT_TEXT),
        'LMSTUDIO_API_BASE': LMSTUDIO_API_BASE,
        'LMSTUDIO_MODEL': LMSTUDIO_MODEL,
        'LMSTUDIO_API_KEY': LMSTUDIO_API_KEY,
        'TRANSCRIBE_ENABLED': bool(TRANSCRIBE_ENABLED),
        'TRANSCRIBE_BACKEND': TRANSCRIBE_BACKEND,
        'TRANSCRIBE_MODEL_PATH': TRANSCRIBE_MODEL_PATH,
        'TRANSCRIBE_LANGUAGE': TRANSCRIBE_LANGUAGE,
        'SUMMARIZE_AUDIO': bool(SUMMARIZE_AUDIO),
        'AUDIO_KEYWORDS_LLM': bool(AUDIO_KEYWORDS_LLM),
        'IMAGES_VISION_ENABLED': bool(IMAGES_VISION_ENABLED),
        'KEYWORDS_TO_TAGS_ENABLED': bool(KEYWORDS_TO_TAGS_ENABLED),
        'TYPE_DETECT_FLOW': TYPE_DETECT_FLOW,
        'TYPE_LLM_OVERRIDE': bool(TYPE_LLM_OVERRIDE),
        'IMPORT_SUBDIR': IMPORT_SUBDIR,
        'MOVE_ON_RENAME': bool(MOVE_ON_RENAME),
        'COLLECTIONS_IN_SEPARATE_DIRS': bool(COLLECTIONS_IN_SEPARATE_DIRS),
        'COLLECTION_TYPE_SUBDIRS': bool(COLLECTION_TYPE_SUBDIRS),
        'TYPE_DIRS': dict(TYPE_DIRS),
        'DEFAULT_USE_LLM': bool(DEFAULT_USE_LLM),
        'DEFAULT_PRUNE': bool(DEFAULT_PRUNE),
        'OCR_LANGS_CFG': OCR_LANGS_CFG,
        'PDF_OCR_PAGES_CFG': int(PDF_OCR_PAGES_CFG),
        'ALWAYS_OCR_FIRST_PAGE_DISSERTATION': bool(ALWAYS_OCR_FIRST_PAGE_DISSERTATION),
        'PROMPTS': dict(PROMPTS),
        'AI_RERANK_LLM': bool(AI_RERANK_LLM),
    }


def _save_runtime_settings_to_disk() -> None:
    try:
        SETTINGS_STORE_PATH.write_text(
            json.dumps(_runtime_settings_snapshot(), ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
    except Exception as exc:
        logger.warning("Не удалось сохранить настройки: %s", exc)


def _load_runtime_settings_from_disk() -> None:
    if not SETTINGS_STORE_PATH.exists():
        return
    try:
        data = json.loads(SETTINGS_STORE_PATH.read_text(encoding='utf-8'))
    except Exception as exc:
        logger.warning("Не удалось прочитать настройки: %s", exc)
        return
    global SCAN_ROOT, EXTRACT_TEXT, LMSTUDIO_API_BASE, LMSTUDIO_MODEL, LMSTUDIO_API_KEY
    global TRANSCRIBE_ENABLED, TRANSCRIBE_BACKEND, TRANSCRIBE_MODEL_PATH, TRANSCRIBE_LANGUAGE
    global SUMMARIZE_AUDIO, AUDIO_KEYWORDS_LLM, IMAGES_VISION_ENABLED, KEYWORDS_TO_TAGS_ENABLED
    global TYPE_DETECT_FLOW, TYPE_LLM_OVERRIDE, IMPORT_SUBDIR, MOVE_ON_RENAME
    global COLLECTIONS_IN_SEPARATE_DIRS, COLLECTION_TYPE_SUBDIRS, TYPE_DIRS
    global DEFAULT_USE_LLM, DEFAULT_PRUNE
    global OCR_LANGS_CFG, PDF_OCR_PAGES_CFG, ALWAYS_OCR_FIRST_PAGE_DISSERTATION, PROMPTS, AI_RERANK_LLM

    if 'SCAN_ROOT' in data:
        try:
            SCAN_ROOT = Path(data['SCAN_ROOT'])
            app.config['UPLOAD_FOLDER'] = str(SCAN_ROOT)
        except Exception:
            logger.warning('Некорректный путь SCAN_ROOT в настройках: %s', data.get('SCAN_ROOT'))
    if 'EXTRACT_TEXT' in data:
        EXTRACT_TEXT = bool(data['EXTRACT_TEXT'])
    if 'LMSTUDIO_API_BASE' in data:
        LMSTUDIO_API_BASE = str(data['LMSTUDIO_API_BASE'] or LMSTUDIO_API_BASE)
    if 'LMSTUDIO_MODEL' in data:
        LMSTUDIO_MODEL = str(data['LMSTUDIO_MODEL'] or LMSTUDIO_MODEL)
    if 'LMSTUDIO_API_KEY' in data:
        LMSTUDIO_API_KEY = str(data['LMSTUDIO_API_KEY'] or '')
    if 'TRANSCRIBE_ENABLED' in data:
        TRANSCRIBE_ENABLED = bool(data['TRANSCRIBE_ENABLED'])
    if 'TRANSCRIBE_BACKEND' in data:
        TRANSCRIBE_BACKEND = str(data['TRANSCRIBE_BACKEND'] or TRANSCRIBE_BACKEND)
    if 'TRANSCRIBE_MODEL_PATH' in data:
        TRANSCRIBE_MODEL_PATH = str(data['TRANSCRIBE_MODEL_PATH'] or TRANSCRIBE_MODEL_PATH)
    if 'TRANSCRIBE_LANGUAGE' in data:
        TRANSCRIBE_LANGUAGE = str(data['TRANSCRIBE_LANGUAGE'] or TRANSCRIBE_LANGUAGE)
    if 'SUMMARIZE_AUDIO' in data:
        SUMMARIZE_AUDIO = bool(data['SUMMARIZE_AUDIO'])
    if 'AUDIO_KEYWORDS_LLM' in data:
        AUDIO_KEYWORDS_LLM = bool(data['AUDIO_KEYWORDS_LLM'])
    if 'IMAGES_VISION_ENABLED' in data:
        IMAGES_VISION_ENABLED = bool(data['IMAGES_VISION_ENABLED'])
    if 'KEYWORDS_TO_TAGS_ENABLED' in data:
        KEYWORDS_TO_TAGS_ENABLED = bool(data['KEYWORDS_TO_TAGS_ENABLED'])
    if 'TYPE_DETECT_FLOW' in data:
        TYPE_DETECT_FLOW = str(data['TYPE_DETECT_FLOW'] or TYPE_DETECT_FLOW)
    if 'TYPE_LLM_OVERRIDE' in data:
        TYPE_LLM_OVERRIDE = bool(data['TYPE_LLM_OVERRIDE'])
    if 'IMPORT_SUBDIR' in data:
        IMPORT_SUBDIR = str(data['IMPORT_SUBDIR'] or '').strip().strip('/\\')
    if 'MOVE_ON_RENAME' in data:
        MOVE_ON_RENAME = bool(data['MOVE_ON_RENAME'])
    if 'COLLECTIONS_IN_SEPARATE_DIRS' in data:
        COLLECTIONS_IN_SEPARATE_DIRS = bool(data['COLLECTIONS_IN_SEPARATE_DIRS'])
    if 'COLLECTION_TYPE_SUBDIRS' in data:
        COLLECTION_TYPE_SUBDIRS = bool(data['COLLECTION_TYPE_SUBDIRS'])
    if 'TYPE_DIRS' in data and isinstance(data['TYPE_DIRS'], dict):
        TYPE_DIRS = {str(k): str(v) for k, v in data['TYPE_DIRS'].items()}
    if 'DEFAULT_USE_LLM' in data:
        DEFAULT_USE_LLM = bool(data['DEFAULT_USE_LLM'])
    if 'DEFAULT_PRUNE' in data:
        DEFAULT_PRUNE = bool(data['DEFAULT_PRUNE'])
    if not COLLECTIONS_IN_SEPARATE_DIRS:
        COLLECTION_TYPE_SUBDIRS = False
    if 'COLLECTIONS_IN_SEPARATE_DIRS' in data:
        COLLECTIONS_IN_SEPARATE_DIRS = bool(data['COLLECTIONS_IN_SEPARATE_DIRS'])
    if 'COLLECTION_TYPE_SUBDIRS' in data:
        COLLECTION_TYPE_SUBDIRS = bool(data['COLLECTION_TYPE_SUBDIRS'])
    if 'TYPE_DIRS' in data and isinstance(data['TYPE_DIRS'], dict):
        TYPE_DIRS = {str(k): str(v) for k, v in data['TYPE_DIRS'].items()}
    if 'DEFAULT_USE_LLM' in data:
        DEFAULT_USE_LLM = bool(data['DEFAULT_USE_LLM'])
    if 'DEFAULT_PRUNE' in data:
        DEFAULT_PRUNE = bool(data['DEFAULT_PRUNE'])
    if not COLLECTIONS_IN_SEPARATE_DIRS:
        COLLECTION_TYPE_SUBDIRS = False
    if 'COLLECTIONS_IN_SEPARATE_DIRS' in data:
        COLLECTIONS_IN_SEPARATE_DIRS = bool(data['COLLECTIONS_IN_SEPARATE_DIRS'])
    if 'COLLECTION_TYPE_SUBDIRS' in data:
        COLLECTION_TYPE_SUBDIRS = bool(data['COLLECTION_TYPE_SUBDIRS'])
    if 'OCR_LANGS_CFG' in data:
        OCR_LANGS_CFG = str(data['OCR_LANGS_CFG'] or OCR_LANGS_CFG)
        os.environ['OCR_LANGS'] = OCR_LANGS_CFG
    if 'PDF_OCR_PAGES_CFG' in data:
        try:
            PDF_OCR_PAGES_CFG = int(data['PDF_OCR_PAGES_CFG'])
        except Exception:
            pass
        else:
            os.environ['PDF_OCR_PAGES'] = str(PDF_OCR_PAGES_CFG)
    if 'ALWAYS_OCR_FIRST_PAGE_DISSERTATION' in data:
        ALWAYS_OCR_FIRST_PAGE_DISSERTATION = bool(data['ALWAYS_OCR_FIRST_PAGE_DISSERTATION'])
        os.environ['OCR_DISS_FIRST_PAGE'] = '1' if ALWAYS_OCR_FIRST_PAGE_DISSERTATION else '0'
    if 'PROMPTS' in data and isinstance(data['PROMPTS'], dict):
        for key, value in data['PROMPTS'].items():
            if isinstance(value, str):
                PROMPTS[key] = value
    if 'AI_RERANK_LLM' in data:
        AI_RERANK_LLM = bool(data['AI_RERANK_LLM'])


# ------------------- Lightweight response helpers -------------------

def json_error(message: str, status: int = 400):
    """Shortcut для JSON-ошибок."""
    return jsonify({"error": str(message)}), int(status)


def _html_page(title: str, body: str, *, extra_head: str = "", status: int = 200) -> Response:
    head = f"""
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>{escape(title)}</title>
    {extra_head}
    """
    html = f"""<!doctype html><html lang=\"ru\"><head>{head}</head><body>{body}</body></html>"""
    resp = make_response(html, status)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return resp




_PREVIEW_HEAD = """
<style>
  :root { color-scheme: dark light; }
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0d1117; color:#c9d1d9; }
  a { color:#58a6ff; }
  .preview-shell { min-height:100vh; display:flex; flex-direction:column; }
  .preview-shell.embedded { min-height:auto; }
  .preview-header { padding:12px 16px; display:flex; align-items:center; gap:12px; justify-content:space-between; background:#161b22; border-bottom:1px solid #30363d; }
  .preview-title { font-size:16px; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .preview-actions { display:flex; gap:8px; }
  .btn { display:inline-flex; align-items:center; justify-content:center; gap:6px; padding:8px 16px; border-radius:999px; font-size:13px;
         border:1px solid #30363d; background:#1f6feb; color:#fff; text-decoration:none; }
  .btn.secondary { background:transparent; color:#c9d1d9; }
  .preview-main { flex:1; overflow:auto; padding:16px; }
  .preview-shell.embedded .preview-main { padding:12px; }
  .preview-card { background:#161b22; border:1px solid #30363d; border-radius:14px; padding:16px; box-shadow:0 16px 48px rgba(0,0,0,0.35); }
  .preview-text { white-space:pre-wrap; max-height:70vh; overflow:auto; word-break:break-word; overflow-wrap:anywhere; background:#0d1117; padding:12px; border-radius:10px; border:1px solid #30363d; }
  .muted { color:#8b949e; font-size:13px; margin:12px 0 6px; }
  .section { margin-top:18px; }
  .section h3 { margin:0 0 8px; font-size:15px; font-weight:600; color:inherit; }
  img.responsive { max-width:100%; height:auto; border-radius:12px; border:1px solid #30363d; }
  .badge { display:inline-block; padding:4px 10px; border-radius:999px; background:#30363d; color:#c9d1d9; font-size:12px; margin-left:6px; }
  .tag { display:inline-block; padding:4px 10px; border-radius:999px; background:#30363d; color:#c9d1d9; font-size:12px; margin:2px 4px 2px 0; }
  mark { background:#bb8009; color:inherit; padding:0 2px; border-radius:3px; }
  embed, iframe { border:none; border-radius:10px; background:#0d1117; }
  audio { width:100%; }
  @media (prefers-color-scheme: light) {
    body { background:#f5f6f8; color:#101828; }
    .preview-header { background:#ffffff; border-color:rgba(15,23,42,0.09); }
    .preview-card { background:#ffffff; border-color:rgba(15,23,42,0.08); box-shadow:0 12px 36px rgba(15,23,42,0.2); }
    .preview-text { background:#f8fafc; border-color:rgba(15,23,42,0.08); }
    .btn { background:#1f6feb; color:#fff; border:1px solid #1f6feb; }
    .btn.secondary { background:#fff; color:#0f172a; border-color:rgba(15,23,42,0.14); }
    .muted { color:#64748b; }
    .badge, .tag { background:rgba(15,23,42,0.08); color:#0f172a; }
  }
</style>
<script>
(function(){
  try {
    const params = new URLSearchParams(window.location.search);
    const mark = (params.get('mark') || '').trim();
    if (!mark) return;
    const terms = Array.from(new Set(mark.split(/[\\s,|]+/).filter(x => x && x.length >= 2))).slice(0, 5);
    if (!terms.length) return;
    const esc = s => s.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&');
    const re = new RegExp('(' + terms.map(esc).join('|') + ')', 'gi');
    document.querySelectorAll('.preview-text').forEach(el => {
      try { el.innerHTML = el.innerHTML.replace(re, '<mark>$1</mark>'); } catch (_) {}
    });
  } catch (err) {
    console.warn('highlight error', err);
  }
})();
</script>
""".strip()


def _render_preview(
    rel_url: str,
    *,
    is_pdf: bool,
    is_text: bool,
    is_audio: bool,
    is_image: bool,
    content: str,
    thumbnail_url: str | None,
    abstract: str,
    audio_url: str | None,
    duration: str | None,
    image_url: str | None,
    keywords: str | None,
    embedded: bool
) -> Response:
    safe_rel = escape(rel_url)
    download_url = escape(url_for('download_file', rel_path=rel_url))
    view_url = escape(url_for('view_file', rel_path=rel_url))
    shell_class = "preview-shell embedded" if embedded else "preview-shell"

    sections: list[str] = []

    def section(title: str, html_content: str) -> None:
        sections.append(f'<div class="section"><h3>{escape(title)}</h3>{html_content}</div>')

    if is_pdf:
        sections.append(f'<embed src="{view_url}" type="application/pdf" width="100%" style="min-height:70vh;" />')
        if content:
            section('Фрагмент содержимого', f'<div class="preview-text">{escape(content)}</div>')
    elif is_audio:
        if audio_url:
            audio = f'<audio controls preload="metadata"><source src="{escape(audio_url)}" /></audio>'
            if duration:
                audio += f'<span class="badge">{escape(duration)}</span>'
            sections.append(audio)
        if abstract:
            section('Резюме', f'<div class="preview-text">{escape(abstract)}</div>')
        if keywords:
            chips = ''.join(f'<span class="tag">{escape(k.strip())}</span>' for k in keywords.split(',') if k.strip())
            if chips:
                section('Ключевые слова', chips)
        if content and not abstract:
            section('Фрагмент стенограммы', f'<div class="preview-text">{escape(content)}</div>')
    elif is_image:
        img_src = escape(image_url or url_for('media_file', rel_path=rel_url))
        sections.append(f'<img class="responsive" src="{img_src}" alt="image preview" />')
        if abstract:
            section('Описание', f'<div class="preview-text">{escape(abstract)}</div>')
        if keywords:
            chips = ''.join(f'<span class="tag">{escape(k.strip())}</span>' for k in keywords.split(',') if k.strip())
            if chips:
                section('Ключевые слова', chips)
    elif is_text:
        section('Текст', f'<div class="preview-text">{escape(content)}</div>')
    elif thumbnail_url:
        sections.append(f'<img class="responsive" src="{escape(thumbnail_url)}" alt="thumbnail" />')
        if content:
            section('Фрагмент содержимого', f'<div class="preview-text">{escape(content)}</div>')
    else:
        if content:
            sections.append('<div class="muted">Предпросмотр для этого формата ограничен. Ниже показан доступный текст:</div>')
            sections.append(f'<div class="preview-text">{escape(content)}</div>')
        else:
            sections.append('<div class="muted">Предпросмотр не поддерживается для этого типа файла.</div>')

    if abstract and not any('Резюме' in s for s in sections):
        section('Резюме', f'<div class="preview-text">{escape(abstract)}</div>')
    if keywords and not any('Ключевые слова' in s for s in sections):
        chips = ''.join(f'<span class="tag">{escape(k.strip())}</span>' for k in keywords.split(',') if k.strip())
        if chips:
            section('Ключевые слова', chips)

    header_actions = f'<a class="btn" href="{download_url}" download>Скачать</a>'
    body = f"""
    <div class=\"{shell_class}\">
      <header class=\"preview-header\">
        <div class=\"preview-title\">Предпросмотр: {safe_rel}</div>
        <div class=\"preview-actions\">{header_actions}</div>
      </header>
      <main class=\"preview-main\">
        <div class=\"preview-card\">{''.join(sections)}</div>
      </main>
    </div>
    """
    return _html_page(f"Предпросмотр — {rel_url}", body, extra_head=_PREVIEW_HEAD)

# ------------------- Вспомогательные функции -------------------

def _normalize_author(val):
    """Convert various author representations (list/dict/etc.) to a string.
    Returns None for empty results.
    """
    try:
        if val is None:
            return None
        # список/кортеж имён или словарей
        if isinstance(val, (list, tuple, set)):
            out = []
            for x in val:
                if x is None:
                    continue
                if isinstance(x, dict):
                    if 'name' in x and x['name']:
                        s = str(x['name']).strip()
                    else:
                        s = " ".join(str(v).strip() for v in x.values() if v)
                else:
                    s = str(x).strip()
                if s:
                    out.append(s)
            return ", ".join(out) if out else None
        # один словарь вида {first,last} или {name}
        if isinstance(val, dict):
            if 'name' in val and val['name']:
                s = str(val['name']).strip()
                return s or None
            s = " ".join(str(v).strip() for v in val.values() if v)
            return s or None
        s = str(val).strip()
        return s or None
    except Exception:
        return None

def _normalize_year(val):
    """Normalize year to a short string; prefer 4-digit if present."""
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return str(int(val))
        s = str(val).strip()
        m = re.search(r"\b(\d{4})\b", s)
        if m:
            return m.group(1)
        return s[:16] if s else None
    except Exception:
        return None

# --------- Расширенное извлечение тегов (общие и специфичные по типу) ---------
def extract_richer_tags(material_type: str, text: str, filename: str = "") -> dict:
    t = (text or "")
    tl = t.lower()
    tags: dict[str, str] = {}
    # предположение языка
    try:
        cyr = sum(1 for ch in t if ('а' <= ch.lower() <= 'я') or (ch in 'ёЁ'))
        lat = sum(1 for ch in t if 'a' <= ch.lower() <= 'z')
        if cyr + lat > 20:
            tags['lang'] = 'ru' if cyr >= lat else 'en'
    except Exception:
        pass
    # распространённые идентификаторы
    m = re.search(r"\b(10\.\d{4,9}\/[\w\-\.:;()\/[\]A-Za-z0-9]+)", t)
    if m:
        tags.setdefault('doi', m.group(1))
    m = re.search(r"\bISBN[:\s]*([0-9\- ]{10,20})", t, flags=re.I)
    if m:
        tags.setdefault('isbn', m.group(1).strip())
    m = re.search(r"\bУДК[:\s]*([\d\.:\-]+)\b", t, flags=re.I)
    if m:
        tags.setdefault('udk', m.group(1))
    m = re.search(r"\bББК[:\s]*([A-ZА-Я0-9\.-/]+)\b", t, flags=re.I)
    if m:
        tags.setdefault('bbk', m.group(1))

    mt = (material_type or '').strip().lower()
    if mt in ("dissertation", "dissertation_abstract"):
        # код специальности вида 05.13.11
        m = re.search(r"\b(\d{2}\.\d{2}\.\d{2})\b", t)
        if m:
            tags.setdefault('specialty', m.group(1))
        if 'автореферат' in tl:
            tags.setdefault('kind', 'автореферат')
    elif mt == 'article':
        m = re.search(r"(journal|transactions|вестник|журнал)[:\s\-]+([^\n\r]{3,80})", tl, flags=re.I)
        if m:
            tags.setdefault('journal', m.group(2).strip().title())
        m = re.search(r"\b(\d+)\b.*?№\s*(\d+)\b.*?([\d]+)[\-–—]([\d]+)", t)
        if m:
            tags.setdefault('volume_issue', f"{m.group(1)}/{m.group(2)}")
            tags.setdefault('pages', f"{m.group(3)}–{m.group(4)}")
    elif mt == 'textbook':
        m = re.search(r"учебн(?:ое|ик|ое\s+пособие)\s*[:\-]?\s*([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault('discipline', m.group(1).strip().title())
    elif mt == 'monograph':
        m = re.search(r"(серия|series)\s*[:\-]?\s*([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault('series', m.group(2).strip().title())
    elif mt == 'standard':
        pats = [
            r"\bГОСТ\s*R?\s*\d{1,5}(?:\.\d+)*[-–—]\d{2,4}\b",
            r"\bСТБ\s*\d{1,5}(?:\.\d+)*[-–—]\d{2,4}\b",
            r"\bСТО\s*[^\s]*\s*\d{1,6}[-–—]?\d{2,4}\b",
            r"\bISO\s*\d{3,5}(?:-\d+)*(?::\d{4})?\b",
            r"\bIEC\s*\d{3,5}(?:-\d+)*(?::\d{4})?\b",
            r"\bСП\s*\d{1,4}\.\d{1,4}-\d{4}\b",
            r"\bСанПиН\s*\d+[\.-]\d+[\.-]\d+\b",
            r"\bТУ\s*[A-Za-zА-Яа-я0-9\./-]+\b",
        ]
        for pat in pats:
            m = re.search(pat, t, flags=re.I)
            if m:
                tags.setdefault('standard', m.group(0))
                break
        if re.search(r"утратил[аи]?\s+силу|замен(ен|яет)|взамен", tl):
            tags.setdefault('status', 'replaced')
        elif re.search(r"введ(ен|ена)\s+впервые|действующ", tl):
            tags.setdefault('status', 'active')
    elif mt == 'proceedings':
        m = re.search(r"(материалы\s+конференции|proceedings\s+of\s+the|international\s+conference|symposium\s+on|workshop\s+on)[^\n\r]{0,120}", tl, flags=re.I)
        if m:
            tags.setdefault('conference', m.group(0).strip().title())
    elif mt == 'report':
        if re.search(r"техническое\s+задание\b|ТЗ\b", tl):
            tags.setdefault('doc_kind', 'Техническое задание')
        if re.search(r"пояснительная\s+записка\b", tl):
            tags.setdefault('doc_kind', 'Пояснительная записка')
    elif mt == 'patent':
        m = re.search(r"\b(?:RU|SU|US|WO|EP)\s?\d{4,10}[A-Z]?\d?\b", t)
        if m:
            tags.setdefault('patent_no', m.group(0))
        m = re.search(r"\b[A-H][0-9]{2}[A-Z]\s*\d+\/\d+\b", t)
        if m:
            tags.setdefault('ipc', m.group(0).replace(' ', ''))
    elif mt == 'presentation':
        if re.search(r"(слайды|slides|powerpoint|презентация)", tl):
            tags.setdefault('slides', 'yes')
    return tags

def _fw_alias_to_repo(ref: str) -> str | None:
    r = (ref or '').strip().lower()
    # Распространённые псевдонимы
    alias = {
        'tiny': 'Systran/faster-whisper-tiny',
        'base': 'Systran/faster-whisper-base',
        'small': 'Systran/faster-whisper-small',
        'medium': 'Systran/faster-whisper-medium',
        'large-v2': 'Systran/faster-whisper-large-v2',
        'large-v3': 'Systran/faster-whisper-large-v3',
        # Сжатые английские варианты
        'distil-small.en': 'Systran/faster-distil-whisper-small.en',
        'distil-medium.en': 'Systran/faster-distil-whisper-medium.en',
        'distil-large-v2': 'Systran/faster-distil-whisper-large-v2',
    }
    if r in alias:
        return alias[r]
    # Также принимаем форму org/name
    if '/' in r and len(r.split('/', 1)[0]) > 0:
        return ref
    return None

def _ensure_faster_whisper_model(model_ref: str) -> str:
    """Гарантировать локальный путь к модели faster-whisper.
    Если указан локальный каталог — используем его. Если это алиас или repo id — скачиваем в кэш.
    Возвращает путь к модели или пустую строку при ошибке.
    """
    try:
        if not model_ref:
            return ''
        p = Path(model_ref).expanduser()
        if p.exists() and p.is_dir():
            return str(p)
        repo = _fw_alias_to_repo(model_ref)
        if not repo:
            return ''
        if hf_snapshot_download is None:
            app.logger.warning("Пакет huggingface_hub не установлен — автозагрузка модели faster-whisper недоступна")
            return ''
        # вычислить целевую директорию внутри кэша
        FW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = repo.replace('/', '__')
        target_dir = FW_CACHE_DIR / safe_name
        if not target_dir.exists() or not any(target_dir.iterdir()):
            # загрузить/снять снимок в target_dir
            hf_snapshot_download(repo_id=repo, local_dir=str(target_dir), local_dir_use_symlinks=False, revision=None)
        return str(target_dir)
    except Exception as e:
        app.logger.warning(f"Failed to resolve faster-whisper model '{model_ref}': {e}")
        return ''

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{BASE_DIR / 'catalogue.db'}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JSON_AS_ASCII"] = False
# Логирование: файл с ротацией до 100 МБ
LOG_DIR = BASE_DIR / 'logs'
try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    LOG_DIR = BASE_DIR
log_path = LOG_DIR / 'agregator.log'
if not any(isinstance(h, RotatingFileHandler) for h in app.logger.handlers):
    rotating_handler = RotatingFileHandler(log_path, maxBytes=100 * 1024 * 1024, backupCount=5, encoding='utf-8')
    rotating_handler.setLevel(logging.INFO)
    rotating_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s'))
    app.logger.addHandler(rotating_handler)
    logging.getLogger().setLevel(logging.INFO)
# Ограничить максимальный размер загрузки (по умолчанию 50 МБ; настраивается через MAX_CONTENT_LENGTH)
try:
    app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH', str(50 * 1024 * 1024)))
except Exception:
    app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
db.init_app(app)


# Модели описаны в models.py и импортируются выше.

# Инициализация конфигурации для перемещения файлов
# На старте используем SCAN_ROOT как корневую папку (UPLOAD_FOLDER задаётся ниже)
app.config.setdefault('UPLOAD_FOLDER', str(SCAN_ROOT))
app.config.setdefault('IMPORT_SUBDIR', IMPORT_SUBDIR)
app.config.setdefault('MOVE_ON_RENAME', MOVE_ON_RENAME)
app.config.setdefault('COLLECTIONS_IN_SEPARATE_DIRS', COLLECTIONS_IN_SEPARATE_DIRS)
app.config.setdefault('COLLECTION_TYPE_SUBDIRS', COLLECTION_TYPE_SUBDIRS)
app.config.setdefault('TYPE_DIRS', TYPE_DIRS)

# ------------------- Утилиты -------------------

ALLOWED_EXTS = {".pdf", ".txt", ".md", ".docx", ".rtf", ".mp3", ".wav", ".m4a", ".flac", ".ogg",
                ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".flac", ".ogg"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

UPLOAD_FOLDER = BASE_DIR / "sample_library"
app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)

_load_runtime_settings_from_disk()
UPLOAD_FOLDER = SCAN_ROOT
app.config['UPLOAD_FOLDER'] = str(SCAN_ROOT)
app.config['COLLECTIONS_IN_SEPARATE_DIRS'] = COLLECTIONS_IN_SEPARATE_DIRS
app.config['COLLECTION_TYPE_SUBDIRS'] = COLLECTION_TYPE_SUBDIRS

# ------------------- Пользователи и доступ -------------------

DEFAULT_ADMIN_USER = (os.getenv('DEFAULT_ADMIN_USER') or 'admin').strip() or 'admin'
DEFAULT_ADMIN_PASSWORD = (os.getenv('DEFAULT_ADMIN_PASSWORD') or '').strip()
LEGACY_ACCESS_CODE = (os.getenv('ACCESS_CODE') or '').strip()
SESSION_KEY = 'user_id'

_PUBLIC_PREFIXES = ('/static/', '/assets/')
_PUBLIC_PATHS = {'/favicon.ico', '/api/auth/login'}

def _log_user_action(user: User | None, action: str, entity: str | None = None, entity_id: int | None = None, detail: str | None = None) -> None:
    try:
        rec = UserActionLog(user_id=user.id if user else None, action=action, entity=entity, entity_id=entity_id, detail=detail)
        db.session.add(rec)
        db.session.commit()
    except Exception:
        db.session.rollback()

def _user_to_payload(user: User) -> dict:
    return {
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'full_name': user.full_name,
        'created_at': user.created_at.isoformat() if user.created_at else None,
        'updated_at': user.updated_at.isoformat() if getattr(user, 'updated_at', None) else None,
        'aiword_access': _has_aiword_access(user),
        'can_upload': _user_can_upload(user),
        'can_import': _user_can_upload(user),
    }

def ensure_default_admin() -> None:
    try:
        with app.app_context():
            db.create_all()
            if User.query.count() == 0:
                password = DEFAULT_ADMIN_PASSWORD or LEGACY_ACCESS_CODE or 'admin123'
                user = User(username=DEFAULT_ADMIN_USER, role='admin')
                user.set_password(password)
                db.session.add(user)
                db.session.commit()
                app.logger.warning(
                    "Создан администратор по умолчанию '%s'. Рекомендуется немедленно сменить пароль.",
                    DEFAULT_ADMIN_USER,
                )
    except Exception as exc:
        app.logger.error("Не удалось создать администратора по умолчанию: %s", exc)

def _load_current_user() -> User | None:
    uid = session.get(SESSION_KEY)
    if not uid:
        g.current_user = None
        g.allowed_collection_ids = set()
        return None
    try:
        user = User.query.get(int(uid))
    except Exception:
        user = None
    if not user:
        session.pop(SESSION_KEY, None)
        g.current_user = None
        g.allowed_collection_ids = set()
        return None
    g.current_user = user
    _ensure_personal_collection(user)
    g.allowed_collection_ids = _compute_allowed_collection_ids(user)
    return user


def _compute_allowed_collection_ids(user: User | None):
    if not user:
        return set()
    if getattr(user, 'role', '') == 'admin':
        return None
    ids: set[int] = set()
    try:
        own = db.session.query(Collection.id).filter(Collection.owner_id == user.id).all()
        ids.update(int(cid) for (cid,) in own)
    except Exception:
        pass
    try:
        membership = db.session.query(CollectionMember.collection_id, CollectionMember.role).filter(CollectionMember.user_id == user.id).all()
        ids.update(int(cid) for (cid, _role) in membership)
    except Exception:
        pass
    try:
        public = db.session.query(Collection.id).filter(Collection.is_private == False).all()
        ids.update(int(cid) for (cid,) in public)
    except Exception:
        pass
    return ids


def _ensure_personal_collection(user: User) -> None:
    try:
        personal = Collection.query.filter_by(owner_id=user.id, is_private=True).first()
        if not personal:
            name = f"Личная коллекция {user.username}"[:120]
            base_slug = _slugify(name)
            if not base_slug:
                base_slug = f"user-{user.id}"
            slug = base_slug
            i = 2
            while Collection.query.filter_by(slug=slug).first() is not None:
                slug = f"{base_slug}-{i}"
                i += 1
            personal = Collection(name=name, slug=slug, owner_id=user.id, is_private=True, searchable=True, graphable=True)
            db.session.add(personal)
            db.session.flush()
        member = CollectionMember.query.filter_by(collection_id=personal.id, user_id=user.id).first()
        if not member:
            db.session.add(CollectionMember(collection_id=personal.id, user_id=user.id, role='owner'))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _has_collection_access(collection_id: int, write: bool = False) -> bool:
    user = _load_current_user()
    if not user:
        return False
    if getattr(user, 'role', '') == 'admin':
        return True
    allowed = getattr(g, 'allowed_collection_ids', set())
    if allowed is None or collection_id in allowed:
        if not write:
            return True
        try:
            col = Collection.query.get(collection_id)
            if col and col.owner_id == user.id:
                return True
        except Exception:
            pass
        try:
            member = CollectionMember.query.filter_by(collection_id=collection_id, user_id=user.id).first()
            if member and member.role in ('owner', 'editor'):
                return True
        except Exception:
            pass
    return False


def _apply_file_access_filter(query):
    allowed = getattr(g, 'allowed_collection_ids', set())
    if allowed is None:
        return query
    if not allowed:
        return query.filter(File.collection_id == -1)
    return query.filter(File.collection_id.in_(allowed))


def _parse_collection_param(value: str | None, require_write: bool = False) -> int | None:
    if not value:
        return None
    try:
        cid = int(value)
    except Exception:
        abort(400)
    if require_write:
        if not _has_collection_access(cid, write=True):
            abort(403)
    else:
        if not _has_collection_access(cid):
            abort(403)
    return cid


def _aiword_allowed_user_ids() -> set[int]:
    try:
        return {row.user_id for row in AiWordAccess.query.all()}
    except Exception:
        return set()


def _ensure_aiword_access() -> User:
    user = _load_current_user()
    if not user:
        abort(401)
    if getattr(user, 'role', '') == 'admin':
        return user
    allowed = _aiword_allowed_user_ids()
    if user.id in allowed:
        return user
    abort(403)


def _has_aiword_access(user: User | None) -> bool:
    if not user:
        return False
    if getattr(user, 'role', '') == 'admin':
        return True
    try:
        return AiWordAccess.query.filter_by(user_id=user.id).first() is not None
    except Exception:
        return False


def _user_can_upload(user: User | None) -> bool:
    if not user:
        return False
    if getattr(user, 'role', '') == 'admin':
        return True
    try:
        if Collection.query.filter(Collection.owner_id == user.id).limit(1).first():
            return True
    except Exception:
        pass
    try:
        member = CollectionMember.query.filter(
            CollectionMember.user_id == user.id,
            CollectionMember.role.in_(('owner', 'editor'))
        ).limit(1).first()
        if member:
            return True
    except Exception:
        pass
    return False


def _llm_parse_purposes(raw: str | None) -> list[str]:
    if raw is None:
        return ['default']
    parts = [p.strip().lower() for p in re.split(r'[;,\s]+', raw) if p.strip()]
    return parts or ['default']


def _llm_normalize_purposes(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        parts = _llm_parse_purposes(value)
    elif isinstance(value, (list, tuple, set)):
        parts = []
        for item in value:
            token = str(item).strip().lower()
            if token:
                parts.append(token)
    else:
        return None
    seen: set[str] = set()
    ordered: list[str] = []
    for token in parts:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ','.join(ordered) if ordered else None


def _invalidate_llm_cache() -> None:
    LLM_ROUND_ROBIN.clear()
    LLM_ENDPOINT_SIGNATURE.clear()
    LLM_ENDPOINT_POOLS.clear()
    LLM_ENDPOINT_UNIQUE.clear()


def _ensure_llm_pool(purpose: str | None) -> None:
    desired = (purpose or 'default').lower()
    base_default = (LMSTUDIO_API_BASE or '').rstrip('/')
    model_default = LMSTUDIO_MODEL
    key_default = LMSTUDIO_API_KEY
    try:
        all_eps = LlmEndpoint.query.order_by(LlmEndpoint.created_at.asc()).all()
        candidates: list[tuple[LlmEndpoint, list[str]]] = []
        for ep in all_eps:
            purposes = _llm_parse_purposes(ep.purpose)
            if desired in purposes or (desired != 'default' and 'default' in purposes):
                candidates.append((ep, purposes))
        if not candidates and desired != 'default':
            for ep in all_eps:
                purposes = _llm_parse_purposes(ep.purpose)
                if 'default' in purposes:
                    candidates.append((ep, purposes))
        if candidates:
            sig = tuple(sorted((ep.id, float(ep.weight or 1.0), tuple(sorted(purposes))) for ep, purposes in candidates))
            if LLM_ENDPOINT_SIGNATURE.get(desired) != sig:
                pool: list[dict[str, str]] = []
                unique: list[dict[str, str]] = []
                seen: set[tuple] = set()
                for ep, _purposes in candidates:
                    entry = {
                        'id': ep.id,
                        'name': ep.name or f'endpoint-{ep.id}',
                        'base_url': (ep.base_url or base_default).rstrip('/'),
                        'model': ep.model or model_default,
                        'api_key': ep.api_key or key_default,
                    }
                    weight = max(1, int(round(float(ep.weight or 1.0))))
                    for _ in range(weight):
                        pool.append(entry)
                    ident = (entry['id'], entry['base_url'], entry['model'], entry.get('api_key'))
                    if ident not in seen:
                        seen.add(ident)
                        unique.append(entry)
                if not pool:
                    default_entry = {
                        'id': None,
                        'name': 'default',
                        'base_url': base_default,
                        'model': model_default,
                        'api_key': key_default,
                    }
                    pool = [default_entry]
                    unique = [default_entry]
                LLM_ENDPOINT_POOLS[desired] = pool
                LLM_ENDPOINT_UNIQUE[desired] = unique
                LLM_ROUND_ROBIN[desired] = itertools.cycle(pool)
                LLM_ENDPOINT_SIGNATURE[desired] = sig
            return
    except Exception:
        pass
    if desired not in LLM_ENDPOINT_POOLS:
        default_entry = {
            'id': None,
            'name': 'default',
            'base_url': base_default,
            'model': model_default,
            'api_key': key_default,
        }
        LLM_ENDPOINT_POOLS[desired] = [default_entry]
        LLM_ENDPOINT_UNIQUE[desired] = [default_entry]
        LLM_ROUND_ROBIN[desired] = itertools.cycle(LLM_ENDPOINT_POOLS[desired])
        LLM_ENDPOINT_SIGNATURE[desired] = tuple()


def _select_llm_endpoint(purpose: str | None = None) -> tuple[str, str, str]:
    desired = (purpose or 'default').lower()
    _ensure_llm_pool(desired)
    pool = LLM_ENDPOINT_POOLS.get(desired) or []
    if not pool:
        return LMSTUDIO_API_BASE, LMSTUDIO_MODEL, LMSTUDIO_API_KEY
    rotation = LLM_ROUND_ROBIN.get(desired)
    if rotation is None:
        rotation = itertools.cycle(pool)
        LLM_ROUND_ROBIN[desired] = rotation
    choice = next(rotation)
    base = choice.get('base_url') or LMSTUDIO_API_BASE
    model = choice.get('model') or LMSTUDIO_MODEL
    key = choice.get('api_key') or LMSTUDIO_API_KEY
    return base, model, key


def _llm_iter_choices(purpose: str | None):
    desired = (purpose or 'default').lower()
    _ensure_llm_pool(desired)
    pool = LLM_ENDPOINT_POOLS.get(desired) or []
    if not pool:
        yield {
            'id': None,
            'name': 'default',
            'base_url': (LMSTUDIO_API_BASE or '').rstrip('/'),
            'model': LMSTUDIO_MODEL,
            'api_key': LMSTUDIO_API_KEY,
        }
        return
    rotation = LLM_ROUND_ROBIN.get(desired)
    if rotation is None:
        rotation = itertools.cycle(pool)
        LLM_ROUND_ROBIN[desired] = rotation
    unique = LLM_ENDPOINT_UNIQUE.get(desired) or []
    max_iters = len(unique) if unique else len(pool) or 1
    seen: set[tuple] = set()
    for _ in range(max_iters):
        choice = next(rotation)
        ident = (choice.get('id'), choice.get('base_url'), choice.get('model'), choice.get('api_key'))
        if ident in seen:
            continue
        seen.add(ident)
        yield choice


def _llm_choice_label(choice: dict) -> str:
    name = str(choice.get('name') or '').strip()
    base = str(choice.get('base_url') or '').strip()
    model = str(choice.get('model') or '').strip()
    ident = choice.get('id')
    parts = []
    if name:
        parts.append(name)
    if model:
        parts.append(model)
    if base:
        parts.append(base)
    if not parts:
        parts.append(f'id={ident}')
    return ' | '.join(parts)


def _llm_response_indicates_busy(response) -> bool:
    if response is None:
        return False
    try:
        status_code = int(response.status_code)
    except Exception:
        status_code = None
    if status_code in LLM_BUSY_HTTP_CODES:
        return True
    payload = None
    try:
        payload = response.json() if hasattr(response, 'json') else None
    except ValueError:
        payload = None
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        status = str(payload.get('status') or '').lower()
        message = str(payload.get('message') or '').lower()
        if status in LLM_BUSY_STATUS_VALUES:
            return True
        if any(token in message for token in ('busy', 'processing', 'queue', 'rate')):
            return True
    text = str(getattr(response, 'text', '') or '').lower()
    if any(token in text for token in ('busy', 'processing', 'queue full')):
        return True
    return False


def _llm_choice_url(choice: dict) -> str:
    base = str(choice.get('base_url') or LMSTUDIO_API_BASE or '').strip()
    if not base:
        return ''
    base = base.rstrip('/')
    if not base:
        return ''
    return base if base.endswith('/chat/completions') else f"{base}/chat/completions"


def _llm_choice_headers(choice: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    key = choice.get('api_key')
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def _is_public_path(path: str) -> bool:
    if any(path.startswith(prefix) for prefix in _PUBLIC_PREFIXES):
        return True
    if path in _PUBLIC_PATHS:
        return True
    if path == '/' or path.startswith('/access'):
        return True
    if request.method == 'GET' and path.startswith('/app'):
        return True
    return False

@app.before_request
def _access_gate():
    path = request.path or '/'
    user = _load_current_user()
    if _is_public_path(path):
        return None
    if user:
        return None
    if path.startswith('/api/') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return ("Не авторизовано", 401)
    return redirect('/app/login')

@app.route('/api/auth/login', methods=['POST'])
def api_auth_login():
    data = request.get_json(silent=True) or request.form or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    if not username or not password:
        return jsonify({'ok': False, 'error': 'Введите логин и пароль'}), 400
    try:
        user = User.query.filter(func.lower(User.username) == username.lower()).first()
    except Exception:
        user = None
    if not user or not user.check_password(password):
        session.pop(SESSION_KEY, None)
        return jsonify({'ok': False, 'error': 'Неверный логин или пароль'}), 401
    session[SESSION_KEY] = user.id
    session.permanent = True
    g.current_user = user
    _log_user_action(user, 'login')
    return jsonify({'ok': True, 'user': _user_to_payload(user)})

@app.route('/api/auth/logout', methods=['POST'])
def api_auth_logout():
    user = _load_current_user()
    session.pop(SESSION_KEY, None)
    g.current_user = None
    _log_user_action(user, 'logout')
    return jsonify({'ok': True})

@app.route('/api/auth/me')
def api_auth_me():
    user = _load_current_user()
    if not user:
        return jsonify({'ok': False}), 401
    return jsonify({'ok': True, 'user': _user_to_payload(user)})

@app.route('/api/profile', methods=['GET', 'POST'])
def api_profile():
    user = _load_current_user()
    if not user:
        return jsonify({'ok': False, 'error': 'Не авторизовано'}), 401
    if request.method == 'GET':
        return jsonify({'ok': True, 'user': _user_to_payload(user)})
    data = request.get_json(silent=True) or {}
    user.full_name = (data.get('full_name') or '').strip() or None
    db.session.commit()
    try:
        _log_user_action(user, 'profile_update', 'user', user.id, detail=json.dumps({'full_name': user.full_name}))
    except Exception:
        pass
    return jsonify({'ok': True, 'user': _user_to_payload(user)})

@app.route('/api/auth/password', methods=['POST'])
def api_auth_change_password():
    user = _load_current_user()
    if not user:
        return jsonify({'ok': False, 'error': 'Не авторизовано'}), 401
    data = request.get_json(silent=True) or {}
    current = data.get('current_password') or ''
    new_password = (data.get('new_password') or '').strip()
    if not user.check_password(current):
        return jsonify({'ok': False, 'error': 'Неверный текущий пароль'}), 400
    if len(new_password) < 6:
        return jsonify({'ok': False, 'error': 'Пароль должен содержать не менее 6 символов'}), 400
    user.set_password(new_password)
    db.session.commit()
    _log_user_action(user, 'change_password')
    return jsonify({'ok': True})

def _require_admin() -> User:
    user = _load_current_user()
    if not user or user.role != 'admin':
        abort(403)
    return user

def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        _require_admin()
        return fn(*args, **kwargs)
    return wrapper

def _admin_count(exclude_id: int | None = None) -> int:
    q = User.query.filter(User.role == 'admin')
    if exclude_id is not None:
        q = q.filter(User.id != exclude_id)
    try:
        return q.count()
    except Exception:
        return 0

@app.route('/api/admin/users', methods=['GET', 'POST'])
def api_admin_users():
    _require_admin()
    if request.method == 'GET':
        users = User.query.order_by(func.lower(User.username)).all()
        return jsonify({'ok': True, 'users': [_user_to_payload(u) for u in users]})
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    role = (data.get('role') or 'user').strip().lower()
    if role not in ('admin', 'user'):
        role = 'user'
    if len(username) < 3:
        return jsonify({'ok': False, 'error': 'Логин должен содержать минимум 3 символа'}), 400
    if len(password) < 6:
        return jsonify({'ok': False, 'error': 'Пароль должен содержать минимум 6 символов'}), 400
    if User.query.filter(func.lower(User.username) == username.lower()).first():
        return jsonify({'ok': False, 'error': 'Пользователь с таким логином уже существует'}), 409
    user = User(username=username, role=role)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    _log_user_action(_load_current_user(), 'user_create', 'user', user.id, detail=json.dumps({'username': username, 'role': role}))
    return jsonify({'ok': True, 'user': _user_to_payload(user)}), 201


@app.route('/api/admin/users/search')
@require_admin
def api_admin_users_search():
    q = (request.args.get('q') or '').strip()
    try:
        limit = min(max(int(request.args.get('limit', '20')), 1), 50)
    except Exception:
        limit = 20
    qry = User.query
    if q:
        like = f"%{q}%"
        qry = qry.filter(or_(User.username.ilike(like), User.full_name.ilike(like)))
    users = qry.order_by(func.lower(User.username)).limit(limit).all()
    return jsonify({'ok': True, 'users': [{
        'id': u.id,
        'username': u.username,
        'full_name': u.full_name,
        'role': u.role,
    } for u in users]})

@app.route('/api/admin/users/<int:user_id>', methods=['PATCH', 'DELETE'])
def api_admin_user_detail(user_id: int):
    admin = _require_admin()
    user = User.query.get_or_404(user_id)
    if request.method == 'DELETE':
        if user.id == admin.id:
            return jsonify({'ok': False, 'error': 'Нельзя удалить собственную учётную запись'}), 400
        if user.role == 'admin' and _admin_count(exclude_id=user.id) == 0:
            return jsonify({'ok': False, 'error': 'Нельзя удалить последнего администратора'}), 400
        db.session.delete(user)
        db.session.commit()
        _log_user_action(admin, 'user_delete', 'user', user_id)
        return jsonify({'ok': True})
    data = request.get_json(silent=True) or {}
    updated = False
    new_role = data.get('role')
    if new_role is not None:
        role = str(new_role).strip().lower()
        if role in ('admin', 'user') and role != user.role:
            if user.role == 'admin' and role != 'admin' and _admin_count(exclude_id=user.id) == 0:
                return jsonify({'ok': False, 'error': 'Должен остаться хотя бы один администратор'}), 400
            user.role = role
            updated = True
    new_password = data.get('password')
    if new_password:
        pw = str(new_password).strip()
        if len(pw) < 6:
            return jsonify({'ok': False, 'error': 'Пароль должен содержать минимум 6 символов'}), 400
        user.set_password(pw)
        updated = True
    if not updated:
        return jsonify({'ok': False, 'error': 'Нет изменений'}), 400
    db.session.commit()
    _log_user_action(admin, 'user_update', 'user', user.id, detail=json.dumps({'role': user.role, 'password_changed': bool(new_password)}))
    return jsonify({'ok': True, 'user': _user_to_payload(user)})

ensure_default_admin()

# ------------------- Инициализация и миграции коллекций -------------------
def _slugify(s: str) -> str:
    s = (s or '').strip().lower()
    s = re.sub(r"[^a-z0-9_\-а-яё]+", "-", s)
    s = re.sub(r"-+", "-", s).strip('-')
    if not s:
        s = 'collection'
    return s

def ensure_collections_schema():
    """Проверить наличие модели/таблицы коллекций и поля files.collection_id, создать базовую коллекцию.
    Привязать базовую коллекцию к файлам, у которых она отсутствует.
    """
    try:
        with app.app_context():
            # Создать таблицы, если их ещё нет
            db.create_all()
            # Добавить столбец collection_id в files, если его нет
            try:
                from sqlalchemy import text as _text
                with db.engine.begin() as conn:
                    rows = list(conn.execute(_text("PRAGMA table_info(files)")))
                    cols = {r[1] for r in rows}
                    if 'collection_id' not in cols:
                        conn.execute(_text("ALTER TABLE files ADD COLUMN collection_id INTEGER"))
                    rows = list(conn.execute(_text("PRAGMA table_info(collections)")))
                    ccols = {r[1] for r in rows}
                    if 'owner_id' not in ccols:
                        conn.execute(_text("ALTER TABLE collections ADD COLUMN owner_id INTEGER"))
                    if 'is_private' not in ccols:
                        conn.execute(_text("ALTER TABLE collections ADD COLUMN is_private BOOLEAN NOT NULL DEFAULT 0"))
                    rows = list(conn.execute(_text("PRAGMA table_info(users)")))
                    ucols = {r[1] for r in rows}
                    if 'full_name' not in ucols:
                        conn.execute(_text("ALTER TABLE users ADD COLUMN full_name TEXT"))
            except Exception:
                pass
            # Убедиться, что базовая коллекция существует
            base = Collection.query.filter_by(slug='base').first()
            if not base:
                base = Collection(name='Базовая коллекция', slug='base', searchable=True, graphable=True)
                db.session.add(base)
                db.session.commit()
            # Назначить базовую коллекцию всем файлам с NULL в collection_id
            try:
                File.query.filter(File.collection_id.is_(None)).update({File.collection_id: base.id})
                db.session.commit()
            except Exception:
                db.session.rollback()
    except Exception:
        # по возможности продолжаем дальше
        pass

# ------------------- Вспомогательные функции безопасности путей -------------------
def _resolve_under_base(rel_path: str) -> tuple[Path, Path | None]:
    """Безопасно разрешать rel_path внутри UPLOAD_FOLDER. Возвращает (base_dir, abs_path или None, если путь вне папки)."""
    base_dir = Path(app.config.get('UPLOAD_FOLDER') or '.').resolve()
    try:
        # Нормализовать разделители и убрать ведущие слэши
        rel = str(rel_path).replace('\\', '/').lstrip('/')
        abs_path = (base_dir / rel).resolve()
        abs_path.relative_to(base_dir)
        return base_dir, abs_path
    except Exception:
        return base_dir, None

# ------------------- Routes -------------------

@app.after_request
def _force_utf8(resp):
    try:
        ct = (resp.headers.get('Content-Type') or '').lower()
        if ct.startswith('text/html'):
            resp.headers['Content-Type'] = 'text/html; charset=utf-8'
        elif ct.startswith('application/json'):
            # обеспечить кодировку utf-8 и для JSON
            resp.headers['Content-Type'] = 'application/json; charset=utf-8'
        return resp
    except Exception:
        return resp

# Загрузка файлов через веб-интерфейс
@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    user = _load_current_user()
    if not user:
        abort(401)
    is_admin = getattr(user, 'role', '') == 'admin'
    if request.method == "GET":
        allowed = getattr(g, 'allowed_collection_ids', set())
        cols_q = Collection.query.order_by(Collection.name.asc())
        if allowed is not None:
            if not allowed:
                cols_q = cols_q.filter(Collection.id == -1)
            else:
                cols_q = cols_q.filter(Collection.id.in_(allowed))
        cols = cols_q.all()
        return jsonify({
            "allowed_extensions": sorted(ALLOWED_EXTS),
            "collections": [{"id": c.id, "name": c.name} for c in cols],
        })

    file = request.files.get("file")
    if not file or not file.filename:
        return json_error("Файл не выбран", 400)

    col_id = request.form.get('collection_id')
    new_col = (request.form.get('new_collection') or '').strip()
    col: Collection | None = None
    allowed = getattr(g, 'allowed_collection_ids', set())
    try:
        if new_col:
            slug = _slugify(new_col)
            col = Collection.query.filter((Collection.slug == slug) | (Collection.name == new_col)).first()
            if not col:
                requested_private = str(request.form.get('private', '')).lower() in ('1','true','yes','on')
                is_private_flag = requested_private or not is_admin
                col = Collection(name=new_col, slug=slug, searchable=True, graphable=True,
                                 owner_id=user.id, is_private=is_private_flag)
                db.session.add(col)
                db.session.commit()
                if user:
                    try:
                        if not CollectionMember.query.filter_by(collection_id=col.id, user_id=user.id).first():
                            db.session.add(CollectionMember(collection_id=col.id, user_id=user.id, role='owner'))
                            db.session.commit()
                    except Exception:
                        db.session.rollback()
                if allowed is not None:
                    try:
                        allowed = set(allowed)
                        allowed.add(col.id)
                        g.allowed_collection_ids = allowed
                    except Exception:
                        pass
        elif col_id:
            try:
                col = Collection.query.get(int(col_id))
            except Exception:
                col = None
    except Exception as e:
        db.session.rollback()
        return json_error(f"Не удалось создать коллекцию: {e}", 500)
    if not col and user:
        try:
            col = Collection.query.filter_by(owner_id=user.id, is_private=True).first()
        except Exception:
            col = None
    if not col:
        col = Collection.query.filter_by(slug='base').first() or Collection.query.first()
    if col:
        if allowed is not None and col.id not in allowed:
            allowed = None  # позволяем перейти к проверке права записи
        if not _has_collection_access(col.id, write=True):
            abort(403)

    safe_name = secure_filename(file.filename)
    ext = Path(safe_name).suffix.lower()
    if ext not in ALLOWED_EXTS:
        return json_error(f"Недопустимый тип файла: {ext}", 415)

    base_dir = SCAN_ROOT / IMPORT_SUBDIR if (IMPORT_SUBDIR or '').strip() else SCAN_ROOT
    base_dir.mkdir(parents=True, exist_ok=True)
    save_path = base_dir / safe_name
    i = 1
    orig_name = Path(safe_name).stem
    while save_path.exists():
        save_path = base_dir / f"{orig_name}_{i}{ext}"
        i += 1
    try:
        file.save(save_path)
    except Exception as e:
        return json_error(f"Не удалось сохранить файл: {e}", 500)

    try:
        rel_path = str(save_path.relative_to(SCAN_ROOT))
    except Exception:
        rel_path = save_path.name
    try:
        sha1 = sha1_of_file(save_path)
    except Exception:
        sha1 = None

    try:
        fobj = File.query.filter_by(path=str(save_path)).first()
        if not fobj:
            fobj = File(
                path=str(save_path),
                rel_path=rel_path,
                filename=Path(save_path).stem,
                ext=Path(save_path).suffix.lower(),
                size=save_path.stat().st_size,
                mtime=save_path.stat().st_mtime,
                sha1=sha1,
                collection_id=(col.id if col else None),
            )
            db.session.add(fobj)
        else:
            if col:
                fobj.collection_id = col.id
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return json_error(f"Ошибка записи в базу: {e}", 500)

    try:
        _log_user_action(_load_current_user(), 'file_upload', 'file', fobj.id if fobj else None, detail=json.dumps({'filename': fobj.filename, 'collection_id': fobj.collection_id}))
    except Exception:
        pass
    return jsonify({
        "status": "ok",
        "file_id": fobj.id if fobj else None,
        "rel_path": rel_path,
    })

# Утилита: безопасная относительная дорожка (для загрузки папок)
def _sanitize_relpath(p: str) -> str:
    p = (p or '').replace('\\', '/').lstrip('/')
    parts = [seg for seg in p.split('/') if seg not in ('', '.', '..')]
    return '/'.join(parts)


def _get_collection_instance(collection_ref) -> Collection | None:
    if isinstance(collection_ref, Collection):
        return collection_ref
    try:
        cid = int(collection_ref)
    except Exception:
        cid = None
    if cid:
        try:
            return Collection.query.get(cid)
        except Exception:
            return None
    return None


def _collection_dir_slug(col: Collection | None) -> str:
    if not col:
        return ''
    slug = (col.slug or '').strip()
    if slug:
        return slug
    name = (col.name or '').strip()
    if name:
        return _slugify(name)
    return f"collection-{col.id or 'unknown'}"


def _collection_root_dir(col: Collection | None, *, ensure: bool = True) -> Path:
    root = Path(SCAN_ROOT)
    if not COLLECTIONS_IN_SEPARATE_DIRS or col is None:
        if ensure:
            root.mkdir(parents=True, exist_ok=True)
        return root
    slug = _collection_dir_slug(col) or f"collection-{col.id or 'unknown'}"
    target = Path(SCAN_ROOT) / 'collections' / slug
    if ensure:
        target.mkdir(parents=True, exist_ok=True)
    return target


def _import_base_dir_for_collection(col: Collection | None) -> Path:
    base_root = _collection_root_dir(col)
    sub = IMPORT_SUBDIR.strip()
    return (base_root / sub) if sub else base_root

@app.route('/import', methods=['GET', 'POST'])
def import_files():
    """Импорт нескольких файлов и папок (через webkitdirectory) с возможным автосканом."""
    user = _load_current_user()
    if not user:
        abort(401)
    is_admin = getattr(user, 'role', '') == 'admin'
    if request.method == 'GET':
        allowed = getattr(g, 'allowed_collection_ids', set())
        q = Collection.query.order_by(Collection.name.asc())
        if allowed is not None:
            if not allowed:
                q = q.filter(Collection.id == -1)
            else:
                q = q.filter(Collection.id.in_(allowed))
        cols = q.all()
        return jsonify({
            "allowed_extensions": sorted(ALLOWED_EXTS),
            "collections": [{"id": c.id, "name": c.name} for c in cols],
        })

    files = request.files.getlist('files')
    if not files:
        return json_error('Файлы не выбраны', 400)

    col_id = request.form.get('collection_id')
    new_col = (request.form.get('new_collection') or '').strip()
    col: Collection | None = None
    allowed = getattr(g, 'allowed_collection_ids', set())
    try:
        if new_col:
            slug = _slugify(new_col)
            col = Collection.query.filter((Collection.slug == slug) | (Collection.name == new_col)).first()
            if not col:
                requested_private = str(request.form.get('private', '')).lower() in ('1','true','yes','on')
                is_private_flag = requested_private or not is_admin
                col = Collection(name=new_col, slug=slug, searchable=True, graphable=True,
                                 owner_id=user.id, is_private=is_private_flag)
                db.session.add(col)
                db.session.commit()
                if user:
                    try:
                        if not CollectionMember.query.filter_by(collection_id=col.id, user_id=user.id).first():
                            db.session.add(CollectionMember(collection_id=col.id, user_id=user.id, role='owner'))
                            db.session.commit()
                    except Exception:
                        db.session.rollback()
                if allowed is not None:
                    try:
                        allowed = set(allowed)
                        allowed.add(col.id)
                        g.allowed_collection_ids = allowed
                    except Exception:
                        pass
        elif col_id:
            try:
                col = Collection.query.get(int(col_id))
            except Exception:
                col = None
    except Exception as e:
        db.session.rollback()
        return json_error(f"Не удалось создать коллекцию: {e}", 500)
    if not col and user:
        try:
            col = Collection.query.filter_by(owner_id=user.id, is_private=True).first()
        except Exception:
            col = None
    if not col:
        col = Collection.query.filter_by(slug='base').first() or Collection.query.first()
    if col:
        if allowed is not None and col.id not in allowed:
            allowed = None
        if not _has_collection_access(col.id, write=True):
            abort(403)

    base_dir = _import_base_dir_for_collection(col)
    saved = 0
    skipped = 0
    duplicates_found = 0
    saved_rel_paths: list[str] = []
    base_dir.mkdir(parents=True, exist_ok=True)

    for fs in files:
        raw_name = fs.filename or ''
        rel = _sanitize_relpath(raw_name)
        ext = Path(rel).suffix.lower()
        if not rel or ext not in ALLOWED_EXTS:
            skipped += 1
            continue
        dest = base_dir / rel
        if dest.exists():
            skipped += 1
            duplicates_found += 1
            app.logger.info(f'Skip duplicate by path: {dest}')
            continue
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            fs.save(dest)
        except Exception as e:
            app.logger.warning(f'Failed to save %s: %s', rel, e)
            skipped += 1
            continue

        try:
            try:
                sha1 = sha1_of_file(dest)
            except Exception:
                sha1 = None

            duplicate_rec = None
            if sha1:
                try:
                    q = File.query.filter(File.sha1 == sha1)
                    if col:
                        q = q.filter(File.collection_id == col.id)
                    else:
                        q = q.filter(File.collection_id.is_(None))
                    duplicate_rec = q.first()
                except Exception:
                    duplicate_rec = None
            if duplicate_rec:
                try:
                    existing_path = Path(duplicate_rec.path) if duplicate_rec.path else None
                except Exception:
                    existing_path = None
                try:
                    if existing_path is None or existing_path.resolve() != dest.resolve():
                        try:
                            dest.unlink()
                        except Exception:
                            pass
                        duplicates_found += 1
                        skipped += 1
                        app.logger.info(f'Skip duplicate by hash: {dest}')
                        continue
                except Exception:
                    pass

            try:
                relp = str(dest.relative_to(SCAN_ROOT))
            except Exception:
                relp = dest.name
            fobj = File.query.filter_by(path=str(dest)).first()
            if not fobj:
                fobj = File(
                    path=str(dest),
                    rel_path=relp,
                    filename=dest.stem,
                    ext=dest.suffix.lower(),
                    size=dest.stat().st_size,
                    mtime=dest.stat().st_mtime,
                    sha1=sha1,
                    collection_id=(col.id if col else None),
                )
                db.session.add(fobj)
            else:
                if col:
                    fobj.collection_id = col.id
            db.session.commit()
            saved_rel_paths.append(relp)
            saved += 1
        except Exception as e:
            db.session.rollback()
            app.logger.warning(f'Failed to register %s in DB: %s', rel, e)
            skipped += 1
            try:
                if dest.exists():
                    dest.unlink()
            except Exception:
                pass

    start_scan = request.form.get('start_scan') == 'on'
    extract_text = request.form.get('extract_text', 'on') == 'on'
    use_llm = request.form.get('use_llm') == 'on'
    prune = request.form.get('prune', 'on') == 'on'
    scan_started = False
    if start_scan and saved_rel_paths:
        if not is_admin:
            try:
                _log_user_action('scan_start_denied', 'collection', col.id if col else None,
                                 detail=json.dumps({'requested': len(saved_rel_paths)}))
            except Exception:
                pass
        else:
            paths_for_scan = [str((SCAN_ROOT / p).resolve()) for p in saved_rel_paths]
            threading.Thread(
                target=_run_scan_with_progress,
                args=(extract_text, use_llm, prune, 0, paths_for_scan),
                daemon=True
            ).start()
            scan_started = True

    try:
        _log_user_action(
            _load_current_user(),
            'import_batch',
            'collection',
            col.id if col else None,
            detail=json.dumps({'saved': saved, 'skipped': skipped, 'duplicates': duplicates_found})
        )
    except Exception:
        pass
    return jsonify({
        "status": "ok",
        "saved": saved,
        "skipped": skipped,
        "duplicates": duplicates_found,
        "scan_started": scan_started,
        "imported": saved_rel_paths,
    })

# Просмотр и скачивание файлов
@app.route("/download/<path:rel_path>")
def download_file(rel_path):
    base_dir, abs_path = _resolve_under_base(rel_path)
    # Пытаемся заранее найти запись в БД и подготовить резерв, если файл переместили
    try:
        rp = str(rel_path)
        rp_alt = rp.replace('/', '\\') if ('/' in rp) else rp.replace('\\', '/')
        f = File.query.filter(or_(File.rel_path == rp, File.rel_path == rp_alt)).first()
    except Exception:
        f = None
    if (abs_path is None or not abs_path.exists()) and f is not None:
        try:
            filename_only = Path(f.rel_path or rp).name
            type_dirs = app.config.get('TYPE_DIRS') or {}
            sub = type_dirs.get((f.material_type or '').strip().lower())
            candidates = []
            if sub:
                candidates.append((base_dir / sub / filename_only).resolve())
            for v in set(type_dirs.values()):
                candidates.append((base_dir / v / filename_only).resolve())
            candidates.append((base_dir / filename_only).resolve())
            for cand in candidates:
                try:
                    cand.relative_to(base_dir)
                except Exception:
                    continue
                if cand.exists():
                    abs_path = cand
                    try:
                        f.path = str(cand)
                        f.rel_path = str(cand.relative_to(base_dir))
                        db.session.commit()
                    except Exception:
                        db.session.rollback()
                    # обновляем rel_path для дальнейших URL
                    rel_path = f.rel_path
                    break
            # В крайнем случае ищем по имени файла во всём base_dir (может быть медленно)
            if not (abs_path and abs_path.exists()):
                try:
                    for cand in base_dir.rglob(filename_only):
                        if cand.is_file():
                            abs_path = cand.resolve()
                            try:
                                abs_path.relative_to(base_dir)
                            except Exception:
                                continue
                            try:
                                f.path = str(cand)
                                f.rel_path = str(cand.relative_to(base_dir))
                                db.session.commit()
                            except Exception:
                                db.session.rollback()
                            rel_path = f.rel_path
                            break
                except Exception:
                    pass
        except Exception:
            pass
    if abs_path is None or not abs_path.exists():
        return ("Файл не найден.", 404)
    return send_from_directory(str(base_dir), str(Path(rel_path).as_posix()), as_attachment=True)

@app.route("/media/<path:rel_path>")
def media_file(rel_path):
    """Отдавать файл встраиваемо (для аудиоплеера в предпросмотре), без принудительного скачивания."""
    base_dir, abs_path = _resolve_under_base(rel_path)
    if abs_path is None or not abs_path.exists():
        # Встроенный 404 упрощает работу iframe/плеера
        return ("Not Found", 404)
    return send_from_directory(str(base_dir), str(Path(rel_path).as_posix()), as_attachment=False)

@app.route("/view/<path:rel_path>")
def view_file(rel_path):
    base_dir, abs_path = _resolve_under_base(rel_path)
    if abs_path is None or not abs_path.exists():
        return ("Файл не найден.", 404)
    ext = abs_path.suffix.lower()
    if ext == ".pdf":
        return send_from_directory(str(base_dir), str(Path(rel_path).as_posix()))
    elif ext in {".txt", ".md"}:
        try:
            content = abs_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            content = "Не удалось прочитать файл."
        return Response(content, mimetype="text/plain; charset=utf-8")
    return json_error("Просмотр этого типа файлов не поддерживается", 415)


@app.route('/preview/<path:rel_path>')
def preview_file(rel_path):
    base_dir, abs_path = _resolve_under_base(rel_path)
    if abs_path is None or not abs_path.exists():
        body = (
            "<div style=\"min-height:100vh;display:flex;align-items:center;justify-content:center;"
            "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;"
            "padding:24px;text-align:center;\">"
            "<div style=\"max-width:400px;background:#161b22;padding:28px;border-radius:18px;"
            "border:1px solid #30363d;box-shadow:0 20px 48px rgba(0,0,0,0.45);\">"
            "Файл не найден или недоступен.</div></div>"
        )
        return _html_page("Файл не найден", body, status=404)

    ext = abs_path.suffix.lower()
    is_pdf = ext == '.pdf'
    is_text = ext in {'.txt', '.md'}
    is_image = ext in IMAGE_EXTS
    is_audio = ext in AUDIO_EXTS
    content = ''
    thumbnail_url = None
    abstract = ''
    audio_url = None
    duration = None
    image_url = None

    try:
        # Кэш‑каталог для текстовых фрагментов
        cache_dir = Path(app.static_folder) / 'cache' / 'text_excerpts'
        cache_dir.mkdir(parents=True, exist_ok=True)
        # по возможности используем sha1 в качестве ключа
        sha = None
        try:
            import hashlib
            with abs_path.open('rb') as fh:
                b = fh.read(1024*16)
                sha = hashlib.sha1(b).hexdigest()
        except Exception:
            sha = None

        cache_file = cache_dir / ((sha or rel_path.replace('/', '_')) + '.txt')
        if cache_file.exists():
            content = cache_file.read_text(encoding='utf-8', errors='ignore')
        else:
            if is_pdf:
                content = extract_text_pdf(abs_path, limit_chars=4000)[:4000]
            elif ext == '.docx':
                content = extract_text_docx(abs_path, limit_chars=4000)
            elif ext == '.rtf':
                content = extract_text_rtf(abs_path, limit_chars=4000)
            elif ext == '.epub':
                content = extract_text_epub(abs_path, limit_chars=4000)
            elif ext == '.djvu':
                content = extract_text_djvu(abs_path, limit_chars=4000)
            elif is_text:
                content = abs_path.read_text(encoding='utf-8', errors='ignore')[:4000]
            # для аудио позднее предпочтём данные из БД
            try:
                cache_file.write_text(content, encoding='utf-8')
            except Exception:
                pass
    except Exception:
        content = ''

    # Пробуем найти запись File, чтобы получить id для ссылки «Подробнее» и ключевые слова
    # Находим запись в БД по rel_path; учитываем различия слэшей/обратных слэшей (Windows)
    rp = str(rel_path)
    rp_alt = rp.replace('/', '\\') if ('/' in rp) else rp.replace('\\', '/')
    f = File.query.filter(or_(File.rel_path == rp, File.rel_path == rp_alt)).first()
    file_id = f.id if f else None
    keywords_str = (f.keywords or '') if f else ''
    if f and (is_audio or (f.material_type or '') == 'audio'):
        is_audio = True
        abstract = (f.abstract or '')
        # Если нет кэша, берём фрагмент транскрипта из БД
        if not content:
            content = (f.text_excerpt or '')[:4000]
        # аудиоплеер ссылается на endpoint скачивания
        # используем rel_path с прямыми слэшами для URL
        audio_url = url_for('media_file', rel_path=str(rel_path).replace('\\','/'))
        try:
            duration = audio_duration_hhmmss(abs_path)
        except Exception:
            duration = None
    if f and (is_image or (f.material_type or '') == 'image'):
        is_image = True
        abstract = (f.abstract or '')
        image_url = url_for('media_file', rel_path=str(rel_path).replace('\\','/'))

    # Сгенерировать миниатюру для PDF (с кэшем)
    if is_pdf:
        try:
            thumb_path = Path(app.static_folder) / 'thumbnails' / (Path(rel_path).stem + '.png')
            if not thumb_path.exists():
                try:
                    import fitz
                    doc = fitz.open(str(abs_path))
                    pix = doc[0].get_pixmap(matrix=fitz.Matrix(1, 1))
                    thumb_path.parent.mkdir(parents=True, exist_ok=True)
                    pix.save(str(thumb_path))
                except Exception as e:
                    app.logger.warning(f"Thumbnail generation failed: {e}")
            if thumb_path.exists():
                thumbnail_url = url_for('static', filename=f'thumbnails/{thumb_path.name}')
        except Exception:
            thumbnail_url = None

    rel_url = str(rel_path).replace('\\','/')
    embedded = str(request.args.get('embedded', '')).strip().lower() in ('1', 'true', 'yes', 'on')
    return _render_preview(
        rel_url,
        is_pdf=is_pdf,
        is_text=is_text,
        is_audio=is_audio,
        is_image=is_image,
        content=content,
        thumbnail_url=thumbnail_url,
        abstract=abstract,
        audio_url=audio_url,
        duration=duration,
        image_url=image_url,
        keywords=keywords_str,
        embedded=embedded,
    )

FILENAME_PATTERNS = [
    # "Author - Title (2021).pdf"
    re.compile(r"^(?P<author>.+?)\s*-\s*(?P<title>.+?)\s*\((?P<year>\d{4})\)$"),
    # "Author_Title_2020.pdf" или "Author Title 2020.pdf"
    re.compile(r"^(?P<author>.+?)[_ ]+(?P<title>.+?)[_ ]+(?P<year>\d{4})$"),
]

def sha1_of_file(fp: Path, chunk=1<<20):
    h = hashlib.sha1()
    with fp.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def extract_text_pdf(fp: Path, limit_chars=40000, force_ocr_first_page: bool = False):
    """Извлечение текста из PDF.
    Fallback: если текста крайне мало — OCR до первых 5 страниц (если установлен pytesseract).
    Также логируем включение/отсутствие OCR и затраченное время в прогресс-лог (если он активен).
    """
    try:
        import time as _time
        doc = fitz.open(fp)
        text_parts = []
        max_ocr_pages = int(os.getenv('PDF_OCR_PAGES', '5'))
        used_ocr_pages = 0
        ocr_time_total = 0.0
        # Попробуем первые N страниц улучшить OCR-ом, если обычный текст слишком скуден
        for idx, page in enumerate(doc):
            raw = page.get_text("text") or ""
            # Для диссертаций может потребоваться OCR первой страницы независимо от настроек
            if idx == 0 and force_ocr_first_page and pytesseract is not None:
                try:
                    t0 = _time.time()
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as tf:
                        pix.save(tf.name)
                        ocr = pytesseract.image_to_string(tf.name, lang=os.getenv('OCR_LANGS', 'rus+eng'))
                        if (ocr or '').strip():
                            # если есть оригинальный текст, добавляем OCR в начало; иначе заменяем
                            if len((raw or '').strip()) < 200:
                                raw = (ocr or '')
                            else:
                                raw = (ocr or '') + "\n" + raw
                            used_ocr_pages += 1
                    ocr_time_total += (_time.time() - t0)
                except Exception as oe:
                    app.logger.info(f"OCR(first page) failed for {fp}: {oe}")
            # Эвристический OCR первых N страниц, когда исходный текст слишком короткий
            if idx < max_ocr_pages and pytesseract is not None and len((raw or '').strip()) < 30:
                try:
                    t0 = _time.time()
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as tf:
                        pix.save(tf.name)
                        ocr = pytesseract.image_to_string(tf.name, lang=os.getenv('OCR_LANGS', 'rus+eng'))
                        if (ocr or '').strip():
                            raw = (ocr or '')
                            used_ocr_pages += 1
                    ocr_time_total += (_time.time() - t0)
                except Exception as oe:
                    app.logger.info(f"OCR failed for page {idx} {fp}: {oe}")
            text_parts.append(raw)
            if sum(len(x) for x in text_parts) >= limit_chars:
                break
        text = "\n".join(text_parts)
        # Эвристика: выявляем «кашу» в тексте (защита от копирования/битые глифы) и переходим к OCR
        try:
            def _ratio_latin_cyr(s: str) -> float:
                if not s:
                    return 0.0
                good = sum(1 for ch in s if ('a' <= ch.lower() <= 'z') or ('а' <= ch.lower() <= 'я') or (ch in 'ёЁ'))
                return good / max(1, len(s))
            # очень мало латинских/кириллических букв и много символов
            rat = _ratio_latin_cyr(text)
            if len(text) >= 400 and rat < 0.15 and pytesseract is not None:
                tstart = _time.time()
                text_ocr = []
                pages_to_ocr = min(len(doc), int(os.getenv('PDF_OCR_PAGES', '5')))
                for idx in range(pages_to_ocr):
                    page = doc[idx]
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as tf:
                        pix.save(tf.name)
                        ocr = pytesseract.image_to_string(tf.name, lang=os.getenv('OCR_LANGS', 'rus+eng'))
                        if ocr:
                            text_ocr.append(ocr)
                if text_ocr:
                    text = ("\n".join(text_ocr) or text)[:limit_chars]
                    used_ocr_pages = max(used_ocr_pages, pages_to_ocr)
                    ocr_time_total += (_time.time() - tstart)
                    try:
                        _scan_log("OCR: заменил искажённый текст извлечённым OCR")
                    except Exception:
                        pass
        except Exception:
            pass
        # Резервный проход: если после обработки текст остаётся коротким — выполняем OCR до 5 страниц без условия длины
        if len(text.strip()) < 200 and pytesseract is not None:
            try:
                tstart = _time.time()
                text_ocr = []
                pages_to_ocr = min(len(doc), int(os.getenv('PDF_OCR_PAGES', '5')))
                for idx in range(pages_to_ocr):
                    page = doc[idx]
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as tf:
                        pix.save(tf.name)
                        ocr = pytesseract.image_to_string(tf.name, lang=os.getenv('OCR_LANGS', 'rus+eng'))
                        if ocr:
                            text_ocr.append(ocr)
                text = ("\n".join(text_ocr) or text)[:limit_chars]
                used_ocr_pages = max(used_ocr_pages, pages_to_ocr)
                ocr_time_total += (_time.time() - tstart)
            except Exception as oe:
                app.logger.info(f"OCR fallback failed {fp}: {oe}")

        # Логирование в прогресс (если доступно)
        try:
            if used_ocr_pages > 0:
                _scan_log(f"OCR: использовано страниц {used_ocr_pages}, время {int(ocr_time_total*1000)} мс")
            elif pytesseract is None:
                _scan_log("OCR недоступен (pytesseract не установлен)")
        except Exception:
            pass
        return text[:limit_chars]
    except Exception as e:
        app.logger.warning(f"PDF extract failed for {fp}: {e}")
        return ""

def extract_text_docx(fp: Path, limit_chars=40000):
    if not docx:
        return ""
    try:
        d = docx.Document(str(fp))
        text = "\n".join([p.text for p in d.paragraphs])
        return text[:limit_chars]
    except Exception as e:
        app.logger.warning(f"DOCX extract failed for {fp}: {e}")
        return ""

def extract_text_rtf(fp: Path, limit_chars=40000):
    if not rtf_to_text:
        return ""
    try:
        text = rtf_to_text(fp.read_text(encoding="utf-8", errors="ignore"))
        return text[:limit_chars]
    except Exception as e:
        app.logger.warning(f"RTF extract failed for {fp}: {e}")
        return ""

def extract_text_epub(fp: Path, limit_chars=40000):
    if not epub:
        return ""
    try:
        book = epub.read_epub(str(fp))
        text = ""
        for item in book.get_items():
            if item.get_type() == epub.ITEM_DOCUMENT:
                text += item.get_content().decode(errors="ignore")
                if len(text) >= limit_chars:
                    break
        return text[:limit_chars]
    except Exception as e:
        app.logger.warning(f"EPUB extract failed for {fp}: {e}")
        return ""

def extract_text_djvu(fp: Path, limit_chars=40000):
    if not djvu:
        return ""
    try:
        with djvu.decode.open(str(fp)) as d:
            text = ""
            for page in d.pages:
                text += page.get_text()
                if len(text) >= limit_chars:
                    break
        return text[:limit_chars]
    except Exception as e:
        app.logger.warning(f"DjVu extract failed for {fp}: {e}")
        return ""

def _ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def _convert_to_wav_pcm16(src: Path, dst: Path, rate=16000):
    if not _ffmpeg_available():
        raise RuntimeError("ffmpeg not found for audio conversion")
    subprocess.run(['ffmpeg','-y','-i',str(src),'-ac','1','-ar',str(rate),'-f','wav',str(dst)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

def _ffprobe_duration_seconds(src: Path) -> float:
    if shutil.which('ffprobe') is None:
        return 0.0
    try:
        out = subprocess.check_output(['ffprobe','-v','error','-show_entries','format=duration','-of','default=nw=1:nk=1',str(src)], stderr=subprocess.DEVNULL)
        return float(out.strip())
    except Exception:
        return 0.0

def audio_duration_hhmmss(src: Path) -> str:
    """Return duration string HH:MM:SS for common audio; best-effort using wave/ffprobe."""
    try:
        if src.suffix.lower() == '.wav':
            with wave.open(str(src), 'rb') as wf:
                frames = wf.getnframes(); rate = wf.getframerate() or 1
                secs = frames / float(rate)
        else:
            secs = _ffprobe_duration_seconds(src)
    except Exception:
        secs = 0.0
    secs = int(round(secs))
    h = secs // 3600; m = (secs % 3600) // 60; s = secs % 60
    return (f"{h:02d}:{m:02d}:{s:02d}") if h else (f"{m:02d}:{s:02d}")

def transcribe_audio(fp: Path, limit_chars=40000,
                     backend_override: str | None = None,
                     model_path_override: str | None = None,
                     lang_override: str | None = None,
                     vad_override: bool | None = None) -> str:
    # Позволяем диагностике работать даже при выключенном глобальном флаге, когда заданы overrides
    if not TRANSCRIBE_ENABLED and backend_override is None:
        return ""
    backend = (backend_override or TRANSCRIBE_BACKEND or '').lower()
    model_path = (model_path_override if model_path_override is not None else TRANSCRIBE_MODEL_PATH) or ''
    lang = (lang_override if (lang_override is not None) else TRANSCRIBE_LANGUAGE) or 'ru'
    try:
        if backend == 'faster-whisper' and FasterWhisperModel:
            # Определить путь к модели (каталог / алиас / repo id)
            resolved = ''
            if model_path:
                if Path(model_path).expanduser().exists():
                    resolved = str(Path(model_path).expanduser())
                else:
                    resolved = _ensure_faster_whisper_model(model_path)
            # Запасной вариант: используем small, если путь не определился
            if not resolved:
                resolved = _ensure_faster_whisper_model(os.getenv('FASTER_WHISPER_DEFAULT_MODEL', 'small'))
            if not resolved:
                app.logger.warning("Не удалось определить путь к модели faster-whisper — укажите TRANSCRIBE_MODEL_PATH или установите huggingface_hub")
                return ""
            model = FasterWhisperModel(resolved, device="cpu", compute_type="int8")

            def _fw_try(vad=True, lng=lang):
                try:
                    segs, info = model.transcribe(str(fp), language=lng, vad_filter=vad)
                    txt = " ".join(((s.text or '').strip()) for s in segs)
                    return (txt or '').strip()
                except Exception as _e:
                    app.logger.info(f"Попытка распознавания faster-whisper не удалась (vad={vad}, lang={lng}): {_e}")
                    return ''

            # Последовательность попыток (или принудительно заданный режим VAD)
            if vad_override is True or vad_override is False:
                text = _fw_try(vad=bool(vad_override), lng=lang)
                if not text:
                    text = _fw_try(vad=bool(vad_override), lng=None)
            else:
                text = _fw_try(vad=True, lng=lang)
                if not text:
                    text = _fw_try(vad=False, lng=lang)
                if not text:
                    text = _fw_try(vad=False, lng=None)
            return (text or '')[:limit_chars]
    except Exception as e:
        app.logger.warning(f"Транскрибация не удалась для {fp}: {e}")
    return ""

def call_lmstudio_summarize(text: str, filename: str) -> str:
    text = (text or "")[: int(os.getenv("SUMMARY_TEXT_LIMIT", "12000"))]
    if not text:
        return ""
    system = PROMPTS.get('summarize_audio_system') or (
        "Ты помощник. Суммаризируй стенограмму аудио в 3–6 предложениях на русском, "
        "выделив тему, основные тезисы и вывод."
    )
    user = f"Файл: {filename}\nСтенограмма:\n{text}"
    last_error: Exception | None = None
    for choice in _llm_iter_choices('summary'):
        label = _llm_choice_label(choice)
        url = _llm_choice_url(choice)
        if not url:
            app.logger.warning(f"Суммаризация не удалась ({label}): пустой URL конечной точки")
            continue
        payload = {
            "model": choice.get('model') or LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.2,
            "max_tokens": 400,
            "top_p": 1.0,
        }
        try:
            r = requests.post(url, headers=_llm_choice_headers(choice), json=payload, timeout=120)
            if _llm_response_indicates_busy(r):
                app.logger.info(f"LLM summary endpoint занята ({label}), переключаемся")
                last_error = RuntimeError('busy')
                continue
            r.raise_for_status()
            data = r.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                return content
        except Exception as e:
            last_error = e
            app.logger.warning(f"Суммаризация не удалась ({label}): {e}")
    if last_error and str(last_error) != 'busy':
        app.logger.warning(f"Суммаризация не удалась: {last_error}")
    return ""

def call_lmstudio_compose(system: str, user: str, *, temperature: float = 0.2, max_tokens: int = 400) -> str:
    last_error: Exception | None = None
    for choice in _llm_iter_choices('compose'):
        label = _llm_choice_label(choice)
        url = _llm_choice_url(choice)
        if not url:
            app.logger.warning(f"Compose LLM endpoint некорректен ({label}): пустой URL")
            continue
        payload = {
            "model": choice.get('model') or LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
            "top_p": 1.0,
        }
        try:
            r = requests.post(url, headers=_llm_choice_headers(choice), json=payload, timeout=120)
            if _llm_response_indicates_busy(r):
                app.logger.info(f"LLM compose endpoint занята ({label}), переключаемся")
                last_error = RuntimeError('busy')
                continue
            r.raise_for_status()
            data = r.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                return content
        except Exception as e:
            last_error = e
            app.logger.warning(f"LM Studio compose failed ({label}): {e}")
    if last_error and str(last_error) != 'busy':
        app.logger.warning(f"LM Studio compose failed: {last_error}")
    return ""
def call_lmstudio_keywords(text: str, filename: str):
    """Извлечь короткий список ключевых слов из стенограммы через LM Studio."""
    text = (text or "").strip()
    if not text:
        return []
    text = text[:int(os.getenv("KWS_TEXT_LIMIT", "8000"))]
    system = PROMPTS.get('keywords_system') or (
        "Ты извлекаешь ключевые слова из стенограммы аудио. Верни только JSON-массив строк на русском: "
        "[\"ключ1\", \"ключ2\", ...]. Без пояснений, не более 12 слов/фраз."
    )
    user = f"Файл: {filename}\nСтенограмма:\n{text}"
    last_error: Exception | None = None
    for choice in _llm_iter_choices('keywords'):
        label = _llm_choice_label(choice)
        url = _llm_choice_url(choice)
        if not url:
            app.logger.warning(f"LLM keywords endpoint некорректен ({label}): пустой URL")
            continue
        payload = {
            "model": choice.get('model') or LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.0,
            "max_tokens": 200,
            "top_p": 1.0,
        }
        try:
            r = requests.post(url, headers=_llm_choice_headers(choice), json=payload, timeout=90)
            if _llm_response_indicates_busy(r):
                app.logger.info(f"LLM keywords endpoint занята ({label}), переключаемся")
                last_error = RuntimeError('busy')
                continue
            r.raise_for_status()
            data = r.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            try:
                obj = json.loads(content)
                if isinstance(obj, list):
                    return [str(x) for x in obj][:12]
            except Exception:
                pass
            m = re.search(r"```json\s*(\[.*?\])\s*```", content, flags=re.S)
            if m:
                try:
                    obj = json.loads(m.group(1))
                    if isinstance(obj, list):
                        return [str(x) for x in obj][:12]
                except Exception:
                    pass
            rough = re.split(r"[\n;,]", content)
            res = [w.strip(" \t\r\n-•") for w in rough if w.strip()]
            if res:
                return res[:12]
        except Exception as e:
            last_error = e
            app.logger.warning(f"Извлечение ключевых слов (LLM) не удалось ({label}): {e}")
    if last_error and str(last_error) != 'busy':
        app.logger.warning(f"Извлечение ключевых слов (LLM) не удалось: {last_error}")
    return []

def call_lmstudio_vision(image_path: Path, filename: str):
    """Распознавание и описание изображения через LM Studio (совместимый с OpenAI Vision)."""
    try:
        import base64
    except Exception as e:
        app.logger.warning(f"Визуальное распознавание недоступно: {e}")
        return {}

    mime = "image/png"
    suf = image_path.suffix.lower()
    if suf in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suf in (".webp",):
        mime = "image/webp"
    elif suf in (".bmp",):
        mime = "image/bmp"
    elif suf in (".tif", ".tiff"):
        mime = "image/tiff"
    try:
        raw = image_path.read_bytes()
    except Exception as e:
        app.logger.warning(f"Визуальное распознавание: не удалось прочитать файл {image_path}: {e}")
        return {}
    data_url = f"data:{mime};base64," + base64.b64encode(raw).decode('ascii')

    system = PROMPTS.get('vision_system') or (
        "Ты помощник по анализу изображений. Опиши изображение 2–4 предложениями на русском и верни 5–12 ключевых слов. "
        "Верни строго JSON: {\\\"description\\\":\\\"...\\\", \\\"keywords\\\":[\\\"...\\\"]}."
    )
    user_content = [
        {"type": "text", "text": f"Файл: {filename}. Опиши и укажи ключевые слова."},
        {"type": "image_url", "image_url": {"url": data_url}},
    ]

    def _extract_kws_from_text(txt: str):
        try:
            t = (txt or '').replace('*', ' ')
            m = re.search(r"(?i)ключ[^\n\r:]*?:\s*(.+)", t)
            if m:
                line = m.group(1).strip()
                line = re.split(r"\n|\r|\u2028|\u2029|\s{2,}", line)[0]
                parts = [p.strip(" \t\r\n,.;·•-") for p in line.split(',')]
                parts = [p for p in parts if p]
                cleaned = re.sub(r"(?i)ключ[^\n\r:]*?:.*", "", t)
                return parts[:16], cleaned.strip()
        except Exception:
            return [], txt
        return [], txt

    last_error: Exception | None = None
    for choice in _llm_iter_choices('vision'):
        label = _llm_choice_label(choice)
        url = _llm_choice_url(choice)
        if not url:
            app.logger.warning(f"LLM vision endpoint некорректен ({label}): пустой URL")
            continue
        payload = {
            "model": choice.get('model') or LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2,
            "max_tokens": 500,
            "top_p": 1.0,
        }
        try:
            r = requests.post(url, headers=_llm_choice_headers(choice), json=payload, timeout=180)
            if _llm_response_indicates_busy(r):
                app.logger.info(f"LLM vision endpoint занята ({label}), переключаемся")
                last_error = RuntimeError('busy')
                continue
            r.raise_for_status()
            data = r.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            try:
                obj = json.loads(content)
                if isinstance(obj, dict):
                    kws_list = obj.get("keywords") if isinstance(obj.get("keywords"), list) else []
                    if not kws_list:
                        kws_list, cleaned = _extract_kws_from_text(obj.get("description") or "")
                        if kws_list:
                            obj["description"] = cleaned
                            obj["keywords"] = kws_list
                    if "keywords" in obj and isinstance(obj["keywords"], list):
                        obj["keywords"] = [str(x) for x in obj["keywords"]][:16]
                    return obj
            except Exception:
                pass
            m = re.search(r"```json\s*(\{.*?\})\s*```", content, flags=re.S)
            if m:
                try:
                    obj = json.loads(m.group(1))
                    if isinstance(obj, dict):
                        kws_list = obj.get("keywords") if isinstance(obj.get("keywords"), list) else []
                        if not kws_list:
                            kws_list, cleaned = _extract_kws_from_text(obj.get("description") or "")
                            if kws_list:
                                obj["description"] = cleaned
                                obj["keywords"] = kws_list
                        if "keywords" in obj and isinstance(obj["keywords"], list):
                            obj["keywords"] = [str(x) for x in obj["keywords"]][:16]
                        return obj
                except Exception:
                    pass
            kws_guess, cleaned = _extract_kws_from_text(content or '')
            return {"description": cleaned.strip()[:2000], "keywords": kws_guess}
        except Exception as e:
            last_error = e
            app.logger.warning(f"Визуальное распознавание не удалось ({label}): {e}")
    if last_error and str(last_error) != 'busy':
        app.logger.warning(f"Визуальное распознавание не удалось: {last_error}")
    return {}

def call_lmstudio_for_metadata(text: str, filename: str):
    """
    Вызов OpenAI-совместимого API (LM Studio) для извлечения метаданных.
    Возвращает dict. Терпимо относится к не-JSON ответам: пытается вытащить из ```json ...``` блока.
    """
    text = (text or "")[: int(os.getenv("LLM_TEXT_LIMIT", "15000"))]

    system = PROMPTS.get('metadata_system') or (
        "Ты помощник по каталогизации научных материалов. "
        "Верни ТОЛЬКО валидный JSON без пояснений. "
        "Ключи: material_type, title, author, year, advisor, keywords (array), "
        "novelty (string), literature (array), organizations (array), classification (array)."
    )
    user = f"Файл: {filename}\nФрагмент текста:\n{text}"

    payload_template = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.0,
        "max_tokens": 800,
        "top_p": 1.0,
    }

    last_error: Exception | None = None
    for choice in _llm_iter_choices('metadata'):
        label = _llm_choice_label(choice)
        url = _llm_choice_url(choice)
        if not url:
            app.logger.warning(f"LLM metadata endpoint некорректен ({label}): пустой URL")
            continue
        payload = dict(payload_template)
        payload["model"] = choice.get('model') or LMSTUDIO_MODEL
        headers = _llm_choice_headers(choice)

        max_retries = 3
        backoff = 1.0
        busy = False
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=120)
                if _llm_response_indicates_busy(r):
                    app.logger.info(f"LLM metadata endpoint занята ({label}), переключаемся")
                    busy = True
                    last_error = RuntimeError('busy')
                    break
                text_snippet = (r.text or '')[:2000]
                if r.status_code != 200:
                    app.logger.warning(f"LM Studio HTTP {r.status_code} ({label}, попытка {attempt}): {text_snippet}")
                    r.raise_for_status()
                data = r.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                try:
                    return json.loads(content)
                except Exception:
                    pass
                m = re.search(r"```json\s*(\{.*?\})\s*```", content, flags=re.S)
                if m:
                    try:
                        return json.loads(m.group(1))
                    except Exception:
                        app.logger.warning("Не удалось разобрать JSON внутри блока ```json из LM Studio")
                m = re.search(r"(\{.*\})", content, flags=re.S)
                if m:
                    try:
                        return json.loads(m.group(1))
                    except Exception:
                        app.logger.warning("Не удалось разобрать JSON‑фрагмент из ответа LM Studio")
                app.logger.warning(f"LLM вернул не‑JSON контент ({label}, первые 300 символов): {content[:300]}")
                return {}
            except requests.exceptions.RequestException as e:
                last_error = e
                app.logger.warning(f"Исключение при запросе к LM Studio ({label}, попытка {attempt}): {e}")
                if attempt < max_retries:
                    import time
                    time.sleep(backoff)
                    backoff *= 2
                    continue
            except ValueError as e:
                app.logger.warning(f"LM Studio вернул неверный JSON ({label}, попытка {attempt}): {e}")
                last_error = e
                break
            except Exception as e:
                app.logger.warning(f"Unexpected error calling LM Studio ({label}, попытка {attempt}): {e}")
                last_error = e
                break
        if busy:
            continue
    if last_error and str(last_error) != 'busy':
        app.logger.warning(f"Метаданные через LLM не получены: {last_error}")
    return {}

def upsert_tag(file_obj: File, key: str, value: str):
    key = (key or "").strip()
    value = (value or "").strip()
    if not key or not value:
        return
    t = Tag.query.filter_by(file_id=file_obj.id, key=key, value=value).first()
    if not t:
        t = Tag(file_id=file_obj.id, key=key, value=value)
        db.session.add(t)

def _upsert_keyword_tags(file_obj: File):
    """Разложить строку ключевых слов на отдельные теги 'ключевое слово'."""
    try:
        # Сначала удалим существующие теги ключевых слов, чтобы не плодить дубли
        try:
            Tag.query.filter_by(file_id=file_obj.id).filter(
                or_(Tag.key == 'ключевое слово', Tag.key == 'keywords')
            ).delete(synchronize_session=False)
        except Exception:
            pass
        raw = (file_obj.keywords or '')
        if not raw:
            return
        parts = re.split(r"[\n,;]+", str(raw))
        seen = set()
        for kw in parts:
            w = (kw or '').strip()
            if not w:
                continue
            wl = w.lower()
            if wl in seen:
                continue
            seen.add(wl)
            upsert_tag(file_obj, 'ключевое слово', w)
    except Exception:
        pass

def normalize_material_type(s: str) -> str:
    s = (s or '').strip().lower()
    mapping = {
        'диссертация': 'dissertation',
        'автореферат': 'dissertation_abstract',
        'автореферат диссертации': 'dissertation_abstract',
        'статья': 'article', 'article': 'article', 'paper': 'article',
        'учебник': 'textbook', 'пособие': 'textbook', 'учебное пособие': 'textbook',
        'монография': 'monograph',
        'отчет': 'report', 'отчёт': 'report', 'report': 'report',
        'патент': 'patent', 'patent': 'patent',
        'презентация': 'presentation', 'presentation': 'presentation',
        'тезисы': 'proceedings', 'proceedings': 'proceedings', 'труды': 'proceedings',
        'стандарт': 'standard', 'gost': 'standard', 'gost r': 'standard', 'standard': 'standard',
        'заметки': 'note', 'note': 'note',
        'document': 'document', 'документ': 'document',
        'image': 'image', 'изображение':'image', 'картинка':'image'
    }
    # пытаемся найти прямое соответствие
    if s in mapping:
        return mapping[s]
    # частичные совпадения
    for k, v in mapping.items():
        if k in s:
            return v
    return s or 'document'

def guess_material_type(ext: str, text_excerpt: str, filename: str = "") -> str:
    """Расширенная эвристика типа материала на основе текста/имени файла."""
    tl = (text_excerpt or "").lower()
    fn = (filename or "").lower()
    # Диссертация / автореферат
    if any(k in tl for k in ["диссертац", "на соискание степени", "автореферат диссертац"]):
        return "dissertation_abstract" if "автореферат" in tl else "dissertation"
    # Учебник / пособие
    if any(k in tl for k in ["учебник", "учебное пособ", "пособие", "для студентов"]):
        return "textbook"
    # Статья / журнал / тезисы
    if any(k in tl for k in ["статья", "журнал", "doi", "удк", "тезисы", "материалы конференц"]):
        return "article"
    # Дополнительные типы
    # Монография
    if any(k in tl for k in ["монография", "monograph"]):
        return "monograph"
    # Стандарты (ГОСТ/ISO/IEC/СТО/СП/СанПиН/ТУ)
    if any(k in tl for k in ["гост", "gost", "iso", "iec", "стб", "сто ", " санпин", " сп ", "ту "]):
        return "standard"
    # Материалы конференций
    if any(k in tl for k in ["материалы конференции", "сборник трудов", "proceedings", "conference", "symposium", "workshop"]):
        return "proceedings"
    # Патент
    if any(k in tl for k in ["патент", "patent", "mpk", "ipc"]):
        return "patent"
    # Отчёт / внутренние документы
    if any(k in tl for k in ["отчет", "отчёт", "техническое задание", "пояснительная записка", "technical specification"]):
        return "report"
    # Презентация
    if any(k in tl for k in ["презентация", "slides", "powerpoint", "слайды"]):
        return "presentation"
    # Монография
    if "монограф" in tl:
        return "monograph"
    # Заметка
    if ext in {".md", ".txt"}:
        return "note"
    return "document"

def _detect_type_pre_llm(ext: str, text_excerpt: str, filename: str) -> str | None:
    flow = [p.strip() for p in (TYPE_DETECT_FLOW or '').split(',') if p.strip()]
    ext = (ext or '').lower()
    # вспомогательная функция для предположений по имени файла и разрешения конфликтов
    def _guess_from_filename(fn: str, ex: str) -> str | None:
        fl = (fn or '').lower()
        if not fl:
            return None
        if any(tok in fl for tok in ["автореферат", "autoreferat", "автoref"]):
            return 'dissertation_abstract'
        if any(tok in fl for tok in ["диссер", "dissert", "thesis"]):
            return 'dissertation'
        if any(tok in fl for tok in ["монограф", "monograph"]):
            return 'monograph'
        if any(tok in fl for tok in ["презентац", "slides", "ppt", "pptx", "keynote"]):
            return 'presentation'
        if any(tok in fl for tok in ["патент", "patent", "ru", "wo", "ep"]):
            return 'patent'
        if any(tok in fl for tok in ["материалы_конференции", "proceedings", "conf", "symposium", "workshop"]):
            return 'proceedings'
        if any(tok in fl for tok in ["гост", "gost", "iso", "iec", "санпин", "сто_", "ту_"]):
            return 'standard'
        if any(tok in fl for tok in ["отчет", "отчёт", "tz_", "тз_"]):
            return 'report'
        return None
    def _resolve_conflicts(proposed: str) -> str:
        pl = (proposed or '').strip().lower() or 'document'
        tl = (text_excerpt or '').lower()
        fl = (filename or '').lower()
        std_hints = ["гост", "gost", "iso", "iec", "сто", "санпин", " сп ", "ту "]
        pat_hints = ["патент", "patent", "ipc", "mpk"]
        if pl == 'article' and (any(p in tl for p in std_hints) or any(p in fl for p in std_hints)):
            return 'standard'
        if any(p in tl for p in pat_hints) or any(p in fl for p in pat_hints):
            return 'patent'
        return pl
    # 1) по расширению
    if 'extension' in flow:
        if ext in IMAGE_EXTS:
            return 'image'
        if ext in AUDIO_EXTS:
            return 'audio'
    # подсказки по имени файла до применения эвристик
    if 'filename' in flow:
        ft = _guess_from_filename(filename, ext)
        if ft and ft != 'document':
            return ft
    # 2) эвристики по тексту/имени
    if 'heuristics' in flow:
        t = guess_material_type(ext, text_excerpt, filename)
        t = _resolve_conflicts(t)
        if t and t != 'document':
            return t
    return None

def looks_like_dissertation_filename(fn: str) -> bool:
    fl = (fn or '').lower()
    if not fl:
        return False
    if any(tok in fl for tok in ["автореферат", "autoreferat", "автoref"]):
        return True
    if any(tok in fl for tok in ["диссер", "dissert", "thesis"]):
        return True
    return False

def extract_tags_for_type(material_type: str, text: str, filename: str = "") -> dict:
    """Извлечение тегов по типу материала простыми регулярками.
    Возвращает dict: {key: value}.
    """
    t = (text or "")
    tl = t.lower()
    tags = {}
    # Общие сведения
    # Приблизительная оценка языка: доля кириллицы против латиницы
    try:
        cyr = sum(1 for ch in t if ('а' <= ch.lower() <= 'я') or (ch in 'ёЁ'))
        lat = sum(1 for ch in t if 'a' <= ch.lower() <= 'z')
        if cyr + lat > 20:
            tags.setdefault('lang', 'ru' if cyr >= lat else 'en')
    except Exception:
        pass
    # Распространённые идентификаторы
    # Общие
    # DOI (цифровой идентификатор объекта)
    m = re.search(r"\b(10\.\d{4,9}\/[\w\-\.:;()\/[\]A-Za-z0-9]+)", t)
    if m:
        tags.setdefault("doi", m.group(1))
    # ISBN (международный книжный номер)
    m = re.search(r"\bISBN[:\s]*([0-9\- ]{10,20})", t, flags=re.I)
    if m:
        tags.setdefault("isbn", m.group(1).strip())

    if material_type in ("dissertation", "dissertation_abstract"):
        # Научный руководитель (варианты написания)
        m = re.search(r"научн[ыийо]{1,3}\s*[-–:]?\s*руководител[ья][\s:–]+(.{3,80})", tl, flags=re.I)
        if m:
            tags.setdefault("научный руководитель", m.group(1).strip().title())
        # Специальность/шифр ВАК вида 05.13.11 или 01.02.03
        m = re.search(r"(?:шифр|специальн)[^\n\r]{0,30}?(\d{2}\.\d{2}\.\d{2})", tl)
        if not m:
            m = re.search(r"\b(\d{2}\.\d{2}\.\d{2})\b", tl)
        if m:
            tags.setdefault("специальность", m.group(1))
        # Организация/вуз (ФГБОУ ВО, университет, институт, академия, НИУ, МГУ, СПбГУ)
        m = re.search(r"(фгбоу\s*во|университет|институт|академия|ниу|мгу|спбгу)[^\n\r]{0,100}", tl)
        if m:
            tags.setdefault("организация", m.group(0).strip().title())
        # Кафедра
        m = re.search(r"кафедра\s+([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault("кафедра", m.group(1).strip().title())
        # Степень
        if re.search(r"кандидат[а]?\b", tl):
            tags.setdefault("степень", "кандидат")
        elif re.search(r"доктор[а]?\b", tl):
            tags.setdefault("степень", "доктор")
        # Новизна (короткий фрагмент)
        m = re.search(r"научн[ао]я\s+новизн[аы][^\n\r:]*[:\-]\s*(.{20,300})", tl)
        if m:
            tags.setdefault("научная новизна", m.group(1).strip().capitalize())
        # Цель исследования
        m = re.search(r"цель\s+исследован[ияи][^:\n\r]*[:\-]\s*(.{10,200})", tl)
        if m:
            tags.setdefault("цель", m.group(1).strip().capitalize())
        # Объект/предмет исследования
        m = re.search(r"объект\s+исследован[ияи][^:\n\r]*[:\-]\s*(.{10,200})", tl)
        if m:
            tags.setdefault("объект", m.group(1).strip().capitalize())
        m = re.search(r"предмет\s+исследован[ияи][^:\n\r]*[:\-]\s*(.{10,200})", tl)
        if m:
            tags.setdefault("предмет", m.group(1).strip().capitalize())
        # Задачи исследования
        m = re.search(r"задач[аи]\s+исследован[ияи][^:\n\r]*[:\-]\s*(.{10,200})", tl)
        if m:
            tags.setdefault("задачи", m.group(1).strip().capitalize())
        # Положения, выносимые на защиту
        m = re.search(r"вынос[иы]м[ае][^\n\r]{0,40}на\s+защит[уы][^:\n\r]*[:\-]\s*(.{20,300})", tl)
        if m:
            tags.setdefault("на защиту", m.group(1).strip().capitalize())
    elif material_type == "article":
        # Журнал/Вестник
        m = re.search(r"(журнал|вестник|труды|transactions|journal)[:\s\-]+([^\n\r]{3,80})", tl, flags=re.I)
        if m:
            tags.setdefault("журнал", m.group(2).strip().title())
        # Номер/том/страницы
        m = re.search(r"том\s*(\d+)\b.*?№\s*(\d+)\b.*?с\.?\s*(\d+)[\-–](\d+)", tl)
        if m:
            tags.setdefault("том/номер", f"{m.group(1)}/{m.group(2)}")
            tags.setdefault("страницы", f"{m.group(3)}–{m.group(4)}")
        else:
            m = re.search(r"№\s*(\d+)\b.*?с\.?\s*(\d+)[\-–](\d+)", tl)
            if m:
                tags.setdefault("номер", m.group(1))
                tags.setdefault("страницы", f"{m.group(2)}–{m.group(3)}")
    elif material_type == "textbook":
        # Дисциплина
        m = re.search(r"по\s+дисциплин[еы]\s*[:\-]?\s*([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault("дисциплина", m.group(1).strip().title())
        # Издательство
        m = re.search(r"издательств[оа]\s*[:\-]?\s*([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault("издательство", m.group(1).strip().title())
    elif material_type == "monograph":
        # Издательство/город/год часто в шапке
        m = re.search(r"(издательство|изд.)\s*[:\-]?\s*([^\n\r]{3,80})", tl)
        if m:
            tags.setdefault("издательство", m.group(2).strip().title())

    return tags

def prune_missing_files():
    """Удаляет из БД файлы, которых нет на диске."""
    removed = 0
    for f in File.query.all():
        try:
            if not Path(f.path).exists():
                db.session.delete(f)
                removed += 1
        except Exception:
            db.session.delete(f)
            removed += 1
    db.session.commit()
    return removed

# ------------------- Routes -------------------

@app.route("/")
def index():
    return redirect(url_for('app_react'))

def _serve_spa() -> Response:
    """Отдаёт собранный React UI либо dev-index, если билд отсутствует."""
    try:
        dist = BASE_DIR / 'frontend' / 'dist'
        idx = dist / 'index.html'
        if idx.exists():
            return send_file(str(idx))
    except Exception:
        pass
    dev_idx = BASE_DIR / 'frontend' / 'index.html'
    if dev_idx.exists():
        return send_file(str(dev_idx))
    body = (
        "<div style=\"min-height:100vh;display:flex;align-items:center;justify-content:center;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;"
        "padding:24px;text-align:center;\">"
        "<div style=\"max-width:520px;background:rgba(22,27,34,0.85);padding:32px;border-radius:18px;"
        "border:1px solid #30363d;box-shadow:0 20px 48px rgba(0,0,0,0.45);\">"
        "<h1 style=\"margin-top:0;font-size:22px;\">React-интерфейс не собран</h1>"
        "<p style=\"line-height:1.6;\">В каталоге <code>frontend/dist</code> не найден файл <code>index.html</code>."
        " Соберите клиент командой <code>npm install && npm run build</code> внутри директории <code>frontend</code> "
        "или запустите dev-сервер (<code>npm run dev</code>) и проксируйте его вручную.</p>"
        "</div></div>"
    )
    return _html_page("Agregator — UI не найден", body, status=404)


@app.route("/app")
def app_react():
    """Точка входа для React SPA."""
    return _serve_spa()


@app.route("/app/<path:_path>")
def app_react_catchall(_path: str):
    """Внутренний роутинг React: любые пути внутри /app/*."""
    return _serve_spa()

@app.route('/assets/<path:filename>')
def vite_assets(filename):
    """Serve Vite built assets if present (dist/assets)."""
    dist_assets = BASE_DIR / 'frontend' / 'dist' / 'assets'
    if dist_assets.exists():
        return send_from_directory(str(dist_assets), filename)
    return ("Not found", 404)

@app.route("/aiword")
def aiword_index():
    _ensure_aiword_access()
    """Serve AiWord app (built with Vite) under /aiword with asset path rewrite and pre‑bootstrap.
    Injects a small script to prefill localStorage with BibTeX from this app's DB and LM Studio config.
    """
    dist = BASE_DIR / 'AiWord' / 'dist'
    idx = dist / 'index.html'
    if not idx.exists():
        return ("AiWord/dist/index.html not found. Please build AiWord or include dist.", 404)
    try:
        html = idx.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return send_file(str(idx))

    # Переписываем абсолютные ссылки на ресурсы в /aiword/assets
    html = html.replace('src="/assets/', 'src="/aiword/assets/')
    html = html.replace('href="/assets/', 'href="/aiword/assets/')
    # Переписываем vite.svg, если он упоминается
    html = html.replace('href="/vite.svg', 'href="/aiword/vite.svg')

    # Готовим bootstrap-скрипт для предварительной загрузки BibTeX и настроек ЛМ
    lm_base = os.getenv('LMSTUDIO_API_BASE', 'http://localhost:1234/v1')
    lm_model = os.getenv('LMSTUDIO_MODEL', 'google/gemma-3n-e4b')
    bootstrap = f"""
    <script>
    (function(){{
      try {{
        // Align LM Studio config with Agregator
        localStorage.setItem('llm-writer-base-url', {lm_base!r});
        localStorage.setItem('llm-writer-model', {lm_model!r});
      }} catch(e){{}}
      // Preload bibliography from Agregator DB
      fetch('/api/aiword/bibtex').then(r=>r.ok?r.text():Promise.resolve('')).then(txt=>{{
        if (!txt) return;
        try {{
          var saved = localStorage.getItem('llm-writer-autosave');
          var data = saved ? JSON.parse(saved) : null;
          if (!data || !data.project || !data.content) {{
            data = {{
              project: {{
                title: 'Новая статья',
                language: 'ru',
                style_guide: 'Пиши чётко и структурированно. Без воды. Сохраняй академический тон, «выводы» — коротко и по делу. Используй Markdown: заголовки, списки, таблицы. Формулы как $...$ или ```math``` при необходимости.',
                persona: 'Ты — научный редактор и соавтор. Помогаешь с планом, структурой, стилем, и аргументацией.'
              }},
              content: '# Заголовок\n\nВставьте/пишите текст здесь...',
              bibText: ''
            }};
          }}
          data.bibText = txt;
          localStorage.setItem('llm-writer-autosave', JSON.stringify(data));
        }} catch(e){{}}
      }}).catch(function(){{}});
    }})();
    </script>
    """
    # Вставляем bootstrap перед первым тегом module-скрипта, если он есть
    marker = '<script type="module"'
    if marker in html:
        html = html.replace(marker, bootstrap + "\n" + marker, 1)
    else:
        html = html.replace('</head>', bootstrap + '\n</head>')
    return html

@app.route('/aiword/assets/<path:filename>')
def aiword_assets(filename):
    _ensure_aiword_access()
    dist_assets = BASE_DIR / 'AiWord' / 'dist' / 'assets'
    if dist_assets.exists():
        return send_from_directory(str(dist_assets), filename)
    return ("Not found", 404)

@app.route('/aiword/vite.svg')
def aiword_vite_svg():
    _ensure_aiword_access()
    p = BASE_DIR / 'AiWord' / 'dist' / 'vite.svg'
    if p.exists():
        return send_file(str(p))
    return ("Not found", 404)


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    smart = str(request.args.get('smart', '')).lower() in ('1','true','yes','on')
    material_type = request.args.get("type", "").strip()
    tag_filters = request.args.getlist("tag")

    # Дополнительные необязательные фильтры (обратная совместимость)
    year_from = (request.args.get("year_from") or "").strip()
    year_to = (request.args.get("year_to") or "").strip()
    size_min = (request.args.get("size_min") or "").strip()
    size_max = (request.args.get("size_max") or "").strip()
    collection_filter = _parse_collection_param(request.args.get('collection_id'))

    query = File.query.join(Collection, File.collection_id == Collection.id)
    query = _apply_file_access_filter(query)
    try:
        query = query.filter(Collection.searchable == True)
    except Exception:
        pass
    if collection_filter is not None:
        query = query.filter(File.collection_id == collection_filter)
    if material_type:
        query = query.filter(File.material_type == material_type)
    if q and not smart:
        like = f"%{q}%"
        query = query.filter(or_(
            File.title.ilike(like),
            File.author.ilike(like),
            File.keywords.ilike(like),
            File.filename.ilike(like),
            File.text_excerpt.ilike(like),
        ))
    if year_from:
        query = query.filter(File.year >= year_from)
    if year_to:
        query = query.filter(File.year <= year_to)
    if size_min:
        try:
            query = query.filter(File.size >= int(size_min))
        except Exception:
            pass
    if size_max:
        try:
            query = query.filter(File.size <= int(size_max))
        except Exception:
            pass
    for tf in tag_filters:
        if "=" in tf:
            k, v = tf.split("=", 1)
            t = aliased(Tag)
            query = query.join(t, t.file_id == File.id).filter(and_(t.key == k, t.value.ilike(f"%{v}%")))
    query = query.distinct()

    rows = query.order_by(File.mtime.desc().nullslast()).limit(200).all()
    if q and smart:
        qlem = list(_expand_synonyms(_lemmas(q)))
        def _match(f: File) -> bool:
            lab = set(_lemmas(_row_text_for_search(f)))
            return any(l in lab for l in qlem)
        rows = [r for r in rows if _match(r)]
    return jsonify([{
        "id": r.id,
        "title": r.title,
        "author": r.author,
        "year": r.year,
        "material_type": r.material_type,
        "path": r.path,
        "rel_path": r.rel_path,
        "text_excerpt": r.text_excerpt,
        "abstract": getattr(r, 'abstract', None),
        "tags": [{"key": t.key, "value": t.value} for t in r.tags]
    } for r in rows])

@app.route("/api/search_v2")
def api_search_v2():
    """Расширенный поиск с пагинацией и доп. фильтрами.
    Query: q, type, tag=key=val (multi), year_from, year_to, size_min, size_max, limit, offset
    Returns: { items: [...], total: n }
    """
    q = request.args.get("q", "").strip()
    smart = str(request.args.get('smart', '')).lower() in ('1','true','yes','on')
    material_type = request.args.get("type", "").strip()
    tag_filters = request.args.getlist("tag")
    year_from = (request.args.get("year_from") or "").strip()
    year_to = (request.args.get("year_to") or "").strip()
    size_min = (request.args.get("size_min") or "").strip()
    size_max = (request.args.get("size_max") or "").strip()
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except Exception:
        limit = 50
    try:
        offset = max(int(request.args.get("offset", 0)), 0)
    except Exception:
        offset = 0

    base = File.query.join(Collection, File.collection_id == Collection.id)
    base = _apply_file_access_filter(base)
    collection_filter = _parse_collection_param(request.args.get('collection_id'))
    if collection_filter is not None:
        base = base.filter(File.collection_id == collection_filter)
    try:
        base = base.filter(Collection.searchable == True)
    except Exception:
        pass
    if material_type:
        base = base.filter(File.material_type == material_type)
    if q and not smart:
        like = f"%{q}%"
        base = base.filter(or_(
            File.title.ilike(like),
            File.author.ilike(like),
            File.keywords.ilike(like),
            File.filename.ilike(like),
            File.text_excerpt.ilike(like),
            File.abstract.ilike(like),
        ))
    if year_from:
        base = base.filter(File.year >= year_from)
    if year_to:
        base = base.filter(File.year <= year_to)
    if size_min:
        try:
            base = base.filter(File.size >= int(size_min))
        except Exception:
            pass
    if size_max:
        try:
            base = base.filter(File.size <= int(size_max))
        except Exception:
            pass
    qx = base
    for tf in tag_filters:
        if "=" in tf:
            k, v = tf.split("=", 1)
            if k == 'author':
                qx = qx.filter(File.author.ilike(f"%{v}%"))
            else:
                t = aliased(Tag)
                qx = qx.join(t, t.file_id == File.id).filter(and_(t.key == k, t.value.ilike(f"%{v}%")))
    qx = qx.distinct()

    if q and smart:
        # Морфологическая фильтрация на сервере с синонимами
        qlem = list(_expand_synonyms(_lemmas(q)))
        # Ограничиваем выборку разумным числом для подсчёта, упорядоченным по свежести
        cap = 10000
        cand = qx.order_by(File.mtime.desc().nullslast()).limit(cap).all()
        def _match(f: File) -> bool:
            lab = set(_lemmas(_row_text_for_search(f)))
            return any(l in lab for l in qlem)
        matched = [r for r in cand if _match(r)]
        total = len(matched)
        rows = matched[offset:offset+limit]
    else:
        total = qx.count()
        rows = qx.order_by(File.mtime.desc().nullslast()).offset(offset).limit(limit).all()
    items = [{
        "id": r.id,
        "title": r.title,
        "author": r.author,
        "year": r.year,
        "material_type": r.material_type,
        "path": r.path,
        "rel_path": r.rel_path,
        "text_excerpt": r.text_excerpt,
        "abstract": getattr(r, 'abstract', None),
        "tags": [{"key": t.key, "value": t.value} for t in r.tags]
    } for r in rows]
    return jsonify({"items": items, "total": total})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    """JSON API для чтения/изменения основных настроек UI/сканирования."""
    _require_admin()
    global SCAN_ROOT, EXTRACT_TEXT, LMSTUDIO_API_BASE, LMSTUDIO_MODEL, LMSTUDIO_API_KEY
    global TRANSCRIBE_ENABLED, TRANSCRIBE_BACKEND, TRANSCRIBE_MODEL_PATH, TRANSCRIBE_LANGUAGE
    global SUMMARIZE_AUDIO, AUDIO_KEYWORDS_LLM, IMAGES_VISION_ENABLED
    global KEYWORDS_TO_TAGS_ENABLED, TYPE_DETECT_FLOW, TYPE_LLM_OVERRIDE
    global IMPORT_SUBDIR, MOVE_ON_RENAME, TYPE_DIRS, DEFAULT_USE_LLM, DEFAULT_PRUNE
    global OCR_LANGS_CFG, PDF_OCR_PAGES_CFG, ALWAYS_OCR_FIRST_PAGE_DISSERTATION
    global PROMPTS, AI_RERANK_LLM, COLLECTIONS_IN_SEPARATE_DIRS, COLLECTION_TYPE_SUBDIRS
    if request.method == 'GET':
        # включаем коллекции для интерфейса React
        try:
            cols = Collection.query.order_by(Collection.name.asc()).all()
            # количество на коллекцию (все файлы, без фильтра searchable)
            counts = {}
            try:
                rows = db.session.query(File.collection_id, func.count(File.id)).group_by(File.collection_id).all()
                counts = {cid: int(cnt) for (cid, cnt) in rows}
            except Exception:
                counts = {}
            collections = [{
                'id': c.id,
                'name': c.name,
                'slug': c.slug,
                'searchable': bool(c.searchable),
                'graphable': bool(c.graphable),
                'is_private': bool(getattr(c, 'is_private', False)),
                'count': int(counts.get(c.id, 0)),
            } for c in cols]
        except Exception:
            collections = []
        try:
            llms = LlmEndpoint.query.order_by(LlmEndpoint.created_at.desc()).all()
            llm_items = [{
                'id': ep.id,
                'name': ep.name,
                'base_url': ep.base_url,
                'model': ep.model,
                'weight': float(ep.weight or 0.0),
                'purpose': ep.purpose,
                'purposes': _llm_parse_purposes(ep.purpose),
            } for ep in llms]
        except Exception:
            llm_items = []
        try:
            ai_entries = AiWordAccess.query.join(User, AiWordAccess.user_id == User.id).all()
            ai_users = [{
                'user_id': entry.user_id,
                'username': entry.user.username if entry.user else None,
                'full_name': entry.user.full_name if entry.user else None,
            } for entry in ai_entries]
        except Exception:
            ai_users = []
        return jsonify({
            'scan_root': str(SCAN_ROOT),
            'extract_text': bool(EXTRACT_TEXT),
            'lm_base': LMSTUDIO_API_BASE,
            'lm_model': LMSTUDIO_MODEL,
            'lm_key': LMSTUDIO_API_KEY,
            'transcribe_enabled': bool(TRANSCRIBE_ENABLED),
            'transcribe_backend': TRANSCRIBE_BACKEND,
            'transcribe_model': TRANSCRIBE_MODEL_PATH,
            'transcribe_language': TRANSCRIBE_LANGUAGE,
            'summarize_audio': bool(SUMMARIZE_AUDIO),
            'audio_keywords_llm': bool(AUDIO_KEYWORDS_LLM),
            'vision_images': bool(IMAGES_VISION_ENABLED),
            'kw_to_tags': bool(KEYWORDS_TO_TAGS_ENABLED),
            'type_detect_flow': TYPE_DETECT_FLOW,
            'type_llm_override': bool(TYPE_LLM_OVERRIDE),
            'import_subdir': IMPORT_SUBDIR,
            'move_on_rename': bool(MOVE_ON_RENAME),
            'collections_in_dirs': bool(COLLECTIONS_IN_SEPARATE_DIRS),
            'collection_type_subdirs': bool(COLLECTION_TYPE_SUBDIRS),
            'type_dirs': TYPE_DIRS,
            'ocr_langs': OCR_LANGS_CFG,
            'pdf_ocr_pages': int(PDF_OCR_PAGES_CFG),
            'prompts': PROMPTS,
            'ai_rerank_llm': bool(AI_RERANK_LLM),
            'ocr_first_page_dissertation': bool(ALWAYS_OCR_FIRST_PAGE_DISSERTATION),
            'collections': collections,
            'llm_endpoints': llm_items,
            'llm_purposes': LLM_PURPOSES,
            'aiword_users': ai_users,
            'default_use_llm': bool(DEFAULT_USE_LLM),
            'default_prune': bool(DEFAULT_PRUNE),
        })
    data = request.json or {}
    SCAN_ROOT = Path(data.get('scan_root') or SCAN_ROOT)
    app.config['UPLOAD_FOLDER'] = str(SCAN_ROOT)
    EXTRACT_TEXT = bool(data.get('extract_text', EXTRACT_TEXT))
    LMSTUDIO_API_BASE = data.get('lm_base') or LMSTUDIO_API_BASE
    LMSTUDIO_MODEL = data.get('lm_model') or LMSTUDIO_MODEL
    LMSTUDIO_API_KEY = data.get('lm_key') or LMSTUDIO_API_KEY
    TRANSCRIBE_ENABLED = bool(data.get('transcribe_enabled', TRANSCRIBE_ENABLED))
    TRANSCRIBE_BACKEND = data.get('transcribe_backend') or TRANSCRIBE_BACKEND
    TRANSCRIBE_MODEL_PATH = data.get('transcribe_model') or TRANSCRIBE_MODEL_PATH
    TRANSCRIBE_LANGUAGE = data.get('transcribe_language') or TRANSCRIBE_LANGUAGE
    SUMMARIZE_AUDIO = bool(data.get('summarize_audio', SUMMARIZE_AUDIO))
    AUDIO_KEYWORDS_LLM = bool(data.get('audio_keywords_llm', AUDIO_KEYWORDS_LLM))
    IMAGES_VISION_ENABLED = bool(data.get('vision_images', IMAGES_VISION_ENABLED))
    KEYWORDS_TO_TAGS_ENABLED = bool(data.get('kw_to_tags', KEYWORDS_TO_TAGS_ENABLED))
    TYPE_DETECT_FLOW = data.get('type_detect_flow') or TYPE_DETECT_FLOW
    TYPE_LLM_OVERRIDE = bool(data.get('type_llm_override', TYPE_LLM_OVERRIDE))
    IMPORT_SUBDIR = data.get('import_subdir') or IMPORT_SUBDIR
    MOVE_ON_RENAME = bool(data.get('move_on_rename', MOVE_ON_RENAME))
    COLLECTIONS_IN_SEPARATE_DIRS = bool(data.get('collections_in_dirs', COLLECTIONS_IN_SEPARATE_DIRS))
    COLLECTION_TYPE_SUBDIRS = bool(data.get('collection_type_subdirs', COLLECTION_TYPE_SUBDIRS))
    TYPE_DIRS = data.get('type_dirs') or TYPE_DIRS
    if not COLLECTIONS_IN_SEPARATE_DIRS:
        COLLECTION_TYPE_SUBDIRS = False
    DEFAULT_USE_LLM = bool(data.get('default_use_llm', DEFAULT_USE_LLM))
    DEFAULT_PRUNE = bool(data.get('default_prune', DEFAULT_PRUNE))
    app.config['UPLOAD_FOLDER'] = str(SCAN_ROOT)
    app.config['IMPORT_SUBDIR'] = IMPORT_SUBDIR
    app.config['MOVE_ON_RENAME'] = MOVE_ON_RENAME
    app.config['COLLECTIONS_IN_SEPARATE_DIRS'] = COLLECTIONS_IN_SEPARATE_DIRS
    app.config['COLLECTION_TYPE_SUBDIRS'] = COLLECTION_TYPE_SUBDIRS
    app.config['TYPE_DIRS'] = TYPE_DIRS
    # Настройки OCR (по возможности подменяем на лету)
    ocr_langs = data.get('ocr_langs')
    if ocr_langs is not None:
        OCR_LANGS_CFG = str(ocr_langs)
        os.environ['OCR_LANGS'] = OCR_LANGS_CFG
    try:
        pdf_pages = int(data.get('pdf_ocr_pages', PDF_OCR_PAGES_CFG))
        PDF_OCR_PAGES_CFG = pdf_pages
        os.environ['PDF_OCR_PAGES'] = str(pdf_pages)
    except Exception:
        pass
    try:
        ALWAYS_OCR_FIRST_PAGE_DISSERTATION = bool(data.get('ocr_first_page_dissertation', ALWAYS_OCR_FIRST_PAGE_DISSERTATION))
        os.environ['OCR_DISS_FIRST_PAGE'] = '1' if ALWAYS_OCR_FIRST_PAGE_DISSERTATION else '0'
    except Exception:
        pass
    collections_payload = data.get('collections')
    if isinstance(collections_payload, list):
        try:
            for entry in collections_payload:
                try:
                    cid = int(entry.get('id'))
                except Exception:
                    continue
                col = Collection.query.get(cid)
                if not col:
                    continue
                if 'searchable' in entry:
                    col.searchable = bool(entry['searchable'])
                if 'graphable' in entry:
                    col.graphable = bool(entry['graphable'])
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            app.logger.warning('Не удалось обновить коллекции в настройках: %s', exc)
    # обновление промптов
    try:
        pr = data.get('prompts')
        if isinstance(pr, dict):
            for k, v in pr.items():
                if isinstance(v, str):
                    PROMPTS[k] = v
    except Exception:
        pass
    try:
        AI_RERANK_LLM = bool(data.get('ai_rerank_llm', AI_RERANK_LLM))
    except Exception:
        pass
    aiword_payload = data.get('aiword_users')
    if aiword_payload is not None:
        try:
            AiWordAccess.query.delete()
            db.session.commit()
            added = set()
            current_user = _load_current_user()
            grant_id = current_user.id if current_user else None
            users_list = aiword_payload if isinstance(aiword_payload, list) else []
            for item in users_list:
                try:
                    uid = int(item)
                except Exception:
                    continue
                if uid in added:
                    continue
                u = User.query.get(uid)
                if not u:
                    continue
                db.session.add(AiWordAccess(user_id=uid, granted_by=grant_id))
                added.add(uid)
            db.session.commit()
            try:
                _log_user_action('aiword_access_update', 'aiword', None, detail=json.dumps(sorted(list(added))))
            except Exception:
                pass
        except Exception:
            db.session.rollback()
    _save_runtime_settings_to_disk()
    return jsonify({'ok': True})

@app.route('/api/facets')
def api_facets():
    """Фасеты для текущих фильтров: типы и теги (с учётом выбранных фильтров)."""
    q = request.args.get("q", "").strip()
    material_type = request.args.get("type", "").strip()
    tag_filters = request.args.getlist("tag")
    year_from = (request.args.get("year_from") or "").strip()
    year_to = (request.args.get("year_to") or "").strip()
    size_min = (request.args.get("size_min") or "").strip()
    size_max = (request.args.get("size_max") or "").strip()
    collection_filter = _parse_collection_param(request.args.get('collection_id'))

    base_query = _apply_file_access_filter(File.query)
    try:
        base_query = base_query.join(Collection, File.collection_id == Collection.id).filter(Collection.searchable == True)
    except Exception:
        pass
    if collection_filter is not None:
        base_query = base_query.filter(File.collection_id == collection_filter)
    if material_type:
        base_query = base_query.filter(File.material_type == material_type)
    if q:
        like = f"%{q}%"
        base_query = base_query.filter(or_(
            File.title.ilike(like),
            File.author.ilike(like),
            File.keywords.ilike(like),
            File.filename.ilike(like),
            File.text_excerpt.ilike(like),
        ))
    if year_from:
        base_query = base_query.filter(File.year >= year_from)
    if year_to:
        base_query = base_query.filter(File.year <= year_to)
    if size_min:
        try:
            base_query = base_query.filter(File.size >= int(size_min))
        except Exception:
            pass
    if size_max:
        try:
            base_query = base_query.filter(File.size <= int(size_max))
        except Exception:
            pass

    # Фасет типов (независим от фильтров тегов)
    types = db.session.query(File.material_type, func.count(File.id))
    types = types.filter(File.id.in_(base_query.with_entities(File.id)))
    types = types.group_by(File.material_type).all()
    types_facet = [[mt, cnt] for (mt, cnt) in types]

    # Фасеты тегов: по каждому ключу, присутствующему в базовой выборке
    base_ids_subq = base_query.with_entities(File.id).subquery()
    base_keys = [row[0] for row in db.session.query(Tag.key).filter(Tag.file_id.in_(base_ids_subq)).distinct().all()]
    selected = {}
    for tf in tag_filters:
        if '=' in tf:
            k, v = tf.split('=', 1)
            selected.setdefault(k, []).append(v)
            if k not in base_keys:
                base_keys.append(k)

    tag_facets = {}
    for key in base_keys:
        qk = base_query
        for tf in tag_filters:
            if '=' not in tf:
                continue
            k, v = tf.split('=', 1)
            if k == key:
                continue
            tk = aliased(Tag)
            qk = qk.join(tk, tk.file_id == File.id).filter(and_(tk.key == k, tk.value.ilike(f"%{v}%")))
        ids_subq = qk.with_entities(File.id).distinct().subquery()
        rows = db.session.query(Tag.value, func.count(Tag.id)) \
            .filter(and_(Tag.file_id.in_(ids_subq), Tag.key == key)) \
            .group_by(Tag.value) \
            .order_by(func.count(Tag.id).desc()) \
            .all()
        tag_facets[key] = [[val, cnt] for (val, cnt) in rows]
        if key in selected:
            present_vals = {val for (val, _c) in tag_facets[key]}
            for v in selected[key]:
                if v not in present_vals:
                    tag_facets[key].append([v, 0])

    return jsonify({"types": types_facet, "tag_facets": tag_facets})

@app.route("/settings", methods=["GET"])
def settings_redirect():
    """Совместимость: перенаправить на новый интерфейс настроек."""
    return redirect('/app/settings')

@app.route('/settings/collections', methods=['POST'])
def settings_collections():
    # Обновляем флаги у существующих коллекций и при необходимости добавляем новую
    try:
        cols = Collection.query.all()
        # обновляем флаги
        for c in cols:
            s_key = f'search_{c.id}'
            g_key = f'graph_{c.id}'
            c.searchable = (request.form.get(s_key) == 'on')
            c.graphable = (request.form.get(g_key) == 'on')
        new_name = (request.form.get('new_name') or '').strip()
        if new_name:
            slug = _slugify(new_name)
            exists = Collection.query.filter((Collection.slug==slug) | (Collection.name==new_name)).first()
            if not exists:
                db.session.add(Collection(name=new_name, slug=slug, searchable=True, graphable=True))
        db.session.commit()
        return jsonify({"status": "ok"})
    except Exception as e:
        db.session.rollback()
        return json_error(f'Ошибка обновления коллекций: {e}', 500)

@app.route('/admin/backup-db', methods=['POST'])
@require_admin
def backup_db():
    # Создаём и отправляем резервную копию SQLite с отметкой времени
    try:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        src = BASE_DIR / 'catalogue.db'
        if not src.exists():
            return json_error('Файл базы не найден', 404)
        bdir = BASE_DIR / 'backups'
        bdir.mkdir(exist_ok=True)
        dst = bdir / f'catalogue_{ts}.db'
        import shutil
        shutil.copy2(src, dst)
        return send_from_directory(bdir, dst.name, as_attachment=True)
    except Exception as e:
        return json_error(f'Ошибка резервного копирования: {e}', 500)

@app.route('/admin/clear-db', methods=['POST'])
@require_admin
def clear_db():
    # Опасно: удалить все записи из основных таблиц
    try:
        Tag.query.delete()
        ChangeLog.query.delete()
        File.query.delete()
        db.session.commit()
        # очищаем статические кэши (текстовые фрагменты и миниатюры)
        try:
            static_dir = Path(app.static_folder)
            txt_cache = static_dir / 'cache' / 'text_excerpts'
            if txt_cache.exists():
                for fp in txt_cache.glob('*.txt'):
                    try: fp.unlink()
                    except Exception: pass
            thumbs = static_dir / 'thumbnails'
            if thumbs.exists():
                for fp in thumbs.glob('*.png'):
                    try: fp.unlink()
                    except Exception: pass
        except Exception:
            pass
        return jsonify({"status": "ok"})
    except Exception as e:
        db.session.rollback()
        return json_error(f'Ошибка очистки: {e}', 500)

@app.route('/admin/import-db', methods=['POST'])
@require_admin
def import_db():
    """Replace current SQLite database with uploaded file.
    Creates an automatic timestamped backup of the current DB before replacing.
    """
    file = request.files.get('dbfile')
    if not file or not file.filename:
        return json_error('Файл базы не выбран', 400)
    filename = secure_filename(file.filename)
    if not filename.lower().endswith('.db'):
        return json_error('Ожидался файл .db (SQLite)', 400)

    try:
        head = file.stream.read(16)
        if head[:15] != b'SQLite format 3':
            file.stream.seek(0)
            return json_error('Файл не похож на SQLite базу', 400)
    except Exception:
        pass
    finally:
        try:
            file.stream.seek(0)
        except Exception:
            pass

    tmp = None
    try:
        dst = BASE_DIR / 'catalogue.db'
        if dst.exists():
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            bdir = BASE_DIR / 'backups'
            bdir.mkdir(exist_ok=True)
            backup_path = bdir / f'catalogue_before_import_{ts}.db'
            import shutil
            shutil.copy2(dst, backup_path)

        tmp = BASE_DIR / f'.upload_import_tmp_{os.getpid()}_{int(time.time())}.db'
        file.save(tmp)

        # Проверка схемы
        try:
            con = sqlite3.connect(str(tmp))
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {r[0] for r in cur.fetchall()}
            required = {'files', 'tags', 'tag_schemas', 'changelog'}
            missing = required - tables
            con.close()
            if missing:
                try:
                    tmp.unlink()
                except Exception:
                    pass
                return json_error('В файле базы нет требуемых таблиц: ' + ', '.join(sorted(missing)), 400)
        except Exception as e:
            if tmp and tmp.exists():
                tmp.unlink()
            return json_error(f'Не удалось проверить схему базы: {e}', 500)

        # Убеждаемся, что SQLAlchemy освобождает файловые дескрипторы
        try:
            db.session.close()
            db.session.remove()
        except Exception:
            pass
        try:
            db.engine.dispose()
        except Exception:
            pass

        import shutil
        shutil.move(str(tmp), str(dst))
        tmp = None

        try:
            static_dir = Path(app.static_folder)
            txt_cache = static_dir / 'cache' / 'text_excerpts'
            if txt_cache.exists():
                for fp in txt_cache.glob('*.txt'):
                    try: fp.unlink()
                    except Exception: pass
            thumbs = static_dir / 'thumbnails'
            if thumbs.exists():
                for fp in thumbs.glob('*.png'):
                    try: fp.unlink()
                    except Exception: pass
        except Exception:
            pass

        return jsonify({"status": "ok"})
    except Exception as e:
        if tmp and tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
        return json_error(f'Ошибка импорта базы: {e}', 500)


def _task_to_dict(task: TaskRecord) -> dict:
    return {
        'id': task.id,
        'name': task.name,
        'status': task.status,
        'progress': float(task.progress or 0.0),
        'payload': task.payload,
        'created_at': task.created_at.isoformat() if task.created_at else None,
        'started_at': task.started_at.isoformat() if task.started_at else None,
        'finished_at': task.finished_at.isoformat() if task.finished_at else None,
        'error': task.error,
    }


def _cleanup_old_tasks() -> None:
    cutoff = datetime.utcnow() - TASK_RETENTION_WINDOW
    try:
        deleted = TaskRecord.query.filter(
            TaskRecord.status.in_(TASK_FINAL_STATUSES),
            TaskRecord.created_at.isnot(None),
            TaskRecord.created_at < cutoff
        ).delete(synchronize_session=False)
        db.session.commit()
        if deleted:
            app.logger.info(f"[tasks] removed {deleted} tasks older than {cutoff.isoformat()} (final statuses)")
    except Exception as exc:
        db.session.rollback()
        app.logger.warning(f"[tasks] cleanup failed: {exc}")


@app.route('/api/admin/tasks', methods=['GET'])
@require_admin
def api_admin_tasks():
    try:
        limit = int(request.args.get('limit', '50'))
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))
    _cleanup_old_tasks()
    cutoff = datetime.utcnow() - TASK_RETENTION_WINDOW
    tasks = TaskRecord.query.order_by(TaskRecord.created_at.desc()) \
        .filter(
            or_(
                TaskRecord.status.notin_(TASK_FINAL_STATUSES),
                TaskRecord.status.is_(None),
                TaskRecord.created_at.is_(None),
                TaskRecord.created_at >= cutoff
            )
        ).limit(limit).all()
    payload = [_task_to_dict(t) for t in tasks]
    if SCAN_PROGRESS.get('running'):
        progress = 0.0
        total = float(SCAN_PROGRESS.get('total') or 0)
        processed = float(SCAN_PROGRESS.get('processed') or 0)
        if total > 0:
            progress = min(1.0, processed / total)
        active_id = SCAN_TASK_ID or 0
        if not any(t.get('id') == active_id for t in payload):
            payload.insert(0, {
                'id': active_id,
                'name': 'scan',
                'status': 'running',
                'progress': progress,
                'payload': json.dumps({
                    'stage': SCAN_PROGRESS.get('stage'),
                    'current': SCAN_PROGRESS.get('current'),
                }),
                'created_at': None,
                'started_at': None,
                'finished_at': None,
                'error': SCAN_PROGRESS.get('error'),
            })
    return jsonify({'ok': True, 'tasks': payload})


@app.route('/api/admin/tasks/<int:task_id>', methods=['PATCH', 'DELETE'])
@require_admin
def api_admin_task_detail(task_id: int):
    task = TaskRecord.query.get_or_404(task_id)
    if request.method == 'DELETE':
        db.session.delete(task)
        db.session.commit()
        return jsonify({'ok': True})
    data = request.get_json(silent=True) or {}
    status = str(data.get('status') or '').strip().lower()
    if status == 'cancel':
        if task.name == 'scan' and SCAN_PROGRESS.get('running') and SCAN_TASK_ID == task.id:
            scan_cancel()
            return jsonify({'ok': True, 'task': _task_to_dict(task)})
        task.status = 'cancelled'
        db.session.commit()
        return jsonify({'ok': True, 'task': _task_to_dict(task)})
    if status:
        task.status = status
    progress = data.get('progress')
    if progress is not None:
        try:
            task.progress = float(progress)
        except Exception:
            pass
    db.session.commit()
    return jsonify({'ok': True, 'task': _task_to_dict(task)})


@app.route('/api/admin/actions', methods=['GET', 'DELETE'])
@require_admin
def api_admin_actions():
    if request.method == 'DELETE':
        payload = request.get_json(silent=True) or {}
        before_raw = str(request.args.get('before') or payload.get('before') or '').strip()
        if not before_raw:
            return jsonify({'ok': False, 'error': 'parameter "before" is required'}), 400
        try:
            if len(before_raw) <= 10:
                before_dt = datetime.strptime(before_raw, '%Y-%m-%d') + timedelta(days=1)
            else:
                before_dt = datetime.fromisoformat(before_raw)
        except Exception:
            return jsonify({'ok': False, 'error': 'invalid datetime format'}), 400
        try:
            deleted = UserActionLog.query.filter(UserActionLog.created_at < before_dt).delete(synchronize_session=False)
            db.session.commit()
            app.logger.info(f"[actions] deleted {deleted} log records older than {before_dt.isoformat()}")
            return jsonify({'ok': True, 'deleted': deleted})
        except Exception as exc:
            db.session.rollback()
            app.logger.warning(f"[actions] failed to delete logs: {exc}")
            return jsonify({'ok': False, 'error': 'delete_failed'}), 500

    try:
        limit = int(request.args.get('limit', '100'))
    except Exception:
        limit = 100
    limit = max(1, min(limit, 500))
    q = UserActionLog.query.order_by(UserActionLog.created_at.desc())
    user_id = request.args.get('user_id')
    if user_id:
        try:
            q = q.filter(UserActionLog.user_id == int(user_id))
        except Exception:
            pass
    action = request.args.get('action')
    if action:
        q = q.filter(UserActionLog.action == action)
    logs = q.limit(limit).all()
    return jsonify({'ok': True, 'actions': [
        {
            'id': log.id,
            'user_id': log.user_id,
            'username': log.user.username if getattr(log, 'user', None) else None,
            'full_name': log.user.full_name if getattr(log, 'user', None) else None,
            'action': log.action,
            'entity': log.entity,
            'entity_id': log.entity_id,
            'detail': log.detail,
            'created_at': log.created_at.isoformat() if log.created_at else None,
        }
        for log in logs
    ]})


@app.route('/api/admin/llm-endpoints', methods=['GET', 'POST'])
@require_admin
def api_admin_llm_endpoints():
    if request.method == 'GET':
        eps = LlmEndpoint.query.order_by(LlmEndpoint.created_at.desc()).all()
        return jsonify({'ok': True, 'items': [
            {
                'id': ep.id,
                'name': ep.name,
                'base_url': ep.base_url,
                'model': ep.model,
                'weight': float(ep.weight or 0.0),
                'purpose': ep.purpose,
                'purposes': _llm_parse_purposes(ep.purpose),
                'created_at': ep.created_at.isoformat() if ep.created_at else None,
            }
            for ep in eps
        ], 'purposes_catalog': LLM_PURPOSES})
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    base_url = (data.get('base_url') or '').strip()
    model = (data.get('model') or '').strip()
    if not name or not base_url or not model:
        return jsonify({'ok': False, 'error': 'name, base_url и model обязательны'}), 400
    normalized_purpose = _llm_normalize_purposes(data.get('purposes') if 'purposes' in data else data.get('purpose'))
    ep = LlmEndpoint(
        name=name,
        base_url=base_url,
        model=model,
        api_key=(data.get('api_key') or '').strip() or None,
        weight=float(data.get('weight') or 1.0),
        purpose=normalized_purpose,
    )
    db.session.add(ep)
    db.session.commit()
    _invalidate_llm_cache()
    _log_user_action(_load_current_user(), 'llm_add', 'llm_endpoint', ep.id, detail=json.dumps({'name': name, 'purpose': ep.purpose}))
    return jsonify({'ok': True, 'item': {
        'id': ep.id,
        'name': ep.name,
        'base_url': ep.base_url,
        'model': ep.model,
        'weight': float(ep.weight or 0.0),
        'purpose': ep.purpose,
        'purposes': _llm_parse_purposes(ep.purpose),
        'created_at': ep.created_at.isoformat() if ep.created_at else None,
    }}), 201


@app.route('/api/admin/llm-endpoints/<int:endpoint_id>', methods=['PATCH', 'DELETE'])
@require_admin
def api_admin_llm_endpoint_detail(endpoint_id: int):
    ep = LlmEndpoint.query.get_or_404(endpoint_id)
    if request.method == 'DELETE':
        db.session.delete(ep)
        db.session.commit()
        _invalidate_llm_cache()
        _log_user_action(_load_current_user(), 'llm_delete', 'llm_endpoint', endpoint_id)
        return jsonify({'ok': True})
    data = request.get_json(silent=True) or {}
    updated = False
    if 'name' in data:
        ep.name = str(data['name']).strip() or ep.name
        updated = True
    if 'base_url' in data:
        ep.base_url = str(data['base_url']).strip() or ep.base_url
        updated = True
    if 'model' in data:
        ep.model = str(data['model']).strip() or ep.model
        updated = True
    if 'api_key' in data:
        ep.api_key = str(data['api_key']).strip() or None
        updated = True
    if 'weight' in data:
        try:
            ep.weight = float(data['weight'])
            updated = True
        except Exception:
            pass
    if 'purposes' in data:
        ep.purpose = _llm_normalize_purposes(data.get('purposes'))
        updated = True
    elif 'purpose' in data:
        ep.purpose = _llm_normalize_purposes(data.get('purpose'))
        updated = True
    if updated:
        db.session.commit()
        _invalidate_llm_cache()
        _log_user_action(_load_current_user(), 'llm_update', 'llm_endpoint', ep.id)
    return jsonify({'ok': True, 'item': {
        'id': ep.id,
        'name': ep.name,
        'base_url': ep.base_url,
        'model': ep.model,
        'weight': float(ep.weight or 0.0),
        'purpose': ep.purpose,
        'purposes': _llm_parse_purposes(ep.purpose),
        'created_at': ep.created_at.isoformat() if ep.created_at else None,
    }})


# ------------------- CLI helpers -------------------

# ------------------- Diagnostics -------------------
@app.route('/diagnostics/transcribe')
def diag_transcribe():
    """Диагностика транскрибации одного файла.
    Параметры:
      - path: абсолютный или относительный к SCAN_ROOT путь
      - file_id: альтернатива path, взять файл из БД по id
      - limit: ограничение символов транскрипта (по умолчанию 1000)
    Возвращает JSON: состояние бэкенда/модели, сведения о файле и образец транскрипта.
    """
    res = {
        "transcribe_enabled": bool(TRANSCRIBE_ENABLED),
        "backend": (TRANSCRIBE_BACKEND or '').lower(),
        "language": TRANSCRIBE_LANGUAGE,
        "model_path": TRANSCRIBE_MODEL_PATH,
        "model_exists": False,
        "ffmpeg": _ffmpeg_available(),
        "ffprobe": shutil.which('ffprobe') is not None,
        "file": {},
        "transcript": {
            "ok": False,
            "len": 0,
            "sample": "",
        },
        "warnings": [],
        "faster_whisper": {},
        "applied": {},
    }
    try:
        mp = TRANSCRIBE_MODEL_PATH or ''
        try:
            res["model_exists"] = bool(mp) and Path(mp).exists()
        except Exception:
            res["model_exists"] = False

        # Детали разрешения модели faster-whisper (опционально)
        try:
            fw = {
                "hf_available": bool(hf_snapshot_download),
                "cache_dir": str(FW_CACHE_DIR),
                "model_ref": (request.args.get('model') or TRANSCRIBE_MODEL_PATH or '').strip(),
                "repo": None,
                "target_dir": None,
                "target_exists": None,
                "downloaded": False,
            }
            mref = fw["model_ref"]
            if mref:
                # Если локальный каталог существует, считаем путь найденным
                p = Path(mref).expanduser()
                if p.exists() and p.is_dir():
                    fw["target_dir"] = str(p)
                    fw["target_exists"] = True
                else:
                    repo = _fw_alias_to_repo(mref) or (mref if '/' in mref else None)
                    fw["repo"] = repo
                    if repo:
                        safe_name = repo.replace('/', '__')
                        target_dir = FW_CACHE_DIR / safe_name
                        fw["target_dir"] = str(target_dir)
                        fw["target_exists"] = target_dir.exists() and any(target_dir.iterdir())
                        # Необязательная загрузка, даже если бэкенд сейчас не faster-whisper
                        if (request.args.get('download') or '').lower() in ('1','true','yes','on'):
                            if hf_snapshot_download is None:
                                res["warnings"].append("Пакет huggingface_hub не установлен — автозагрузка невозможна")
                            else:
                                FW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                                hf_snapshot_download(repo_id=repo, local_dir=str(target_dir), local_dir_use_symlinks=False, revision=None)
                                fw["downloaded"] = True
                                fw["target_exists"] = target_dir.exists() and any(target_dir.iterdir())
                    else:
                        res["warnings"].append("Неизвестная ссылка на модель faster-whisper (не каталог, не алиас и не repo id)")
            res["faster_whisper"] = fw
        except Exception as e:
            res["warnings"].append(f"fw_resolve:{e}")

        # Переопределения (backend/lang/vad/model) из query-параметров
        eff_backend = (request.args.get('backend') or '').strip().lower() or None
        eff_lang = (request.args.get('lang') or '').strip() or None
        vad_param = (request.args.get('vad') or '').strip().lower()
        if vad_param in ('1','true','yes','on'): eff_vad = True
        elif vad_param in ('0','false','no','off'): eff_vad = False
        else: eff_vad = None
        model_override = (request.args.get('model') or '').strip() or None
        res["applied"] = {"backend": eff_backend, "lang": eff_lang, "vad": eff_vad, "model": model_override}

        # Нормализация backend: поддерживаем только faster-whisper
        if eff_backend and eff_backend != 'faster-whisper':
            res["warnings"].append("Бэкенд не поддерживается (Vosk удалён); используются настройки faster-whisper")
            eff_backend = None

        # Разрешение пути к файлу
        p = None
        q_path = (request.args.get('path') or '').strip()
        q_id = (request.args.get('file_id') or '').strip()
        if q_path:
            pp = Path(q_path)
            if not pp.is_absolute():
                root = Path(SCAN_ROOT)
                # Пробуем типовые варианты, чтобы избежать дублирования корневой папки в пути
                candidates = [
                    root / q_path,
                    root.parent / q_path,
                ]
                picked = None
                for cand in candidates:
                    try:
                        if cand.exists():
                            picked = cand
                            break
                    except Exception:
                        pass
                pp = picked or (root / q_path)
            p = pp
        elif q_id:
            try:
                f = File.query.get(int(q_id))
                if f: p = Path(f.path)
            except Exception:
                p = None
        # Разрешить диагностику только модели (без файла)
        if not p:
            return jsonify(res)

        # метаданные файла
        file_info = {
            "requested": q_path or q_id,
            "path": str(p) if p else None,
            "exists": bool(p and p.exists()),
            "ext": (p.suffix.lower() if p else None),
            "size": (p.stat().st_size if p and p.exists() else None),
        }
        if p and p.exists():
            try:
                file_info["duration_seconds"] = _ffprobe_duration_seconds(p)
                file_info["duration_str"] = audio_duration_hhmmss(p)
            except Exception:
                file_info["duration_seconds"] = None
                file_info["duration_str"] = None
        res["file"] = file_info

        # Выполнить транскрибацию при наличии файла и разрешённых настройках (для аудио)
        if not TRANSCRIBE_ENABLED and eff_backend is None:
            res["warnings"].append("TRANSCRIBE_ENABLED is off")
            return jsonify(res)
        if not (p and p.exists() and p.is_file()):
            return jsonify(res)

        limit = 0
        try:
            limit = int(request.args.get('limit', '1000'))
        except Exception:
            limit = 1000
        limit = max(200, min(20000, limit or 1000))

        try:
            if p.suffix.lower() in AUDIO_EXTS:
                tx = transcribe_audio(p, limit_chars=limit,
                                      backend_override=eff_backend,
                                      model_path_override=model_override,
                                      lang_override=eff_lang,
                                      vad_override=eff_vad)
                sample = (tx or '')[:200]
                res["transcript"].update({"ok": bool(tx), "len": len(tx or ''), "sample": sample})
            elif p.suffix.lower() in IMAGE_EXTS and IMAGES_VISION_ENABLED:
                vis = call_lmstudio_vision(p, p.name)
                desc = (vis.get('description') or '') if isinstance(vis, dict) else ''
                sample = desc[:200]
                res["transcript"].update({"ok": bool(sample), "len": len(desc), "sample": sample})
            else:
                res["warnings"].append("Файл не аудио/изображение или распознавание изображений выключено")
        except Exception as e:
            res["warnings"].append(f"transcribe_error:{e}")
        return jsonify(res)
    except Exception as e:
        res["warnings"].append(f"unexpected:{e}")
        return jsonify(res), 500

# ------------------- Statistics & Visualization -------------------
from collections import Counter, defaultdict

@app.route("/api/stats")
def api_stats():
    # Агрегация по авторам, годам, типам материалов
    try:
        files = File.query.join(Collection, File.collection_id == Collection.id).filter(Collection.searchable == True).all()
    except Exception:
        files = File.query.all()
    authors = Counter()
    years = Counter()
    types = Counter()
    exts = Counter()
    size_buckets = Counter()
    months = Counter()
    # новые распределения
    weekdays = Counter()  # 0..6 (Mon..Sun)
    hours = Counter()     # 0..23
    # дополнительные агрегации
    kw = Counter()
    tag_keys = Counter()
    tag_values = Counter()
    total_files = 0
    total_size = 0
    # средний размер по типам
    size_sum_by_type = Counter()
    size_cnt_by_type = Counter()
    # заполненность ключевых полей
    meta_presence = Counter()
    def bucket_size(sz):
        if sz is None or sz <= 0:
            return "неизв."
        mb = sz / (1024*1024)
        if mb < 1: return "< 1 МБ"
        if mb < 10: return "1–10 МБ"
        if mb < 50: return "10–50 МБ"
        if mb < 100: return "50–100 МБ"
        return "> 100 МБ"
    for f in files:
        total_files += 1
        try:
            total_size += int(f.size or 0)
        except Exception:
            pass
        if f.author:
            authors[f.author] += 1
        if f.year:
            years[f.year] += 1
        if f.material_type:
            types[f.material_type] += 1
        if f.ext:
            exts[f.ext.lower().lstrip('.')] += 1
        size_buckets[bucket_size(f.size or 0)] += 1
        if f.mtime:
            try:
                from datetime import datetime, timedelta
                d = datetime.fromtimestamp(f.mtime)
                months[d.strftime('%Y-%m')] += 1
                weekdays[d.weekday()] += 1  # Понедельник=0
                hours[d.hour] += 1
            except Exception:
                pass
        # ключевые слова
        if f.keywords:
            for part in re.split(r"[\n,;]+", f.keywords):
                w = (part or '').strip()
                if w:
                    kw[w] += 1
        # ключи тегов
        try:
            for t in f.tags:
                if t.key:
                    tag_keys[t.key] += 1
                if t.value:
                    tag_values[str(t.value).strip()] += 1
        except Exception:
            pass
        # средний размер по типам
        if f.material_type and (f.size or 0) > 0:
            size_sum_by_type[f.material_type] += int(f.size or 0)
            size_cnt_by_type[f.material_type] += 1
        # заполненность полей
        if f.title:
            meta_presence['Название'] += 1
        if f.author:
            meta_presence['Автор'] += 1
        if f.year:
            meta_presence['Год'] += 1
        if f.keywords:
            meta_presence['Ключевые слова'] += 1
        try:
            if f.tags and len(f.tags) > 0:
                meta_presence['Теги'] += 1
        except Exception:
            pass
    # подготовка выходных структур
    # средний размер по типам (в МБ, округляем до десятых)
    avg_size_type = []
    for mt in size_sum_by_type.keys():
        cnt = max(1, size_cnt_by_type[mt])
        avg_mb = (size_sum_by_type[mt] / cnt) / (1024*1024)
        avg_size_type.append((mt, round(avg_mb, 1)))
    avg_size_type.sort(key=lambda x: x[1], reverse=True)
    # недели: упорядочим Пн..Вс
    weekday_names = ['Пн','Вт','Ср','Чт','Пт','Сб','Вс']
    weekdays_list = []
    for i in range(7):
        weekdays_list.append((weekday_names[i], int(weekdays.get(i, 0))))
    # часы 0..23
    hours_list = [(str(h), int(hours.get(h, 0))) for h in range(24)]
    # коллекции: количество файлов по коллекциям (searchable)
    collections_counts = []
    try:
        q = db.session.query(Collection.name, func.count(File.id)) \
            .join(File, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True) \
            .group_by(Collection.id) \
            .order_by(func.count(File.id).desc())
        collections_counts = [(name, int(cnt)) for (name, cnt) in q.all()]
    except Exception:
        pass
    # общий размер по коллекциям (только searchable)
    collections_total_size = []
    try:
        rows = db.session.query(Collection.name, func.coalesce(func.sum(File.size), 0)) \
            .join(File, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True) \
            .group_by(Collection.id) \
            .order_by(func.coalesce(func.sum(File.size), 0).desc()) \
            .all()
        collections_total_size = [(name, int(total or 0)) for (name, total) in rows]
    except Exception:
        pass

    # самые крупные файлы (только searchable)
    largest_files = []
    try:
        rows = db.session.query(File.filename, File.size) \
            .join(Collection, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True) \
            .order_by(File.size.desc().nullslast()) \
            .limit(15).all()
        largest_files = [(fn or '(без имени)', int(sz or 0)) for (fn, sz) in rows]
    except Exception:
        pass

    return jsonify({
        "authors": authors.most_common(20),
        "authors_cloud": authors.most_common(100),
        "years": sorted(years.items()),
        "types": types.most_common(),
        "exts": exts.most_common(),
        "sizes": sorted(size_buckets.items(), key=lambda x: ["неизв.","< 1 МБ","1–10 МБ","10–50 МБ","50–100 МБ","> 100 МБ"].index(x[0]) if x[0] in ["неизв.","< 1 МБ","1–10 МБ","10–50 МБ","50–100 МБ","> 100 МБ"] else 999),
        "months": sorted(months.items()),
        "top_keywords": kw.most_common(30),
        "tag_keys": tag_keys.most_common(30),
        "tag_values_cloud": tag_values.most_common(150),
        "weekdays": weekdays_list,
        "hours": hours_list,
        "avg_size_type": avg_size_type,
        "meta_presence": sorted(meta_presence.items(), key=lambda x: x[0]),
        "total_files": total_files,
        "total_size_bytes": total_size,
        "collections_counts": collections_counts,
        "collections_total_size": collections_total_size,
        "largest_files": largest_files,
    })

@app.route('/api/stats/tag-values')
def api_stats_tag_values():
    """Возвращает топ значений для заданного ключа тега.
    Query: key, limit=30
    Учитывает только коллекции с searchable=true.
    """
    key = (request.args.get('key') or '').strip()
    if not key:
        return jsonify({"items": []})
    try:
        try:
            lim = min(max(int(request.args.get('limit', '30')), 1), 200)
        except Exception:
            lim = 30
        rows = db.session.query(Tag.value, func.count(Tag.id)) \
            .join(File, File.id == Tag.file_id) \
            .join(Collection, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True) \
            .filter(Tag.key == key) \
            .group_by(Tag.value) \
            .order_by(func.count(Tag.id).desc()) \
            .limit(lim).all()
        return jsonify({"items": [[v or '', int(c or 0)] for (v, c) in rows]})
    except Exception as e:
        return jsonify({"items": [], "error": str(e)}), 500

@app.route("/stats")
def stats_redirect():
    return redirect('/app/stats')


@app.route('/graph')
def graph_redirect():
    return redirect('/app/graph')

@app.cli.command("init-db")
def init_db():
    db.create_all()
    # Заполняем или обновляем TagSchema известными ключами (идемпотентно)
    seeds = [
            ("dissertation", "научный руководитель", "ФИО научного руководителя"),
            ("dissertation", "специальность", "Код/направление ВАК"),
            ("dissertation", "организация", "Базовая организация / вуз"),
            ("dissertation", "степень", "Кандидат / Доктор"),
            ("textbook", "дисциплина", "Учебная дисциплина"),
            ("textbook", "издательство", "Издательство"),
            ("article", "журнал", "Журнал / сборник"),
            ("article", "номер", "Номер выпуска"),
            ("article", "страницы", "Диапазон страниц"),
            ("article", "doi", "Digital Object Identifier"),
            ("monograph", "издательство", "Издательство"),
            ("any", "isbn", "Международный стандартный номер книги"),
        ]
    # Дополняем дополнительными ключами для новой таксономии тегов
    seeds += [
            ("any", "lang", "Язык текста (ru/en/...)"),
            ("any", "ext", "Расширение файла (без точки)"),
            ("any", "pages", "Число страниц (для PDF)"),
            ("any", "doi", "Digital Object Identifier"),
            ("any", "udk", "Универсальный десятичный классификатор (УДК)"),
            ("any", "bbk", "Библиотечно-библиографическая классификация (ББК)"),
            ("article", "journal", "Название журнала / сборника"),
            ("article", "volume_issue", "Том/номер журнала"),
            ("article", "pages", "Страницы в выпуске"),
            ("standard", "standard", "Код стандарта (ГОСТ/ISO/IEC/СТО/СП/СанПиН/ТУ)"),
            ("standard", "status", "Статус стандарта (active/replaced)"),
            ("proceedings", "conference", "Название конференции/симпозиума"),
            ("report", "doc_kind", "Вид документа (ТЗ/Пояснительная записка и т.п.)"),
            ("report", "organization", "Организация-издатель/разработчик"),
            ("patent", "patent_no", "Номер патента"),
            ("patent", "ipc", "Класс международной патентной классификации (IPC/МПК)"),
            ("presentation", "slides", "Признак презентации (слайды)"),
        ]
    added = 0
    for mt, k, d in seeds:
        exists = TagSchema.query.filter_by(material_type=mt, key=k).first()
        if not exists:
            db.session.add(TagSchema(material_type=mt, key=k, description=d))
            added += 1
    db.session.commit()
    print(f"DB initialized. Added {added} tag schema rows (existing preserved).")

# ------------------- App bootstrap -------------------

from routes import routes
app.register_blueprint(routes)

# ------------------- Single-file Refresh API -------------------
@app.route("/api/files/<int:file_id>/refresh", methods=["POST"])
def api_file_refresh(file_id):
    """Re-extract text, refresh tags, and optionally re-run LLM for a single file.
    Respects runtime settings for LLM, audio summary and keywords. Always re-extracts text.
    """
    user = _load_current_user()
    f = File.query.get_or_404(file_id)
    refresh_task: TaskRecord | None = None
    if user:
        try:
            refresh_task = TaskRecord(
                name='refresh',
                status='running',
                progress=0.0,
                payload=json.dumps({'file_id': file_id, 'user_id': user.id})
            )
            refresh_task.started_at = datetime.utcnow()
            db.session.add(refresh_task)
            db.session.commit()
        except Exception:
            db.session.rollback()
            refresh_task = None
    refresh_params = {k: request.args.get(k) for k in ('use_llm', 'kws_audio', 'summarize')}
    log_detail = {'file_id': file_id, 'params': {k: v for k, v in refresh_params.items() if v is not None}}
    try:
        detail_payload = json.dumps(log_detail, ensure_ascii=False)
    except Exception:
        detail_payload = str(log_detail)
    try:
        _log_user_action(user, 'file_refresh_start', 'file', file_id, detail=detail_payload[:2000])
    except Exception:
        pass
    try:
        app.logger.info(
            "[user-action] user=%s action=file_refresh_start file_id=%s params=%s",
            getattr(user, 'username', None) or 'anonymous',
            file_id,
            log_detail.get('params')
        )
    except Exception:
        pass
    log: list[str] = []
    def _log(m: str):
        log.append(m)
    try:
        p = Path(f.path)
        if not p.exists() or not p.is_file():
            return jsonify({"ok": False, "error": "file_not_found", "log": ["file not found on disk"]}), 404
        ext = p.suffix.lower()
        filename = p.stem

        # очищаем кэшированные артефакты для этого файла (миниатюра и текстовый фрагмент)
        try:
            # кэш текстового фрагмента
            cache_dir = Path(app.static_folder) / 'cache' / 'text_excerpts'
            key = (f.sha1 or (f.rel_path or '').replace('/', '_')) + '.txt'
            fp = cache_dir / key
            if fp.exists():
                fp.unlink()
        except Exception:
            pass
        try:
            # миниатюра PDF
            if ext == '.pdf':
                thumb = Path(app.static_folder) / 'thumbnails' / (p.stem + '.png')
                if thumb.exists():
                    thumb.unlink()
        except Exception:
            pass

        # извлекаем текст (всегда при обновлении)
        text_excerpt = ""
        _log(f"extract: start for {p.name}")
        if ext == ".pdf":
            force = ALWAYS_OCR_FIRST_PAGE_DISSERTATION and (((f.material_type or '').lower() in ('dissertation','dissertation_abstract')) or looks_like_dissertation_filename(filename))
            text_excerpt = extract_text_pdf(p, limit_chars=40000, force_ocr_first_page=force)
        elif ext == ".docx":
            text_excerpt = extract_text_docx(p, limit_chars=40000)
        elif ext == ".rtf":
            text_excerpt = extract_text_rtf(p, limit_chars=40000)
        elif ext == ".epub":
            text_excerpt = extract_text_epub(p, limit_chars=40000)
        elif ext == ".djvu":
            text_excerpt = extract_text_djvu(p, limit_chars=40000)
        elif ext in AUDIO_EXTS:
            text_excerpt = transcribe_audio(p, limit_chars=40000)
        if text_excerpt:
            f.text_excerpt = text_excerpt[:40000]
        _log(f"extract: done chars={len(text_excerpt or '')}")
        # Общие теги: расширение и страницы PDF
        try:
            upsert_tag(f, 'ext', ext.lstrip('.'))
        except Exception:
            pass
        if ext == '.pdf':
            try:
                with fitz.open(str(p)) as _doc:
                    upsert_tag(f, 'pages', str(len(_doc)))
            except Exception:
                pass

        # Теги, специфичные для аудио
        if ext in AUDIO_EXTS:
            try:
                upsert_tag(f, 'формат', ext.lstrip('.'))
                upsert_tag(f, 'длительность', audio_duration_hhmmss(p))
            except Exception:
                pass

        # Эвристики по имени файла (заполняем только пустые поля)
        title = author = year = None
        for pat in FILENAME_PATTERNS:
            m = pat.match(filename)
            if m:
                gd = m.groupdict()
                title = gd.get("title")
                author = gd.get("author")
                year = gd.get("year")
                break
        if title and not f.title:
            f.title = title
        if author and not f.author:
            f.author = author
        if year and not f.year:
            f.year = year

        # Определяем тип материала, если не указан; корректируем аудио
        if not f.material_type:
            # Явно проставим тип по расширению
            if ext in IMAGE_EXTS:
                f.material_type = 'image'
            elif ext in AUDIO_EXTS:
                f.material_type = 'audio'
            else:
                f.material_type = guess_material_type(ext, text_excerpt, filename)
        if ext in AUDIO_EXTS and (f.material_type or '') == 'document':
            f.material_type = 'audio'
        if ext in IMAGE_EXTS and (f.material_type or '') == 'document':
            f.material_type = 'image'

        # Теги, зависящие от типа
        try:
            ttags = extract_tags_for_type(f.material_type or '', text_excerpt or '', filename)
            if ttags:
                db.session.flush()
                for k, v in ttags.items():
                    upsert_tag(f, k, v)
            _log(f"type-tags: {len(ttags or {})}")
        except Exception:
            _log("type-tags: error")
        # Извлекаем встроенные ключевые слова («Ключевые слова:»), если есть в тексте
        try:
            import re as _re
            kw_m = _re.search(r"(?:Ключевые\s+слова|Keywords)\s*[:\-]\s*(.+)", (text_excerpt or ''), flags=_re.IGNORECASE)
            if kw_m:
                f.keywords = kw_m.group(1).strip()[:2000]
                if KEYWORDS_TO_TAGS_ENABLED:
                    db.session.flush(); _upsert_keyword_tags(f)
                _log("inline-keywords: ok")
        except Exception:
            _log("inline-keywords: error")
        # Дополнительные расширенные теги
        try:
            rtags = extract_richer_tags(f.material_type or '', text_excerpt or '', filename)
            if rtags:
                db.session.flush()
                for k, v in rtags.items():
                    upsert_tag(f, k, v)
            _log(f"richer-tags: {len(rtags or {})}")
        except Exception:
            _log("richer-tags: error")

        # Необязательное обогащение через LLM по настройкам или флагам
        use_llm = (request.args.get('use_llm') in ('1','true','yes','on')) if ('use_llm' in request.args) else DEFAULT_USE_LLM
        do_summarize = (request.args.get('summarize') in ('1','true','yes','on')) if ('summarize' in request.args) else SUMMARIZE_AUDIO
        kws_audio_on = (request.args.get('kws_audio') in ('1','true','yes','on')) if ('kws_audio' in request.args) else AUDIO_KEYWORDS_LLM

        if use_llm and (text_excerpt or ext in {'.txt', '.md'}):
            llm_text = text_excerpt or ""
            if not llm_text and ext in {".txt", ".md"}:
                try:
                    llm_text = p.read_text(encoding="utf-8", errors="ignore")[:15000]
                except Exception:
                    pass
            _log(f"llm: metadata start (len={len(llm_text)})")
            meta = call_lmstudio_for_metadata(llm_text, p.name)
            if meta:
                mt_meta = normalize_material_type(meta.get("material_type"))
                # Не позволяем LLM менять тип для явных форматов
                if ext in AUDIO_EXTS:
                    f.material_type = 'audio'
                elif ext in IMAGE_EXTS:
                    f.material_type = 'image'
                elif mt_meta:
                    f.material_type = mt_meta or f.material_type
                _t = (meta.get("title") or "").strip()
                if _t:
                    f.title = _t
                _a = _normalize_author(meta.get("author"))
                if _a:
                    f.author = _a
                _y = _normalize_year(meta.get("year"))
                if _y:
                    f.year = _y
                _adv = meta.get("advisor")
                if _adv is not None:
                    _adv_s = str(_adv).strip()
                    if _adv_s:
                        f.advisor = _adv_s
                kws = meta.get("keywords") or []
                if isinstance(kws, list) and kws:
                    f.keywords = ", ".join([str(x) for x in kws][:50])
                    if KEYWORDS_TO_TAGS_ENABLED:
                        db.session.flush()
                        _upsert_keyword_tags(f)
                if meta.get("novelty"):
                    db.session.flush()
                    upsert_tag(f, "научная новизна", str(meta.get("novelty")))
                for key in ("literature", "organizations", "classification"):
                    val = meta.get(key)
                    if isinstance(val, list) and val:
                        db.session.flush()
                        upsert_tag(f, key, "; ".join([str(x) for x in val]))
            else:
                _log("llm: metadata empty")

        # Резюме и ключевые слова для аудио
        if ext in AUDIO_EXTS:
            if kws_audio_on and (f.text_excerpt or '') and not (f.keywords or '').strip():
                try:
                    kws = call_lmstudio_keywords(f.text_excerpt, p.name)
                    if kws:
                        f.keywords = ", ".join(kws)
                        db.session.flush()
                        _upsert_keyword_tags(f)
                    _log(f"llm: audio keywords {len(kws or [])}")
                except Exception:
                    _log("llm: audio keywords error")
        # Анализ изображений (vision)
        if ext in IMAGE_EXTS and IMAGES_VISION_ENABLED:
            try:
                vis = call_lmstudio_vision(p, p.name)
                if isinstance(vis, dict):
                    desc = (vis.get('description') or '')
                    if desc:
                        f.abstract = desc[:8000]
                    kws = vis.get('keywords') or []
                    if isinstance(kws, list) and kws:
                        f.keywords = ", ".join([str(x) for x in kws][:50])
                        if KEYWORDS_TO_TAGS_ENABLED:
                            db.session.flush()
                            _upsert_keyword_tags(f)
                _log("vision: ok")
            except Exception:
                _log("vision: error")
            if do_summarize and (f.text_excerpt or ''):
                try:
                    summ = call_lmstudio_summarize(f.text_excerpt, p.name)
                    if summ:
                        f.abstract = summ[:2000]
                    _log("llm: summarize ok")
                except Exception:
                    _log("llm: summarize error")

        db.session.flush()
        # Базовые теги из полей
        if f.material_type:
            upsert_tag(f, "тип", f.material_type)
        if f.author:
            upsert_tag(f, "автор", f.author)
        if f.year:
            upsert_tag(f, "год", str(f.year))
        if refresh_task:
            try:
                refresh_task.status = 'completed'
                refresh_task.progress = 1.0
                refresh_task.finished_at = datetime.utcnow()
                refresh_task.error = None
                db.session.add(refresh_task)
            except Exception:
                logger.warning('Не удалось обновить статус задачи refresh #%s', refresh_task.id if refresh_task else None)
        db.session.commit()

        data = file_to_dict(f)
        data["abstract"] = f.abstract
        data["text_excerpt"] = f.text_excerpt
        return jsonify({"ok": True, "file": data, "log": log})
    except Exception as e:
        db.session.rollback()
        log.append(f"exception: {e}")
        if refresh_task:
            try:
                task_obj = TaskRecord.query.get(refresh_task.id)
                if task_obj:
                    task_obj.status = 'error'
                    task_obj.error = str(e)
                    task_obj.finished_at = datetime.utcnow()
                    db.session.commit()
            except Exception:
                db.session.rollback()
        return jsonify({"ok": False, "error": str(e), "log": log}), 500

# ------------------- Filename Suggestion & Rename -------------------
def _safe_filename_component(s: str, max_len: int = 80) -> str:
    s = (s or '').strip()
    # Заменим небезопасные символы
    s = re.sub(r"[<>:\\/\\|?*\r\n\t\0]", " ", s)
    # Уберём кавычки
    s = s.replace('"', ' ').replace("'", ' ')
    # Превратим последовательности пробелов/точек в один пробел
    s = re.sub(r"[ ]+", " ", s)
    s = s.strip().strip('.')
    # Замена пробелов на подчёркивания
    s = s.replace(' ', '_')
    # Схлопнем повторные знаки подчёркивания
    s = re.sub(r"_+", "_", s)
    # Ограничение длины
    if len(s) > max_len:
        s = s[:max_len]
    return s or "file"

def _extract_lastname(full: str) -> str:
    a = (full or '').strip()
    if not a:
        return ''
    # несколько авторов — берём первого
    a = re.split(r"[,&;/]|\band\b", a, flags=re.I)[0]
    # удалим инициалы вида "И.И." и одиночные
    a = re.sub(r"\b[А-ЯA-Z]\.[А-ЯA-Z]\.?", "", a)
    a = re.sub(r"\b[А-ЯA-Z]\.\b", "", a)
    parts = [p for p in re.split(r"\s+", a) if p]
    if not parts:
        return ''
    # Русские ФИО часто в формате: Фамилия Имя Отчество — берём 1-ю
    cyr = all(re.match(r"^[А-Яа-я\-]+$", p) for p in parts)
    if cyr and len(parts) >= 2:
        # если 3 слова и последнее похоже на отчество (окончания -вич/-вна), используем первое
        if len(parts) >= 3 and re.search(r"(вич|вна|вны|ызы)$", parts[-1], flags=re.I):
            return parts[0]
        # две части: обычно Фамилия Имя — первое
        return parts[0]
    # Иначе — лучшее эвристическое: последнее слово
    return parts[-1]

def _mt_abbr(mt: str) -> str:
    m = (mt or '').lower()
    return {
        'dissertation': 'ДИС', 'dissertation_abstract': 'АВТ',
        'article': 'СТ', 'textbook': 'УЧ', 'monograph': 'МОНО',
        'report': 'ОТЧ', 'patent': 'ПАТ', 'presentation': 'ПРЕЗ',
        'proceedings': 'ТЕЗ', 'standard': 'СТД', 'note': 'ЗАМ',
        'document': 'ДОК', 'audio': 'АУД', 'image': 'ИЗО'
    }.get(m, 'ДОК')

def _degree_abbr(file_obj: File) -> str:
    # по тегу 'степень'
    deg = ''
    try:
        for t in file_obj.tags:
            if (t.key or '').lower() == 'степень':
                deg = (t.value or '').lower()
                break
    except Exception:
        pass
    if 'доктор' in deg:
        return 'ДН'
    if 'кандид' in deg:
        return 'КН'
    return ''

def _build_suggested_basename(file_obj: File) -> str:
    mt = (file_obj.material_type or '').lower()
    abbr = _mt_abbr(mt)
    ctx = {
        'abbr': abbr,
        'degree': _degree_abbr(file_obj),
        'title': (file_obj.title or '').strip(),
        'author_last': _extract_lastname(file_obj.author or ''),
        'year': (file_obj.year or '').strip(),
        'filename': file_obj.filename or '',
    }
    # Используем шаблоны из настроек, если заданы
    pattern = RENAME_PATTERNS.get(mt) or RENAME_PATTERNS.get('default')
    base = None
    if pattern:
        try:
            base = pattern.format(**ctx)
        except Exception:
            base = None
    if not base:
        # Запасной вариант (жёстко заданные правила)
        if mt in ('dissertation','dissertation_abstract'):
            base = f"{abbr}.{(ctx['degree'] + '.') if ctx['degree'] else ''}{ctx['title']}.{ctx['author_last']}"
        elif mt == 'article':
            base = f"СТ.{ctx['title']}.{ctx['author_last'] or ctx['year']}"
        elif mt == 'textbook':
            base = f"УЧ.{ctx['title']}.{ctx['author_last'] or ctx['year']}"
        elif mt == 'monograph':
            base = f"МОНО.{ctx['title']}.{ctx['author_last'] or ctx['year']}"
        elif mt == 'image':
            base = f"ИЗО.{ctx['title'] or ctx['filename']}"
        elif mt == 'audio':
            base = f"АУД.{ctx['title'] or ctx['filename']}"
        else:
            base = f"{abbr}.{ctx['title'] or ctx['filename']}.{ctx['author_last'] or ctx['year']}"

    parts = [p for p in [ _safe_filename_component(x) for x in (base or '').split('.') ] if p]
    name = '.'.join(parts) if parts else _safe_filename_component(file_obj.filename)
    return name[:120]


def _determine_target_dir_for_file(file_obj: File, current_path: Path, material_type: str) -> Path:
    col = getattr(file_obj, 'collection', None)
    if not isinstance(col, Collection):
        col = _get_collection_instance(file_obj.collection_id)
    material_type = (material_type or '').strip().lower()
    fallback_sub = TYPE_DIRS.get(material_type) or TYPE_DIRS.get('other', 'other')
    if COLLECTIONS_IN_SEPARATE_DIRS:
        base_root = _collection_root_dir(col)
        if col and COLLECTION_TYPE_SUBDIRS:
            return base_root / fallback_sub
        return base_root
    if MOVE_ON_RENAME:
        return Path(SCAN_ROOT) / fallback_sub
    return current_path.parent


def _rename_file_record(file_obj: File, base_name: str | None = None) -> Path:
    p_old = Path(file_obj.path)
    if not p_old.exists():
        raise FileNotFoundError(f"Файл {file_obj.path} не найден")
    base = (base_name or _build_suggested_basename(file_obj)) or file_obj.filename or p_old.stem
    base = _safe_filename_component(base, max_len=120)
    ext = file_obj.ext or p_old.suffix
    mt = (file_obj.material_type or '').strip().lower()
    target_dir = _determine_target_dir_for_file(file_obj, p_old, mt)
    target_dir.mkdir(parents=True, exist_ok=True)
    p_new = target_dir / (base + (ext or ''))
    i = 1
    while p_new.exists() and p_new != p_old:
        p_new = target_dir / (f"{base}_{i}" + (ext or ''))
        i += 1
    renamed = p_new != p_old
    if renamed:
        p_old.rename(p_new)
        if (file_obj.ext or '').lower() == '.pdf':
            try:
                old_thumb = Path(app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                if old_thumb.exists():
                    old_thumb.unlink()
            except Exception:
                pass
    file_obj.path = str(p_new)
    try:
        file_obj.rel_path = str(p_new.relative_to(Path(SCAN_ROOT)))
    except Exception:
        file_obj.rel_path = p_new.name
    file_obj.filename = p_new.stem
    try:
        file_obj.mtime = p_new.stat().st_mtime
    except Exception:
        pass
    if renamed:
        try:
            db.session.add(ChangeLog(file_id=file_obj.id, action='rename', field='filename', old_value=p_old.name, new_value=p_new.name))
        except Exception:
            pass
    return p_new

@app.route('/api/files/<int:file_id>/rename-suggest', methods=['GET'])
def api_rename_suggest(file_id):
    f = File.query.get_or_404(file_id)
    try:
        suggested = _build_suggested_basename(f)
        return jsonify({"ok": True, "suggested": suggested, "ext": f.ext or ''})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/files/<int:file_id>/rename', methods=['POST'])
def api_rename_apply(file_id):
    f = File.query.get_or_404(file_id)
    data = request.json or {}
    base = (data.get('base') or '').strip()
    if not base:
        base = _build_suggested_basename(f)
    base = _safe_filename_component(base, max_len=120)
    try:
        p_new = _rename_file_record(f, base)
        db.session.commit()
        return jsonify({"ok": True, "new_name": Path(p_new).name, "rel_path": f.rel_path})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/collections/<int:collection_id>/rename-all', methods=['POST'])
@require_admin
def api_collection_rename_all(collection_id: int):
    col = Collection.query.get_or_404(collection_id)
    files = File.query.filter(File.collection_id == collection_id).all()
    renamed = 0
    errors: list[dict[str, str]] = []
    for f in files:
        try:
            _rename_file_record(f)
            db.session.commit()
            renamed += 1
        except Exception as exc:
            db.session.rollback()
            errors.append({'id': str(f.id), 'error': str(exc)})
    try:
        _log_user_action(_load_current_user(), 'collection_rename_all', 'collection', col.id, detail=json.dumps({'renamed': renamed, 'errors': len(errors)}))
    except Exception:
        pass
    return jsonify({'ok': True, 'renamed': renamed, 'errors': errors})


def _delete_collection_files(col_id: int) -> tuple[int, list[str]]:
    files = File.query.filter(File.collection_id == col_id).all()
    removed = 0
    errors: list[str] = []
    for f in files:
        path = Path(f.path) if f.path else None
        if path and path.exists():
            try:
                path.unlink()
                removed += 1
            except Exception as exc:
                errors.append(f"{path}: {exc}")
        db.session.delete(f)
    return removed, errors


def _remove_collection_directory_path(path: Path | None) -> None:
    if path is None:
        return
    try:
        if path.exists():
            shutil.rmtree(path)
    except Exception:
        pass


@app.route('/api/collections/<int:collection_id>', methods=['DELETE'])
@require_admin
def api_delete_collection(collection_id: int):
    col = Collection.query.get_or_404(collection_id)
    collection_root: Path | None = None
    if COLLECTIONS_IN_SEPARATE_DIRS:
        try:
            collection_root = _collection_root_dir(col, ensure=False)
        except Exception:
            collection_root = None
    removed_files, errors = _delete_collection_files(collection_id)
    try:
        CollectionMember.query.filter_by(collection_id=collection_id).delete()
        db.session.delete(col)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({'ok': False, 'error': str(exc)}), 500
    _remove_collection_directory_path(collection_root)
    try:
        _log_user_action(_load_current_user(), 'collection_delete', 'collection', collection_id, detail=json.dumps({'removed_files': removed_files, 'errors': len(errors)}))
    except Exception:
        pass
    return jsonify({'ok': True, 'removed_files': removed_files, 'errors': errors})

# ------------------- Scan with Progress -------------------
import threading, time

SCAN_PROGRESS = {
    "running": False,
    "stage": "idle",
    "total": 0,
    "processed": 0,
    "added": 0,
    "updated": 0,
    "removed": 0,
    "current": "",
    "use_llm": False,
    "error": None,
    "started_at": None,
    "updated_at": None,
    "eta_seconds": None,
    "history": [],
    "task_id": None,
}
SCAN_CANCEL = False
SCAN_TASK_ID: int | None = None

def _iter_files_for_scan(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        yield path


def _bool_from_form(form, key: str, default: bool) -> bool:
    if form is None:
        return bool(default)
    if key in form:
        val = str(form.get(key) or '').strip().lower()
        return val in ('1', 'true', 'yes', 'on')
    return bool(default)

def _purge_cached_artifacts(records: list[tuple[File, Path]]):
    """Удаляет кэшированные превью и миниатюры для указанных файлов."""
    if not records:
        return
    try:
        static_dir = Path(app.static_folder or 'static').resolve()
    except Exception:
        return
    txt_cache = static_dir / 'cache' / 'text_excerpts'
    thumbs_dir = static_dir / 'thumbnails'
    for file_obj, path in records:
        try:
            if txt_cache.exists():
                key_base = file_obj.sha1 or (file_obj.rel_path or '').replace('/', '_')
                if key_base:
                    fp = txt_cache / f"{key_base}.txt"
                    if fp.exists():
                        fp.unlink()
        except Exception:
            pass
        try:
            if path.suffix.lower() == '.pdf' and thumbs_dir.exists():
                thumb = thumbs_dir / f"{path.stem}.png"
                if thumb.exists():
                    thumb.unlink()
        except Exception:
            pass

MAX_LOG_LINES = 200

def _scan_log(msg: str, level: str = "info"):
    SCAN_PROGRESS.setdefault("history", [])
    entry = {"t": time.time(), "level": level, "msg": str(msg)}
    SCAN_PROGRESS["history"].append(entry)
    if len(SCAN_PROGRESS["history"]) > MAX_LOG_LINES:
        SCAN_PROGRESS["history"] = SCAN_PROGRESS["history"][-MAX_LOG_LINES:]
    SCAN_PROGRESS["updated_at"] = time.time()
    log_line = f"[scan] {msg}"
    try:
        if level == 'error':
            app.logger.error(log_line)
        elif level in ('warn', 'warning'):
            app.logger.warning(log_line)
        else:
            app.logger.info(log_line)
    except Exception:
        pass

def _update_eta():
    try:
        st = SCAN_PROGRESS
        total = int(st.get("total") or 0)
        processed = int(st.get("processed") or 0)
        started = st.get("started_at") or time.time()
        if processed <= 0 or total <= 0:
            st["eta_seconds"] = None
            return
        elapsed = max(0.001, time.time() - float(started))
        rate = processed / elapsed
        remain = max(0, total - processed)
        eta = remain / rate if rate > 0 else None
        st["eta_seconds"] = int(eta) if eta is not None else None
    except Exception:
        st = SCAN_PROGRESS
        st["eta_seconds"] = None

def _run_scan_with_progress(extract_text: bool, use_llm: bool, prune: bool, skip: int = 0, targets: list | None = None):
    global SCAN_PROGRESS, SCAN_CANCEL, SCAN_TASK_ID
    with app.app_context():
        try:
            task = None
            if SCAN_TASK_ID:
                try:
                    task = TaskRecord.query.get(SCAN_TASK_ID)
                    if task:
                        task.status = 'running'
                        task.started_at = datetime.utcnow()
                        task.progress = 0.0
                        db.session.commit()
                except Exception:
                    db.session.rollback()
                    task = None
            SCAN_PROGRESS.update({
                "running": True,
                "stage": "counting",
                "processed": 0,
                "added": 0,
                "updated": 0,
                "removed": 0,
                "error": None,
                "use_llm": bool(use_llm),
                "started_at": time.time(),
                "updated_at": time.time(),
                "eta_seconds": None,
                "history": [],
            })
            _scan_log("Начало сканирования")
            root = Path(SCAN_ROOT)
            # определяем набор для сканирования: явные цели или полный корневой обход
            if targets:
                try:
                    file_list = [Path(p) for p in targets]
                except Exception:
                    file_list = []
                # фильтруем по разрешённым путям и наличию
                file_list = [p for p in file_list if p.exists() and p.suffix.lower() in ALLOWED_EXTS]
                _scan_log(f"Сканирование только добавленных файлов: {len(file_list)}")
            else:
                file_list = list(_iter_files_for_scan(root))
            total = len(file_list)
            SCAN_PROGRESS["total"] = total
            if task:
                try:
                    task.progress = 0.0
                    task.payload = json.dumps({
                        "total": total,
                        "use_llm": bool(use_llm),
                        "extract_text": bool(extract_text),
                        "prune": bool(prune),
                    })
                    db.session.commit()
                except Exception:
                    db.session.rollback()
            # поддержка продолжения: пропускаем первые N файлов, если задано
            skip = max(0, min(int(skip or 0), total))
            if skip:
                _scan_log(f"Продолжение: пропуск первых {skip} из {total}")
                file_list = file_list[skip:]
                SCAN_PROGRESS["processed"] = skip
            _scan_log(f"Найдено файлов: {len(file_list)}")

            added = updated = 0
            cancelled = False
            for idx, path in enumerate(file_list, start=1):
                if SCAN_CANCEL:
                    _scan_log("Отмена пользователем", level="warn")
                    cancelled = True
                    break
                SCAN_PROGRESS.update({
                    "stage": "processing",
                    "processed": (skip + idx),
                    "current": str(path.name),
                    "updated_at": time.time()
                })
                _update_eta()
                if task and total:
                    try:
                        task.progress = min(1.0, float(skip + idx) / float(total))
                        if idx == 1 or idx % 10 == 0:
                            db.session.commit()
                    except Exception:
                        db.session.rollback()
                if idx == 1 or idx % 10 == 0:
                    _scan_log(f"Обработка: {path.name}")

                ext = path.suffix.lower()
                # по возможности вычисляем относительный путь к SCAN_ROOT
                try:
                    rel_path = str(path.relative_to(root))
                except Exception:
                    rel_path = path.name
                size = path.stat().st_size
                mtime = path.stat().st_mtime
                filename = path.stem

                file_obj = File.query.filter_by(path=str(path)).first()
                if not file_obj:
                    sha1 = sha1_of_file(path)
                    file_obj = File(path=str(path), rel_path=rel_path, filename=filename,
                                    ext=ext, size=size, mtime=mtime, sha1=sha1)
                    db.session.add(file_obj)
                    added += 1
                    SCAN_PROGRESS["added"] = added
                else:
                    if (file_obj.size != size) or (file_obj.mtime != mtime):
                        sha1 = sha1_of_file(path)
                        file_obj.sha1 = sha1
                        file_obj.size = size
                        file_obj.mtime = mtime
                        file_obj.filename = filename
                    updated += 1
                    SCAN_PROGRESS["updated"] = updated

                # Извлечение текста (на основе текущей логики)
                text_excerpt = ""
                if extract_text:
                    if ext == ".pdf":
                        text_excerpt = extract_text_pdf(path, limit_chars=40000)
                    elif ext == ".docx":
                        text_excerpt = extract_text_docx(path, limit_chars=40000)
                    elif ext == ".rtf":
                        text_excerpt = extract_text_rtf(path, limit_chars=40000)
                    elif ext == ".epub":
                        text_excerpt = extract_text_epub(path, limit_chars=40000)
                    elif ext == ".djvu":
                        text_excerpt = extract_text_djvu(path, limit_chars=40000)
                    elif ext in AUDIO_EXTS:
                        _scan_log(f"Транскрибация аудио: {path.name}")
                        text_excerpt = transcribe_audio(path, limit_chars=40000)
                    if text_excerpt:
                        file_obj.text_excerpt = text_excerpt[:40000]
                    # Общие теги: расширение и страницы PDF
                    try:
                        upsert_tag(file_obj, 'ext', ext.lstrip('.'))
                    except Exception:
                        pass
                    if ext == '.pdf':
                        try:
                            with fitz.open(str(path)) as _doc:
                                upsert_tag(file_obj, 'pages', str(len(_doc)))
                        except Exception:
                            pass
                    # Теги, специфичные для аудио
                    if ext in AUDIO_EXTS:
                        try:
                            upsert_tag(file_obj, 'формат', ext.lstrip('.'))
                            upsert_tag(file_obj, 'длительность', audio_duration_hhmmss(path))
                        except Exception:
                            pass
                        # Лёгкие ключевые слова из транскрипта через LLM
                        try:
                            if AUDIO_KEYWORDS_LLM and (file_obj.text_excerpt or '') and not (file_obj.keywords or '').strip():
                                kws = call_lmstudio_keywords(file_obj.text_excerpt, path.name)
                                if kws:
                                    file_obj.keywords = ", ".join(kws)
                        except Exception as _e:
                            _scan_log(f"audio keywords llm failed: {_e}", level="warn")
                    # Теги, специфичные для изображений
                    if ext in IMAGE_EXTS:
                        try:
                            upsert_tag(file_obj, 'формат', ext.lstrip('.'))
                            if PILImage is not None:
                                with PILImage.open(str(path)) as im:
                                    w, h = im.size
                                upsert_tag(file_obj, 'разрешение', f'{w}x{h}')
                                orient = 'портрет' if h >= w else 'альбом'
                                upsert_tag(file_obj, 'ориентация', orient)
                        except Exception:
                            pass
                        # Лёгкие ключевые слова из транскрипта через LLM
                        try:
                            if AUDIO_KEYWORDS_LLM and (file_obj.text_excerpt or '') and not (file_obj.keywords or '').strip():
                                kws = call_lmstudio_keywords(file_obj.text_excerpt, path.name)
                                if kws:
                                    file_obj.keywords = ", ".join(kws)
                        except Exception as _e:
                            _scan_log(f"audio keywords llm failed: {_e}", level="warn")

                # Встроенные ключевые слова в тексте для диссертаций/статей
                try:
                    import re as _re
                    kw_m = _re.search(r"(?:Ключевые\s+слова|Keywords)\s*[:\-]\s*(.+)", (text_excerpt or ''), flags=_re.IGNORECASE)
                    if kw_m:
                        file_obj.keywords = kw_m.group(1).strip()[:2000]
                        if KEYWORDS_TO_TAGS_ENABLED:
                            db.session.flush(); _upsert_keyword_tags(file_obj)
                except Exception:
                    pass

                # Эвристики на основе имени файла
                title, author, year = None, None, None
                for pat in FILENAME_PATTERNS:
                    m = pat.match(filename)
                    if m:
                        gd = m.groupdict()
                        title = gd.get("title") or title
                        author = gd.get("author") or author
                        year = gd.get("year") or year
                        break

                if title and not file_obj.title:
                    file_obj.title = title
                if author and not file_obj.author:
                    file_obj.author = author
                if year and not file_obj.year:
                    file_obj.year = year

                if not file_obj.material_type:
                    cand = _detect_type_pre_llm(ext, text_excerpt, filename)
                    if cand:
                        file_obj.material_type = cand
                if ext in AUDIO_EXTS and (file_obj.material_type or '') == 'document':
                    file_obj.material_type = 'audio'
                if ext in IMAGE_EXTS and (file_obj.material_type or '') == 'document':
                    file_obj.material_type = 'image'

                # Типо-зависимые теги (до LLM)
                    try:
                        ttags = extract_tags_for_type(file_obj.material_type or '', text_excerpt or '', filename)
                        if ttags:
                            db.session.flush()
                            for k, v in ttags.items():
                                upsert_tag(file_obj, k, v)
                    except Exception as e:
                        _scan_log(f"type tags error: {e}", level="warn")
                    # Дополнительные расширенные теги
                    try:
                        rtags = extract_richer_tags(file_obj.material_type or '', text_excerpt or '', filename)
                        if rtags:
                            db.session.flush()
                            for k, v in rtags.items():
                                upsert_tag(file_obj, k, v)
                    except Exception as e:
                        _scan_log(f"richer tags error: {e}", level="warn")

                # Обогащение через LLM (может быть медленным)
                if use_llm and (text_excerpt or ext in {'.txt', '.md'} or ext in IMAGE_EXTS or ext in {'.pdf','.docx','.rtf','.epub','.djvu'}):
                    SCAN_PROGRESS["stage"] = "llm"
                    SCAN_PROGRESS["updated_at"] = time.time()
                    _scan_log(f"LLM-анализ: {path.name}")
                    llm_text = text_excerpt or ""
                    if not llm_text and ext in {".txt", ".md"}:
                        try:
                            llm_text = Path(path).read_text(encoding="utf-8", errors="ignore")[:15000]
                        except Exception:
                            pass
                    # Лёгкое извлечение для распространённых форматов документов, если extract_text выключен
                    if not llm_text and ext in {'.pdf','.docx','.rtf','.epub','.djvu'}:
                        try:
                            if ext == '.pdf':
                                llm_text = extract_text_pdf(path, limit_chars=12000, force_ocr_first_page=(ALWAYS_OCR_FIRST_PAGE_DISSERTATION and looks_like_dissertation_filename(filename)))
                            elif ext == '.docx':
                                llm_text = extract_text_docx(path, limit_chars=12000)
                            elif ext == '.rtf':
                                llm_text = extract_text_rtf(path, limit_chars=12000)
                            elif ext == '.epub':
                                llm_text = extract_text_epub(path, limit_chars=12000)
                            elif ext == '.djvu':
                                llm_text = extract_text_djvu(path, limit_chars=12000)
                        except Exception as _e:
                            _scan_log(f"llm lightweight extract error: {_e}", level='warn')
                    meta = call_lmstudio_for_metadata(llm_text, path.name)
                    if meta:
                        mt_meta = normalize_material_type(meta.get("material_type"))
                        # Для очевидных форматов не даём LLM переопределять тип
                        if ext in AUDIO_EXTS:
                            file_obj.material_type = 'audio'
                        elif ext in IMAGE_EXTS:
                            file_obj.material_type = 'image'
                        elif TYPE_LLM_OVERRIDE and mt_meta:
                            file_obj.material_type = mt_meta
                        _t = (meta.get("title") or "").strip()
                        if _t:
                            file_obj.title = _t
                        _a = _normalize_author(meta.get("author"))
                        if _a:
                            file_obj.author = _a
                        _y = _normalize_year(meta.get("year"))
                        if _y:
                            file_obj.year = _y
                        _adv = meta.get("advisor")
                        if _adv is not None:
                            _adv_s = str(_adv).strip()
                            if _adv_s:
                                file_obj.advisor = _adv_s
                        kws = meta.get("keywords") or []
                        if isinstance(kws, list):
                            file_obj.keywords = ", ".join([str(x) for x in kws][:50])
                        if meta.get("novelty"):
                            db.session.flush()
                            upsert_tag(file_obj, "научная новизна", str(meta.get("novelty")))
                    for key in ("literature", "organizations", "classification"):
                        val = meta.get(key)
                        if isinstance(val, list) and val:
                            db.session.flush()
                            upsert_tag(file_obj, key, "; ".join([str(x) for x in val]))
                    # Резюме для аудио
                    if ext in AUDIO_EXTS and SUMMARIZE_AUDIO and (file_obj.text_excerpt or ''):
                        summ = call_lmstudio_summarize(file_obj.text_excerpt, path.name)
                        if summ:
                            file_obj.abstract = summ[:2000]
                    # Анализ изображений через Vision
                    if ext in IMAGE_EXTS and IMAGES_VISION_ENABLED:
                        try:
                            vis = call_lmstudio_vision(path, path.name)
                            if isinstance(vis, dict):
                                desc = (vis.get('description') or '')
                                if desc:
                                    file_obj.abstract = desc[:8000]
                                kws = vis.get('keywords') or []
                                if isinstance(kws, list) and kws:
                                    file_obj.keywords = ", ".join([str(x) for x in kws][:50])
                        except Exception as e:
                            _scan_log(f"vision error: {e}", level="warn")
                    # Повторный прогон типовых тегов
                    try:
                        ttags = extract_tags_for_type(file_obj.material_type or '', text_excerpt or '', filename)
                        if ttags:
                            db.session.flush()
                            for k, v in ttags.items():
                                upsert_tag(file_obj, k, v)
                    except Exception as e:
                        _scan_log(f"type tags error(2): {e}", level="warn")
                    # Добавочные расширенные теги после LLM
                    try:
                        rtags = extract_richer_tags(file_obj.material_type or '', text_excerpt or '', filename)
                        if rtags:
                            db.session.flush()
                            for k, v in rtags.items():
                                upsert_tag(file_obj, k, v)
                    except Exception as e:
                        _scan_log(f"richer tags error(2): {e}", level="warn")

                db.session.flush()
                # Базовые теги
                if file_obj.material_type:
                    upsert_tag(file_obj, "тип", file_obj.material_type)
                if file_obj.author:
                    upsert_tag(file_obj, "автор", file_obj.author)
                if file_obj.year:
                    upsert_tag(file_obj, "год", str(file_obj.year))

                db.session.commit()

            removed = 0
            if prune and not SCAN_CANCEL:
                SCAN_PROGRESS["stage"] = "prune"
                SCAN_PROGRESS["updated_at"] = time.time()
                _scan_log("Удаление отсутствующих файлов")
                removed = prune_missing_files()
                db.session.commit()
            SCAN_PROGRESS.update({"removed": removed, "stage": "done", "running": False, "updated_at": time.time()})
            _scan_log("Сканирование завершено")
            if task:
                try:
                    task.status = 'cancelled' if cancelled else 'completed'
                    task.finished_at = datetime.utcnow()
                    task.progress = 1.0 if not cancelled else task.progress
                    db.session.commit()
                except Exception:
                    db.session.rollback()
        except Exception as e:
            SCAN_PROGRESS.update({"error": str(e), "running": False, "stage": "error", "updated_at": time.time()})
            _scan_log(f"Ошибка: {e}", level="error")
            if task:
                try:
                    task.status = 'error'
                    task.error = str(e)
                    task.finished_at = datetime.utcnow()
                    db.session.commit()
                except Exception:
                    db.session.rollback()
        finally:
            SCAN_CANCEL = False
            SCAN_TASK_ID = None
            SCAN_PROGRESS["task_id"] = None
            SCAN_PROGRESS["scope"] = None

@app.route("/scan/start", methods=["POST"])
@require_admin
def scan_start():
    global SCAN_CANCEL, SCAN_TASK_ID
    if SCAN_PROGRESS.get("running"):
        return jsonify({"status": "busy"}), 409
    extract_text = _bool_from_form(request.form, "extract_text", EXTRACT_TEXT)
    use_llm = _bool_from_form(request.form, "use_llm", DEFAULT_USE_LLM)
    prune = _bool_from_form(request.form, "prune", DEFAULT_PRUNE)
    SCAN_PROGRESS["scope"] = {
        "type": "library",
        "label": "Вся библиотека",
        "extract_text": bool(extract_text),
        "use_llm": bool(use_llm),
        "prune": bool(prune),
    }
    SCAN_CANCEL = False
    skip = 0
    try:
        skip = int(request.form.get('skip', '0') or 0)
    except Exception:
        skip = 0
    # Очищаем статические кэши перед полным сканом: текстовые фрагменты и миниатюры
    try:
        static_dir = Path(app.static_folder)
        txt_cache = static_dir / 'cache' / 'text_excerpts'
        if txt_cache.exists():
            for fp in txt_cache.glob('*.txt'):
                try: fp.unlink()
                except Exception: pass
        thumbs = static_dir / 'thumbnails'
        if thumbs.exists():
            for fp in thumbs.glob('*.png'):
                try: fp.unlink()
                except Exception: pass
    except Exception:
        pass
    payload = {
        "extract_text": extract_text,
        "use_llm": use_llm,
        "prune": prune,
        "skip": skip,
    }
    try:
        task = TaskRecord(name='scan', status='queued', payload=json.dumps(payload), progress=0.0)
        db.session.add(task)
        db.session.commit()
        SCAN_TASK_ID = task.id
        SCAN_PROGRESS["task_id"] = task.id
    except Exception:
        db.session.rollback()
        SCAN_TASK_ID = None
        SCAN_PROGRESS["task_id"] = None
    _log_user_action(_load_current_user(), 'scan_start', 'scan', SCAN_TASK_ID, detail=json.dumps(payload))
    t = threading.Thread(target=_run_scan_with_progress, args=(extract_text, use_llm, prune, skip), daemon=True)
    t.start()
    return jsonify({"status": "started"})


@app.route("/scan/collection/<int:collection_id>", methods=["POST"])
@require_admin
def scan_collection(collection_id: int):
    global SCAN_CANCEL, SCAN_TASK_ID
    if SCAN_PROGRESS.get("running"):
        return jsonify({"status": "busy"}), 409
    collection = Collection.query.get(collection_id)
    if not collection:
        return jsonify({"status": "not_found", "error": "collection_not_found"}), 404
    extract_text = _bool_from_form(request.form, "extract_text", EXTRACT_TEXT)
    use_llm = _bool_from_form(request.form, "use_llm", DEFAULT_USE_LLM)
    prune = _bool_from_form(request.form, "prune", DEFAULT_PRUNE)
    files = File.query.filter(File.collection_id == collection_id).all()
    if not files:
        return jsonify({"status": "empty", "collection_id": collection_id, "files": 0})

    targets: list[str] = []
    records: list[tuple[File, Path]] = []
    missing = 0
    for file_obj in files:
        path_str = (file_obj.path or '').strip()
        path = Path(path_str) if path_str else None
        if not path or not path.exists():
            fallback = None
            if file_obj.rel_path:
                try:
                    fallback = (Path(SCAN_ROOT) / Path(file_obj.rel_path)).resolve()
                except Exception:
                    fallback = None
            if fallback and fallback.exists():
                path = fallback
            else:
                missing += 1
                continue
        if not path.is_file():
            missing += 1
            continue
        ext = path.suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        resolved = path.resolve()
        targets.append(str(resolved))
        records.append((file_obj, resolved))

    if not targets:
        return jsonify({"status": "empty", "collection_id": collection_id, "files": 0, "missing": missing})

    _purge_cached_artifacts(records)
    SCAN_CANCEL = False
    SCAN_PROGRESS["scope"] = {
        "type": "collection",
        "collection_id": collection.id,
        "label": f"Коллекция «{collection.name}»",
        "count": len(targets),
        "extract_text": bool(extract_text),
        "use_llm": bool(use_llm),
        "prune": bool(prune),
    }

    payload = {
        "extract_text": extract_text,
        "use_llm": use_llm,
        "prune": prune,
        "collection_id": collection.id,
        "collection_name": collection.name,
        "targets": len(targets),
        "missing": missing,
    }
    try:
        task = TaskRecord(name='scan', status='queued', payload=json.dumps(payload), progress=0.0)
        db.session.add(task)
        db.session.commit()
        SCAN_TASK_ID = task.id
        SCAN_PROGRESS["task_id"] = task.id
    except Exception:
        db.session.rollback()
        SCAN_TASK_ID = None
        SCAN_PROGRESS["task_id"] = None

    try:
        _log_user_action(_load_current_user(), 'scan_collection_start', 'collection', collection.id, detail=json.dumps(payload))
    except Exception:
        pass

    t = threading.Thread(target=_run_scan_with_progress, args=(extract_text, use_llm, prune, 0, targets), daemon=True)
    t.start()
    return jsonify({"status": "started", "files": len(targets), "missing": missing})

@app.route("/scan/status")
def scan_status():
    return jsonify(SCAN_PROGRESS)

@app.route("/scan/cancel", methods=["POST"])
@require_admin
def scan_cancel():
    global SCAN_CANCEL
    SCAN_CANCEL = True
    try:
        if SCAN_TASK_ID:
            task = TaskRecord.query.get(SCAN_TASK_ID)
            if task and task.status not in ('completed','error','cancelled'):
                task.status = 'cancelling'
                db.session.commit()
    except Exception:
        db.session.rollback()
    _log_user_action(_load_current_user(), 'scan_cancel', 'scan', SCAN_TASK_ID)
    return jsonify({"status": "cancelling"})


# ------------------- AI Search (MVP) -------------------

def _ai_expand_keywords(query: str) -> list[str]:
    q = (query or "").strip()
    if not q:
        return []
    # TTL в минутах; по умолчанию 20
    try:
        ttl_min = int(os.getenv("AI_EXPAND_TTL_MIN", "20") or 20)
    except Exception:
        ttl_min = 20
    key = _sha256(q)
    now = _now()
    cached = AI_EXPAND_CACHE.get(key)
    if cached and (now - cached[0]) < ttl_min * 60:
        return cached[1]
    # Запрашиваем ключевые слова у LLM; при сбое используем простые токены
    kws = []
    try:
        kws = call_lmstudio_keywords(q, "ai-search") or []
    except Exception:
        kws = []
    if not kws:
        # запасной вариант: наивное разделение на токены
        toks = [t.strip() for t in re.split(r"[\s,;]+", q) if t.strip()]
        kws = toks[:12]
    # удаляем дубликаты, сохраняя порядок
    seen = set()
    res = []
    for w in kws:
        lw = w.lower()
        if lw not in seen:
            seen.add(lw)
            res.append(w)
    AI_EXPAND_CACHE[key] = (now, res)
    return res


def _read_cached_excerpt_for_file(f: File) -> str:
    try:
        # Предпочитаем фрагмент из базы данных
        if (f.text_excerpt or '').strip():
            return (f.text_excerpt or '')
        cache_dir = Path(app.static_folder) / 'cache' / 'text_excerpts'
        key = (f.sha1 or (f.rel_path or '').replace('/', '_')) + '.txt'
        fp = cache_dir / key
        if fp.exists():
            return fp.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        pass
    return ''


def _collect_snippets(text: str, terms: list[str], max_snips: int = 2) -> list[str]:
    t = (text or '')
    if not t:
        return []
    tl = t.lower()
    outs: list[tuple[int, str]] = []
    windows = []
    for raw in terms:
        term = (raw or '').strip()
        if not term:
            continue
        ql = term.lower()
        pos = 0
        found_any = False
        for _i in range(3):  # максимум 3 позиционирования на термин
            idx = tl.find(ql, pos)
            if idx < 0:
                break
            found_any = True
            start = max(0, idx - 80)
            end = min(len(t), idx + len(term) + 80)
            windows.append((start, end))
            pos = idx + len(term)
        if not found_any and len(ql) >= 3:
            # пытаемся разделить термин на подслова
            for part in re.split(r"[\s\-_/]+", ql):
                if len(part) < 3:
                    continue
                idx = tl.find(part)
                if idx >= 0:
                    start = max(0, idx - 80)
                    end = min(len(t), idx + len(part) + 80)
                    windows.append((start, end))
    # объединяем перекрывающиеся окна
    windows.sort()
    merged = []
    for w in windows:
        if not merged or w[0] > merged[-1][1] + 20:
            merged.append(list(w))
        else:
            merged[-1][1] = max(merged[-1][1], w[1])
    for a, b in merged[:max_snips]:
        snip = t[a:b]
        # схлопываем переводы строк для компактности
        snip = re.sub(r"\s+", " ", snip).strip()
        outs.append((a, snip))
    outs.sort(key=lambda x: x[0])
    return [s for _pos, s in outs]


def _split_text_chunks(text: str, chunk_chars: int, max_chunks: int) -> list[str]:
    if not text:
        return []
    chunk_chars = max(256, int(chunk_chars or 1024))
    max_chunks = max(1, int(max_chunks or 1))
    limit = min(len(text), chunk_chars * max_chunks)
    trimmed = text[:limit]
    return [trimmed[i:i+chunk_chars] for i in range(0, len(trimmed), chunk_chars)]


def _iter_document_chunks(path: Path, chunk_chars: int = 6000, max_chunks: int = 30):
    if not path or not path.exists() or not path.is_file():
        return
    chunk_chars = max(512, int(chunk_chars or 2000))
    max_chunks = max(1, int(max_chunks or 1))
    ext = path.suffix.lower()
    yielded = 0
    try:
        if ext in {'.txt', '.md', '.markdown', '.csv', '.tsv', '.json', '.yaml', '.yml', '.log'}:
            with path.open('r', encoding='utf-8', errors='ignore') as fh:
                while yielded < max_chunks:
                    chunk = fh.read(chunk_chars)
                    if not chunk:
                        break
                    yielded += 1
                    yield chunk
            return
        if ext == '.pdf' and fitz is not None:
            with fitz.open(path) as doc:
                buffer = ''
                for page in doc:
                    buffer += page.get_text('text') or ''
                    while len(buffer) >= chunk_chars and yielded < max_chunks:
                        yield buffer[:chunk_chars]
                        buffer = buffer[chunk_chars:]
                        yielded += 1
                        if yielded >= max_chunks:
                            return
                if buffer and yielded < max_chunks:
                    yield buffer[:chunk_chars]
            return
        if ext == '.docx' and docx is not None:
            document = docx.Document(str(path))
            buffer = ''
            for para in document.paragraphs:
                buffer += (para.text or '') + '\n'
                while len(buffer) >= chunk_chars and yielded < max_chunks:
                    yield buffer[:chunk_chars]
                    buffer = buffer[chunk_chars:]
                    yielded += 1
                    if yielded >= max_chunks:
                        return
            if buffer and yielded < max_chunks:
                yield buffer[:chunk_chars]
            return
        if ext == '.rtf' and rtf_to_text is not None:
            text = rtf_to_text(path.read_text(encoding='utf-8', errors='ignore'))
            for chunk in _split_text_chunks(text, chunk_chars, max_chunks):
                yield chunk
            return
        if ext == '.epub' and epub is not None:
            book = epub.read_epub(str(path))
            buffer = ''
            for item in book.get_items():
                if item.get_type() != epub.ITEM_DOCUMENT:
                    continue
                buffer += item.get_content().decode(errors='ignore')
                while len(buffer) >= chunk_chars and yielded < max_chunks:
                    yield buffer[:chunk_chars]
                    buffer = buffer[chunk_chars:]
                    yielded += 1
                    if yielded >= max_chunks:
                        return
            if buffer and yielded < max_chunks:
                yield buffer[:chunk_chars]
            return
        if ext == '.djvu' and djvu is not None:
            with djvu.decode.open(str(path)) as d:
                buffer = ''
                for page in d.pages:
                    buffer += page.get_text()
                    while len(buffer) >= chunk_chars and yielded < max_chunks:
                        yield buffer[:chunk_chars]
                        buffer = buffer[chunk_chars:]
                        yielded += 1
                        if yielded >= max_chunks:
                            return
            if buffer and yielded < max_chunks:
                yield buffer[:chunk_chars]
            return
    except Exception:
        pass
    text = ''
    try:
        if ext == '.pdf':
            text = extract_text_pdf(path, limit_chars=chunk_chars * max_chunks)
        elif ext == '.docx':
            text = extract_text_docx(path, limit_chars=chunk_chars * max_chunks)
        elif ext == '.rtf':
            text = extract_text_rtf(path, limit_chars=chunk_chars * max_chunks)
        elif ext == '.epub':
            text = extract_text_epub(path, limit_chars=chunk_chars * max_chunks)
        elif ext == '.djvu':
            text = extract_text_djvu(path, limit_chars=chunk_chars * max_chunks)
        else:
            text = path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        text = ''
    for chunk in _split_text_chunks(text, chunk_chars, max_chunks):
        yield chunk


def _deep_scan_file(file_obj: File, terms: list[str], raw_query: str, *, chunk_chars: int = 5000, max_chunks: int = 40, max_snippets: int = 3):
    if not file_obj or not file_obj.path:
        return {"hit": False, "snippets": [], "matched_terms": set(), "boost": 0.0, "sources": []}
    path = Path(file_obj.path)
    if not path.exists() or not path.is_file():
        return {"hit": False, "snippets": [], "matched_terms": set(), "boost": 0.0, "sources": []}
    term_map = {str(t).lower(): t for t in terms if t}
    raw_lower = (raw_query or '').strip().lower()
    phrase_pat = re.compile(re.escape(raw_lower), flags=re.IGNORECASE) if raw_lower and len(raw_lower) >= 3 else None
    snippets: list[str] = []
    matched: set[str] = set()
    sources_used: set[str] = set()
    # Дополнительные текстовые данные (транскрипты, резюме, подписи) из БД/тегов
    extra_texts: list[tuple[str, str]] = []
    try:
        if (file_obj.text_excerpt or '').strip():
            extra_texts.append(('excerpt', file_obj.text_excerpt))
        if (getattr(file_obj, 'abstract', None) or '').strip():
            extra_texts.append(('abstract', getattr(file_obj, 'abstract')))
        for tag in getattr(file_obj, 'tags', []) or []:
            try:
                key = str(getattr(tag, 'key', '') or '')
                val = str(getattr(tag, 'value', '') or '')
            except Exception:
                continue
            key_l = key.lower()
            if not val or len(val.strip()) < 40:
                continue
            if any(token in key_l for token in ('transcript', 'стенограм', 'caption', 'описан', 'vision', 'summary', 'ocr')):
                extra_texts.append((f'tag:{key}', val))
    except Exception:
        pass
    try:
        for chunk in _iter_document_chunks(path, chunk_chars=chunk_chars, max_chunks=max_chunks):
            if not chunk:
                continue
            chunk_lower = chunk.lower()
            chunk_hits = {orig for low, orig in term_map.items() if low and low in chunk_lower}
            phrase_hit = phrase_pat.search(chunk) if phrase_pat else None
            if not chunk_hits and not phrase_hit:
                continue
            sn = _collect_snippets(chunk, terms, max_snips=1)
            if sn:
                snippets.extend(sn)
            else:
                cleaned = re.sub(r"\s+", " ", chunk).strip()
                if cleaned:
                    snippets.append(cleaned[:300])
            matched.update(chunk_hits)
            sources_used.add('full-text')
            if len(snippets) >= max_snippets:
                break
    except Exception:
        pass
    # При необходимости обогащаем дополнительными текстовыми источниками (транскрипты, подписи, резюме)
    if len(snippets) < max_snippets:
        for label, text in extra_texts:
            if not text:
                continue
            sn = _collect_snippets(text, terms, max_snips=1)
            cleaned = ''
            if sn:
                for item in sn:
                    if item not in snippets:
                        snippets.append(item)
            else:
                cleaned = re.sub(r"\s+", " ", text).strip()
                if cleaned:
                    snippets.append(cleaned[:300])
            if (sn and sn[0]) or cleaned:
                sources_used.add(label)
            if len(snippets) >= max_snippets:
                break
    if not snippets:
        return {"hit": False, "snippets": [], "matched_terms": matched, "boost": 0.0, "sources": sorted(sources_used)}
    boost = 1.5 * min(len(snippets), max_snippets) + 0.4 * len(matched)
    return {
        "hit": True,
        "snippets": snippets[:max_snippets],
        "matched_terms": matched,
        "boost": boost,
        "sources": sorted(sources_used),
    }


# Базовый список русских/английских стоп-слов для исключения неинформативных токенов в ИИ-поиске
STOP_WORDS = set([
    # Русские местоимения, частицы, предлоги, союзы, служебные слова
    'и','или','а','но','же','то','ли','не','ни','да','уж','вот','как','так','что','кто','где','когда','зачем','почему','какой','какая','какие','каков','это','эта','этот','эти','того','тому','этом','этих','тех','там','тут','здесь','бы','либо','пусть','дабы','быть','есть','нет','между','через','после','перед','около','возле','у','к','ко','от','до','для','по','под','над','о','об','обо','при','без','из','из‑за','изза','с','со','же','ну','в','во','на','над','ё','же','уж','еще','ещё','также','тоже','сам','сама','сами','само','свой','своя','свои','своё','мой','моя','мои','моё','твой','твоя','твои','твоё','наш','наша','наши','наше','ваш','ваша','ваши','ваше','тот','та','то','те','эт','прочее','другой','другая','другие','иной','иная','иное','иные',
    # Английские
    'the','a','an','and','or','but','if','then','else','when','where','why','how','what','who','whom','whose','this','that','these','those','is','are','was','were','be','been','being','to','of','in','on','for','with','at','by','from','as','about','into','through','after','over','between','out','against','during','without','before','under','around','among','it','its','we','you','they','he','she','them','his','her','their','our','your','my','me','us','do','does','did','not','no','yes'
])



class _ProgressLogger:
    def __init__(self, emitter=None):
        self.lines: list[str] = []
        self.emitter = emitter

    def add(self, line: str) -> None:
        text = str(line)
        self.lines.append(text)
        try:
            app.logger.info(f"[ai-search] {text}")
        except Exception:
            pass
        if self.emitter:
            try:
                self.emitter(text)
            except Exception:
                pass



def _format_sse(payload: dict) -> str:
    try:
        return 'data: ' + json.dumps(payload, ensure_ascii=False) + '\n\n'
    except Exception:
        return 'data: {}\n\n'


def _tokenize_query(q: str) -> list[str]:
    s = (q or '').lower()
    # оставляем буквы, цифры, дефис, подчёркивание; разделяем по остальным символам
    parts = re.split(r"[^\w\-]+", s)
    filtered: list[str] = []
    for p in parts:
        if not p:
            continue
        if len(p) < 2:
            continue
        # отбрасываем токены, состоящие только из цифр
        if p.isdigit():
            continue
        # стоп-слова
        if p in STOP_WORDS:
            continue
        filtered.append(p)
    # удаляем дубликаты, сохраняя порядок
    seen = set()
    out: list[str] = []
    for p in filtered:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out[:16]


def _idf_for_terms(terms: list[str]) -> dict[str, float]:
    # вычисляем частоты документов по объединению полей файла и тегов
    idf: dict[str, float] = {}
    try:
        allowed = getattr(g, 'allowed_collection_ids', set())
        total_q = db.session.query(func.count(File.id))
        if allowed is not None:
            if not allowed:
                N = 1
            else:
                total_q = total_q.filter(File.collection_id.in_(allowed))
                N = total_q.scalar() or 1
        else:
            N = total_q.scalar() or 1
    except Exception:
        N = 1
    for w in terms:
        like = f"%{w}%"
        try:
            q = db.session.query(func.count(func.distinct(File.id))) \
                .outerjoin(Tag, Tag.file_id == File.id) \
                .filter(or_(
                    File.title.ilike(like),
                    File.author.ilike(like),
                    File.keywords.ilike(like),
                    File.text_excerpt.ilike(like),
                    File.abstract.ilike(like),
                    Tag.value.ilike(like),
                    Tag.key.ilike(like),
                ))
            if allowed is not None:
                if not allowed:
                    df = 0
                    idf[w] = 1.0
                    continue
                q = q.filter(File.collection_id.in_(allowed))
            df = int(q.scalar() or 0)
        except Exception:
            df = 0
        # сглаживание add-one
        val = float((1.0 + (N / (1.0 + df))))
        # логарифмическая шкала, минимум 1.0
        try:
            import math
            val = max(1.0, math.log(val + 1.0))
        except Exception:
            val = 1.0
        idf[w] = val
    return idf


def _ai_search_core(data: dict | None, progress_cb=None) -> dict:
    data = data or {}
    query = (data.get('query') or '').strip()
    if not query:
        raise ValueError("query is required")
    try:
        top_k = int(data.get('top_k') or 10)
    except Exception:
        top_k = 10
    original_top_k = top_k
    top_k = max(1, min(5, top_k))
    deep_search = bool(data.get('deep_search'))
    progress = _ProgressLogger(progress_cb)
    progress.add(f"Запрос: {query}")
    if top_k != original_top_k:
        progress.add(f"Запрошено Top K = {original_top_k}, ограничиваем до {top_k}")
    else:
        progress.add(f"Top K = {top_k}")
    progress.add("Режим: " + ("глубокий" if deep_search else "быстрый"))
    sources = data.get('sources') or {}
    use_tags = sources.get('tags', True) if isinstance(sources, dict) else True
    use_text = sources.get('text', True) if isinstance(sources, dict) else True
    progress.add(f"Источники: теги {'вкл' if use_tags else 'выкл'}, метаданные {'вкл' if use_text else 'выкл'}")
    # Необязательный фильтр по коллекциям (список id)
    if 'collection_id' in data and 'collection_ids' not in data:
        data['collection_ids'] = [data['collection_id']]
    try:
        collection_ids = [int(x) for x in (data.get('collection_ids') or []) if str(x).strip()]
    except Exception:
        collection_ids = []
    allowed = getattr(g, 'allowed_collection_ids', set())
    if allowed is not None:
        if collection_ids:
            collection_ids = [cid for cid in collection_ids if cid in allowed]
        else:
            collection_ids = list(allowed)
    # Необязательный фильтр по типам материалов (список строк)
    material_types = []
    try:
        material_types = [str(x).strip().lower() for x in (data.get('material_types') or []) if str(x).strip()]
    except Exception:
        material_types = []
    # Необязательный диапазон лет
    year_from = (data.get('year_from') or '').strip()
    year_to = (data.get('year_to') or '').strip()
    # Необязательные фильтры тегов: ["key=value", ...]
    tag_filters = []
    try:
        tag_filters = [str(x) for x in (data.get('tag_filters') or []) if '=' in str(x)]
    except Exception:
        tag_filters = []

    # Расширяем и токенизируем
    keywords = _ai_expand_keywords(query)
    base_tokens = _tokenize_query(query)
    extra_tokens = []
    for w in keywords:
        extra_tokens.extend(_tokenize_query(w))
    # уникальные термины (токены) с сохранением порядка, отдаём приоритет base_tokens
    seen = set()
    terms: list[str] = []
    for w in base_tokens + extra_tokens:
        if w and w not in seen:
            seen.add(w)
            terms.append(w)
    if not terms and query:
        # запасной вариант: используем исходный запрос как термин
        terms = _tokenize_query(query) or [query.lower()]
    if terms:
        preview = ', '.join(terms[:6])
        if len(terms) > 6:
            preview += '…'
        progress.add(f"Ключевые термины: {preview}")
    else:
        progress.add("Ключевые термины: не выделены")

    # Предварительно считаем IDF по каждому термину
    idf = _idf_for_terms(terms)

    # Накапливаем кандидатов с оценками и попаданиями
    scores: dict[int, float] = {}
    hits: dict[int, list[dict]] = {}

    term_hits: dict[int, set[str]] = {}
    def add_score(fid: int, delta: float, hit: dict | None = None, term: str | None = None):
        scores[fid] = scores.get(fid, 0.0) + float(delta)
        if hit:
            hits.setdefault(fid, []).append(hit)
        if term:
            s = term_hits.setdefault(fid, set())
            if term:
                s.add(term)

    # Совпадения по тегам
    if use_tags:
        for w in terms:
            like = f"%{w}%"
            try:
                q = db.session.query(Tag.file_id, Tag.key, Tag.value) \
                    .join(File, Tag.file_id == File.id) \
                    .join(Collection, File.collection_id == Collection.id) \
                    .filter(Collection.searchable == True) \
                    .filter(or_(Tag.value.ilike(like), Tag.key.ilike(like)))
                if collection_ids:
                    q = q.filter(File.collection_id.in_(collection_ids))
                if material_types:
                    q = q.filter(func.lower(File.material_type).in_(material_types))
                if year_from:
                    q = q.filter(File.year >= year_from)
                if year_to:
                    q = q.filter(File.year <= year_to)
                rows = q \
                     .limit(4000).all()
                for fid, k, v in rows:
                    add_score(fid, AI_SCORE_TAG * idf.get(w, 1.0), {"type": "tag", "key": k, "value": v, "term": w}, term=w)
            except Exception:
                pass
        progress.add(f"Теги: найдено кандидатов {len(scores)}")
    else:
        progress.add("Теги: пропущено (источник отключён)")

    # Совпадения по полям файла
    if use_text:
        for w in terms:
            like = f"%{w}%"
            try:
                q = db.session.query(File.id, File.title, File.author, File.keywords, File.text_excerpt, File.abstract) \
                    .join(Collection, File.collection_id == Collection.id) \
                    .filter(Collection.searchable == True) \
                    .filter(or_(
                        File.title.ilike(like),
                        File.author.ilike(like),
                        File.keywords.ilike(like),
                        File.text_excerpt.ilike(like),
                        File.abstract.ilike(like),
                    ))
                if collection_ids:
                    q = q.filter(File.collection_id.in_(collection_ids))
                if material_types:
                    q = q.filter(func.lower(File.material_type).in_(material_types))
                if year_from:
                    q = q.filter(File.year >= year_from)
                if year_to:
                    q = q.filter(File.year <= year_to)
                rows = q.limit(4000).all()
                for fid, title, author, kws, excerpt, abstract in rows:
                    if title and re.search(re.escape(w), title, flags=re.I):
                        add_score(fid, AI_SCORE_TITLE * idf.get(w, 1.0), {"type": "title", "term": w}, term=w)
                    if author and re.search(re.escape(w), author, flags=re.I):
                        add_score(fid, AI_SCORE_AUTHOR * idf.get(w, 1.0), {"type": "author", "term": w}, term=w)
                    if kws and re.search(re.escape(w), kws, flags=re.I):
                        add_score(fid, AI_SCORE_KEYWORDS * idf.get(w, 1.0), {"type": "keywords", "term": w}, term=w)
                    if excerpt and re.search(re.escape(w), excerpt, flags=re.I):
                        add_score(fid, AI_SCORE_EXCERPT * idf.get(w, 1.0), {"type": "excerpt", "term": w}, term=w)
                    if abstract and re.search(re.escape(w), abstract, flags=re.I):
                        add_score(fid, AI_SCORE_ABSTRACT * idf.get(w, 1.0), {"type": "abstract", "term": w}, term=w)
            except Exception:
                pass
        progress.add(f"Метаданные: найдено кандидатов {len(scores)}")
    else:
        progress.add("Метаданные: пропущено (источник отключён)")

    # Формируем результаты
    file_ids = list(scores.keys())
    results = []
    if file_ids:
        q_files = File.query.join(Collection, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True) \
            .filter(File.id.in_(file_ids))
        if collection_ids:
            q_files = q_files.filter(File.collection_id.in_(collection_ids))
        if material_types:
            q_files = q_files.filter(func.lower(File.material_type).in_(material_types))
        if year_from:
            q_files = q_files.filter(File.year >= year_from)
        if year_to:
            q_files = q_files.filter(File.year <= year_to)
        # Применяем фильтры тегов на финальном этапе
        for tf in tag_filters:
            try:
                k, v = tf.split('=', 1)
                k = (k or '').strip(); v = (v or '').strip()
                if not k or not v:
                    continue
                if k.lower() == 'author':
                    q_files = q_files.filter(File.author.ilike(f"%{v}%"))
                else:
                    tflt = aliased(Tag)
                    q_files = q_files.join(tflt, tflt.file_id == File.id).filter(and_(tflt.key == k, tflt.value.ilike(f"%{v}%")))
            except Exception:
                continue
        q_files = q_files.all()
        id2file = {f.id: f for f in q_files}
        for fid, sc in scores.items():
            f = id2file.get(fid)
            if not f:
                continue
            # усиление за точную фразу (исходный запрос как фраза)
            phrase_boost = 0.0
            qraw = query.strip()
            if len(qraw) >= 3:
                try:
                    pat = re.escape(qraw)
                    if f.title and re.search(pat, f.title, flags=re.I):
                        phrase_boost += AI_BOOST_PHRASE
                    if f.keywords and re.search(pat, f.keywords, flags=re.I):
                        phrase_boost += AI_BOOST_PHRASE * 0.6
                except Exception:
                    pass
            # усиление за покрытие разных терминов
            n_terms = len(term_hits.get(fid, set()))
            coverage_boost = max(0, n_terms - 1) * AI_BOOST_MULTI
            # сниппеты из кэшированного фрагмента/аннотации
            snips = []
            snippet_sources: list[str] = []
            try:
                text_cache = _read_cached_excerpt_for_file(f)
                snips = _collect_snippets(text_cache, terms, max_snips=2) if text_cache else []
                if snips:
                    snippet_sources.append('excerpt-cache')
            except Exception:
                snips = []
            if (not snips or len(snips) < 2) and getattr(f, 'abstract', None):
                try:
                    extra = _collect_snippets(getattr(f, 'abstract'), terms, max_snips=1)
                    added = False
                    for item in extra:
                        if item and item not in snips:
                            snips.append(item)
                            added = True
                    if added:
                        snippet_sources.append('abstract')
                except Exception:
                    pass
            # усиление за близость: несколько терминов в одном сниппете
            prox_boost = 0.0
            if snips:
                for s in snips:
                    present = 0
                    sl = s.lower()
                    for w in terms:
                        if w in sl:
                            present += 1
                    if present >= 2:
                        prox_boost += (present - 1) * AI_BOOST_SNIPPET_COOCCUR
            results.append({
                "file_id": fid,
                "rel_path": f.rel_path,
                "title": f.title or f.filename,
                "score": round(sc + phrase_boost + coverage_boost + prox_boost, 3),
                "hits": hits.get(fid, []),
                "snippets": snips,
                "snippet_sources": snippet_sources,
            })
        # сортируем по убыванию балла, затем по дате изменения
        results.sort(key=lambda x: (x.get('score') or 0.0, id2file.get(x['file_id']).mtime or 0.0), reverse=True)
        progress.add(f"Ранжирование: {len(results)} кандидатов")
        for idx, res in enumerate(results, start=1):
            cand_title = (res.get('title') or res.get('rel_path') or f"file-{res.get('file_id')}").strip()
            progress.add(f"Кандидат [{idx}]: {cand_title}")
        if deep_search and results:
            deep_limit = min(len(results), max(top_k * 2, 5))
            progress.add(f"Глубокий поиск по контенту: проверяем {deep_limit} файлов")
            for idx, res in enumerate(results[:deep_limit], start=1):
                f = id2file.get(res['file_id'])
                if not f:
                    continue
                scan = _deep_scan_file(f, terms, query, chunk_chars=5000, max_chunks=40, max_snippets=3)
                boost = float(scan.get('boost') or 0.0)
                if boost:
                    res['score'] = round((res.get('score') or 0.0) + boost, 3)
                matched_terms = scan.get('matched_terms') or set()
                if matched_terms:
                    res['matched_terms'] = sorted({str(t) for t in matched_terms})
                if scan.get('hit'):
                    new_snips = scan.get('snippets') or []
                    if new_snips:
                        base_snips = res.get('snippets') or []
                        combined = base_snips + [sn for sn in new_snips if sn not in base_snips]
                        res['snippets'] = combined[:3]
                    sources = scan.get('sources') or []
                    if sources:
                        current_sources = set(res.get('snippet_sources') or [])
                        for src in sources:
                            if src not in current_sources:
                                current_sources.add(src)
                        res['snippet_sources'] = sorted(current_sources)
                title = (f.title or f.filename or f.rel_path or f"file-{f.id}").strip()
                srcs = ", ".join(scan.get('sources') or [])
                status = 'совпадения' if scan.get('hit') else 'нет'
                if srcs:
                    status += f"; источники: {srcs}"
                progress.add(f"Глубокий поиск [{idx}/{deep_limit}]: {title} — {status}")
            results.sort(key=lambda x: (x.get('score') or 0.0, id2file.get(x['file_id']).mtime or 0.0), reverse=True)
        results = results[:top_k]
        progress.add(f"Отобрано документов: {len(results)}")
        for idx, res in enumerate(results, start=1):
            title = (res.get('title') or res.get('rel_path') or f"file-{res.get('file_id')}").strip()
            sources = res.get('snippet_sources') or []
            if not sources:
                summary = 'не найдены'
            else:
                summary = ', '.join(sources)
            progress.add(f"Снипеты [{idx}] {title}: {summary}")
    else:
        progress.add("Совпадения не найдены")

    # Необязательный короткий ответ, используя сниппеты как контекст (поисковый промпт)
    answer = ""
    if results:
        try:
            topn = results[:10]
            lines = []
            for i, r in enumerate(topn):
                sn = " ".join((r.get('snippets') or []))[:400]
                title = r.get('title') or r.get('rel_path') or f"file-{r.get('file_id')}"
                lines.append(f"[{i+1}] {title}: {sn}")
            progress.add(f"LLM ответ: используем {len(topn)} фрагментов, top_k={top_k}, deep_search={'on' if deep_search else 'off'}")
            system = (
                "Ты помощник поиска. Сформулируй краткий, фактический ответ на вопрос пользователя, "
                "используя ТОЛЬКО предоставленные фрагменты. Не выдумывай и не обобщай сверх текста. "
                "Ссылайся на источники квадратными скобками [n] там, где берешь факт. Не упоминай слова 'стенограмма' или подобные."
            )
            user_msg = f"Вопрос: {query}\nФрагменты:\n" + "\n".join(lines)
            answer = (call_lmstudio_compose(system, user_msg, temperature=0.1, max_tokens=350) or "").strip()
            if answer:
                progress.add(f"LLM ответ готов ({len(answer)} символов)")
            else:
                progress.add("LLM ответ пуст")
        except Exception:
            answer = ""
            progress.add("LLM ответ: ошибка генерации")

    # Необязательно: лёгкое реранжирование топ-15 через LLM с контекстом сниппетов
    if AI_RERANK_LLM and results:
        try:
            top = results[:15]
            prompt_lines = [f"[{i+1}] id={it['file_id']} :: { (it.get('snippets') or [''])[0] }" for i, it in enumerate(top)]
            prompt = "\n".join(prompt_lines)
            sys = "Ты ранжируешь источники по релевантности к запросу. Верни JSON-массив id в порядке убывания релевантности."
            user = f"Запрос: {query}\nИсточники:\n{prompt}\nОтвети только JSON массивом id."
            order = None
            last_error: Exception | None = None
            for choice in _llm_iter_choices('rerank'):
                label = _llm_choice_label(choice)
                url = _llm_choice_url(choice)
                if not url:
                    app.logger.warning(f"LLM rerank endpoint некорректен ({label}): пустой URL")
                    continue
                payload = {
                    "model": choice.get('model') or LMSTUDIO_MODEL,
                    "messages": [
                        {"role": "system", "content": sys},
                        {"role": "user", "content": user},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 200,
                }
                try:
                    rr = requests.post(url, headers=_llm_choice_headers(choice), json=payload, timeout=60)
                    if _llm_response_indicates_busy(rr):
                        app.logger.info(f"LLM rerank endpoint занята ({label}), переключаемся")
                        last_error = RuntimeError('busy')
                        continue
                    rr.raise_for_status()
                    dd = rr.json()
                    content = (dd.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
                    try:
                        order = json.loads(content)
                    except Exception:
                        m = re.search(r"\[(?:\s*\d+\s*,?\s*)+\]", content)
                        if m:
                            order = json.loads(m.group(0))
                    if order is not None:
                        break
                except Exception as e:
                    last_error = e
                    app.logger.warning(f"LLM rerank failed ({label}): {e}")
                    continue
            if order is None and last_error and str(last_error) != 'busy':
                app.logger.warning(f"LLM rerank failed: {last_error}")
            if isinstance(order, list) and all(isinstance(x, int) for x in order):
                pos = {int(fid): i for i, fid in enumerate(order)}
                results.sort(key=lambda x: (pos.get(int(x['file_id']), 10**6), -(x.get('score') or 0.0)))
                progress.add("Реранжирование LLM: порядок обновлён")
        except Exception:
            pass

    return {
        "query": query,
        "keywords": terms,
        "answer": answer,
        "items": results,
        "progress": progress.lines,
    }


@app.route('/api/ai-search', methods=['POST'])
def api_ai_search():
    data = request.get_json(silent=True) or {}
    user = _load_current_user()
    query_preview = str(data.get('query') or '').strip()[:200]
    detail_obj = {
        'query_preview': query_preview,
        'top_k': data.get('top_k'),
        'deep_search': data.get('deep_search'),
        'sources': data.get('sources'),
    }
    try:
        detail_payload = json.dumps(detail_obj, ensure_ascii=False)
    except Exception:
        detail_payload = str(detail_obj)
    try:
        _log_user_action(user, 'ai_search', 'search', None, detail=detail_payload[:2000])
    except Exception:
        pass
    try:
        app.logger.info(
            "[user-action] user=%s action=ai_search query_preview=%s top_k=%s deep_search=%s",
            getattr(user, 'username', None) or 'anonymous',
            query_preview,
            detail_obj.get('top_k'),
            detail_obj.get('deep_search')
        )
    except Exception:
        pass
    try:
        result = _ai_search_core(data)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("AI search failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, **result})


@app.route('/api/ai-search/stream', methods=['POST'])
def api_ai_search_stream():
    data = request.get_json(silent=True) or {}
    user = _load_current_user()
    query_preview = str(data.get('query') or '').strip()[:200]
    detail_obj = {
        'query_preview': query_preview,
        'top_k': data.get('top_k'),
        'deep_search': data.get('deep_search'),
        'sources': data.get('sources'),
        'stream': True,
    }
    try:
        detail_payload = json.dumps(detail_obj, ensure_ascii=False)
    except Exception:
        detail_payload = str(detail_obj)
    try:
        _log_user_action(user, 'ai_search_stream', 'search', None, detail=detail_payload[:2000])
    except Exception:
        pass
    try:
        app.logger.info(
            "[user-action] user=%s action=ai_search_stream query_preview=%s top_k=%s deep_search=%s",
            getattr(user, 'username', None) or 'anonymous',
            query_preview,
            detail_obj.get('top_k'),
            detail_obj.get('deep_search')
        )
    except Exception:
        pass
    queue: Queue = Queue()

    def emit(line: str) -> None:
        queue.put({'type': 'progress', 'line': line})

    def worker() -> None:
        try:
            result = _ai_search_core(data, progress_cb=emit)
            queue.put({'type': 'result', 'payload': result})
        except ValueError as exc:
            queue.put({'type': 'error', 'message': str(exc), 'status': 400})
        except Exception as exc:
            logger.exception("AI search stream failed", exc_info=True)
            queue.put({'type': 'error', 'message': str(exc), 'status': 500})
        finally:
            queue.put({'type': 'done'})

    worker_wrapped = copy_current_app_context(worker)
    threading.Thread(target=worker_wrapped, daemon=True).start()

    def event_stream():
        while True:
            event = queue.get()
            typ = event.get('type')
            if typ == 'progress':
                yield _format_sse({'type': 'progress', 'line': event.get('line')})
            elif typ == 'result':
                payload = event.get('payload') or {}
                yield _format_sse({'type': 'result', 'payload': payload})
            elif typ == 'error':
                payload = {'type': 'error', 'message': event.get('message', 'unknown')}
                status = event.get('status')
                if status:
                    payload['status'] = status
                yield _format_sse(payload)
            elif typ == 'done':
                yield 'data: [DONE]\n\n'
                break

    return Response(stream_with_context(event_stream()), mimetype='text/event-stream')


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_collections_schema()
    app.run(host="0.0.0.0", port=5050, debug=False, use_reloader=False, threaded=True)
