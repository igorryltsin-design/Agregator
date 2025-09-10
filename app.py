# REST API and import/export routes moved to `routes.py` as a Blueprint.
import os
import re
import json
import hashlib
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import sqlite3
import threading
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import aliased
from models import db, File, Tag, TagSchema, ChangeLog, upsert_tag, file_to_dict

import fitz  # PyMuPDF
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
    import pytesseract  # optional OCR for image-based PDFs
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

# Simple in‑memory cache for AI keyword expansions
AI_EXPAND_CACHE: dict[str, tuple[float, list[str]]] = {}

# Scoring weights and options (tunable via env)
def _getf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

AI_SCORE_TITLE = _getf('AI_SCORE_TITLE', 2.5)
AI_SCORE_AUTHOR = _getf('AI_SCORE_AUTHOR', 1.5)
AI_SCORE_KEYWORDS = _getf('AI_SCORE_KEYWORDS', 1.2)
AI_SCORE_EXCERPT = _getf('AI_SCORE_EXCERPT', 1.0)
AI_SCORE_TAG = _getf('AI_SCORE_TAG', 1.0)
AI_BOOST_PHRASE = _getf('AI_BOOST_PHRASE', 3.0)
AI_BOOST_MULTI = _getf('AI_BOOST_MULTI', 0.6)  # extra per additional distinct term
AI_BOOST_SNIPPET_COOCCUR = _getf('AI_BOOST_SNIPPET_COOCCUR', 0.8)

def _now() -> float:
    return time.time()

def _sha256(s: str) -> str:
    try:
        return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()
    except Exception:
        return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

# ------------------- Configuration -------------------

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
DEFAULT_USE_LLM = getenv_bool("DEFAULT_USE_LLM", False)
DEFAULT_PRUNE = getenv_bool("DEFAULT_PRUNE", True)

LMSTUDIO_API_BASE = os.getenv("LMSTUDIO_API_BASE", "http://localhost:1234/v1")
# Default LLM model
LMSTUDIO_MODEL = os.getenv("LMSTUDIO_MODEL", "google/gemma-3n-e4b")
LMSTUDIO_API_KEY = os.getenv("LMSTUDIO_API_KEY", "")
TRANSCRIBE_ENABLED = getenv_bool("TRANSCRIBE_ENABLED", True)
TRANSCRIBE_BACKEND = os.getenv("TRANSCRIBE_BACKEND", "faster-whisper")
TRANSCRIBE_MODEL_PATH = os.getenv("TRANSCRIBE_MODEL_PATH", os.getenv('FASTER_WHISPER_DEFAULT_MODEL', 'small'))
TRANSCRIBE_LANGUAGE = os.getenv("TRANSCRIBE_LANGUAGE", "ru")
IMAGES_VISION_ENABLED = getenv_bool("IMAGES_VISION_ENABLED", False)
KEYWORDS_TO_TAGS_ENABLED = getenv_bool("KEYWORDS_TO_TAGS_ENABLED", True)
# Order of pre-LLM type detection steps
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
# AI rerank (placed here after getenv_bool is defined)
AI_RERANK_LLM = getenv_bool('AI_RERANK_LLM', False)

# Куда сохранять загруженные файлы: подпапка внутри SCAN_ROOT
# По умолчанию используем 'import' (можно поменять в Настройках)
IMPORT_SUBDIR = os.getenv("IMPORT_SUBDIR", "import").strip().strip("/\\")

# Перемещать ли файл в подпапку по типу при переименовании
MOVE_ON_RENAME = getenv_bool("MOVE_ON_RENAME", True)

# Карта подпапок по типам материалов (относительно SCAN_ROOT)
TYPE_DIRS = {
    "dissertation": "dissertations",
    "dissertation_abstract": "dissertations",
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
# Simple keywords extraction for audio transcripts via LLM (lightweight prompt)
AUDIO_KEYWORDS_LLM = getenv_bool("AUDIO_KEYWORDS_LLM", True)

# Cache dir for faster-whisper models (when auto-downloading by alias)
FW_CACHE_DIR = Path(os.getenv("FASTER_WHISPER_CACHE_DIR", str((Path(__file__).parent / "models" / "faster-whisper").resolve())))

# ------------------- Helpers -------------------

def _normalize_author(val):
    """Convert various author representations (list/dict/etc.) to a string.
    Returns None for empty results.
    """
    try:
        if val is None:
            return None
        # list/tuple of names or dicts
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
        # single dict like {first,last} or {name}
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

# --------- Richer tag extraction (general + type-specific) ---------
def extract_richer_tags(material_type: str, text: str, filename: str = "") -> dict:
    t = (text or "")
    tl = t.lower()
    tags: dict[str, str] = {}
    # language guess
    try:
        cyr = sum(1 for ch in t if ('а' <= ch.lower() <= 'я') or (ch in 'ёЁ'))
        lat = sum(1 for ch in t if 'a' <= ch.lower() <= 'z')
        if cyr + lat > 20:
            tags['lang'] = 'ru' if cyr >= lat else 'en'
    except Exception:
        pass
    # common identifiers
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
        # specialty code like 05.13.11
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
    # Common aliases
    alias = {
        'tiny': 'Systran/faster-whisper-tiny',
        'base': 'Systran/faster-whisper-base',
        'small': 'Systran/faster-whisper-small',
        'medium': 'Systran/faster-whisper-medium',
        'large-v2': 'Systran/faster-whisper-large-v2',
        'large-v3': 'Systran/faster-whisper-large-v3',
        # English distilled variants
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
        # Compute target dir inside cache
        FW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = repo.replace('/', '__')
        target_dir = FW_CACHE_DIR / safe_name
        if not target_dir.exists() or not any(target_dir.iterdir()):
            # download/snapshot into target_dir
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
db.init_app(app)


# Модели описаны в models.py и импортируются выше.

# Инициализация конфигурации для перемещения файлов
# На старте используем SCAN_ROOT как корневую папку (UPLOAD_FOLDER задаётся ниже)
app.config.setdefault('UPLOAD_FOLDER', str(SCAN_ROOT))
app.config.setdefault('IMPORT_SUBDIR', IMPORT_SUBDIR)
app.config.setdefault('MOVE_ON_RENAME', MOVE_ON_RENAME)
app.config.setdefault('TYPE_DIRS', TYPE_DIRS)

# ------------------- Utilities -------------------

ALLOWED_EXTS = {".pdf", ".txt", ".md", ".docx", ".rtf", ".mp3", ".wav", ".m4a", ".flac", ".ogg",
                ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".flac", ".ogg"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

UPLOAD_FOLDER = BASE_DIR / "sample_library"
app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)

# ------------------- Routes -------------------

@app.after_request
def _force_utf8(resp):
    try:
        ct = (resp.headers.get('Content-Type') or '').lower()
        if ct.startswith('text/html'):
            resp.headers['Content-Type'] = 'text/html; charset=utf-8'
        elif ct.startswith('application/json'):
            # ensure utf-8 for JSON too
            resp.headers['Content-Type'] = 'application/json; charset=utf-8'
        return resp
    except Exception:
        return resp

# Загрузка файлов через веб-интерфейс
@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("Файл не выбран.", "danger")
            return redirect(request.url)
        ext = Path(file.filename).suffix.lower()
        if ext not in ALLOWED_EXTS:
            flash(f"Недопустимый тип файла: {ext}", "danger")
            return redirect(request.url)
        # Сохраняем в общую папку импорта внутри корня
        base_dir = SCAN_ROOT / IMPORT_SUBDIR if (IMPORT_SUBDIR or '').strip() else SCAN_ROOT
        save_path = base_dir / file.filename
        # Избежать перезаписи
        i = 1
        orig_name = Path(file.filename).stem
        while save_path.exists():
            save_path = UPLOAD_FOLDER / f"{orig_name}_{i}{ext}"
            i += 1
        file.save(save_path)
        flash("Файл успешно загружен.", "success")
        return redirect(url_for("index"))
    return render_template("upload.html")

# Утилита: безопасная относительная дорожка (для загрузки папок)
def _sanitize_relpath(p: str) -> str:
    p = (p or '').replace('\\', '/').lstrip('/')
    parts = [seg for seg in p.split('/') if seg not in ('', '.', '..')]
    return '/'.join(parts)

@app.route('/import', methods=['GET', 'POST'])
def import_files():
    """Импорт нескольких файлов и папок (через webkitdirectory) с возможным автосканом."""
    if request.method == 'POST':
        files = request.files.getlist('files')
        if not files:
            flash('Файлы не выбраны.', 'warning')
            return redirect(url_for('import_files'))
        saved = 0
        skipped = 0
        saved_paths = []
        # Базовая директория импорта: SCAN_ROOT/IMPORT_SUBDIR (если задана), иначе SCAN_ROOT
        base_dir = SCAN_ROOT / IMPORT_SUBDIR if (IMPORT_SUBDIR or '').strip() else SCAN_ROOT
        for fs in files:
            raw_name = fs.filename or ''
            rel = _sanitize_relpath(raw_name)
            ext = Path(rel).suffix.lower()
            if not rel or ext not in ALLOWED_EXTS:
                skipped += 1
                continue
            dest = base_dir / rel
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                fs.save(dest)
                saved += 1
                saved_paths.append(str(dest))
            except Exception as e:
                app.logger.warning(f'Failed to save {rel}: {e}')
                skipped += 1

        flash(f'Загружено: {saved}, пропущено: {skipped}.', 'success' if saved else 'warning')

        # Опционально сразу запустить скан
        start_scan = request.form.get('start_scan') == 'on'
        extract_text = request.form.get('extract_text', 'on') == 'on'
        use_llm = request.form.get('use_llm') == 'on'
        prune = request.form.get('prune', 'on') == 'on'
        if start_scan and saved_paths:
            # Запуск фонового скана только по загруженным файлам
            t = threading.Thread(target=_run_scan_with_progress, args=(extract_text, use_llm, prune, 0, saved_paths), daemon=True)
            t.start()
            flash('Сканирование запущено.', 'info')
            return redirect(url_for('settings'))
        return redirect(url_for('index'))

    # GET
    return render_template('import.html', allowed_exts=', '.join(sorted(ALLOWED_EXTS)))

# Просмотр и скачивание файлов
@app.route("/download/<path:rel_path>")
def download_file(rel_path):
    base_dir = Path(app.config.get('UPLOAD_FOLDER') or '.')
    abs_path = base_dir / rel_path
    # Try to resolve DB record early and provide a fallback if file was moved
    try:
        rp = str(rel_path)
        rp_alt = rp.replace('/', '\\') if ('/' in rp) else rp.replace('\\', '/')
        f = File.query.filter(or_(File.rel_path == rp, File.rel_path == rp_alt)).first()
    except Exception:
        f = None
    if not abs_path.exists() and f is not None:
        try:
            filename_only = Path(f.rel_path or rp).name
            type_dirs = app.config.get('TYPE_DIRS') or {}
            sub = type_dirs.get((f.material_type or '').strip().lower())
            candidates = []
            if sub:
                candidates.append(base_dir / sub / filename_only)
            for v in set(type_dirs.values()):
                candidates.append(base_dir / v / filename_only)
            candidates.append(base_dir / filename_only)
            for cand in candidates:
                if cand.exists():
                    abs_path = cand
                    try:
                        f.path = str(cand)
                        f.rel_path = str(cand.relative_to(base_dir))
                        db.session.commit()
                    except Exception:
                        db.session.rollback()
                    # update rel_path for downstream URLs
                    rel_path = f.rel_path
                    break
            # As a last resort, search by filename across base_dir (can be slow)
            if not abs_path.exists():
                try:
                    for cand in base_dir.rglob(filename_only):
                        if cand.is_file():
                            abs_path = cand
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
    if not abs_path.exists():
        flash("Файл не найден.", "danger")
        return redirect(url_for("index"))
    return send_from_directory(str(base_dir), rel_path, as_attachment=True)

@app.route("/media/<path:rel_path>")
def media_file(rel_path):
    """Отдавать файл встраиваемо (для аудиоплеера в предпросмотре), без принудительного скачивания."""
    base_dir = Path(app.config.get('UPLOAD_FOLDER') or '.')
    abs_path = base_dir / rel_path
    if not abs_path.exists():
        # Inline 404 keeps iframe/player flow simple
        return ("Not Found", 404)
    return send_from_directory(str(base_dir), rel_path, as_attachment=False)

@app.route("/view/<path:rel_path>")
def view_file(rel_path):
    base_dir = Path(app.config.get('UPLOAD_FOLDER') or '.')
    abs_path = base_dir / rel_path
    if not abs_path.exists():
        flash("Файл не найден.", "danger")
        return redirect(url_for("index"))
    ext = abs_path.suffix.lower()
    if ext == ".pdf":
        return send_from_directory(str(base_dir), rel_path)
    elif ext in {".txt", ".md"}:
        try:
            content = abs_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            content = "Не удалось прочитать файл."
        return render_template("view_text.html", filename=rel_path, content=content)
    else:
        flash("Просмотр этого типа файлов не поддерживается.", "warning")
        return redirect(url_for("index"))


@app.route('/preview/<path:rel_path>')
def preview_file(rel_path):
    base_dir = Path(app.config.get('UPLOAD_FOLDER') or '.')
    abs_path = base_dir / rel_path
    if not abs_path.exists():
        flash('Файл не найден.', 'danger')
        return redirect(url_for('index'))

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
        # key by sha1 when possible
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
            # for audio, prefer DB fields later
            try:
                cache_file.write_text(content, encoding='utf-8')
            except Exception:
                pass
    except Exception:
        content = ''

    # Пробуем найти запись File, чтобы получить id для ссылки «Подробнее» и ключевые слова
    # Resolve DB record by rel_path; handle slash/backslash differences (Windows)
    rp = str(rel_path)
    rp_alt = rp.replace('/', '\\') if ('/' in rp) else rp.replace('\\', '/')
    f = File.query.filter(or_(File.rel_path == rp, File.rel_path == rp_alt)).first()
    file_id = f.id if f else None
    keywords_str = (f.keywords or '') if f else ''
    if f and (is_audio or (f.material_type or '') == 'audio'):
        is_audio = True
        abstract = (f.abstract or '')
        # If no cached content, use transcript excerpt from DB
        if not content:
            content = (f.text_excerpt or '')[:4000]
        # audio player points to download endpoint
        # use URL-friendly rel path (forward slashes)
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

    # Для embedded=1 отдаём минимальный шаблон для iframe (без навбара)
    rel_url = str(rel_path).replace('\\','/')
    if request.args.get('embedded') in ('1', 'true', 'yes'):
        return render_template('preview_embedded.html', filename=rel_url, is_pdf=is_pdf, is_text=is_text, is_audio=is_audio, is_image=is_image, content=content, thumbnail_url=thumbnail_url, file_id=file_id, abstract=abstract, audio_url=audio_url, duration=duration, image_url=image_url, keywords=keywords_str)
    return render_template('preview.html', filename=rel_url, is_pdf=is_pdf, is_text=is_text, is_audio=is_audio, is_image=is_image, content=content, thumbnail_url=thumbnail_url, file_id=file_id, abstract=abstract, audio_url=audio_url, duration=duration, image_url=image_url, keywords=keywords_str)

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

def extract_text_pdf(fp: Path, limit_chars=40000):
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
            if idx < max_ocr_pages and pytesseract is not None and len(raw.strip()) < 30:
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
        # Fallback: если и после прохода текст совсем короткий — OCR до 5 страниц без условия длины
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
    if not LMSTUDIO_API_BASE:
        return ""
    text = (text or "")[: int(os.getenv("SUMMARY_TEXT_LIMIT", "12000"))]
    if not text:
        return ""
    system = PROMPTS.get('summarize_audio_system') or (
        "Ты помощник. Суммаризируй стенограмму аудио в 3–6 предложениях на русском, "
        "выделив тему, основные тезисы и вывод."
    )
    user = f"Файл: {filename}\nСтенограмма:\n{text}"
    try:
        base = LMSTUDIO_API_BASE.rstrip('/')
        url = base + "/chat/completions" if not base.endswith("/chat/completions") else base
        headers = {"Content-Type": "application/json"}
        if LMSTUDIO_API_KEY:
            headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"
        payload = {"model": LMSTUDIO_MODEL, "messages": [{"role":"system","content":system},{"role":"user","content":user}], "temperature": 0.2, "max_tokens": 400, "top_p": 1.0}
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        app.logger.warning(f"Суммаризация не удалась: {e}")
        return ""

def call_lmstudio_compose(system: str, user: str, *, temperature: float = 0.2, max_tokens: int = 400) -> str:
    if not LMSTUDIO_API_BASE:
        return ""
    try:
        base = LMSTUDIO_API_BASE.rstrip('/')
        url = base + "/chat/completions" if not base.endswith("/chat/completions") else base
        headers = {"Content-Type": "application/json"}
        if LMSTUDIO_API_KEY:
            headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"
        payload = {
            "model": LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
            "top_p": 1.0,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        app.logger.warning(f"LM Studio compose failed: {e}")
        return ""
def call_lmstudio_keywords(text: str, filename: str):
    """Извлечь короткий список ключевых слов из стенограммы через LM Studio.
    Быстрое извлечение: низкая температура и лаконичный ответ. Возвращает list[str].
    """
    if not LMSTUDIO_API_BASE:
        return []
    text = (text or "").strip()
    if not text:
        return []
    # Ограничим объём стенограммы в запросе
    text = text[:int(os.getenv("KWS_TEXT_LIMIT", "8000"))]
    system = PROMPTS.get('keywords_system') or (
        "Ты извлекаешь ключевые слова из стенограммы аудио. Верни только JSON-массив строк на русском: "
        "[\"ключ1\", \"ключ2\", ...]. Без пояснений, не более 12 слов/фраз."
    )
    user = f"Файл: {filename}\nСтенограмма:\n{text}"
    try:
        base = LMSTUDIO_API_BASE.rstrip('/')
        url = base + "/chat/completions" if not base.endswith("/chat/completions") else base
        headers = {"Content-Type": "application/json"}
        if LMSTUDIO_API_KEY:
            headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"
        payload = {
            "model": LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.0,
            "max_tokens": 200,
            "top_p": 1.0,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=90)
        r.raise_for_status()
        data = r.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        # Попытка разобрать сразу JSON-массив
        try:
            obj = json.loads(content)
            if isinstance(obj, list):
                return [str(x) for x in obj][:12]
        except Exception:
            pass
        # Попытка достать массив из блока ```json ... ```
        m = re.search(r"```json\s*(\[.*?\])\s*```", content, flags=re.S)
        if m:
            try:
                obj = json.loads(m.group(1))
                if isinstance(obj, list):
                    return [str(x) for x in obj][:12]
            except Exception:
                pass
        # Резерв: разбиение по запятым/точкам с запятой/переносам строк
        rough = re.split(r"[\n;,]", content)
        res = [w.strip(" \t\r\n-•") for w in rough if w.strip()]
        return res[:12]
    except Exception as e:
        app.logger.warning(f"Извлечение ключевых слов (LLM) не удалось: {e}")
        return []

def call_lmstudio_vision(image_path: Path, filename: str):
    """Распознавание и описание изображения через LM Studio (совместимый с OpenAI Vision).
    Возвращает dict: { description: str, keywords: list[str] }.
    Используем base64 data URL, чтобы не требовать внешний HTTP‑доступ.
    """
    try:
        if not LMSTUDIO_API_BASE:
            return {}
        import base64
        mime = "image/png"
        suf = image_path.suffix.lower()
        if suf in (".jpg", ".jpeg"): mime = "image/jpeg"
        elif suf in (".webp",): mime = "image/webp"
        elif suf in (".bmp",): mime = "image/bmp"
        elif suf in (".tif", ".tiff"): mime = "image/tiff"
        raw = image_path.read_bytes()
        data_url = f"data:{mime};base64," + base64.b64encode(raw).decode('ascii')

        system = PROMPTS.get('vision_system') or (
            "Ты помощник по анализу изображений. Опиши изображение 2–4 предложениями на русском и верни 5–12 ключевых слов. "
            "Верни строго JSON: {\\\"description\\\":\\\"...\\\", \\\"keywords\\\":[\\\"...\\\"]}."
        )
        user_content = [
            {"type": "text", "text": f"Файл: {filename}. Опиши и укажи ключевые слова."},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]
        base = LMSTUDIO_API_BASE.rstrip('/')
        url = base + "/chat/completions" if not base.endswith("/chat/completions") else base
        headers = {"Content-Type": "application/json"}
        if LMSTUDIO_API_KEY:
            headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"
        payload = {
            "model": LMSTUDIO_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2,
            "max_tokens": 500,
            "top_p": 1.0,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        # вспомогательная: извлечь ключевые слова из свободного текста
        def _extract_kws_from_text(txt: str):
            try:
                t = (txt or '').replace('*', ' ')
                m = re.search(r"(?i)ключ[^\n\r:]*?:\s*(.+)", t)
                if m:
                    line = m.group(1).strip()
                    # обрезаем по переносам/двум пробелам/двум точкам
                    line = re.split(r"\n|\r|\u2028|\u2029|\s{2,}", line)[0]
                    parts = [p.strip(" \t\r\n,.;·•-") for p in line.split(',')]
                    parts = [p for p in parts if p]
                    # вырежем блок из описания
                    cleaned = re.sub(r"(?i)ключ[^\n\r:]*?:.*", "", t)
                    return parts[:16], cleaned.strip()
            except Exception:
                return [], txt
            return [], txt
        try:
            obj = json.loads(content)
            if isinstance(obj, dict):
                # нормализуем keywords
                kws_list = obj.get("keywords") if isinstance(obj.get("keywords"), list) else []
                if not kws_list:
                    # попробуем вытащить из description
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
        # Фолбэк: описание из текста + ключевые слова по шаблону "Ключевые слова: ..."
        kws_guess, cleaned = _extract_kws_from_text(content or '')
        return {"description": cleaned.strip()[:2000], "keywords": kws_guess}
    except Exception as e:
        app.logger.warning(f"Визуальное распознавание не удалось: {e}")
        return {}

def call_lmstudio_for_metadata(text: str, filename: str):
    """
    Вызов OpenAI-совместимого API (LM Studio) для извлечения метаданных.
    Возвращает dict. Терпимо относится к не-JSON ответам: пытается вытащить из ```json ...``` блока.
    """
    if not LMSTUDIO_API_BASE:
        return {}

    text = (text or "")[: int(os.getenv("LLM_TEXT_LIMIT", "15000"))]

    system = PROMPTS.get('metadata_system') or (
        "Ты помощник по каталогизации научных материалов. "
        "Верни ТОЛЬКО валидный JSON без пояснений. "
        "Ключи: material_type, title, author, year, advisor, keywords (array), "
        "novelty (string), literature (array), organizations (array), classification (array)."
    )
    user = f"Файл: {filename}\nФрагмент текста:\n{text}"

    base = LMSTUDIO_API_BASE.rstrip("/")
    url = base + "/chat/completions" if not base.endswith("/chat/completions") else base

    headers = {"Content-Type": "application/json"}
    if LMSTUDIO_API_KEY:
        headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"

    payload = {
        "model": LMSTUDIO_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.0,
        "max_tokens": 800,
        "top_p": 1.0
        # Важное: без response_format — многие локальные серверы его не понимают
    }

    # Добавим простой retry с экспоненциальной задержкой и расширенным логированием
    max_retries = 3
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=120)
            # Логируем статус и короткий фрагмент тела для диагностики
            text_snippet = (r.text or '')[:2000]
            if r.status_code != 200:
                app.logger.warning(f"LM Studio HTTP {r.status_code} (попытка {attempt}): {text_snippet}")
                r.raise_for_status()
            data = r.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

            # Попробуем сразу JSON
            try:
                return json.loads(content)
            except Exception:
                pass

            # Попробуем из блока ```json ... ```
            m = re.search(r"```json\s*(\{.*?\})\s*```", content, flags=re.S)
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    app.logger.warning("Не удалось разобрать JSON внутри блока ```json из LM Studio")

            # Попробуем от первой фигурной скобки
            m = re.search(r"(\{.*\})", content, flags=re.S)
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    app.logger.warning("Не удалось разобрать JSON‑фрагмент из ответа LM Studio")

            app.logger.warning(f"LLM вернул не‑JSON контент (первые 300 символов): {content[:300]}")
            return {}

        except requests.exceptions.RequestException as e:
            app.logger.warning(f"Исключение при запросе к LM Studio (попытка {attempt}): {e}")
            if attempt < max_retries:
                import time
                time.sleep(backoff)
                backoff *= 2
                continue
            return {}
        except ValueError as e:
            # Ошибка парсинга JSON ответа
            app.logger.warning(f"LM Studio returned invalid JSON (attempt {attempt}): {e}")
            return {}
        except Exception as e:
            app.logger.warning(f"Unexpected error calling LM Studio (attempt {attempt}): {e}")
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
    # try direct mapping
    if s in mapping:
        return mapping[s]
    # partial matches
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
    # Additional types
    # Monograph
    if any(k in tl for k in ["монография", "monograph"]):
        return "monograph"
    # Standards (ГОСТ/ISO/IEC/СТО/СП/СанПиН/ТУ)
    if any(k in tl for k in ["гост", "gost", "iso", "iec", "стб", "сто ", " санпин", " сп ", "ту "]):
        return "standard"
    # Proceedings / conference
    if any(k in tl for k in ["материалы конференции", "сборник трудов", "proceedings", "conference", "symposium", "workshop"]):
        return "proceedings"
    # Patent
    if any(k in tl for k in ["патент", "patent", "mpk", "ipc"]):
        return "patent"
    # Report / internal docs
    if any(k in tl for k in ["отчет", "отчёт", "техническое задание", "пояснительная записка", "technical specification"]):
        return "report"
    # Presentation
    if any(k in tl for k in ["презентация", "slides", "powerpoint", "слайды"]):
        return "presentation"
    # Монография
    if "монограф" in tl:
        return "monograph"
    # Note
    if ext in {".md", ".txt"}:
        return "note"
    return "document"

def _detect_type_pre_llm(ext: str, text_excerpt: str, filename: str) -> str | None:
    flow = [p.strip() for p in (TYPE_DETECT_FLOW or '').split(',') if p.strip()]
    ext = (ext or '').lower()
    # filename-based guess helper and conflict resolver
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
    # filename-based cues before heuristics
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

def extract_tags_for_type(material_type: str, text: str, filename: str = "") -> dict:
    """Извлечение тегов по типу материала простыми регулярками.
    Возвращает dict: {key: value}.
    """
    t = (text or "")
    tl = t.lower()
    tags = {}
    # General
    # Language guess (rough): Cyrillic vs Latin share
    try:
        cyr = sum(1 for ch in t if ('а' <= ch.lower() <= 'я') or (ch in 'ёЁ'))
        lat = sum(1 for ch in t if 'a' <= ch.lower() <= 'z')
        if cyr + lat > 20:
            tags.setdefault('lang', 'ru' if cyr >= lat else 'en')
    except Exception:
        pass
    # Common identifiers
    # Общие
    # DOI
    m = re.search(r"\b(10\.\d{4,9}\/[\w\-\.:;()\/[\]A-Za-z0-9]+)", t)
    if m:
        tags.setdefault("doi", m.group(1))
    # ISBN
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
    # Фасеты: типы и ключи тегов
    types = db.session.query(File.material_type, func.count(File.id)).group_by(File.material_type).all()

    q = request.args.get("q", "").strip()
    material_type = request.args.get("type", "").strip()
    tag_filters = request.args.getlist("tag")  # формата key=value
    year_from = request.args.get("year_from", "").strip()
    year_to = request.args.get("year_to", "").strip()
    size_min = request.args.get("size_min", "").strip()
    size_max = request.args.get("size_max", "").strip()

    # base query for facets (without tag filters)
    base_query = File.query
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

    # final query with tag filters applied (use aliased joins to avoid conflicts)
    query = base_query
    for tf in tag_filters:
        if "=" in tf:
            k, v = tf.split("=", 1)
            t = aliased(Tag)
            query = query.join(t, t.file_id == File.id).filter(and_(t.key == k, t.value.ilike(f"%{v}%")))
    query = query.distinct()
    # Independent facets: for each key, compute counts with all other tag filters applied, excluding itself
    # 1) Collect all facet keys present in base universe and selected filters
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
        # build query with all tag filters except this key
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
        tag_facets[key] = [(val, cnt) for (val, cnt) in rows]
        # ensure selected values for this key are present even when zero
        if key in selected:
            present_vals = {val for (val, _c) in tag_facets[key]}
            for v in selected[key]:
                if v not in present_vals:
                    tag_facets[key].append((v, 0))

    files = query.order_by(File.mtime.desc().nullslast()).limit(200).all()
    return render_template(
        "index.html",
        files=files,
        types=types,
        tag_facets=tag_facets,
        q=q,
        material_type=material_type,
        tag_filters=tag_filters,
        year_from=year_from,
        year_to=year_to,
        size_min=size_min,
        size_max=size_max
    )

@app.route("/file/<int:file_id>")
def file_detail(file_id):
    f = File.query.get_or_404(file_id)
    return render_template("detail.html", f=f)

@app.route("/scan", methods=["POST"])
def scan():
    root = Path(request.form.get("root") or SCAN_ROOT)
    do_extract = request.form.get("extract_text", "on") == "on"
    use_llm = request.form.get("use_llm", "off") == "on"
    do_prune = request.form.get("prune", "on") == "on"

    root = root.expanduser().resolve()
    added, updated = 0, 0

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue

        rel_path = str(path.relative_to(root))
        sha1 = sha1_of_file(path)
        size = path.stat().st_size
        mtime = path.stat().st_mtime
        filename = path.stem

        file_obj = File.query.filter_by(path=str(path)).first()
        if not file_obj:
            file_obj = File(path=str(path), rel_path=rel_path, filename=filename,
                            ext=ext, size=size, mtime=mtime, sha1=sha1)
            db.session.add(file_obj)
            added += 1
        else:
            if file_obj.sha1 != sha1 or file_obj.mtime != mtime:
                file_obj.sha1 = sha1
                file_obj.size = size
                file_obj.mtime = mtime
                file_obj.filename = filename
                updated += 1

        # Извлечение текста/транскрибация (PDF/документы/аудио)
        text_excerpt = ""
        if do_extract:
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
                text_excerpt = transcribe_audio(path, limit_chars=40000)
            if text_excerpt:
                file_obj.text_excerpt = text_excerpt[:40000]
            # General tags: file extension and PDF pages
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
            # Audio-specific tags
            if ext in AUDIO_EXTS:
                try:
                    upsert_tag(file_obj, 'формат', ext.lstrip('.'))
                    upsert_tag(file_obj, 'длительность', audio_duration_hhmmss(path))
                except Exception:
                    pass
                # Lightweight LLM keywords from transcript (optional)
                try:
                    if AUDIO_KEYWORDS_LLM and (file_obj.text_excerpt or '') and not (file_obj.keywords or '').strip():
                        kws = call_lmstudio_keywords(file_obj.text_excerpt, path.name)
                        if kws:
                            file_obj.keywords = ", ".join(kws)
                            db.session.flush()
                            _upsert_keyword_tags(file_obj)
                except Exception as _e:
                    app.logger.info(f"audio keywords llm failed: {_e}")
            # Image-specific tags (разрешение)
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
            # Изображения: описание/ключевые слова запрашиваем ниже через LLM (vision)

        # Эвристики по имени файла
        title, author, year = None, None, None
        for pat in FILENAME_PATTERNS:
            m = pat.match(filename)
            if m:
                gd = m.groupdict()
                title = gd.get("title")
                author = gd.get("author")
                year = gd.get("year")
                break
        if title and not file_obj.title:
            file_obj.title = title
        if author and not file_obj.author:
            file_obj.author = author
        if year and not file_obj.year:
            file_obj.year = year

        # Определим тип, если пустой — в соответствии с порядком
        if not file_obj.material_type:
            cand = _detect_type_pre_llm(ext, text_excerpt, filename)
            if cand:
                file_obj.material_type = cand

        # Типо-зависимые теги (до LLM)
        try:
            ttags = extract_tags_for_type(file_obj.material_type or '', text_excerpt or '', filename)
            if ttags:
                db.session.flush()
                for k, v in ttags.items():
                    upsert_tag(file_obj, k, v)
        except Exception as e:
            app.logger.info(f"type-specific tags failed: {e}")
        # Additional richer tags
        try:
            rtags = extract_richer_tags(file_obj.material_type or '', text_excerpt or '', filename)
            if rtags:
                db.session.flush()
                for k, v in rtags.items():
                    upsert_tag(file_obj, k, v)
        except Exception:
            pass

        # LLM-дозаполнение
        if use_llm and (text_excerpt or ext in {'.txt', '.md'}):
            llm_text = text_excerpt if text_excerpt else ""
            if not llm_text and ext in {".txt", ".md"}:
                try:
                    llm_text = Path(path).read_text(encoding="utf-8", errors="ignore")[:15000]
                except Exception:
                    pass
            meta = call_lmstudio_for_metadata(llm_text, path.name)
            if meta:
                mt_meta = normalize_material_type(meta.get("material_type"))
                if TYPE_LLM_OVERRIDE and mt_meta:
                    file_obj.material_type = mt_meta
                # normalize fields to strings as needed
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
                    if KEYWORDS_TO_TAGS_ENABLED:
                        if KEYWORDS_TO_TAGS_ENABLED:
                            db.session.flush()
                            _upsert_keyword_tags(file_obj)
                # динамические теги
                if meta.get("novelty"):
                    db.session.flush()
                    upsert_tag(file_obj, "научная новизна", str(meta.get("novelty")))
                for key in ("literature", "organizations", "classification"):
                    val = meta.get(key)
                    if isinstance(val, list) and val:
                        db.session.flush()
                        upsert_tag(file_obj, key, "; ".join([str(x) for x in val]))
                # Попробуем дополнительно типовые теги после LLM (если изменился материал_type)
                try:
                    ttags = extract_tags_for_type(file_obj.material_type or '', text_excerpt or '', filename)
                    if ttags:
                        db.session.flush()
                        for k, v in ttags.items():
                            upsert_tag(file_obj, k, v)
                except Exception:
                    pass

        # Vision LLM для изображений (вне текстовой ветки)
        if use_llm and (ext in IMAGE_EXTS) and IMAGES_VISION_ENABLED:
            try:
                vis = call_lmstudio_vision(path, path.name)
                if isinstance(vis, dict):
                    desc = (vis.get('description') or '')
                    if desc:
                        file_obj.abstract = desc[:8000]
                    kws = vis.get('keywords') or []
                    if isinstance(kws, list) and kws:
                        file_obj.keywords = ", ".join([str(x) for x in kws][:50])
                        if KEYWORDS_TO_TAGS_ENABLED:
                            db.session.flush()
                            _upsert_keyword_tags(file_obj)
            except Exception:
                pass

        db.session.flush()  # нужен id для тегов

        # Базовые теги из полей
        if file_obj.material_type:
            upsert_tag(file_obj, "тип", file_obj.material_type)
        if file_obj.author:
            upsert_tag(file_obj, "автор", file_obj.author)
        if file_obj.year:
            upsert_tag(file_obj, "год", str(file_obj.year))

    removed = prune_missing_files() if do_prune else 0
    db.session.commit()
    flash(f"Сканирование завершено. Добавлено: {added}, обновлено: {updated}, удалено: {removed}.", "success")
    return redirect(url_for("index"))

@app.route("/edit/<int:file_id>", methods=["GET", "POST"])
def edit(file_id):
    f = File.query.get_or_404(file_id)
    if request.method == "POST":
        changes = []
        def log_change(action, field, old, new, info=None):
            changes.append(ChangeLog(file_id=f.id, action=action, field=field, old_value=old, new_value=new, info=info))

        # Основные поля
        for field in ["title", "author", "year", "advisor", "material_type", "keywords"]:
            old = getattr(f, field)
            new = request.form.get(field) or old
            if old != new:
                log_change("edit", field, old, new)
                setattr(f, field, new)

        # динамические теги
        keys = request.form.getlist("tag_key[]")
        vals = request.form.getlist("tag_value[]")
        Tag.query.filter_by(file_id=f.id).delete()
        if f.material_type:
            upsert_tag(f, "тип", f.material_type)
        if f.author:
            upsert_tag(f, "автор", f.author)
        if f.year:
            upsert_tag(f, "год", str(f.year))
        for k, v in zip(keys, vals):
            upsert_tag(f, k, v)
            log_change("tag_add", k, None, v)

        for c in changes:
            db.session.add(c)
        db.session.commit()
        flash("Сохранено.", "success")
        return redirect(url_for("file_detail", file_id=f.id))

    tags = Tag.query.filter_by(file_id=f.id).all()
    return render_template("edit.html", f=f, tags=tags)

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    material_type = request.args.get("type", "").strip()
    tag_filters = request.args.getlist("tag")

    query = File.query
    if material_type:
        query = query.filter(File.material_type == material_type)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            File.title.ilike(like),
            File.author.ilike(like),
            File.keywords.ilike(like),
            File.filename.ilike(like),
            File.text_excerpt.ilike(like),
        ))
    for tf in tag_filters:
        if "=" in tf:
            k, v = tf.split("=", 1)
            t = aliased(Tag)
            query = query.join(t, t.file_id == File.id).filter(and_(t.key == k, t.value.ilike(f"%{v}%")))
    query = query.distinct()

    rows = query.order_by(File.mtime.desc().nullslast()).limit(200).all()
    return jsonify([{
        "id": r.id,
        "title": r.title,
        "author": r.author,
        "year": r.year,
        "material_type": r.material_type,
        "path": r.path,
        "rel_path": r.rel_path,
        "tags": [{"key": t.key, "value": t.value} for t in r.tags]
    } for r in rows])

@app.route("/settings", methods=["GET", "POST"])
def settings():
    global SCAN_ROOT, EXTRACT_TEXT, LMSTUDIO_API_BASE, LMSTUDIO_MODEL, LMSTUDIO_API_KEY
    global TRANSCRIBE_ENABLED, TRANSCRIBE_BACKEND, TRANSCRIBE_MODEL_PATH, TRANSCRIBE_LANGUAGE
    global SUMMARIZE_AUDIO, AUDIO_KEYWORDS_LLM, IMAGES_VISION_ENABLED, RENAME_PATTERNS
    global KEYWORDS_TO_TAGS_ENABLED, TYPE_DETECT_FLOW, TYPE_LLM_OVERRIDE, PROMPTS
    global IMPORT_SUBDIR, MOVE_ON_RENAME, TYPE_DIRS
    if request.method == "POST":
        SCAN_ROOT = Path(request.form.get("scan_root") or SCAN_ROOT)
        # Привязка корня просмотра/скачивания
        app.config['UPLOAD_FOLDER'] = str(SCAN_ROOT)
        EXTRACT_TEXT = request.form.get("extract_text", "on") == "on"
        LMSTUDIO_API_BASE = request.form.get("lm_base") or LMSTUDIO_API_BASE
        LMSTUDIO_MODEL = request.form.get("lm_model") or LMSTUDIO_MODEL
        LMSTUDIO_API_KEY = request.form.get("lm_key") or LMSTUDIO_API_KEY
        # scanning extras
        lang = request.form.get("ocr_langs") or OCR_LANGS_CFG
        pages = request.form.get("pdf_ocr_pages") or PDF_OCR_PAGES_CFG
        use_llm = request.form.get("default_use_llm") == "on"
        prune = request.form.get("default_prune") == "on"
        # update globals and env for runtime
        globals()["OCR_LANGS_CFG"] = lang
        globals()["PDF_OCR_PAGES_CFG"] = int(pages)
        globals()["DEFAULT_USE_LLM"] = use_llm
        globals()["DEFAULT_PRUNE"] = prune
        os.environ["OCR_LANGS"] = lang
        os.environ["PDF_OCR_PAGES"] = str(pages)
        # audio transcription settings
        TRANSCRIBE_ENABLED = request.form.get("transcribe_enabled") == "on"
        # Vosk удалён: принудительно используем faster-whisper
        TRANSCRIBE_BACKEND = 'faster-whisper'
        TRANSCRIBE_MODEL_PATH = request.form.get("transcribe_model") or TRANSCRIBE_MODEL_PATH
        TRANSCRIBE_LANGUAGE = request.form.get("transcribe_language") or TRANSCRIBE_LANGUAGE
        SUMMARIZE_AUDIO = request.form.get("summarize_audio") == "on"
        AUDIO_KEYWORDS_LLM = request.form.get("audio_keywords_llm") == "on"
        IMAGES_VISION_ENABLED = request.form.get("vision_images") == "on"
        KEYWORDS_TO_TAGS_ENABLED = request.form.get("kw_to_tags") == "on"
        TYPE_DETECT_FLOW = (request.form.get('type_detect_flow') or TYPE_DETECT_FLOW).strip() or TYPE_DETECT_FLOW
        TYPE_LLM_OVERRIDE = request.form.get('type_llm_override') == 'on'
        # import/rename folders
        IMPORT_SUBDIR = (request.form.get('import_subdir') or IMPORT_SUBDIR or '').strip().strip('/\\')
        MOVE_ON_RENAME = request.form.get('move_on_rename') == 'on'
        app.config['IMPORT_SUBDIR'] = IMPORT_SUBDIR
        app.config['MOVE_ON_RENAME'] = MOVE_ON_RENAME
        # type dirs mapping
        rawtd = (request.form.get('type_dirs') or '').strip()
        if rawtd:
            new_map = {}
            for line in rawtd.splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    k = (k or '').strip().lower()
                    v = (v or '').strip().strip('/\\')
                    if k:
                        new_map[k] = v
            if new_map:
                TYPE_DIRS = {**TYPE_DIRS, **new_map}
        app.config['TYPE_DIRS'] = TYPE_DIRS
        # prompts
        try:
            pm = request.form.get('prompt_metadata');
            if pm is not None: PROMPTS['metadata_system'] = pm
            pa = request.form.get('prompt_audio_sum');
            if pa is not None: PROMPTS['summarize_audio_system'] = pa
            pk = request.form.get('prompt_keywords');
            if pk is not None: PROMPTS['keywords_system'] = pk
            pv = request.form.get('prompt_vision');
            if pv is not None: PROMPTS['vision_system'] = pv
        except Exception:
            pass
        # rename patterns
        rawp = (request.form.get('rename_patterns') or '').strip()
        if rawp:
            new_map = {}
            for line in rawp.splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    new_map[k.strip()] = v.strip()
            if new_map:
                RENAME_PATTERNS = new_map
        flash("Настройки сохранены на время работы приложения.", "success")
        return redirect(url_for("settings"))
    return render_template("settings.html",
                           scan_root=str(SCAN_ROOT),
                           extract_text=EXTRACT_TEXT,
                           lm_base=LMSTUDIO_API_BASE,
                           lm_model=LMSTUDIO_MODEL,
                           lm_key=LMSTUDIO_API_KEY,
                           ocr_langs=OCR_LANGS_CFG,
                           pdf_ocr_pages=PDF_OCR_PAGES_CFG,
                           default_use_llm=DEFAULT_USE_LLM,
                           default_prune=DEFAULT_PRUNE,
                           transcribe_enabled=TRANSCRIBE_ENABLED,
                           transcribe_backend=TRANSCRIBE_BACKEND,
                           transcribe_model=TRANSCRIBE_MODEL_PATH,
                           transcribe_language=TRANSCRIBE_LANGUAGE,
                           summarize_audio=SUMMARIZE_AUDIO,
                           audio_keywords_llm=AUDIO_KEYWORDS_LLM,
                           vision_images=IMAGES_VISION_ENABLED,
                           rename_patterns=RENAME_PATTERNS,
                           import_subdir=IMPORT_SUBDIR,
                           move_on_rename=MOVE_ON_RENAME,
                           type_dirs=TYPE_DIRS,
                           kw_to_tags=KEYWORDS_TO_TAGS_ENABLED,
                           type_detect_flow=TYPE_DETECT_FLOW,
                           type_llm_override=TYPE_LLM_OVERRIDE,
                           prompt_metadata=PROMPTS.get('metadata_system',''),
                           prompt_audio_sum=PROMPTS.get('summarize_audio_system',''),
                           prompt_keywords=PROMPTS.get('keywords_system',''),
                           prompt_vision=PROMPTS.get('vision_system',''))

@app.route('/admin/backup-db', methods=['POST'])
def backup_db():
    # Create and send a timestamped backup of the SQLite DB
    try:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        src = BASE_DIR / 'catalogue.db'
        if not src.exists():
            flash('Файл базы не найден.', 'danger')
            return redirect(url_for('settings'))
        bdir = BASE_DIR / 'backups'
        bdir.mkdir(exist_ok=True)
        dst = bdir / f'catalogue_{ts}.db'
        import shutil
        shutil.copy2(src, dst)
        return send_from_directory(bdir, dst.name, as_attachment=True)
    except Exception as e:
        flash(f'Ошибка резервного копирования: {e}', 'danger')
    return redirect(url_for('settings'))

@app.route('/admin/clear-db', methods=['POST'])
def clear_db():
    # Danger: delete all records from main tables
    try:
        Tag.query.delete()
        ChangeLog.query.delete()
        File.query.delete()
        db.session.commit()
        flash('База очищена.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка очистки: {e}', 'danger')
    return redirect(url_for('settings'))

@app.route('/admin/import-db', methods=['POST'])
def import_db():
    """Replace current SQLite database with uploaded file.
    Creates an automatic timestamped backup of the current DB before replacing.
    """
    file = request.files.get('dbfile')
    if not file or file.filename == '':
        flash('Файл базы не выбран.', 'danger')
        return redirect(url_for('settings'))
    filename = secure_filename(file.filename)
    if not filename.lower().endswith('.db'):
        flash('Ожидался файл .db (SQLite).', 'danger')
        return redirect(url_for('settings'))
    try:
        # Read small header to verify SQLite signature
        head = file.stream.read(16)
        file.stream.seek(0)
        if head[:15] != b'SQLite format 3':
            flash('Файл не похож на SQLite базу.', 'danger')
            return redirect(url_for('settings'))
    except Exception:
        pass  # allow even if header check fails; continue

    try:
        dst = BASE_DIR / 'catalogue.db'
        # Backup current DB
        if dst.exists():
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            bdir = BASE_DIR / 'backups'
            bdir.mkdir(exist_ok=True)
            bkp = bdir / f'catalogue_before_import_{ts}.db'
            import shutil
            shutil.copy2(dst, bkp)

        # Save upload to temp file first
        tmp = BASE_DIR / f'.upload_import_tmp_{os.getpid()}.db'
        file.save(tmp)

        # Schema validation
        try:
            con = sqlite3.connect(str(tmp))
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {r[0] for r in cur.fetchall()}
            required = {'files', 'tags', 'tag_schemas', 'changelog'}
            missing = required - tables
            con.close()
            if missing:
                flash('В файле базы нет требуемых таблиц: ' + ', '.join(sorted(missing)), 'danger')
                try: tmp.unlink() 
                except Exception: pass
                return redirect(url_for('settings'))
        except Exception as e:
            flash(f'Не удалось проверить схему базы: {e}', 'danger')
            try: tmp.unlink()
            except Exception: pass
            return redirect(url_for('settings'))

        # Ensure SQLAlchemy releases file handles
        try:
            db.session.close()
            db.session.remove()
        except Exception:
            pass
        try:
            db.engine.dispose()
        except Exception:
            pass

        # Replace DB
        import shutil
        shutil.move(str(tmp), str(dst))
        flash('База успешно импортирована. При необходимости перезапустите приложение.', 'success')
    except Exception as e:
        flash(f'Ошибка импорта базы: {e}', 'danger')
    finally:
        try:
            if 'tmp' in locals() and tmp.exists():
                tmp.unlink()
        except Exception:
            pass
    return redirect(url_for('settings'))


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
                # If local directory exists, treat it as resolved
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
                        # Optional download, even if backend is not currently faster-whisper
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
                # Try common variants to avoid duplicate root folder in the path
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

        # file meta
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
                from datetime import datetime
                d = datetime.fromtimestamp(f.mtime)
                months[d.strftime('%Y-%m')] += 1
                weekdays[d.weekday()] += 1  # Monday=0
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
    # avg size by type (в МБ, округляем до десятых)
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
    return jsonify({
        "authors": authors.most_common(20),
        "years": sorted(years.items()),
        "types": types.most_common(),
        "exts": exts.most_common(),
        "sizes": sorted(size_buckets.items(), key=lambda x: ["неизв.","< 1 МБ","1–10 МБ","10–50 МБ","50–100 МБ","> 100 МБ"].index(x[0]) if x[0] in ["неизв.","< 1 МБ","1–10 МБ","10–50 МБ","50–100 МБ","> 100 МБ"] else 999),
        "months": sorted(months.items()),
        "top_keywords": kw.most_common(30),
        "tag_keys": tag_keys.most_common(30),
        "weekdays": weekdays_list,
        "hours": hours_list,
        "avg_size_type": avg_size_type,
        "meta_presence": sorted(meta_presence.items(), key=lambda x: x[0]),
    })

@app.route("/stats")
def stats():
    return render_template("stats.html")


@app.route('/graph')
def graph():
    # Страница с визуализацией графа связей (данные берутся через JS из /api/stats или /api/files)
    return render_template('graph.html')

@app.cli.command("init-db")
def init_db():
    db.create_all()
    # Seed or update TagSchema with known keys (idempotent)
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
    # Extend with additional keys for new tag taxonomy
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
    f = File.query.get_or_404(file_id)
    try:
        p = Path(f.path)
        if not p.exists() or not p.is_file():
            return jsonify({"error": "file_not_found"}), 404
        ext = p.suffix.lower()
        filename = p.stem

        # extract text (always on for refresh)
        text_excerpt = ""
        if ext == ".pdf":
            text_excerpt = extract_text_pdf(p, limit_chars=40000)
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
        # General tags: ext and PDF pages
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

        # audio-specific tags
        if ext in AUDIO_EXTS:
            try:
                upsert_tag(f, 'формат', ext.lstrip('.'))
                upsert_tag(f, 'длительность', audio_duration_hhmmss(p))
            except Exception:
                pass

        # filename heuristics (only fill empty fields)
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

        # guess material type if missing; adjust audio
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

        # type-specific tags
        try:
            ttags = extract_tags_for_type(f.material_type or '', text_excerpt or '', filename)
            if ttags:
                db.session.flush()
                for k, v in ttags.items():
                    upsert_tag(f, k, v)
        except Exception:
            pass
        # Additional richer tags
        try:
            rtags = extract_richer_tags(f.material_type or '', text_excerpt or '', filename)
            if rtags:
                db.session.flush()
                for k, v in rtags.items():
                    upsert_tag(f, k, v)
        except Exception:
            pass

        # optional LLM enrichment per settings or explicit flags
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
            meta = call_lmstudio_for_metadata(llm_text, p.name)
            if meta:
                f.material_type = normalize_material_type(meta.get("material_type")) or f.material_type
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

        # audio summary and keywords
        if ext in AUDIO_EXTS:
            if kws_audio_on and (f.text_excerpt or '') and not (f.keywords or '').strip():
                try:
                    kws = call_lmstudio_keywords(f.text_excerpt, p.name)
                    if kws:
                        f.keywords = ", ".join(kws)
                        db.session.flush()
                        _upsert_keyword_tags(f)
                except Exception:
                    pass
        # vision for images
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
            except Exception:
                pass
            if do_summarize and (f.text_excerpt or ''):
                try:
                    summ = call_lmstudio_summarize(f.text_excerpt, p.name)
                    if summ:
                        f.abstract = summ[:2000]
                except Exception:
                    pass

        db.session.flush()
        # base tags from fields
        if f.material_type:
            upsert_tag(f, "тип", f.material_type)
        if f.author:
            upsert_tag(f, "автор", f.author)
        if f.year:
            upsert_tag(f, "год", str(f.year))
        db.session.commit()

        data = file_to_dict(f)
        data["abstract"] = f.abstract
        data["text_excerpt"] = f.text_excerpt
        return jsonify({"ok": True, "file": data})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

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
    # удалим инициалы
    a = re.sub(r"\b[А-ЯA-Z]\.[А-ЯA-Z]\.?", "", a)
    parts = [p for p in re.split(r"\s+", a) if p]
    return parts[-1] if parts else ''

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
    ext = f.ext or Path(f.path).suffix
    try:
        p_old = Path(f.path)
        # target directory — by type if enabled, else current parent
        mt = (f.material_type or '').lower().strip()
        target_sub = TYPE_DIRS.get(mt) or TYPE_DIRS.get('other', 'other')
        d = (SCAN_ROOT / target_sub) if MOVE_ON_RENAME else p_old.parent
        d.mkdir(parents=True, exist_ok=True)
        p_new = d / (base + (ext or ''))
        # Разрулим коллизию
        i = 1
        while p_new.exists() and p_new != p_old:
            p_new = d / (f"{base}_{i}" + (ext or ''))
            i += 1
        # Переименуем на диске
        p_old.rename(p_new)
        # Удалим старый thumbnail для PDF
        try:
            if (f.ext or '').lower() == '.pdf':
                old_thumb = Path(app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                if old_thumb.exists():
                    old_thumb.unlink()
        except Exception:
            pass
        # Обновим запись в БД
        f.path = str(p_new)
        # пересчитаем rel_path относительно SCAN_ROOT
        try:
            f.rel_path = str(p_new.relative_to(Path(SCAN_ROOT)))
        except Exception:
            f.rel_path = p_new.name
        f.filename = p_new.stem
        f.mtime = p_new.stat().st_mtime
        # Лог изменения
        try:
            db.session.add(ChangeLog(file_id=f.id, action='rename', field='filename', old_value=p_old.name, new_value=p_new.name))
        except Exception:
            pass
        db.session.commit()
        return jsonify({"ok": True, "new_name": p_new.name, "rel_path": f.rel_path})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

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
}
SCAN_CANCEL = False

def _iter_files_for_scan(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        yield path

MAX_LOG_LINES = 200

def _scan_log(msg: str, level: str = "info"):
    SCAN_PROGRESS.setdefault("history", [])
    entry = {"t": time.time(), "level": level, "msg": str(msg)}
    SCAN_PROGRESS["history"].append(entry)
    if len(SCAN_PROGRESS["history"]) > MAX_LOG_LINES:
        SCAN_PROGRESS["history"] = SCAN_PROGRESS["history"][-MAX_LOG_LINES:]
    SCAN_PROGRESS["updated_at"] = time.time()

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
    global SCAN_PROGRESS, SCAN_CANCEL
    with app.app_context():
        try:
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
            # determine scan set: explicit targets or full root scan
            if targets:
                try:
                    file_list = [Path(p) for p in targets]
                except Exception:
                    file_list = []
                # filter by allowed and existence
                file_list = [p for p in file_list if p.exists() and p.suffix.lower() in ALLOWED_EXTS]
                _scan_log(f"Сканирование только добавленных файлов: {len(file_list)}")
            else:
                file_list = list(_iter_files_for_scan(root))
            total = len(file_list)
            SCAN_PROGRESS["total"] = total
            # resume support: skip first N files if requested
            skip = max(0, min(int(skip or 0), total))
            if skip:
                _scan_log(f"Продолжение: пропуск первых {skip} из {total}")
                file_list = file_list[skip:]
                SCAN_PROGRESS["processed"] = skip
            _scan_log(f"Найдено файлов: {len(file_list)}")

            added = updated = 0
            for idx, path in enumerate(file_list, start=1):
                if SCAN_CANCEL:
                    _scan_log("Отмена пользователем", level="warn")
                    break
                SCAN_PROGRESS.update({
                    "stage": "processing",
                    "processed": (skip + idx),
                    "current": str(path.name),
                    "updated_at": time.time()
                })
                _update_eta()
                if idx == 1 or idx % 10 == 0:
                    _scan_log(f"Обработка: {path.name}")

                ext = path.suffix.lower()
                # compute relative path to SCAN_ROOT when possible
                try:
                    rel_path = str(path.relative_to(root))
                except Exception:
                    rel_path = path.name
                sha1 = sha1_of_file(path)
                size = path.stat().st_size
                mtime = path.stat().st_mtime
                filename = path.stem

                file_obj = File.query.filter_by(path=str(path)).first()
                if not file_obj:
                    file_obj = File(path=str(path), rel_path=rel_path, filename=filename,
                                    ext=ext, size=size, mtime=mtime, sha1=sha1)
                    db.session.add(file_obj)
                    added += 1
                    SCAN_PROGRESS["added"] = added
                else:
                    if file_obj.sha1 != sha1 or file_obj.mtime != mtime:
                        file_obj.sha1 = sha1
                        file_obj.size = size
                        file_obj.mtime = mtime
                        file_obj.filename = filename
                    updated += 1
                    SCAN_PROGRESS["updated"] = updated

                # Text extraction (based on existing logic)
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
                    # General tags: extension and PDF pages
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
                    # Audio-specific tags
                    if ext in AUDIO_EXTS:
                        try:
                            upsert_tag(file_obj, 'формат', ext.lstrip('.'))
                            upsert_tag(file_obj, 'длительность', audio_duration_hhmmss(path))
                        except Exception:
                            pass
                        # Lightweight keywords from transcript via LLM
                        try:
                            if AUDIO_KEYWORDS_LLM and (file_obj.text_excerpt or '') and not (file_obj.keywords or '').strip():
                                kws = call_lmstudio_keywords(file_obj.text_excerpt, path.name)
                                if kws:
                                    file_obj.keywords = ", ".join(kws)
                        except Exception as _e:
                            _scan_log(f"audio keywords llm failed: {_e}", level="warn")
                    # Image-specific tags
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
                        # Lightweight keywords from transcript via LLM
                        try:
                            if AUDIO_KEYWORDS_LLM and (file_obj.text_excerpt or '') and not (file_obj.keywords or '').strip():
                                kws = call_lmstudio_keywords(file_obj.text_excerpt, path.name)
                                if kws:
                                    file_obj.keywords = ", ".join(kws)
                        except Exception as _e:
                            _scan_log(f"audio keywords llm failed: {_e}", level="warn")

                # Filename heuristics
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
                    # Additional richer tags
                    try:
                        rtags = extract_richer_tags(file_obj.material_type or '', text_excerpt or '', filename)
                        if rtags:
                            db.session.flush()
                            for k, v in rtags.items():
                                upsert_tag(file_obj, k, v)
                    except Exception as e:
                        _scan_log(f"richer tags error: {e}", level="warn")

                # LLM-добавление (может быть медленным)
                if use_llm and (text_excerpt or ext in {'.txt', '.md'} or ext in IMAGE_EXTS):
                    SCAN_PROGRESS["stage"] = "llm"
                    SCAN_PROGRESS["updated_at"] = time.time()
                    _scan_log(f"LLM-анализ: {path.name}")
                    llm_text = text_excerpt or ""
                    if not llm_text and ext in {".txt", ".md"}:
                        try:
                            llm_text = Path(path).read_text(encoding="utf-8", errors="ignore")[:15000]
                        except Exception:
                            pass
                    meta = call_lmstudio_for_metadata(llm_text, path.name)
                    if meta:
                        mt_meta = normalize_material_type(meta.get("material_type"))
                        if TYPE_LLM_OVERRIDE and mt_meta:
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
                    # Vision для изображений
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
                    # Additional richer tags after LLM
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
        except Exception as e:
            SCAN_PROGRESS.update({"error": str(e), "running": False, "stage": "error", "updated_at": time.time()})
            _scan_log(f"Ошибка: {e}", level="error")
        finally:
            SCAN_CANCEL = False

@app.route("/scan/start", methods=["POST"])
def scan_start():
    global SCAN_CANCEL
    if SCAN_PROGRESS.get("running"):
        return jsonify({"status": "busy"}), 409
    extract_text = request.form.get("extract_text", "on") == "on"
    use_llm = request.form.get("use_llm") == "on" if "use_llm" in request.form else DEFAULT_USE_LLM
    prune = request.form.get("prune") == "on" if "prune" in request.form else DEFAULT_PRUNE
    SCAN_CANCEL = False
    skip = 0
    try:
        skip = int(request.form.get('skip', '0') or 0)
    except Exception:
        skip = 0
    t = threading.Thread(target=_run_scan_with_progress, args=(extract_text, use_llm, prune, skip), daemon=True)
    t.start()
    return jsonify({"status": "started"})

@app.route("/scan/status")
def scan_status():
    return jsonify(SCAN_PROGRESS)

@app.route("/scan/cancel", methods=["POST"])
def scan_cancel():
    global SCAN_CANCEL
    SCAN_CANCEL = True
    return jsonify({"status": "cancelling"})


# ------------------- AI Search (MVP) -------------------

def _ai_expand_keywords(query: str) -> list[str]:
    q = (query or "").strip()
    if not q:
        return []
    # TTL in minutes; default 20
    try:
        ttl_min = int(os.getenv("AI_EXPAND_TTL_MIN", "20") or 20)
    except Exception:
        ttl_min = 20
    key = _sha256(q)
    now = _now()
    cached = AI_EXPAND_CACHE.get(key)
    if cached and (now - cached[0]) < ttl_min * 60:
        return cached[1]
    # Ask LLM for keywords; fallback to simple tokens
    kws = []
    try:
        kws = call_lmstudio_keywords(q, "ai-search") or []
    except Exception:
        kws = []
    if not kws:
        # naive token split fallback
        toks = [t.strip() for t in re.split(r"[\s,;]+", q) if t.strip()]
        kws = toks[:12]
    # de-dup while preserving order
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
        # Prefer DB excerpt
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
        for _i in range(3):  # up to 3 spots per term
            idx = tl.find(ql, pos)
            if idx < 0:
                break
            found_any = True
            start = max(0, idx - 80)
            end = min(len(t), idx + len(term) + 80)
            windows.append((start, end))
            pos = idx + len(term)
        if not found_any and len(ql) >= 3:
            # try split term into sub-words
            for part in re.split(r"[\s\-_/]+", ql):
                if len(part) < 3:
                    continue
                idx = tl.find(part)
                if idx >= 0:
                    start = max(0, idx - 80)
                    end = min(len(t), idx + len(part) + 80)
                    windows.append((start, end))
    # merge overlapping windows
    windows.sort()
    merged = []
    for w in windows:
        if not merged or w[0] > merged[-1][1] + 20:
            merged.append(list(w))
        else:
            merged[-1][1] = max(merged[-1][1], w[1])
    for a, b in merged[:max_snips]:
        snip = t[a:b]
        # collapse newlines to keep compact
        snip = re.sub(r"\s+", " ", snip).strip()
        outs.append((a, snip))
    outs.sort(key=lambda x: x[0])
    return [s for _pos, s in outs]


def _tokenize_query(q: str) -> list[str]:
    s = (q or '').lower()
    # keep letters, digits, hyphen, underscore; split on others
    parts = re.split(r"[^\w\-]+", s)
    parts = [p for p in parts if p and len(p) >= 2]
    # de-dup preserve order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out[:16]


def _idf_for_terms(terms: list[str]) -> dict[str, float]:
    # compute document frequencies over union of file fields and tags
    idf: dict[str, float] = {}
    try:
        N = db.session.query(func.count(File.id)).scalar() or 1
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
                    Tag.value.ilike(like),
                    Tag.key.ilike(like),
                ))
            df = int(q.scalar() or 0)
        except Exception:
            df = 0
        # add-one smoothing
        val = float((1.0 + (N / (1.0 + df))))
        # log scale, min 1.0
        try:
            import math
            val = max(1.0, math.log(val + 1.0))
        except Exception:
            val = 1.0
        idf[w] = val
    return idf


@app.route('/api/ai-search', methods=['POST'])
def api_ai_search():
    data = request.get_json(silent=True) or {}
    query = (data.get('query') or '').strip()
    if not query:
        return jsonify({"ok": False, "error": "query is required"}), 400
    try:
        top_k = int(data.get('top_k') or 10)
    except Exception:
        top_k = 10
    sources = data.get('sources') or {}
    use_tags = sources.get('tags', True) if isinstance(sources, dict) else True
    use_text = sources.get('text', True) if isinstance(sources, dict) else True

    # Expand and tokenize
    keywords = _ai_expand_keywords(query)
    base_tokens = _tokenize_query(query)
    extra_tokens = []
    for w in keywords:
        extra_tokens.extend(_tokenize_query(w))
    # unique terms (tokens) preserving order, prefer base_tokens first
    seen = set()
    terms: list[str] = []
    for w in base_tokens + extra_tokens:
        if w and w not in seen:
            seen.add(w)
            terms.append(w)
    if not terms and query:
        # fallback: at least use the raw query as term
        terms = _tokenize_query(query) or [query.lower()]

    # Precompute IDF per term
    idf = _idf_for_terms(terms)

    # Accumulate candidates with scores and hits
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

    # Tag matches
    if use_tags:
        for w in terms:
            like = f"%{w}%"
            try:
                rows = db.session.query(Tag.file_id, Tag.key, Tag.value) \
                    .filter(or_(Tag.value.ilike(like), Tag.key.ilike(like))) \
                    .limit(4000).all()
                for fid, k, v in rows:
                    add_score(fid, AI_SCORE_TAG * idf.get(w, 1.0), {"type": "tag", "key": k, "value": v, "term": w}, term=w)
            except Exception:
                pass

    # File field matches
    if use_text:
        for w in terms:
            like = f"%{w}%"
            try:
                rows = db.session.query(File.id, File.title, File.author, File.keywords, File.text_excerpt) \
                    .filter(or_(
                        File.title.ilike(like),
                        File.author.ilike(like),
                        File.keywords.ilike(like),
                        File.text_excerpt.ilike(like),
                    )).limit(4000).all()
                for fid, title, author, kws, excerpt in rows:
                    if title and re.search(re.escape(w), title, flags=re.I):
                        add_score(fid, AI_SCORE_TITLE * idf.get(w, 1.0), {"type": "title", "term": w}, term=w)
                    if author and re.search(re.escape(w), author, flags=re.I):
                        add_score(fid, AI_SCORE_AUTHOR * idf.get(w, 1.0), {"type": "author", "term": w}, term=w)
                    if kws and re.search(re.escape(w), kws, flags=re.I):
                        add_score(fid, AI_SCORE_KEYWORDS * idf.get(w, 1.0), {"type": "keywords", "term": w}, term=w)
                    if excerpt and re.search(re.escape(w), excerpt, flags=re.I):
                        add_score(fid, AI_SCORE_EXCERPT * idf.get(w, 1.0), {"type": "excerpt", "term": w}, term=w)
            except Exception:
                pass

    # Compose results
    file_ids = list(scores.keys())
    results = []
    if file_ids:
        q_files = File.query.filter(File.id.in_(file_ids)).all()
        id2file = {f.id: f for f in q_files}
        for fid, sc in scores.items():
            f = id2file.get(fid)
            if not f:
                continue
            # phrase boost (raw query as a phrase)
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
            # distinct term coverage boost
            n_terms = len(term_hits.get(fid, set()))
            coverage_boost = max(0, n_terms - 1) * AI_BOOST_MULTI
            # snippets
            snips = []
            try:
                text = _read_cached_excerpt_for_file(f)
                snips = _collect_snippets(text, terms, max_snips=2) if text else []
            except Exception:
                snips = []
            # proximity boost: multiple terms in same snippet
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
            })
        # sort by score desc, then recent mtime desc
        results.sort(key=lambda x: (x.get('score') or 0.0, id2file.get(x['file_id']).mtime or 0.0), reverse=True)
        results = results[:max(1, top_k)]

    # Optional short answer using snippets as context (search-oriented prompt)
    answer = ""
    if results:
        try:
            topn = results[:10]
            lines = []
            for i, r in enumerate(topn):
                sn = " ".join((r.get('snippets') or []))[:400]
                title = r.get('title') or r.get('rel_path') or f"file-{r.get('file_id')}"
                lines.append(f"[{i+1}] {title}: {sn}")
            system = (
                "Ты помощник поиска. Сформулируй краткий, фактический ответ на вопрос пользователя, "
                "используя ТОЛЬКО предоставленные фрагменты. Не выдумывай и не обобщай сверх текста. "
                "Ссылайся на источники квадратными скобками [n] там, где берешь факт. Не упоминай слова 'стенограмма' или подобные."
            )
            user_msg = f"Вопрос: {query}\nФрагменты:\n" + "\n".join(lines)
            answer = (call_lmstudio_compose(system, user_msg, temperature=0.1, max_tokens=350) or "").strip()
        except Exception:
            answer = ""

    # Optional: LLM-based shallow rerank of top 15 based on snippet context
    if AI_RERANK_LLM and results:
        try:
            top = results[:15]
            prompt_lines = [f"[{i+1}] id={it['file_id']} :: { (it.get('snippets') or [''])[0] }" for i, it in enumerate(top)]
            prompt = "\n".join(prompt_lines)
            sys = "Ты ранжируешь источники по релевантности к запросу. Верни JSON-массив id в порядке убывания релевантности."
            user = f"Запрос: {query}\nИсточники:\n{prompt}\nОтвети только JSON массивом id."
            base = LMSTUDIO_API_BASE.rstrip('/')
            url = base + "/chat/completions" if not base.endswith("/chat/completions") else base
            headers = {"Content-Type": "application/json"}
            if LMSTUDIO_API_KEY:
                headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"
            payload = {"model": LMSTUDIO_MODEL, "messages": [{"role":"system","content":sys},{"role":"user","content":user}], "temperature": 0.0, "max_tokens": 200}
            rr = requests.post(url, headers=headers, json=payload, timeout=60)
            rr.raise_for_status()
            dd = rr.json()
            content = (dd.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
            order = None
            try:
                order = json.loads(content)
            except Exception:
                m = re.search(r"\[(?:\s*\d+\s*,?\s*)+\]", content)
                if m:
                    order = json.loads(m.group(0))
            if isinstance(order, list) and all(isinstance(x, int) for x in order):
                pos = {int(fid): i for i, fid in enumerate(order)}
                results.sort(key=lambda x: (pos.get(int(x['file_id']), 10**6), -(x.get('score') or 0.0)))
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "query": query,
        "keywords": terms,
        "answer": answer,
        "items": results,
    })

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(host="0.0.0.0", port=5050, debug=False, use_reloader=False, threaded=False)
