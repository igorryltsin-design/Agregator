from flask import Blueprint, jsonify, request, Response, g, abort
from io import StringIO
import csv
from pathlib import Path
import json

from models import (File, Tag, db, upsert_tag, file_to_dict, ChangeLog,
                    Collection, CollectionMember, User, UserActionLog)
from flask import current_app
import re

# --- Natasha/pymorphy2 helpers for RU morphology and synonyms ---
try:
    from razdel import tokenize as _ru_tokenize
except Exception:
    _ru_tokenize = None
try:
    import pymorphy2
    _morph = pymorphy2.MorphAnalyzer()
except Exception:
    _morph = None

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
        return [t.text for t in _ru_tokenize(s)]
    # резерв: простое разбиение
    return re.sub(r"[^\w\d]+", " ", s).strip().split()

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

routes = Blueprint('routes', __name__)

def _current_user():
    return getattr(g, 'current_user', None)


def _require_admin():
    user = _current_user()
    if not user or getattr(user, 'role', '') != 'admin':
        abort(403)
    return user


def _allowed_collection_ids():
    return getattr(g, 'allowed_collection_ids', set())


def _apply_collection_filter(query, collection_model=None):
    allowed = _allowed_collection_ids()
    if allowed is None:
        return query
    model = collection_model or Collection
    if not allowed:
        return query.filter(model.id == -1)
    return query.filter(model.id.in_(allowed))


def _log_user_action(action: str, entity: str | None = None, entity_id: int | None = None, detail: str | None = None):
    user = _current_user()
    try:
        rec = UserActionLog(user_id=user.id if user else None,
                            action=action,
                            entity=entity,
                            entity_id=entity_id,
                            detail=detail)
        db.session.add(rec)
        db.session.commit()
    except Exception:
        db.session.rollback()


def _can_write_collection(collection_id: int | None) -> bool:
    if collection_id is None:
        return False
    user = _current_user()
    if not user:
        return False
    if getattr(user, 'role', '') == 'admin':
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


def _require_collection_write(collection_id: int | None):
    if not _can_write_collection(collection_id):
        abort(403)


def _collection_to_dict(col: Collection, include_members: bool = False) -> dict:
    info = {
        'id': col.id,
        'name': col.name,
        'slug': col.slug,
        'searchable': bool(col.searchable),
        'graphable': bool(col.graphable),
        'owner_id': col.owner_id,
        'owner_username': col.owner.username if getattr(col, 'owner', None) else None,
        'is_private': bool(getattr(col, 'is_private', False)),
        'created_at': col.created_at.isoformat() if getattr(col, 'created_at', None) else None,
        'count': len(col.files or []),
    }
    if include_members:
        members = []
        try:
            for m in col.members or []:
                members.append({
                    'user_id': m.user_id,
                    'role': m.role,
                    'username': m.user.username if m.user else None,
                })
        except Exception:
            members = []
        info['members'] = members
    return info


def _require_collection_owner_or_admin(col: Collection):
    user = _current_user()
    if not user:
        abort(403)
    if getattr(user, 'role', '') == 'admin':
        return user
    if col.owner_id == user.id:
        return user
    member = CollectionMember.query.filter_by(collection_id=col.id, user_id=user.id).first()
    if member and member.role == 'owner':
        return user
    abort(403)

@routes.route("/api/files", methods=["GET"])
def api_files():
    user = _current_user()
    if not user:
        abort(401)
    try:
        files = File.query.join(Collection, File.collection_id == Collection.id) \
            .filter(Collection.searchable == True)
        files = _apply_collection_filter(files, Collection) \
            .order_by(File.mtime.desc().nullslast()).limit(200).all()
    except Exception:
        q = File.query
        allowed = _allowed_collection_ids()
        if allowed is not None:
            if not allowed:
                q = q.filter(File.collection_id == -1)
            else:
                q = q.filter(File.collection_id.in_(allowed))
        files = q.order_by(File.mtime.desc().nullslast()).limit(200).all()
    return jsonify([file_to_dict(f) for f in files])

@routes.route("/api/files/<int:file_id>", methods=["GET"])
def api_file_detail(file_id):
    f = File.query.get_or_404(file_id)
    allowed = _allowed_collection_ids()
    if allowed is not None and f.collection_id not in allowed:
        abort(403)
    return jsonify(file_to_dict(f))


@routes.route('/api/graph')
def api_graph():
    if not _current_user():
        abort(401)
    """Граф по выбранным тегам.
    Query:
      - keys: список ключей тегов через запятую (напр. author,advisor,organization). Если пусто — используется author.
      - limit: макс. файлов (по умолчанию 500) для ограничения размера графа.
    Узлы: file:<id> (type=work) и tag:<key>:<value> (type=tag:key)
    Рёбра: file -> tag (label=key)
    """
    keys_param = (request.args.get('keys') or '').strip()
    tag_keys = [k.strip() for k in keys_param.split(',') if k.strip()] or ['author']
    try:
        limit = max(min(int(request.args.get('limit', '500')), 2000), 1)
    except Exception:
        limit = 500
    q = File.query.join(Collection, File.collection_id == Collection.id)
    q = _apply_collection_filter(q, Collection)
    try:
        q = q.filter(Collection.graphable == True)
    except Exception:
        pass
    # фильтры по годам (строковое поле сохранено для совместимости)
    year_from = (request.args.get('year_from') or '').strip()
    year_to = (request.args.get('year_to') or '').strip()
    if year_from:
        q = q.filter(File.year >= year_from)
    if year_to:
        q = q.filter(File.year <= year_to)
    files = q.order_by(File.mtime.desc().nullslast()).limit(limit).all()
    nodes: list[dict] = []
    edges: list[dict] = []
    file_ids = set()
    tag_node_ids: dict[tuple[str, str], str] = {}
    next_tag_id = 100000
    for f in files:
        fid = f.id
        if fid not in file_ids:
            nodes.append({"id": f"file-{fid}", "label": f.title or f.filename or str(fid), "type": "work"})
            file_ids.add(fid)
        # Собираем подходящие теги
        for t in (f.tags or []):
            if t.key in tag_keys and (t.value or '').strip():
                key = t.key
                val = t.value.strip()
                nid = tag_node_ids.get((key, val))
                if not nid:
                    nid = f"tag-{key}-{next_tag_id}"
                    tag_node_ids[(key, val)] = nid
                    nodes.append({"id": nid, "label": val, "type": f"tag:{key}"})
                    next_tag_id += 1
                edges.append({"from": f"file-{fid}", "to": nid, "label": key})
        # Резервный вариант по полям File (например author)
        if 'author' in tag_keys and (f.author or '').strip():
            val = f.author.strip()
            nid = tag_node_ids.get(('author', val))
            if not nid:
                nid = f"tag-author-{next_tag_id}"
                tag_node_ids[('author', val)] = nid
                nodes.append({"id": nid, "label": val, "type": "tag:author"})
                next_tag_id += 1
            edges.append({"from": f"file-{fid}", "to": nid, "label": 'author'})
    # Необязательный умный поиск на сервере с русскими леммами и синонимами
    q_str = (request.args.get('q') or '').strip()
    smart = str(request.args.get('smart', '')).lower() in ('1','true','yes','on')
    if q_str and smart:
        q_lemmas = list(_expand_synonyms(_lemmas(q_str)))
        def match(label: str) -> bool:
            lab = set(_lemmas(label or ''))
            return any(l in lab for l in q_lemmas)
        keep_ids = set()
        for n in nodes:
            if match(n.get('label') or ''):
                keep_ids.add(n['id'])
        # Оставляем рёбра только когда обе вершины сохранены; расширяем множество, чтобы включить соседей и не изолировать совпадения
        neigh_keep = set(keep_ids)
        for e in edges:
            if e['from'] in keep_ids or e['to'] in keep_ids:
                neigh_keep.add(e['from']); neigh_keep.add(e['to'])
        nodes = [n for n in nodes if n['id'] in neigh_keep]
        kept = set(n['id'] for n in nodes)
        edges = [e for e in edges if e['from'] in kept and e['to'] in kept]
    return jsonify({"nodes": nodes, "edges": edges, "keys": tag_keys})


@routes.route('/api/graph/build', methods=['POST'])
def api_graph_build():
    """Создать недостающие теги для автора и научного руководителя.
    Это помогает заполнить связи, используемые в графе.
    """
    _require_admin()
    files = File.query.all()
    created = 0
    for f in files:
        # автор
        if f.author:
            if not any(t.key == 'author' and t.value == f.author for t in f.tags):
                upsert_tag(f, 'author', f.author)
                created += 1
        # научный руководитель -> тег, похожий на организацию
        if f.advisor:
            if not any(t.key == 'advisor' and t.value == f.advisor for t in f.tags):
                upsert_tag(f, 'advisor', f.advisor)
                created += 1
    db.session.commit()
    return jsonify({"created_tags": created})

@routes.route("/api/files", methods=["POST"])
def api_file_create():
    data = request.json or {}
    user = _current_user()
    if not user:
        abort(401)
    col_id = data.get('collection_id')
    if not col_id:
        try:
            personal = Collection.query.filter_by(owner_id=user.id, is_private=True).first()
            if personal:
                col_id = personal.id
        except Exception:
            col_id = None
        if not col_id:
            try:
                base_col = Collection.query.filter_by(slug='base').first()
                col_id = base_col.id if base_col else None
            except Exception:
                col_id = None
    _require_collection_write(col_id)
    file_path = Path(current_app.config.get('UPLOAD_FOLDER') or '.') / (data.get('filename') or '')
    f = File(
        title=data.get("title"),
        author=data.get("author"),
        year=data.get("year"),
        material_type=data.get("material_type"),
        filename=data.get("filename"),
        keywords=data.get("keywords"),
        collection_id=col_id,
        rel_path=data.get("filename"),
        path=str(file_path),
    )
    db.session.add(f)
    db.session.flush()
    for tag in data.get("tags", []):
        upsert_tag(f, tag.get("key"), tag.get("value"))
    db.session.commit()
    _log_user_action('file_create', 'file', f.id, detail=str(data))
    return jsonify(file_to_dict(f)), 201

@routes.route("/api/files/<int:file_id>", methods=["PUT"])
def api_file_update(file_id):
    f = File.query.get_or_404(file_id)
    _require_collection_write(f.collection_id)
    data = request.json or {}
    old_type = (f.material_type or '').strip().lower()
    actor = _current_user()
    for field in ["title", "author", "year", "material_type", "filename", "keywords"]:
        if field in data:
            setattr(f, field, data[field])
    Tag.query.filter_by(file_id=f.id).delete()
    for tag in data.get("tags", []):
        upsert_tag(f, tag.get("key"), tag.get("value"))
    # Автоперенос при изменении типа
    try:
        new_type = (f.material_type or '').strip().lower()
        move_enabled = bool(current_app.config.get('MOVE_ON_RENAME', True))
        if move_enabled and new_type and new_type != old_type:
            base_dir = Path(current_app.config.get('UPLOAD_FOLDER') or '.')
            type_dirs = current_app.config.get('TYPE_DIRS') or {}
            target_sub = type_dirs.get(new_type) or type_dirs.get('other') or 'other'
            target_dir = base_dir / target_sub
            target_dir.mkdir(parents=True, exist_ok=True)
            p_old = Path(f.path)
            ext = f.ext or p_old.suffix
            base = f.filename or p_old.stem
            p_new = target_dir / (base + (ext or ''))
            i = 1
            while p_new.exists() and p_new.resolve() != p_old.resolve():
                p_new = target_dir / (f"{base}_{i}" + (ext or ''))
                i += 1
            # переносим файл
            p_old.rename(p_new)
            # очищаем старую миниатюру для PDF
            try:
                if (ext or '').lower() == '.pdf':
                    thumb = Path(current_app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                    if thumb.exists(): thumb.unlink()
            except Exception:
                pass
            # обновляем поля в БД
            f.path = str(p_new)
            try:
                f.rel_path = str(p_new.relative_to(base_dir))
            except Exception:
                f.rel_path = p_new.name
            f.filename = p_new.stem
            try:
                f.mtime = p_new.stat().st_mtime
            except Exception:
                pass
            try:
                db.session.add(ChangeLog(file_id=f.id, action='move', field='material_type', old_value=old_type, new_value=new_type, info=f"{p_old} -> {p_new}"))
            except Exception:
                pass
    except Exception as e:
        current_app.logger.warning(f"Auto-move on type change failed: {e}")
    db.session.commit()
    try:
        tag_snapshot = []
        for tag in data.get("tags", []):
            k = (tag or {}).get('key')
            v = (tag or {}).get('value')
            if k and v:
                tag_snapshot.append(f"{k}={v}")
        current_app.logger.info(
            "[tags] user=%s file=%s updated fields=%s tags=%s",
            getattr(actor, 'username', None) or 'system',
            f.id,
            {k: data.get(k) for k in ("title", "author", "year", "material_type", "keywords") if k in data},
            '; '.join(tag_snapshot)
        )
    except Exception:
        pass
    _log_user_action('file_update', 'file', f.id, detail=str(data))
    return jsonify(file_to_dict(f))

@routes.route('/api/files/move-by-type', methods=['POST'])
def api_move_by_type():
    """Перенести группу файлов в подпапки по текущему типу.
    JSON: {"ids":[...]} или {"all":true}
    """
    _require_admin()
    data = request.json or {}
    ids = data.get('ids') or []
    move_all = bool(data.get('all'))
    base_dir = Path(current_app.config.get('UPLOAD_FOLDER') or '.')
    type_dirs = current_app.config.get('TYPE_DIRS') or {}
    moved = 0
    skipped = 0
    errors = []
    q = File.query
    if not move_all:
        if not ids:
            return jsonify({"ok": False, "error": "ids or all=true required"}), 400
        q = q.filter(File.id.in_(ids))
    files = q.all()
    for f in files:
        try:
            mt = (f.material_type or '').strip().lower()
            sub = type_dirs.get(mt) or type_dirs.get('other') or 'other'
            target_dir = base_dir / sub
            p_old = Path(f.path)
            if not p_old.exists():
                skipped += 1
                continue
            # пропускаем, если файл уже в целевой папке
            try:
                if target_dir.resolve() == p_old.parent.resolve():
                    skipped += 1
                    continue
            except Exception:
                pass
            target_dir.mkdir(parents=True, exist_ok=True)
            ext = f.ext or p_old.suffix
            base = f.filename or p_old.stem
            p_new = target_dir / (base + (ext or ''))
            i = 1
            while p_new.exists() and p_new.resolve() != p_old.resolve():
                p_new = target_dir / (f"{base}_{i}" + (ext or ''))
                i += 1
            p_old.rename(p_new)
            # очищаем старую миниатюру для PDF
            try:
                if (ext or '').lower() == '.pdf':
                    thumb = Path(current_app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                    if thumb.exists(): thumb.unlink()
            except Exception:
                pass
            # обновляем запись в БД
            f.path = str(p_new)
            try:
                f.rel_path = str(p_new.relative_to(base_dir))
            except Exception:
                f.rel_path = p_new.name
            f.filename = p_new.stem
            try:
                f.mtime = p_new.stat().st_mtime
            except Exception:
                pass
            try:
                db.session.add(ChangeLog(file_id=f.id, action='move', field='material_type', old_value=mt, new_value=mt, info=f"{p_old} -> {p_new}"))
            except Exception:
                pass
            moved += 1
        except Exception as e:
            errors.append(str(e))
    db.session.commit()
    return jsonify({"ok": True, "moved": moved, "skipped": skipped, "errors": errors})

@routes.route("/api/files/<int:file_id>", methods=["DELETE"])
def api_file_delete(file_id):
    _require_admin()
    f = File.query.get_or_404(file_id)
    remove_fs = str(request.args.get('rm', '')).lower() in ('1','true','yes','on')

    # Опционально удалить сам файл; всегда удаляем производные артефакты
    warnings = []
    if remove_fs:
        try:
            fp = Path(f.path).resolve()
            if fp.exists() and fp.is_file():
                fp.unlink()
        except Exception as e:
            current_app.logger.warning(f"Failed to delete file on disk: {e}")
            warnings.append(f"fs:{e}")

    # Удалить сгенерированный thumbnail
    try:
        thumb = Path(current_app.static_folder) / 'thumbnails' / (Path(f.rel_path).stem + '.png')
        if thumb.exists():
            thumb.unlink()
    except Exception as e:
        current_app.logger.warning(f"Failed to delete thumbnail: {e}")
        warnings.append(f"thumb:{e}")

    # Удалить кэшированный фрагмент текста
    try:
        cache_dir = Path(current_app.static_folder) / 'cache' / 'text_excerpts'
        key = (f.sha1 or (f.rel_path or '').replace('/', '_')) + '.txt'
        cache_file = cache_dir / key
        if cache_file.exists():
            cache_file.unlink()
    except Exception as e:
        current_app.logger.warning(f"Failed to delete cached excerpt: {e}")
        warnings.append(f"cache:{e}")

    db.session.delete(f)
    db.session.commit()
    # Всегда возвращаем успех для упрощения UI; проблемы пишем в лог
    return "", 204

@routes.route('/api/admin/collections', methods=['GET'])
def api_admin_collections():
    _require_admin()
    cols = Collection.query.order_by(Collection.name.asc()).all()
    return jsonify({'ok': True, 'collections': [_collection_to_dict(c, include_members=True) for c in cols]})


@routes.route('/api/collections/<int:collection_id>/members', methods=['GET', 'POST'])
def api_collection_members(collection_id: int):
    col = Collection.query.get_or_404(collection_id)
    actor = _require_collection_owner_or_admin(col)
    if request.method == 'GET':
        return jsonify({'ok': True, 'members': _collection_to_dict(col, include_members=True)['members']})
    data = request.get_json(silent=True) or {}
    user_id = data.get('user_id')
    role = str(data.get('role') or 'viewer').strip().lower()
    if role not in ('viewer', 'editor', 'owner'):
        role = 'viewer'
    try:
        user_id = int(user_id)
    except Exception:
        return jsonify({'ok': False, 'error': 'user_id обязателен'}), 400
    if role == 'owner' and getattr(actor, 'role', '') != 'admin':
        role = 'editor'
    member = CollectionMember.query.filter_by(collection_id=collection_id, user_id=user_id).first()
    if member:
        member.role = role
    else:
        member = CollectionMember(collection_id=collection_id, user_id=user_id, role=role)
        db.session.add(member)
    db.session.commit()
    _log_user_action('collection_member_add', 'collection', collection_id, detail=json.dumps({'user_id': user_id, 'role': role}))
    col = Collection.query.get(collection_id)
    return jsonify({'ok': True, 'members': _collection_to_dict(col, include_members=True)['members']})


@routes.route('/api/collections/<int:collection_id>/members/<int:user_id>', methods=['PATCH', 'DELETE'])
def api_collection_member_detail(collection_id: int, user_id: int):
    col = Collection.query.get_or_404(collection_id)
    actor = _require_collection_owner_or_admin(col)
    member = CollectionMember.query.filter_by(collection_id=collection_id, user_id=user_id).first()
    if not member:
        abort(404)
    if request.method == 'DELETE':
        if user_id == col.owner_id and getattr(actor, 'role', '') != 'admin':
            return jsonify({'ok': False, 'error': 'Нельзя удалить владельца коллекции'}), 400
        db.session.delete(member)
        db.session.commit()
        _log_user_action('collection_member_remove', 'collection', collection_id, detail=json.dumps({'user_id': user_id}))
        return jsonify({'ok': True})
    data = request.get_json(silent=True) or {}
    role = str(data.get('role') or '').strip().lower()
    if role in ('viewer', 'editor', 'owner'):
        if role == 'owner' and getattr(actor, 'role', '') != 'admin':
            role = 'editor'
        if user_id == col.owner_id and role != 'owner' and getattr(actor, 'role', '') != 'admin':
            return jsonify({'ok': False, 'error': 'Нельзя менять роль владельца'}), 400
        member.role = role
        db.session.commit()
        _log_user_action('collection_member_update', 'collection', collection_id, detail=json.dumps({'user_id': user_id, 'role': role}))
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Некорректная роль'}), 400

@routes.route("/export/csv")
def export_csv():
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Name', 'Tags'])
    files = File.query.all()
    for file in files:
        writer.writerow([file.id, file.filename, ', '.join(f"{t.key}={t.value}" for t in file.tags)])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=export.csv"})

@routes.route("/export/bibtex")
def export_bibtex():
    output = StringIO()
    files = File.query.all()
    for file in files:
        output.write(f"@misc{{{file.id},\n  title={{ {file.filename} }},\n  tags={{ {', '.join(f'{t.key}={t.value}' for t in file.tags)} }}\n}}\n")
    return Response(output.getvalue(), mimetype="text/x-bibtex", headers={"Content-Disposition": "attachment;filename=export.bib"})

@routes.route("/api/aiword/bibtex")
def api_aiword_bibtex():
    """Сгенерировать BibTeX на основе каталога для AiWord.
    Пытаемся сопоставить material_type и теги стандартным полям BibTeX.
    """
    def tag_get(f: File, names: list[str]) -> str | None:
        want = {n.lower() for n in names}
        for t in (f.tags or []):
            if (t.key or '').lower() in want and (t.value or '').strip():
                return t.value.strip()
        return None

    def bib_for_file(f: File) -> str:
        mt = (f.material_type or '').strip().lower()
        key = f"f{f.id or 0}"
        # выбираем тип записи
        if mt == 'article':
            et = 'article'
        elif mt in ('textbook', 'monograph', 'book'):
            et = 'book'
        elif mt in ('inproceedings', 'proceedings'):
            et = 'inproceedings'
        elif mt in ('dissertation', 'dissertation_abstract'):
            # по умолчанию используем phdthesis
            deg = tag_get(f, ['degree', 'степень']) or ''
            et = 'phdthesis' if 'доктор' in deg.lower() else 'mastersthesis'
        else:
            et = 'misc'

        title = (f.title or f.filename or '').strip() or 'Untitled'
        author = (f.author or '').strip()
        year = (f.year or '').strip()
        # Распространённые теги
        doi = tag_get(f, ['doi'])
        pages = tag_get(f, ['pages', 'страницы'])
        journal = tag_get(f, ['journal', 'журнал'])
        vol_issue = tag_get(f, ['volume_issue'])
        publisher = tag_get(f, ['publisher', 'издательство'])
        school = tag_get(f, ['organization', 'организация'])
        # Формируем поля
        fields: list[str] = [f"title={{ {title} }}"]
        if author:
            fields.append(f"author={{ {author} }}")
        if year:
            fields.append(f"year={{ {year} }}")
        if et == 'article':
            if journal:
                fields.append(f"journal={{ {journal} }}")
            if vol_issue and '/' in vol_issue:
                v, n = vol_issue.split('/', 1)
                v = v.strip(); n = n.strip()
                if v:
                    fields.append(f"volume={{ {v} }}")
                if n:
                    fields.append(f"number={{ {n} }}")
            if pages:
                fields.append(f"pages={{ {pages} }}")
        elif et in ('book',):
            if publisher:
                fields.append(f"publisher={{ {publisher} }}")
            if pages:
                fields.append(f"pages={{ {pages} }}")
        elif et in ('inproceedings',):
            bt = tag_get(f, ['conference'])
            if bt:
                fields.append(f"booktitle={{ {bt} }}")
            if publisher:
                fields.append(f"publisher={{ {publisher} }}")
            if pages:
                fields.append(f"pages={{ {pages} }}")
        elif et in ('phdthesis','mastersthesis'):
            if school:
                fields.append(f"school={{ {school} }}")
            if pages:
                fields.append(f"pages={{ {pages} }}")
        if doi:
            fields.append(f"doi={{ {doi} }}")
        # Ссылка на страницу элемента внутри приложения
        try:
            fields.append(f"url={{ /file/{f.id} }}")
        except Exception:
            pass

        return f"@{et}{{{key},\n  " + ",\n  ".join(fields) + "\n}\n"

    # Отдаём предпочтение только коллекциям с поиском; при ошибке берём все
    try:
        q = File.query.join(Collection, File.collection_id == Collection.id).filter(Collection.searchable == True)
        q = _apply_collection_filter(q, Collection)
        files = q.all()
    except Exception:
        q = File.query
        allowed = _allowed_collection_ids()
        if allowed is not None:
            if not allowed:
                q = q.filter(File.collection_id == -1)
            else:
                q = q.filter(File.collection_id.in_(allowed))
        files = q.all()
    out = StringIO()
    for f in files:
        try:
            out.write(bib_for_file(f))
        except Exception:
            # резервный минимальный вариант записи misc
            out.write(f"@misc{{f{getattr(f,'id',0)}, title={{ {getattr(f,'title',None) or getattr(f,'filename','')} }} }}\n")
    return Response(out.getvalue(), mimetype="text/x-bibtex")

@routes.route('/api/collections')
def api_collections():
    allowed = _allowed_collection_ids()
    q = Collection.query.order_by(Collection.name.asc())
    if allowed is not None:
        if not allowed:
            q = q.filter(Collection.id == -1)
        else:
            q = q.filter(Collection.id.in_(allowed))
    cols = q.all()
    out = []
    for c in cols:
        out.append({
            'id': c.id,
            'name': c.name,
            'slug': c.slug,
            'searchable': bool(getattr(c, 'searchable', True)),
            'graphable': bool(getattr(c, 'graphable', True)),
            'count': len(c.files or []),
        })
    return jsonify(out)
