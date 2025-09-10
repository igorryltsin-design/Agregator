from flask import Blueprint, jsonify, request, redirect, url_for, flash, render_template, Response, send_from_directory
from io import StringIO
import csv
from pathlib import Path

from models import File, Tag, db, upsert_tag, file_to_dict, ChangeLog
from flask import current_app

routes = Blueprint('routes', __name__)

@routes.route("/api/files", methods=["GET"])
def api_files():
    files = File.query.order_by(File.mtime.desc().nullslast()).limit(200).all()
    return jsonify([file_to_dict(f) for f in files])

@routes.route("/api/files/<int:file_id>", methods=["GET"])
def api_file_detail(file_id):
    f = File.query.get_or_404(file_id)
    return jsonify(file_to_dict(f))


@routes.route('/api/graph')
def api_graph():
    """Построить простой граф (узлы и ребра) на основе файлов и их авторов.
    Узлы: file:<id> (тип work) и author:<name> (тип author).
    Рёбра: от файла к автору.
    """
    files = File.query.limit(500).all()
    nodes = []
    edges = []
    author_ids = {}
    next_author_id = 100000
    for f in files:
        fid = f.id
        nodes.append({"id": f"file-{fid}", "label": f.title or f.filename or str(fid), "type": "work"})
        if f.author:
            a = f.author.strip()
            if a not in author_ids:
                author_ids[a] = f"author-{next_author_id}"
                nodes.append({"id": author_ids[a], "label": a, "type": "author"})
                next_author_id += 1
            edges.append({"from": f"file-{fid}", "to": author_ids[a], "label": "author"})
    return jsonify({"nodes": nodes, "edges": edges})


@routes.route('/api/graph/build', methods=['POST'])
def api_graph_build():
    """Создать недостающие теги для автора и научного руководителя.
    Это помогает заполнить связи, используемые в графе.
    """
    files = File.query.all()
    created = 0
    for f in files:
        # author
        if f.author:
            if not any(t.key == 'author' and t.value == f.author for t in f.tags):
                upsert_tag(f, 'author', f.author)
                created += 1
        # advisor -> organization-like tag
        if f.advisor:
            if not any(t.key == 'advisor' and t.value == f.advisor for t in f.tags):
                upsert_tag(f, 'advisor', f.advisor)
                created += 1
    db.session.commit()
    return jsonify({"created_tags": created})

@routes.route("/api/files", methods=["POST"])
def api_file_create():
    data = request.json or {}
    f = File(
        title=data.get("title"),
        author=data.get("author"),
        year=data.get("year"),
        material_type=data.get("material_type"),
        filename=data.get("filename"),
        keywords=data.get("keywords"),
    rel_path=data.get("filename"),
    path=str(Path(current_app.config.get('UPLOAD_FOLDER', '.')) / data.get("filename", "")),
    )
    db.session.add(f)
    db.session.flush()
    for tag in data.get("tags", []):
        upsert_tag(f, tag.get("key"), tag.get("value"))
    db.session.commit()
    return jsonify(file_to_dict(f)), 201

@routes.route("/api/files/<int:file_id>", methods=["PUT"])
def api_file_update(file_id):
    f = File.query.get_or_404(file_id)
    data = request.json or {}
    old_type = (f.material_type or '').strip().lower()
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
            # move
            p_old.rename(p_new)
            # cleanup old thumbnail for PDFs
            try:
                if (ext or '').lower() == '.pdf':
                    thumb = Path(current_app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                    if thumb.exists(): thumb.unlink()
            except Exception:
                pass
            # update DB fields
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
    return jsonify(file_to_dict(f))

@routes.route('/api/files/move-by-type', methods=['POST'])
def api_move_by_type():
    """Перенести группу файлов в подпапки по текущему типу.
    JSON: {"ids":[...]} или {"all":true}
    """
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
            # skip if already in target
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
            # cleanup old thumbnail for PDFs
            try:
                if (ext or '').lower() == '.pdf':
                    thumb = Path(current_app.static_folder) / 'thumbnails' / (p_old.stem + '.png')
                    if thumb.exists(): thumb.unlink()
            except Exception:
                pass
            # update DB
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

@routes.route("/import/csv", methods=["GET", "POST"])
def import_csv():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("Файл не выбран.", "danger")
            return redirect(request.url)
        try:
            stream = StringIO(file.stream.read().decode("utf-8"))
            reader = csv.DictReader(stream)
            count = 0
            for row in reader:
                f = File(
                    title=row.get("title"),
                    author=row.get("author"),
                    year=row.get("year"),
                    material_type=row.get("material_type"),
                    filename=row.get("filename"),
                    keywords=row.get("keywords"),
                    rel_path=row.get("filename"),
                    path=str(Path(current_app.config.get('UPLOAD_FOLDER', '.')) / row.get("filename")),
                )
                db.session.add(f)
                db.session.flush()
                tags = row.get("tags", "").split(";")
                for tag in tags:
                    if "=" in tag:
                        k, v = tag.split("=", 1)
                        upsert_tag(f, k.strip(), v.strip())
                count += 1
            db.session.commit()
            flash(f"Импортировано {count} записей.", "success")
            return redirect(url_for("index"))
        except Exception as e:
            flash(f"Ошибка импорта: {e}", "danger")
            return redirect(request.url)
    return render_template("import_csv.html")
