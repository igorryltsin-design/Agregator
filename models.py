import hashlib
from datetime import datetime
from pathlib import Path
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class ChangeLog(db.Model):
    __tablename__ = "changelog"
    id = db.Column(db.Integer, primary_key=True)
    file_id = db.Column(db.Integer, db.ForeignKey("files.id", ondelete="CASCADE"), index=True, nullable=True)
    action = db.Column(db.String, nullable=False)
    field = db.Column(db.String, nullable=True)
    old_value = db.Column(db.String, nullable=True)
    new_value = db.Column(db.String, nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    info = db.Column(db.String, nullable=True)


class File(db.Model):
    __tablename__ = "files"
    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.String, unique=True, nullable=False)
    rel_path = db.Column(db.String, nullable=False)
    filename = db.Column(db.String, nullable=False)
    ext = db.Column(db.String, nullable=True)
    size = db.Column(db.Integer, nullable=True)
    mtime = db.Column(db.Float, nullable=True)
    sha1 = db.Column(db.String, nullable=True, index=True)

    material_type = db.Column(db.String, nullable=True)
    title = db.Column(db.String, nullable=True)
    author = db.Column(db.String, nullable=True)
    year = db.Column(db.String, nullable=True)
    advisor = db.Column(db.String, nullable=True)
    keywords = db.Column(db.String, nullable=True)
    abstract = db.Column(db.Text, nullable=True)
    text_excerpt = db.Column(db.Text, nullable=True)

    tags = db.relationship("Tag", backref="file", cascade="all, delete-orphan")


class Tag(db.Model):
    __tablename__ = "tags"
    id = db.Column(db.Integer, primary_key=True)
    file_id = db.Column(db.Integer, db.ForeignKey("files.id", ondelete="CASCADE"), index=True, nullable=False)
    key = db.Column(db.String, index=True, nullable=False)
    value = db.Column(db.String, index=True, nullable=False)


class TagSchema(db.Model):
    __tablename__ = "tag_schemas"
    id = db.Column(db.Integer, primary_key=True)
    material_type = db.Column(db.String, index=True, nullable=False)
    key = db.Column(db.String, index=True, nullable=False)
    description = db.Column(db.String, nullable=True)


def upsert_tag(file_obj: File, key: str, value: str):
    key = (key or "").strip()
    value = (value or "").strip()
    if not key or not value:
        return
    t = Tag.query.filter_by(file_id=file_obj.id, key=key, value=value).first()
    if not t:
        t = Tag(file_id=file_obj.id, key=key, value=value)
        db.session.add(t)


def file_to_dict(f: File):
    return {
        "id": f.id,
        "title": f.title,
        "author": f.author,
        "year": f.year,
        "material_type": f.material_type,
        "filename": f.filename,
        "keywords": f.keywords,
        "tags": [{"key": t.key, "value": t.value} for t in f.tags],
        "rel_path": f.rel_path,
        "path": f.path,
        "size": f.size,
        "mtime": f.mtime,
        "sha1": f.sha1,
    }
