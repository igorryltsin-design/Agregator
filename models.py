import hashlib
from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String, unique=True, nullable=False)
    password_hash = db.Column(db.String, nullable=False)
    role = db.Column(db.String, nullable=False, default="user")
    full_name = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password.strip())

    def check_password(self, password: str) -> bool:
        if not password:
            return False
        try:
            return check_password_hash(self.password_hash, password)
        except Exception:
            return False

class Collection(db.Model):
    __tablename__ = "collections"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    slug = db.Column(db.String, unique=True, nullable=False)
    searchable = db.Column(db.Boolean, default=True, nullable=False)
    graphable = db.Column(db.Boolean, default=True, nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    is_private = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    owner = db.relationship("User", backref="collections", lazy=True)


class CollectionMember(db.Model):
    __tablename__ = "collection_members"
    id = db.Column(db.Integer, primary_key=True)
    collection_id = db.Column(db.Integer, db.ForeignKey("collections.id", ondelete="CASCADE"), index=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    role = db.Column(db.String, nullable=False, default="viewer")  # viewer/editor
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    collection = db.relationship("Collection", backref="members", lazy=True)
    user = db.relationship("User", backref="collection_memberships", lazy=True)


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
    collection_id = db.Column(db.Integer, db.ForeignKey("collections.id", ondelete="SET NULL"), index=True, nullable=True)
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

    collection = db.relationship("Collection", backref="files", lazy=True)
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


class UserActionLog(db.Model):
    __tablename__ = "user_action_log"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    action = db.Column(db.String, nullable=False)
    entity = db.Column(db.String, nullable=True)
    entity_id = db.Column(db.Integer, nullable=True)
    detail = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User", backref="action_logs", lazy=True)


class TaskRecord(db.Model):
    __tablename__ = "task_records"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False, default="pending")
    payload = db.Column(db.Text, nullable=True)
    progress = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
    error = db.Column(db.String, nullable=True)


class LlmEndpoint(db.Model):
    __tablename__ = "llm_endpoints"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    base_url = db.Column(db.String, nullable=False)
    model = db.Column(db.String, nullable=False)
    api_key = db.Column(db.String, nullable=True)
    weight = db.Column(db.Float, nullable=False, default=1.0)
    purpose = db.Column(db.String, nullable=True)  # e.g. rerank, summary, transcription
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AiWordAccess(db.Model):
    __tablename__ = "aiword_access"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False)
    granted_by = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User", foreign_keys=[user_id], backref="aiword_permission", lazy=True)
    granted_by_user = db.relationship("User", foreign_keys=[granted_by], lazy=True)


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
        "collection_id": getattr(f, 'collection_id', None),
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
