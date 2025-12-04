import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


INVALID_CHARS = r'[<>:"/\\\\|?*]'
DEFAULT_SOURCE_ID = "default"


def safe_name(name: str, max_length: int = 80) -> str:
    """Convert an arbitrary title into a safe folder name."""
    if not name:
        name = "Untitled"
    cleaned = re.sub(INVALID_CHARS, "_", name).strip()
    cleaned = re.sub(r"\\s+", " ", cleaned)
    if len(cleaned) > max_length:
        cleaned = cleaned[: max_length - 3].rstrip() + "..."
    return cleaned or "Untitled"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ts_to_date_str(ts: Optional[float]) -> str:
    if not ts:
        return "1970-01-01"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def ts_to_human(ts: Optional[float]) -> str:
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def generate_conversation_id(raw_id: Optional[str]) -> str:
    return raw_id or str(uuid.uuid4())


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def flatten(iterable: Iterable[Iterable[Any]]) -> Iterable[Any]:
    for item in iterable:
        yield from item


def now_ts() -> float:
    return time.time()


def normalize_source_id(raw: Optional[str]) -> str:
    """Sanitize source/account id for safe use in paths and keys."""
    value = (raw or DEFAULT_SOURCE_ID).strip()
    if not value:
        value = DEFAULT_SOURCE_ID
    # Allow only alnum, dot, dash, underscore; replace others with dash
    value = re.sub(r"[^a-zA-Z0-9._-]+", "-", value)
    value = value.strip("-_.")
    value = value.lower() or DEFAULT_SOURCE_ID
    return value


def make_project_uid(source_id: str, project_id: str) -> str:
    return f"{source_id}:{project_id}"


def split_project_uid(uid: str) -> Tuple[str, str]:
    if ":" in uid:
        left, right = uid.split(":", 1)
        return left or DEFAULT_SOURCE_ID, right or "no_project"
    return DEFAULT_SOURCE_ID, uid


def make_conversation_uid(source_id: str, conversation_id: str) -> str:
    return f"{source_id}:{conversation_id}"


def split_conversation_uid(uid: str) -> Tuple[str, str]:
    if ":" in uid:
        left, right = uid.split(":", 1)
        return left or DEFAULT_SOURCE_ID, right or uid
    return DEFAULT_SOURCE_ID, uid


def load_project_overrides(root: Path) -> Dict[str, Any]:
    """Read user-defined overrides (names, moves, project_moves, projects) from project_overrides.json."""
    primary = root.parent / "project_overrides.json"
    fallback = root / "project_overrides.json"
    path = primary if primary.exists() else fallback

    def parse(path: Path) -> Dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    # Migrate old location -> new location
    if not primary.exists() and fallback.exists():
        data = parse(fallback)
        try:
            ensure_dir(primary.parent)
            write_json(
                primary,
                data if isinstance(data, dict) else {"names": {}, "moves": {}, "project_moves": {}, "projects": []},
            )
            fallback.unlink()
        except Exception:
            pass
        path = primary

    if path.exists():
        raw = parse(path)
    else:
        raw = {}
        try:
            ensure_dir(primary.parent)
            write_json(primary, {"names": {}, "moves": {}, "project_moves": {}, "projects": []})
        except Exception:
            pass

    names: Dict[str, str] = {}
    moves: Dict[str, str] = {}
    project_moves: Dict[str, str] = {}
    projects: List[str] = []

    # Structured format: {"names": {...}, "moves": {...}, "project_moves": {...}, "projects": [...]}
    if isinstance(raw, dict) and ("names" in raw or "moves" in raw or "project_moves" in raw or "projects" in raw):
        for key, value in (raw.get("names") or {}).items():
            if value is None:
                continue
            name = str(value).strip()
            if name:
                names[str(key)] = name
        for key, value in (raw.get("moves") or {}).items():
            if value is None:
                continue
            target = str(value).strip()
            if target:
                moves[str(key)] = target
        for key, value in (raw.get("project_moves") or {}).items():
            if value is None:
                continue
            target = str(value).strip()
            if target:
                project_moves[str(key)] = target
        for value in (raw.get("projects") or []):
            try:
                item = str(value).strip()
            except Exception:
                continue
            if item:
                projects.append(item)
    # Legacy flat dict: treat as names only
    elif isinstance(raw, dict):
        for key, value in raw.items():
            if value is None:
                continue
            name = str(value).strip()
            if name:
                names[str(key)] = name

    projects = list(dict.fromkeys(projects))
    return {"names": names, "moves": moves, "project_moves": project_moves, "projects": projects}


def save_project_overrides(root: Path, overrides: Dict[str, Any]) -> None:
    """Persist overrides (names, moves, project_moves, projects) to project_overrides.json."""
    names = overrides.get("names") or {}
    moves = overrides.get("moves") or {}
    project_moves = overrides.get("project_moves") or {}
    projects = overrides.get("projects") or []
    normalized_projects = []
    for p in projects:
        try:
            val = str(p).strip()
        except Exception:
            continue
        if val:
            normalized_projects.append(val)
    path = root.parent / "project_overrides.json"
    write_json(path, {"names": names, "moves": moves, "project_moves": project_moves, "projects": normalized_projects})
