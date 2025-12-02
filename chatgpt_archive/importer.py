import json
import re
import shutil
import sqlite3
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import html

from .utils import (
    DEFAULT_SOURCE_ID,
    ensure_dir,
    generate_conversation_id,
    load_project_overrides,
    make_conversation_uid,
    make_project_uid,
    normalize_source_id,
    safe_name,
    split_project_uid,
    ts_to_date_str,
    ts_to_human,
    write_json,
)


@dataclass
class ImportOptions:
    export_path: Path
    output_root: Path
    allow_network_images: bool = False  # reserved flag, not implemented
    incremental: bool = False
    source_id: str = DEFAULT_SOURCE_ID


def _unpack_export(path: Path) -> Tuple[Path, Optional[tempfile.TemporaryDirectory]]:
    """Return path to folder with conversations.json. Unpack zip if needed."""
    if path.is_dir():
        return path, None
    if not path.suffix.lower().endswith("zip"):
        raise ValueError(f"Unsupported export type: {path}")
    tmpdir = tempfile.TemporaryDirectory(prefix="chatgpt-export-")
    with zipfile.ZipFile(path, "r") as zf:
        zf.extractall(tmpdir.name)
    return Path(tmpdir.name), tmpdir


def _load_export_conversations(base: Path) -> List[Dict[str, Any]]:
    conversations_path = base / "conversations.json"
    if not conversations_path.exists():
        # Some exports могут иметь дополнительную вложенную папку
        candidates = list(base.glob("*/conversations.json"))
        if candidates:
            conversations_path = candidates[0]
        else:
            raise FileNotFoundError(f"conversations.json not found in {base}")
    with conversations_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("conversations.json must contain a JSON array")
    return data


def _build_asset_index(base: Path) -> Dict[str, Path]:
    """Create mapping asset_id -> file path for exported images/assets."""
    index: Dict[str, Path] = {}
    for path in base.rglob("file_*"):
        name = path.name
        if not name.startswith("file_"):
            continue
        asset_id = name.split("-")[0]  # strip "-sanitized" suffix if present
        index.setdefault(asset_id, path)
    return index


def _collect_gizmo_id(messages: List[Dict[str, Any]]) -> Optional[str]:
    for msg in messages:
        metadata = msg.get("metadata") or {}
        gizmo = metadata.get("gizmo_id")
        if gizmo:
            return gizmo
    return None


def _collect_model(messages: List[Dict[str, Any]]) -> Optional[str]:
    for msg in reversed(messages):
        metadata = msg.get("metadata") or {}
        model = metadata.get("model_slug") or metadata.get("model") or metadata.get("model_name")
        if model:
            return model
    return None


def _extract_message_payload(message: Dict[str, Any], asset_index: Dict[str, Path]) -> Tuple[str, List[Dict[str, Any]]]:
    """Return text and attachments for a message."""
    content = message.get("content")
    if not content:
        return "", []
    content_type = content.get("content_type")
    parts = content.get("parts") or []
    text_parts: List[str] = []
    attachments: List[Dict[str, Any]] = []

    def handle_part(part: Any) -> None:
        if isinstance(part, dict) and part.get("asset_pointer"):
            pointer = part.get("asset_pointer")
            asset_id = pointer.split("://")[-1] if pointer else None
            if asset_id:
                src = asset_index.get(asset_id)
                attachments.append(
                    {
                        "asset_id": asset_id,
                        "pointer": pointer,
                        "source_path": str(src) if src else None,
                        "width": part.get("width"),
                        "height": part.get("height"),
                        "size_bytes": part.get("size_bytes"),
                    }
                )
            return
        if isinstance(part, dict) and "text" in part:
            text_parts.append(str(part.get("text", "")))
            return
        if isinstance(part, str):
            text_parts.append(part)

    if content_type in ("text", "multimodal_text"):
        for p in parts:
            handle_part(p)
    else:
        # Пропускаем служебные типы (thoughts, code, system и т.п.), они не для показа
        return "", attachments

    raw_text = "\n".join(text_parts).strip()
    cleaned = _strip_inline_markers(raw_text)
    return cleaned, attachments


def _strip_inline_markers(text: str) -> str:
    """Remove inline markers like financeturn0finance0 or cite tags."""
    if not text:
        return text
    # Удаляем блоки в формате ... (ChatGPT reasoning/refs маркеры)
    text = re.sub(r"[^]*", "", text)
    # Удаляем упоминания cite|finance маркеров в квадратных скобках, если остались
    text = re.sub(r"\[cite:[^\]]+\]", "", text)
    text = re.sub(r"\[finance:[^\]]+\]", "", text)
    # Удаляем псевдо-ссылки формата 【turn6file4†L31-L39】
    text = re.sub(r"【[^】]*】", "", text)
    return text.strip()


def _build_primary_path(mapping: Dict[str, Any], current_node: Optional[str]) -> List[str]:
    """Pick a linearized path through the mapping. Prefers current_node chain, otherwise picks the latest leaf."""
    if not mapping:
        return []

    def walk_to_root(node_id: str) -> List[str]:
        chain: List[str] = []
        while node_id:
            chain.append(node_id)
            node = mapping.get(node_id) or {}
            node_id = node.get("parent")
        return list(reversed(chain))

    if current_node and current_node in mapping:
        return walk_to_root(current_node)

    best_node = None
    best_ts = -1.0
    for node in mapping.values():
        msg = node.get("message") or {}
        ts = msg.get("create_time") or msg.get("update_time") or 0
        if ts >= best_ts:
            best_ts = ts
            best_node = node.get("id")
    if best_node:
        return walk_to_root(best_node)

    # Fallback to arbitrary order
    return [node.get("id") for node in mapping.values() if node.get("id")]


def _extract_messages(mapping: Dict[str, Any], current_node: Optional[str], asset_index: Dict[str, Path]) -> List[Dict[str, Any]]:
    path_ids = _build_primary_path(mapping, current_node)
    messages: List[Dict[str, Any]] = []
    for node_id in path_ids:
        node = mapping.get(node_id) or {}
        message = node.get("message")
        if not message:
            continue
        role = (message.get("author") or {}).get("role") or "unknown"
        if role not in ("user", "assistant"):
            continue  # скрываем system/tool и прочие служебные сообщения
        metadata = message.get("metadata") or {}
        if metadata.get("is_system_message"):
            continue
        if metadata.get("reasoning_status"):
            continue  # скрываем служебные reasoning/coT сообщения ассистента
        text, attachments = _extract_message_payload(message, asset_index)
        if not text and not attachments:
            continue
        ts = message.get("create_time") or message.get("update_time")
        messages.append(
            {
                "id": node_id,
                "role": role,
                "text": text,
                "timestamp": ts,
                "metadata": metadata,
                "attachments": attachments,
            }
        )
    return messages


def _build_conversation_folder(title: str, created_at: Optional[float], conversation_id: str) -> str:
    date_prefix = ts_to_date_str(created_at)
    readable_title = safe_name(title) if title else f"Untitled-{conversation_id[:8]}"
    return f"{date_prefix} - {readable_title}"


def _copy_attachments(messages: List[Dict[str, Any]], asset_index: Dict[str, Path], chat_dir: Path, rel_folder: str) -> None:
    image_dir = chat_dir / "images"
    copied: Dict[str, Path] = {}
    for msg in messages:
        for attachment in msg.get("attachments", []) or []:
            asset_id = attachment.get("asset_id")
            src = asset_index.get(asset_id) if asset_id else None
            if not src or not src.exists():
                continue
            ensure_dir(image_dir)
            dest_name = src.name  # сохраняем оригинальное имя (с суффиксом -sanitized)
            dest_path = image_dir / dest_name
            if asset_id not in copied:
                shutil.copy(src, dest_path)
                copied[asset_id] = dest_path
            rel_path = f"{rel_folder}/images/{dest_name}"
            attachment["local_path"] = f"images/{dest_name}"
            attachment["path"] = rel_path  # для веб-выдачи через /files/


def _write_markdown(conv_title: str, created_at: Optional[float], messages: List[Dict[str, Any]], dest: Path) -> None:
    lines = [f"# {conv_title or 'Untitled'}", f"Дата: {ts_to_human(created_at)}", "", "---", ""]
    for msg in messages:
        role = msg.get("role", "unknown").capitalize()
        text = msg.get("text") or ""
        lines.append(f"**{role}:**  ")
        lines.append(text)
        attachments = msg.get("attachments") or []
        for att in attachments:
            if att.get("local_path"):
                lines.append(f"![image]({att['local_path']})")
        lines.append("")
    dest.write_text("\n".join(lines), encoding="utf-8")


def _write_html(conv_title: str, created_at: Optional[float], messages: List[Dict[str, Any]], dest: Path) -> None:
    esc = html.escape
    lines = [
        "<!doctype html>",
        "<html><head>",
        '<meta charset="UTF-8" />',
        f"<title>{esc(conv_title or 'Untitled')}</title>",
        "<style>",
        "html,body{min-height:100%; margin:0; padding:0; background:#0f172a;}",
        "body{display:flex; justify-content:center; font-family:Arial, sans-serif; color:#e5e7eb;}",
        ".page{width:min(50vw, 900px); padding:24px; margin:24px auto;}",
        ".msg{border:1px solid rgba(255,255,255,0.08); border-radius:12px; padding:12px; margin-bottom:12px; background:#0c1220;}",
        ".role{font-weight:700; text-transform:uppercase; font-size:12px; letter-spacing:1px; color:#38bdf8; margin-bottom:6px;}",
        ".assistant .role{color:#fbbf24;}",
        ".text{white-space:pre-wrap; font-size:14px; line-height:1.5;}",
        ".attachments img{max-width:100%; border-radius:8px; margin-top:8px;}",
        "</style>",
        "</head><body>",
        '<div class="page">',
        f"<h1>{esc(conv_title or 'Untitled')}</h1>",
        f"<div>Дата: {esc(ts_to_human(created_at))}</div>",
        "<hr/>",
    ]
    for msg in messages:
        role = msg.get("role", "unknown")
        text = msg.get("text") or ""
        lines.append(f'<div class="msg {esc(role)}">')
        lines.append(f'<div class="role">{esc(role)}</div>')
        lines.append(f'<div class="text">{esc(text)}</div>')
        attachments = msg.get("attachments") or []
        if attachments:
            lines.append('<div class="attachments">')
            for att in attachments:
                local = att.get("local_path")
                if local:
                    lines.append(f'<img src="{esc(local)}" alt="{esc(att.get("asset_id","image"))}"/>')
            lines.append("</div>")
        lines.append("</div>")
    lines.append("</div></body></html>")
    dest.write_text("\n".join(lines), encoding="utf-8")


def _write_obsidian(
    conv_title: str,
    created_at: Optional[float],
    project_id: str,
    model: Optional[str],
    messages: List[Dict[str, Any]],
    dest: Path,
) -> None:
    """Generate Obsidian-friendly Markdown with фронтматтером."""
    frontmatter = [
        "---",
        f'title: "{conv_title or "Untitled"}"',
        f'date: "{ts_to_human(created_at)}"',
        f'project: "{project_id}"',
        f'model: "{model or ""}"',
        "---",
        "",
    ]
    body: List[str] = []
    for msg in messages:
        role = msg.get("role", "unknown").capitalize()
        body.append(f"### {role}")
        body.append(msg.get("text") or "")
        attachments = msg.get("attachments") or []
        for att in attachments:
            if att.get("local_path"):
                # Obsidian поддерживает относительные ссылки на изображения
                body.append(f"![[{att['local_path']}]]")
        body.append("")
    dest.write_text("\n".join(frontmatter + body), encoding="utf-8")


def _prepare_database(path: Path, rebuild: bool) -> sqlite3.Connection:
    ensure_dir(path.parent)
    file_existed = path.exists()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.OperationalError:
        # На некоторых FS (сетевые/смонтированные) WAL может быть недоступен.
        pass
    def reset_schema() -> None:
        conn.executescript(
            """
            DROP TABLE IF EXISTS conversations;
            DROP TABLE IF EXISTS messages;
            DROP TABLE IF EXISTS projects;
            DROP TABLE IF EXISTS messages_fts;
            DROP TABLE IF EXISTS imports;
            CREATE TABLE conversations (
                conversation_uid TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                conversation_id TEXT,
                project_id TEXT,
                project_uid TEXT,
                title TEXT,
                created_at REAL,
                updated_at REAL,
                snippet TEXT,
                folder TEXT,
                model TEXT
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_uid TEXT,
                source_id TEXT,
                role TEXT,
                content TEXT,
                created_at REAL
            );
            CREATE TABLE projects (
                project_uid TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                human_name TEXT,
                conversation_count INTEGER,
                first_message_time REAL,
                last_message_time REAL
            );
            CREATE VIRTUAL TABLE messages_fts USING fts5(
                content,
                conversation_uid UNINDEXED,
                role UNINDEXED,
                source_id UNINDEXED
            );
            CREATE TABLE imports (
                import_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                started_at REAL,
                completed_at REAL,
                conversations INTEGER
            );
            """
        )

    needs_reset = rebuild or not file_existed
    if not needs_reset:
        try:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(conversations)")]
            needs_reset = "conversation_uid" not in cols or "source_id" not in cols
        except sqlite3.DatabaseError:
            needs_reset = True

    if needs_reset:
        reset_schema()
    return conn


def _refresh_projects_table(conn: sqlite3.Connection, output_root: Path, name_overrides: Dict[str, str]) -> None:
    rows = conn.execute(
        "SELECT source_id, project_id, COUNT(*) AS cnt, MIN(created_at) AS first_message_time, MAX(updated_at) AS last_message_time "
        "FROM conversations GROUP BY source_id, project_id"
    ).fetchall()
    conn.execute("DELETE FROM projects")
    project_rows: List[Tuple[Any, ...]] = []
    projects_root = output_root / "projects"
    for r in rows:
        source_id = r["source_id"]
        project_id = r["project_id"]
        project_uid = make_project_uid(source_id, project_id)
        default_name = "Без проекта" if project_id == "no_project" else f"Project {project_id[:8]}"
        human_name = name_overrides.get(project_uid) or name_overrides.get(project_id) or default_name
        meta_payload = {
            "project_id": project_id,
            "project_uid": project_uid,
            "source_id": source_id,
            "human_name": human_name,
            "conversation_count": r["cnt"],
            "first_message_time": r["first_message_time"],
            "last_message_time": r["last_message_time"],
        }
        meta_path = projects_root / source_id / project_id / "_meta.json"
        ensure_dir(meta_path.parent)
        write_json(meta_path, meta_payload)
        project_rows.append(
            (
                project_uid,
                source_id,
                project_id,
                human_name,
                r["cnt"],
                r["first_message_time"],
                r["last_message_time"],
            )
        )
    if project_rows:
        conn.executemany(
            "INSERT INTO projects (project_uid, source_id, project_id, human_name, conversation_count, first_message_time, last_message_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
            project_rows,
        )


def import_archive(options: ImportOptions) -> Dict[str, Any]:
    export_base, tmpdir = _unpack_export(options.export_path)
    try:
        raw_conversations = _load_export_conversations(export_base)
        asset_index = _build_asset_index(export_base)
        source_id = normalize_source_id(options.source_id)
        source_folder = source_id
        output_root = options.output_root
        ensure_dir(output_root)
        projects_root = output_root / "projects"
        ensure_dir(projects_root)
        overrides = load_project_overrides(output_root)
        name_overrides = overrides.get("names", {})
        move_overrides = overrides.get("moves", {})

        db_path = output_root / "index.db"
        conn = _prepare_database(db_path, rebuild=not options.incremental)

        existing_map: Dict[str, Dict[str, Any]] = {}
        if options.incremental:
            try:
                cursor = conn.execute(
                    "SELECT conversation_uid, updated_at, folder FROM conversations WHERE source_id = ?",
                    (source_id,),
                )
                for row in cursor.fetchall():
                    existing_map[row[0]] = {"updated_at": row[1], "folder": row[2]}
            except Exception:
                existing_map = {}

        conversations_rows: List[Tuple[Any, ...]] = []
        messages_rows: List[Tuple[Any, ...]] = []
        fts_rows: List[Tuple[Any, ...]] = []
        model_set = set()
        imported_count = 0
        skipped_existing = 0

        for idx, conversation in enumerate(raw_conversations):
            conv_id = generate_conversation_id(conversation.get("id"))
            conv_uid = make_conversation_uid(source_id, conv_id)
            title = conversation.get("title") or f"Untitled {conv_id[:8]}"
            mapping = conversation.get("mapping") or {}
            current_node = conversation.get("current_node")
            messages = _extract_messages(mapping, current_node, asset_index)
            if not messages:
                continue
            created_at = conversation.get("create_time") or messages[0].get("timestamp")
            updated_at = conversation.get("update_time") or messages[-1].get("timestamp")
            gizmo_id = _collect_gizmo_id(messages) or "no_project"
            model = _collect_model(messages)
            if model:
                model_set.add(model)
            target_project_override = move_overrides.get(conv_uid) or move_overrides.get(conv_id)
            if target_project_override:
                override_source, override_project = split_project_uid(str(target_project_override))
                if override_source == source_id:
                    gizmo_id = override_project

            existing_info = existing_map.get(conv_uid)
            existing_updated = (existing_info or {}).get("updated_at")
            if options.incremental and existing_info and updated_at and existing_updated and updated_at <= existing_updated:
                skipped_existing += 1
                continue

            folder_name = _build_conversation_folder(title, created_at, conv_id)
            project_uid = make_project_uid(source_id, gizmo_id)
            project_dir = projects_root / source_folder / gizmo_id
            chat_dir = project_dir / folder_name
            if chat_dir.exists():
                shutil.rmtree(chat_dir, ignore_errors=True)
            ensure_dir(chat_dir)
            rel_folder = f"projects/{source_folder}/{gizmo_id}/{folder_name}"

            if options.incremental:
                # Удаляем старые данные, если были, и старую папку чата (вдруг имя изменилось)
                if existing_info:
                    old_folder = existing_info.get("folder")
                    if old_folder:
                        old_path = (output_root / old_folder).resolve()
                        try:
                            if output_root in old_path.parents or output_root == old_path:
                                shutil.rmtree(old_path, ignore_errors=True)
                        except Exception:
                            pass
                conn.execute("DELETE FROM messages WHERE conversation_uid = ?", (conv_uid,))
                conn.execute("DELETE FROM messages_fts WHERE conversation_uid = ?", (conv_uid,))
                conn.execute("DELETE FROM conversations WHERE conversation_uid = ?", (conv_uid,))

            _copy_attachments(messages, asset_index, chat_dir, rel_folder)

            conversation_json = {
                "conversation_uid": conv_uid,
                "conversation_id": conv_id,
                "project_id": gizmo_id,
                "project_uid": project_uid,
                "source_id": source_id,
                "source_index": idx,
                "title": title,
                "created_at": created_at,
                "updated_at": updated_at,
                "messages": messages,
                "metadata": {
                    "gizmo_id": gizmo_id,
                    "model": model,
                    "source_id": source_id,
                },
                "files": {
                    "markdown": "conversation.md",
                    "html": "conversation.html",
                    "obsidian": "conversation-obsidian.md",
                },
            }
            write_json(chat_dir / "conversation.json", conversation_json)
            _write_markdown(title, created_at, messages, chat_dir / "conversation.md")
            _write_html(title, created_at, messages, chat_dir / "conversation.html")
            _write_obsidian(title, created_at, project_uid, model, messages, chat_dir / "conversation-obsidian.md")

            snippet = ""
            for msg in messages:
                if msg.get("role") == "user":
                    snippet = (msg.get("text") or "")[:240]
                    break

            conversations_rows.append(
                (
                    conv_uid,
                    source_id,
                    conv_id,
                    gizmo_id,
                    project_uid,
                    title,
                    created_at,
                    updated_at,
                    snippet,
                    rel_folder,
                    model,
                )
            )
            for msg in messages:
                messages_rows.append(
                    (conv_uid, source_id, msg.get("role"), msg.get("text"), msg.get("timestamp"))
                )
                fts_rows.append((msg.get("text") or "", conv_uid, msg.get("role"), source_id))
            imported_count += 1

        # Persist DB data
        conn.executemany(
            "INSERT INTO conversations (conversation_uid, source_id, conversation_id, project_id, project_uid, title, created_at, updated_at, snippet, folder, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            conversations_rows,
        )
        conn.executemany(
            "INSERT INTO messages (conversation_uid, source_id, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
            messages_rows,
        )
        conn.executemany(
            "INSERT INTO messages_fts (content, conversation_uid, role, source_id) VALUES (?, ?, ?, ?)",
            fts_rows,
        )

        _refresh_projects_table(conn, output_root, name_overrides)
        conn.commit()

        return {
            "conversations": len(conversations_rows),
            "projects": conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0],
            "models": sorted(model_set),
            "db_path": str(db_path),
            "output_root": str(output_root),
            "source_id": source_id,
            "imported_conversations": imported_count,
            "append_mode": options.incremental,
            "skipped_existing": skipped_existing,
        }
    finally:
        if tmpdir:
            tmpdir.cleanup()
