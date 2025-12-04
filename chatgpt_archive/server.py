import http.server
import json
import mimetypes
import re
import shutil
import socketserver
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, quote, urlparse, unquote

from .importer import ImportOptions, import_archive, _prepare_database
from .utils import (
    DEFAULT_SOURCE_ID,
    ensure_dir,
    load_project_overrides,
    make_project_uid,
    normalize_source_id,
    save_project_overrides,
    split_project_uid,
    write_json,
)

def normalize_project_name(value: str) -> str:
    raw = (value or "").strip()
    try:
        normalized = unicodedata.normalize("NFKC", raw)
    except Exception:
        normalized = raw
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()


def unique_preserve_order(seq: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def build_content_disposition(filename: str) -> str:
    """Return ASCII-safe Content-Disposition with UTF-8 filename* for non-ASCII names."""
    base = filename or "export.txt"
    ascii_name = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("_") or "export.txt"
    try:
        encoded = quote(base, safe="")
        return f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{encoded}'
    except Exception:
        return f'attachment; filename="{ascii_name}"'


class ArchiveServer:
    def __init__(self, root: Path, host: str = "127.0.0.1", port: int = 8000) -> None:
        self.root = root.resolve()
        self.host = host
        self.port = port
        self.db_path = self.root / "index.db"
        if not self.db_path.exists():
            # Создаем пустую схему, чтобы UI мог стартовать "с нуля"
            _prepare_database(self.db_path, rebuild=True).close()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.OperationalError:
            pass
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(conversations)")}
        if "conversation_uid" not in cols:
            raise RuntimeError("index.db uses a legacy schema; re-import the archive with the updated CLI.")
        self.static_dir = Path(__file__).resolve().parent / "templates"

    def _reload_connection(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.OperationalError:
            pass

    def _send_json(self, handler: http.server.SimpleHTTPRequestHandler, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _send_text(
        self, handler: http.server.SimpleHTTPRequestHandler, payload: str, status: int = 200, filename: str = "export.txt"
    ) -> None:
        body = payload.encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "text/plain; charset=utf-8")
        handler.send_header("Content-Disposition", build_content_disposition(filename))
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _serve_file(self, handler: http.server.SimpleHTTPRequestHandler, rel_path: str) -> None:
        fs_path = (self.root / rel_path).resolve()
        if self.root not in fs_path.parents and self.root != fs_path:
            handler.send_response(403)
            handler.end_headers()
            return
        if not fs_path.exists():
            handler.send_response(404)
            handler.end_headers()
            return
        data = fs_path.read_bytes()
        mime, _ = mimetypes.guess_type(str(fs_path))
        handler.send_response(200)
        handler.send_header("Content-Type", mime or "application/octet-stream")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    def _handle_projects(self, handler: http.server.SimpleHTTPRequestHandler) -> None:
        rows = self.conn.execute(
            "SELECT project_uid, source_id, project_id, human_name, conversation_count, first_message_time, last_message_time "
            "FROM projects ORDER BY source_id, human_name"
        ).fetchall()
        overrides = load_project_overrides(self.root)
        name_overrides = overrides.get("names", {})
        payload = []
        for r in rows:
            item = dict(r)
            override = name_overrides.get(item["project_uid"]) or name_overrides.get(item["project_id"])
            if override:
                item["human_name"] = override
            payload.append(item)
        self._send_json(handler, payload)

    def _find_project_uids_by_name(self, raw_name: str) -> List[str]:
        """Find all project_uids that correspond to a human_name (overrides + DB), normalized like the UI does."""
        normalized = normalize_project_name(raw_name)
        if not normalized:
            return []
        rows = self.conn.execute("SELECT project_uid, project_id, human_name FROM projects").fetchall()
        # 1) match by stored human_name
        matches = [r["project_uid"] for r in rows if normalize_project_name(r["human_name"]) == normalized]
        # 2) also respect overrides that might differ from the DB (defensive)
        overrides = load_project_overrides(self.root)
        name_overrides = overrides.get("names", {})
        for key, name in name_overrides.items():
            if normalize_project_name(name) != normalized:
                continue
            if ":" in key:
                matches.append(key)
            else:
                # key is project_id without source; map to all matching rows
                matches.extend([r["project_uid"] for r in rows if r["project_id"] == key])
        return unique_preserve_order(matches)

    def _handle_models(self, handler: http.server.SimpleHTTPRequestHandler) -> None:
        rows = self.conn.execute("SELECT DISTINCT model FROM conversations WHERE model IS NOT NULL").fetchall()
        models = [r["model"] for r in rows if r["model"]]
        self._send_json(handler, models)

    def _handle_conversations(self, handler: http.server.SimpleHTTPRequestHandler, query: Dict[str, List[str]]) -> None:
        clauses = []
        params: List[Any] = []
        join_fragments: List[str] = []
        q = (query.get("q") or [""])[0].strip()
        project_id = (query.get("project_id") or [""])[0].strip()
        project_name_raw = (query.get("project_name") or [""])[0]
        project_name = project_name_raw.strip()
        source_id = (query.get("source_id") or [""])[0].strip()
        role = (query.get("role") or [""])[0].strip()
        model = (query.get("model") or [""])[0].strip()
        date_from = (query.get("date_from") or [""])[0].strip()
        date_to = (query.get("date_to") or [""])[0].strip()

        use_fts = bool(q or role)
        if use_fts:
            join_fragments.append("JOIN messages_fts f ON c.conversation_uid = f.conversation_uid")
        project_uids_for_name: List[str] = []
        if project_name:
            project_uids_for_name = self._find_project_uids_by_name(project_name_raw)
            if not project_uids_for_name:
                return self._send_json(handler, [])
        if project_id:
            if ":" in project_id:
                clauses.append("c.project_uid = ?")
                params.append(project_id)
            else:
                clauses.append("c.project_id = ?")
                params.append(project_id)
        if project_uids_for_name:
            placeholders = ",".join("?" for _ in project_uids_for_name)
            clauses.append(f"c.project_uid IN ({placeholders})")
            params.extend(project_uids_for_name)
        if source_id:
            clauses.append("c.source_id = ?")
            params.append(source_id)
        if model:
            if model.lower() == "research":
                clauses.append("c.model LIKE 'research%'")
            elif model.lower() == "chat":
                clauses.append("(c.model IS NULL OR c.model NOT LIKE 'research%')")
            else:
                clauses.append("c.model = ?")
                params.append(model)
        if date_from:
            clauses.append("c.updated_at >= ?")
            params.append(float(date_from))
        if date_to:
            clauses.append("c.updated_at <= ?")
            params.append(float(date_to))
        if role and use_fts:
            clauses.append("f.role = ?")
            params.append(role)
        if q:
            clauses.append("messages_fts MATCH ?")
            params.append(q)

        where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
        join_sql = " ".join(join_fragments)
        sql = (
            "SELECT DISTINCT c.conversation_uid as conversation_id, c.conversation_id as original_id, c.source_id, c.project_uid, c.project_id, "
            "c.title, c.created_at, c.updated_at, c.snippet, c.folder, c.model "
            "FROM conversations c "
            f"{join_sql} "
            f"{where_sql} "
            "ORDER BY c.updated_at DESC LIMIT 400"
        )
        rows = self.conn.execute(sql, params).fetchall()
        self._send_json(handler, [dict(r) for r in rows])

    def _handle_conversation(self, handler: http.server.SimpleHTTPRequestHandler, conversation_id: str) -> None:
        row = self.conn.execute(
            "SELECT conversation_uid, conversation_id AS original_id, source_id, project_id, project_uid, folder "
            "FROM conversations WHERE conversation_uid = ?",
            (conversation_id,),
        ).fetchone()
        if not row and ":" not in conversation_id:
            row = self.conn.execute(
                "SELECT conversation_uid, conversation_id AS original_id, source_id, project_id, project_uid, folder "
                "FROM conversations WHERE conversation_id = ? ORDER BY updated_at DESC LIMIT 1",
                (conversation_id,),
            ).fetchone()
        if not row:
            self._send_json(handler, {"error": "Not found"}, status=404)
            return
        folder = row["folder"]
        base_path = (self.root / folder).resolve()
        if self.root not in base_path.parents and self.root != base_path:
            self._send_json(handler, {"error": "Invalid path"}, status=400)
            return
        json_path = base_path / "conversation.json"
        md_path = base_path / "conversation.md"
        html_path = base_path / "conversation.html"
        obsidian_path = base_path / "conversation-obsidian.md"
        payload: Dict[str, Any] = {
            "conversation_id": row["conversation_uid"],
            "original_id": row["original_id"],
            "project_id": row["project_uid"],
            "project_uid": row["project_uid"],
            "project_code": row["project_id"],
            "source_id": row["source_id"],
        }
        try:
            payload["conversation"] = json.loads(json_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            payload["conversation"] = None
        payload["markdown"] = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
        payload["html"] = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
        payload["obsidian"] = obsidian_path.read_text(encoding="utf-8") if obsidian_path.exists() else ""
        payload["paths"] = {
            "json": str(json_path),
            "markdown": str(md_path),
            "html": str(html_path),
            "obsidian": str(obsidian_path),
            "web": {
                "json": f"/files/{folder}/conversation.json",
                "markdown": f"/files/{folder}/conversation.md",
                "html": f"/files/{folder}/conversation.html",
                "obsidian": f"/files/{folder}/conversation-obsidian.md",
            },
        }
        self._send_json(handler, payload)

    def _recalculate_projects(self) -> None:
        overrides = load_project_overrides(self.root)
        name_overrides = overrides.get("names", {})
        rows = self.conn.execute(
            "SELECT source_id, project_id, COUNT(*) as cnt, MIN(created_at) as first_message_time, MAX(updated_at) as last_message_time "
            "FROM conversations GROUP BY source_id, project_id"
        ).fetchall()
        self.conn.execute("DELETE FROM projects")
        project_rows = []
        projects_root = self.root / "projects"
        for r in rows:
            pid = r["project_id"]
            source_id = r["source_id"]
            project_uid = make_project_uid(source_id, pid)
            default_name = "Без проекта" if pid == "no_project" else f"Project {pid[:8]}"
            human_name = name_overrides.get(project_uid) or name_overrides.get(pid) or default_name
            meta_payload = {
                "project_id": pid,
                "project_uid": project_uid,
                "source_id": source_id,
                "human_name": human_name,
                "conversation_count": r["cnt"],
                "first_message_time": r["first_message_time"],
                "last_message_time": r["last_message_time"],
            }
            meta_path = projects_root / source_id / pid / "_meta.json"
            ensure_dir(meta_path.parent)
            write_json(meta_path, meta_payload)
            project_rows.append(
                (
                    project_uid,
                    source_id,
                    pid,
                    human_name,
                    r["cnt"],
                    r["first_message_time"],
                    r["last_message_time"],
                )
            )
        if project_rows:
            self.conn.executemany(
                "INSERT INTO projects (project_uid, source_id, project_id, human_name, conversation_count, first_message_time, last_message_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                project_rows,
            )
        self.conn.commit()

    def _handle_export_txt(self, handler: http.server.SimpleHTTPRequestHandler, query: Dict[str, List[str]]) -> None:
        project_filter = (query.get("project_id") or [""])[0].strip()
        project_name_raw = (query.get("project_name") or [""])[0]
        project_name = project_name_raw.strip()
        source_filter = (query.get("source_id") or [""])[0].strip()
        clauses = []
        params: List[Any] = []
        join_fragments: List[str] = []
        if project_filter:
            if ":" in project_filter:
                clauses.append("c.project_uid = ?")
            else:
                clauses.append("c.project_id = ?")
            params.append(project_filter)
        project_uids_for_name: List[str] = []
        if project_name:
            project_uids_for_name = self._find_project_uids_by_name(project_name_raw)
            if not project_uids_for_name:
                return self._send_json(handler, {"error": "No data to export"}, status=404)
        if project_uids_for_name:
            placeholders = ",".join("?" for _ in project_uids_for_name)
            clauses.append(f"c.project_uid IN ({placeholders})")
            params.extend(project_uids_for_name)
        if source_filter:
            clauses.append("c.source_id = ?")
            params.append(source_filter)
        where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
        join_sql = " ".join(join_fragments)

        projects_map = {
            row["project_uid"]: row["human_name"]
            for row in self.conn.execute("SELECT project_uid, human_name FROM projects").fetchall()
        }
        overrides = load_project_overrides(self.root)
        name_overrides = overrides.get("names", {})

        rows = self.conn.execute(
            "SELECT c.source_id, c.project_id, c.project_uid, c.title, c.conversation_uid, c.created_at, c.updated_at, "
            "m.role, m.content, m.created_at AS message_created "
            "FROM conversations c "
            "JOIN messages m ON c.conversation_uid = m.conversation_uid "
            f"{join_sql} "
            f"{where_sql} "
            "ORDER BY c.source_id, c.project_uid, c.updated_at, c.conversation_uid, m.created_at",
            params,
        ).fetchall()

        if not rows:
            return self._send_json(handler, {"error": "No data to export"}, status=404)

        def fmt_ts(ts: Any) -> str:
            try:
                if ts is None:
                    return ""
                return datetime.fromtimestamp(float(ts)).isoformat(sep=" ", timespec="seconds")
            except Exception:
                return ""

        lines: List[str] = []
        current_source = None
        current_project = None
        current_conv = None
        for r in rows:
            source_id = r["source_id"]
            pid = r["project_id"]
            project_uid = r["project_uid"]
            conv_id = r["conversation_uid"]
            if source_id != current_source:
                if lines:
                    lines.append("")
                lines.append(f"### Account: {source_id}")
                current_source = source_id
                current_project = None
                current_conv = None
            if project_uid != current_project:
                if lines:
                    lines.append("")
                project_name = (
                    name_overrides.get(project_uid)
                    or name_overrides.get(pid)
                    or projects_map.get(project_uid)
                    or pid
                    or "unknown"
                )
                lines.append(f"=== Project: {project_name} ({project_uid}) ===")
                current_project = project_uid
                current_conv = None
            if conv_id != current_conv:
                title = (r["title"] or "Untitled").replace("\n", " ").strip()
                upd = fmt_ts(r["updated_at"])
                lines.append(f"-- Chat: {title} [{conv_id}]{f' | Updated: {upd}' if upd else ''}")
                current_conv = conv_id
            role = r["role"] or "unknown"
            role_label = "User" if role == "user" else "Assistant" if role == "assistant" else role
            content = r["content"] or ""
            ts = fmt_ts(r["message_created"])
            prefix = f"{role_label}{f' @ {ts}' if ts else ''}: "
            lines.append(prefix + content.replace("\r\n", "\n"))

        if project_filter:
            filename = f"project-{project_filter}.txt"
        elif project_name:
            filename = f"project-{project_name}.txt"
        elif source_filter:
            filename = f"account-{source_filter}.txt"
        else:
            filename = "all-projects.txt"
        body = "\n\n".join(lines).strip() + "\n"
        self._send_text(handler, body, filename=filename)

    def _handle_project_rename(self, handler: http.server.SimpleHTTPRequestHandler, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return self._send_json(handler, {"error": "Invalid JSON"}, status=400)

        raw_project = str(payload.get("project_uid") or payload.get("project_id") or "").strip()
        source_hint = str(payload.get("source_id") or "").strip() or None
        human_name = str(payload.get("human_name") or "").strip()
        if not raw_project or not human_name:
            return self._send_json(handler, {"error": "project_id/project_uid and human_name are required"}, status=400)

        source_id, project_id = split_project_uid(raw_project)
        if source_hint:
            source_id = source_hint
        project_uid = make_project_uid(source_id, project_id)

        row = self.conn.execute(
            "SELECT project_uid, source_id, project_id, conversation_count, first_message_time, last_message_time FROM projects WHERE project_uid = ?",
            (project_uid,),
        ).fetchone()
        if not row:
            return self._send_json(handler, {"error": "Project not found"}, status=404)

        overrides = load_project_overrides(self.root)
        names = overrides.get("names", {})
        moves = overrides.get("moves", {})
        names[project_uid] = human_name
        overrides = {"names": names, "moves": moves}
        save_project_overrides(self.root, overrides)

        self.conn.execute("UPDATE projects SET human_name = ? WHERE project_uid = ?", (human_name, project_uid))
        self.conn.commit()

        meta_path = self.root / "projects" / source_id / project_id / "_meta.json"
        ensure_dir(meta_path.parent)
        meta_payload = {
            "project_id": project_id,
            "project_uid": project_uid,
            "source_id": source_id,
            "human_name": human_name,
            "conversation_count": row["conversation_count"],
            "first_message_time": row["first_message_time"],
            "last_message_time": row["last_message_time"],
        }
        write_json(meta_path, meta_payload)
        self._send_json(handler, meta_payload)

    def _handle_conversation_move(self, handler: http.server.SimpleHTTPRequestHandler, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return self._send_json(handler, {"error": "Invalid JSON"}, status=400)

        conversation_id = str(payload.get("conversation_id") or "").strip()
        target_project_raw = str(payload.get("target_project_id") or "").strip()
        target_source_hint = str(payload.get("target_source_id") or "").strip() or None
        if not conversation_id or not target_project_raw:
            return self._send_json(handler, {"error": "conversation_id and target_project_id are required"}, status=400)

        row = self.conn.execute(
            "SELECT conversation_uid, conversation_id AS original_id, source_id, project_id, folder FROM conversations WHERE conversation_uid = ?",
            (conversation_id,),
        ).fetchone()
        if not row and ":" not in conversation_id:
            row = self.conn.execute(
                "SELECT conversation_uid, conversation_id AS original_id, source_id, project_id, folder FROM conversations WHERE conversation_id = ? ORDER BY updated_at DESC LIMIT 1",
                (conversation_id,),
            ).fetchone()
        if not row:
            return self._send_json(handler, {"error": "Conversation not found"}, status=404)
        conv_uid = row["conversation_uid"]
        source_id = row["source_id"]
        target_source, target_project_id = split_project_uid(target_project_raw)
        if target_source_hint:
            target_source = target_source_hint
        if target_source != source_id:
            return self._send_json(handler, {"error": "Cross-account moves are not supported"}, status=400)
        if row["project_id"] == target_project_id:
            project_uid = make_project_uid(target_source, target_project_id)
            return self._send_json(handler, {"status": "ok", "project_id": project_uid})

        src_folder = Path(row["folder"])
        folder_name = src_folder.name
        src_fs = (self.root / src_folder).resolve()
        dest_rel = Path("projects") / target_source / target_project_id / folder_name
        dest_fs = (self.root / dest_rel).resolve()
        if dest_fs.exists() and dest_fs != src_fs:
            dest_rel = Path("projects") / target_source / target_project_id / f"{folder_name}-{conv_uid[:8]}"
            dest_fs = (self.root / dest_rel).resolve()

        ensure_dir(dest_fs.parent)
        if src_fs.exists():
            shutil.move(str(src_fs), str(dest_fs))

        project_uid = make_project_uid(target_source, target_project_id)
        self.conn.execute(
            "UPDATE conversations SET project_id = ?, project_uid = ?, folder = ? WHERE conversation_uid = ?",
            (target_project_id, project_uid, str(dest_rel), conv_uid),
        )

        overrides = load_project_overrides(self.root)
        names = overrides.get("names", {})
        moves = overrides.get("moves", {})
        moves[conv_uid] = project_uid
        save_project_overrides(self.root, {"names": names, "moves": moves})

        json_path = dest_fs / "conversation.json"
        if json_path.exists():
            try:
                convo = json.loads(json_path.read_text(encoding="utf-8"))
                convo["project_uid"] = project_uid
                convo["project_id"] = target_project_id
                convo["source_id"] = target_source
                convo.setdefault("metadata", {})
                convo["metadata"]["gizmo_id"] = target_project_id
                convo["metadata"]["source_id"] = target_source
                write_json(json_path, convo)
            except Exception:
                pass

        self._recalculate_projects()
        self._send_json(
            handler,
            {"status": "ok", "project_id": project_uid, "folder": str(dest_rel)},
        )

    def _handle_conversation_delete(self, handler: http.server.SimpleHTTPRequestHandler, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return self._send_json(handler, {"error": "Invalid JSON"}, status=400)

        conversation_id = str(payload.get("conversation_id") or "").strip()
        if not conversation_id:
            return self._send_json(handler, {"error": "conversation_id is required"}, status=400)

        row = self.conn.execute(
            "SELECT conversation_uid, conversation_id AS original_id, project_uid, folder FROM conversations WHERE conversation_uid = ?",
            (conversation_id,),
        ).fetchone()
        if not row and ":" not in conversation_id:
            row = self.conn.execute(
                "SELECT conversation_uid, conversation_id AS original_id, project_uid, folder FROM conversations WHERE conversation_id = ? ORDER BY updated_at DESC LIMIT 1",
                (conversation_id,),
            ).fetchone()
        if not row:
            return self._send_json(handler, {"error": "Conversation not found"}, status=404)

        conv_uid = row["conversation_uid"]
        folder_path = (self.root / row["folder"]).resolve()
        if folder_path.exists():
            shutil.rmtree(folder_path, ignore_errors=True)

        self.conn.execute("DELETE FROM messages WHERE conversation_uid = ?", (conv_uid,))
        self.conn.execute("DELETE FROM messages_fts WHERE conversation_uid = ?", (conv_uid,))
        self.conn.execute("DELETE FROM conversations WHERE conversation_uid = ?", (conv_uid,))
        self.conn.commit()

        overrides = load_project_overrides(self.root)
        names = overrides.get("names", {})
        moves = overrides.get("moves", {})
        if conv_uid in moves:
            moves.pop(conv_uid, None)
            save_project_overrides(self.root, {"names": names, "moves": moves})

        self._recalculate_projects()
        self._send_json(handler, {"status": "ok"})

    def _list_available_exports(self) -> List[Path]:
        # Сканируем корень архива на наличие .zip экспортов
        return sorted([p for p in self.root.glob("*.zip") if p.is_file()], key=lambda p: p.name)

    def _handle_imports_list(self, handler: http.server.SimpleHTTPRequestHandler) -> None:
        payload = []
        for p in self._list_available_exports():
            try:
                stat = p.stat()
                payload.append(
                    {
                        "name": p.name,
                        "path": p.name,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                    }
                )
            except OSError:
                continue
        self._send_json(handler, payload)

    def _handle_import_run(self, handler: http.server.SimpleHTTPRequestHandler, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return self._send_json(handler, {"error": "Invalid JSON"}, status=400)

        archive = str(payload.get("archive") or payload.get("path") or "").strip()
        account = str(payload.get("account") or payload.get("source_id") or "").strip() or DEFAULT_SOURCE_ID
        incremental = bool(payload.get("incremental", True))
        if not archive:
            return self._send_json(handler, {"error": "archive is required"}, status=400)

        candidate = Path(archive)
        if not candidate.is_absolute():
            candidate = (self.root / candidate).resolve()
        if self.root not in candidate.parents and self.root != candidate:
            return self._send_json(handler, {"error": "archive must be inside archive root"}, status=400)
        if not candidate.exists():
            return self._send_json(handler, {"error": "archive not found"}, status=404)

        try:
            result = import_archive(
                ImportOptions(
                    export_path=candidate,
                    output_root=self.root,
                    allow_network_images=False,
                    incremental=incremental,
                    source_id=account,
                )
            )
            # Переподключаемся к БД на свежую схему/данные
            self._reload_connection()
        except Exception as exc:  # pragma: no cover - surface to UI
            return self._send_json(handler, {"error": str(exc)}, status=500)

        self._send_json(handler, {"status": "ok", "archive": archive, "result": result})

    def _handle_reset(self, handler: http.server.SimpleHTTPRequestHandler, body: bytes = b"") -> None:
        try:
            payload = json.loads(body.decode("utf-8") or "{}") if body else {}
        except json.JSONDecodeError:
            return self._send_json(handler, {"error": "Invalid JSON"}, status=400)

        source_raw = str(payload.get("source_id") or payload.get("account") or "").strip()
        source_id = normalize_source_id(source_raw) if source_raw else ""

        if source_id:
            # Удаляем данные конкретного аккаунта, оставляя остальные нетронутыми
            projects_root = self.root / "projects"
            target_dir = projects_root / source_id
            try:
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
            except Exception:
                pass

            self.conn.execute("DELETE FROM messages WHERE source_id = ?", (source_id,))
            self.conn.execute("DELETE FROM messages_fts WHERE source_id = ?", (source_id,))
            self.conn.execute("DELETE FROM conversations WHERE source_id = ?", (source_id,))
            self.conn.execute("DELETE FROM projects WHERE source_id = ?", (source_id,))
            self.conn.execute("DELETE FROM imports WHERE source_id = ?", (source_id,))
            self.conn.commit()

            self._recalculate_projects()

            try:
                if projects_root.exists() and not any(projects_root.iterdir()):
                    shutil.rmtree(projects_root, ignore_errors=True)
            except Exception:
                pass

            return self._send_json(handler, {"status": "ok", "source_id": source_id})

        # Удаляем сгенерированные артефакты архива, не трогая .zip экспорты
        targets = [
            self.db_path,
            self.db_path.with_suffix(".db-shm"),
            self.db_path.with_suffix(".db-wal"),
            self.root / "projects",
        ]
        for t in targets:
            try:
                if t.is_dir():
                    shutil.rmtree(t, ignore_errors=True)
                elif t.exists():
                    t.unlink()
            except Exception:
                continue
        _prepare_database(self.db_path, rebuild=True).close()
        self._reload_connection()
        self._send_json(handler, {"status": "ok"})

    def _make_handler(self):
        server = self

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(server.static_dir), **kwargs)

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                # Keep CLI output quiet
                return

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/api/projects":
                    return server._handle_projects(self)
                if parsed.path == "/api/models":
                    return server._handle_models(self)
                if parsed.path == "/api/conversations":
                    return server._handle_conversations(self, parse_qs(parsed.query))
                if parsed.path.startswith("/api/conversation/"):
                    conversation_id = parsed.path.rsplit("/", 1)[-1]
                    return server._handle_conversation(self, conversation_id)
                if parsed.path == "/api/export/txt":
                    return server._handle_export_txt(self, parse_qs(parsed.query))
                if parsed.path == "/api/imports":
                    return server._handle_imports_list(self)
                if parsed.path == "/api/reset":
                    return server._send_json(self, {"error": "POST required"}, status=405)
                if parsed.path.startswith("/files/"):
                    rel_path = unquote(parsed.path[len("/files/") :])
                    return server._serve_file(self, rel_path)
                if parsed.path == "/api/ping":
                    return server._send_json(self, {"status": "ok"})
                return super().do_GET()

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/api/project/rename":
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length > 0 else b""
                    return server._handle_project_rename(self, body)
                if parsed.path == "/api/conversation/move":
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length > 0 else b""
                    return server._handle_conversation_move(self, body)
                if parsed.path == "/api/conversation/delete":
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length > 0 else b""
                    return server._handle_conversation_delete(self, body)
                if parsed.path == "/api/imports":
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length > 0 else b""
                    return server._handle_import_run(self, body)
                if parsed.path == "/api/reset":
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length > 0 else b""
                    return server._handle_reset(self, body)
                self.send_response(404)
                self.end_headers()

        return Handler

    def serve(self) -> None:
        handler_cls = self._make_handler()
        with socketserver.TCPServer((self.host, self.port), handler_cls) as httpd:
            print(f"Serving archive from {self.root} on http://{self.host}:{self.port}")
            httpd.serve_forever()
