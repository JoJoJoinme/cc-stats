import csv
import html
import io
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..analysis import ANALYSIS_VERSION, enrich_session
from ..config import default_db_path
from ..models import SessionRecord, ToolCallRecord, TurnRecord
from ..utils import compact_ws, shorten, utc_now

try:
    import markdown as _markdown_lib
except Exception:
    _markdown_lib = None


SESSION_COLUMNS = [
    "session_id",
    "native_session_id",
    "source",
    "user_id",
    "host_name",
    "platform",
    "started_at",
    "ended_at",
    "duration_sec",
    "turn_count",
    "assistant_turn_count",
    "user_turn_count",
    "repo",
    "cwd",
    "git_branch",
    "tool_call_count",
    "tool_diversity",
    "model_primary",
    "models_json",
    "total_input_tokens",
    "total_output_tokens",
    "total_cache_creation_tokens",
    "total_cache_read_tokens",
    "total_cost",
    "category",
    "category_confidence",
    "summary",
    "has_skill",
    "has_mcp",
    "has_subagent",
    "raw_path",
    "raw_payload_json",
    "last_message_uuid",
    "status",
    "parent_native_session_id",
    "root_native_session_id",
    "extra_json",
    "ingested_at",
]


def connect_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    resolved = Path(db_path or default_db_path())
    resolved.parent.mkdir(parents=True, exist_ok=True)
    # FastAPI may enter/exit sync dependencies on different worker threads.
    # Each request still gets its own short-lived connection, so disabling the
    # same-thread guard here is safe and avoids spurious template/API failures.
    conn = sqlite3.connect(str(resolved), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    try:
        refresh_session_analysis(conn)
    except Exception:
        pass
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          native_session_id TEXT NOT NULL,
          source TEXT NOT NULL,
          user_id TEXT NOT NULL,
          host_name TEXT NOT NULL,
          platform TEXT NOT NULL,
          started_at TEXT,
          ended_at TEXT,
          duration_sec INTEGER,
          turn_count INTEGER NOT NULL DEFAULT 0,
          assistant_turn_count INTEGER NOT NULL DEFAULT 0,
          user_turn_count INTEGER NOT NULL DEFAULT 0,
          repo TEXT,
          cwd TEXT,
          git_branch TEXT,
          tool_call_count INTEGER NOT NULL DEFAULT 0,
          tool_diversity INTEGER NOT NULL DEFAULT 0,
          model_primary TEXT,
          models_json TEXT NOT NULL DEFAULT '[]',
          total_input_tokens INTEGER NOT NULL DEFAULT 0,
          total_output_tokens INTEGER NOT NULL DEFAULT 0,
          total_cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
          total_cache_read_tokens INTEGER NOT NULL DEFAULT 0,
          total_cost REAL NOT NULL DEFAULT 0,
          category TEXT NOT NULL DEFAULT 'other',
          category_confidence REAL NOT NULL DEFAULT 0,
          summary TEXT NOT NULL DEFAULT '',
          has_skill INTEGER NOT NULL DEFAULT 0,
          has_mcp INTEGER NOT NULL DEFAULT 0,
          has_subagent INTEGER NOT NULL DEFAULT 0,
          raw_path TEXT,
          raw_payload_json TEXT NOT NULL DEFAULT '{}',
          last_message_uuid TEXT,
          status TEXT,
          parent_native_session_id TEXT,
          root_native_session_id TEXT,
          extra_json TEXT NOT NULL DEFAULT '{}',
          ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS turns (
          session_id TEXT NOT NULL,
          turn_idx INTEGER NOT NULL,
          started_at TEXT,
          ended_at TEXT,
          user_text TEXT NOT NULL DEFAULT '',
          assistant_text TEXT NOT NULL DEFAULT '',
          user_prompt_chars INTEGER NOT NULL DEFAULT 0,
          assistant_chars INTEGER NOT NULL DEFAULT 0,
          tool_call_count INTEGER NOT NULL DEFAULT 0,
          duration_sec INTEGER,
          category_hint TEXT,
          PRIMARY KEY (session_id, turn_idx),
          FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS tool_calls (
          session_id TEXT NOT NULL,
          turn_idx INTEGER NOT NULL,
          call_idx INTEGER NOT NULL,
          ts TEXT,
          tool_name TEXT NOT NULL,
          tool_kind TEXT NOT NULL,
          skill_name TEXT,
          mcp_server TEXT,
          subagent_type TEXT,
          success INTEGER,
          input_size INTEGER NOT NULL DEFAULT 0,
          output_size INTEGER NOT NULL DEFAULT 0,
          error_text TEXT,
          raw_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY (session_id, turn_idx, call_idx),
          FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON sessions(started_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
        CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_repo ON sessions(repo);
        CREATE INDEX IF NOT EXISTS idx_sessions_category ON sessions(category);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
        CREATE INDEX IF NOT EXISTS idx_tool_calls_name ON tool_calls(tool_name);
        CREATE INDEX IF NOT EXISTS idx_tool_calls_kind ON tool_calls(tool_kind);
        CREATE INDEX IF NOT EXISTS idx_tool_calls_mcp_server ON tool_calls(mcp_server);
        CREATE INDEX IF NOT EXISTS idx_tool_calls_skill_name ON tool_calls(skill_name);
        """
    )
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS session_search
            USING fts5(session_id, native_session_id, source, user_id, repo, category, summary, content)
            """
        )
    except sqlite3.OperationalError:
        pass
    conn.commit()


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _pretty_json(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return value
        return json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _render_message_html(text: Optional[str]) -> str:
    source = _restore_markdown_layout((text or "").strip())
    if not source:
        return ""
    if _markdown_lib is not None:
        return _markdown_lib.markdown(
            source,
            extensions=[
                "fenced_code",
                "tables",
                "sane_lists",
                "nl2br",
            ],
            output_format="html5",
        )

    escaped = html.escape(source)
    paragraphs = [part for part in escaped.split("\n\n") if part.strip()]
    if not paragraphs:
        return ""
    return "".join("<p>{0}</p>".format(part.replace("\n", "<br>\n")) for part in paragraphs)


def _restore_markdown_layout(text: str) -> str:
    restored = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not restored:
        return ""

    restored = re.sub(
        r"```([A-Za-z0-9_+-]*)\s+(.*?)\s+```",
        lambda match: "\n\n```{0}\n{1}\n```\n\n".format(match.group(1), match.group(2).strip()),
        restored,
        flags=re.DOTALL,
    )
    restored = re.sub(r"\s+(#{1,6}\s+)", r"\n\n\1", restored)
    restored = re.sub(r"\s+(-\s+\*\*)", r"\n\1", restored)
    restored = re.sub(r"\s+(\*\s+\*\*)", r"\n\1", restored)
    restored = re.sub(r"\s+(\d+\.\s+\*\*)", r"\n\1", restored)
    restored = re.sub(r"\s+(\d+\.\s+)", r"\n\1", restored)
    restored = re.sub(r"\n{3,}", "\n\n", restored)
    return restored.strip()


_TRANSCRIPT_TRIM_RULES = [
    (
        "environment details",
        re.compile(r"<environment_details>[\s\S]*$", re.IGNORECASE),
    ),
    (
        "task progress hint",
        re.compile(r"# task_progress RECOMMENDED[\s\S]*?(?=<environment_details>|$)", re.IGNORECASE),
    ),
    (
        "task resumption scaffold",
        re.compile(r"\[TASK RESUMPTION\][\s\S]*?(?=<environment_details>|$)", re.IGNORECASE),
    ),
]


def _prepare_transcript_view(text: Optional[str]) -> Dict[str, Any]:
    original = text or ""
    display = original
    hidden_segments = []

    for label, pattern in _TRANSCRIPT_TRIM_RULES:
        while True:
            match = pattern.search(display)
            if not match:
                break
            segment = compact_ws(match.group(0))
            if segment:
                hidden_segments.append(
                    {
                        "label": label,
                        "preview": shorten(segment, 220),
                    }
                )
            display = (display[: match.start()] + "\n" + display[match.end() :]).strip()

    display = re.sub(r"</?(task|feedback)>", "", display, flags=re.IGNORECASE)
    display = re.sub(r"\n{3,}", "\n\n", display).strip()
    compact_display = compact_ws(display)
    fallback_preview = shorten(compact_ws(original), 72)

    return {
        "display_text": display,
        "has_visible_text": bool(compact_display),
        "hidden_segments": hidden_segments,
        "was_trimmed": bool(hidden_segments),
        "preview_text": shorten(compact_display, 72) if compact_display else "",
        "fallback_preview": fallback_preview,
        "is_scaffold_only": bool(hidden_segments) and not compact_display,
    }


def _raw_session_detail(conn: sqlite3.Connection, session_id: str) -> Optional[Dict[str, Any]]:
    session = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
    if not session:
        return None
    turns = conn.execute(
        "SELECT * FROM turns WHERE session_id = ? ORDER BY turn_idx ASC",
        (session_id,),
    ).fetchall()
    tool_calls = conn.execute(
        "SELECT * FROM tool_calls WHERE session_id = ? ORDER BY turn_idx ASC, call_idx ASC",
        (session_id,),
    ).fetchall()
    detail = dict(session)
    detail["turns"] = [dict(row) for row in turns]
    detail["tool_calls"] = [dict(row) for row in tool_calls]
    return detail


def _session_from_detail(detail: Dict[str, Any]) -> SessionRecord:
    turns = [TurnRecord(**turn) for turn in detail.get("turns", [])]
    tool_calls = [
        ToolCallRecord(
            session_id=call["session_id"],
            turn_idx=call["turn_idx"],
            call_idx=call["call_idx"],
            ts=call.get("ts"),
            tool_name=call["tool_name"],
            tool_kind=call["tool_kind"],
            skill_name=call.get("skill_name"),
            mcp_server=call.get("mcp_server"),
            subagent_type=call.get("subagent_type"),
            success=None if call.get("success") is None else bool(call["success"]),
            input_size=call.get("input_size", 0),
            output_size=call.get("output_size", 0),
            error_text=call.get("error_text"),
            raw_json=_json_loads(call.get("raw_json"), {}),
        )
        for call in detail.get("tool_calls", [])
    ]
    payload = {
        "session_id": detail["session_id"],
        "native_session_id": detail["native_session_id"],
        "source": detail["source"],
        "user_id": detail["user_id"],
        "host_name": detail["host_name"],
        "platform": detail["platform"],
        "started_at": detail.get("started_at"),
        "ended_at": detail.get("ended_at"),
        "duration_sec": detail.get("duration_sec"),
        "turn_count": detail.get("turn_count", 0),
        "assistant_turn_count": detail.get("assistant_turn_count", 0),
        "user_turn_count": detail.get("user_turn_count", 0),
        "repo": detail.get("repo"),
        "cwd": detail.get("cwd"),
        "git_branch": detail.get("git_branch"),
        "tool_call_count": detail.get("tool_call_count", 0),
        "tool_diversity": detail.get("tool_diversity", 0),
        "model_primary": detail.get("model_primary"),
        "models": _json_loads(detail.get("models_json"), []),
        "total_input_tokens": detail.get("total_input_tokens", 0),
        "total_output_tokens": detail.get("total_output_tokens", 0),
        "total_cache_creation_tokens": detail.get("total_cache_creation_tokens", 0),
        "total_cache_read_tokens": detail.get("total_cache_read_tokens", 0),
        "total_cost": detail.get("total_cost", 0.0),
        "category": detail.get("category", "other"),
        "category_confidence": detail.get("category_confidence", 0.0),
        "summary": detail.get("summary", ""),
        "has_skill": bool(detail.get("has_skill")),
        "has_mcp": bool(detail.get("has_mcp")),
        "has_subagent": bool(detail.get("has_subagent")),
        "raw_path": detail.get("raw_path"),
        "raw_payload": _json_loads(detail.get("raw_payload_json"), {}),
        "last_message_uuid": detail.get("last_message_uuid"),
        "status": detail.get("status"),
        "parent_native_session_id": detail.get("parent_native_session_id"),
        "root_native_session_id": detail.get("root_native_session_id"),
        "extra_json": _json_loads(detail.get("extra_json"), {}),
        "turns": turns,
        "tool_calls": tool_calls,
    }
    return SessionRecord(**payload)


def _persist_session_analysis(conn: sqlite3.Connection, session: SessionRecord) -> None:
    conn.execute(
        """
        UPDATE sessions
        SET category = ?, category_confidence = ?, summary = ?, extra_json = ?
        WHERE session_id = ?
        """,
        (
            session.category,
            session.category_confidence,
            session.summary,
            json.dumps(session.extra_json, ensure_ascii=False),
            session.session_id,
        ),
    )


def refresh_session_analysis(conn: sqlite3.Connection, limit: Optional[int] = None) -> int:
    rows = conn.execute(
        f"""
        SELECT session_id, extra_json
        FROM sessions
        ORDER BY COALESCE(started_at, ended_at) DESC
        {"LIMIT ?" if limit else ""}
        """,
        (() if not limit else (limit,)),
    ).fetchall()
    updated = 0
    with conn:
        for row in rows:
            current_extra = _json_loads(row["extra_json"], {})
            current_insights = current_extra.get("insights") or {}
            if current_extra.get("analysis_version") == ANALYSIS_VERSION and current_insights.get("analysis_version") == ANALYSIS_VERSION:
                continue
            detail = _raw_session_detail(conn, row["session_id"])
            if not detail:
                continue
            session = enrich_session(_session_from_detail(detail))
            _persist_session_analysis(conn, session)
            updated += 1
    return updated


def _attach_insights(item: Dict[str, Any]) -> Dict[str, Any]:
    extra = _json_loads(item.get("extra_json"), {})
    insights = extra.get("insights") or {}
    item["extra"] = extra
    item["insights"] = insights
    item["case_title"] = insights.get("case_title") or item.get("summary") or item.get("session_id")
    item["topic_signature"] = insights.get("topic_signature") or item["case_title"]
    item["quality_score"] = int(insights.get("quality_score") or 0)
    item["quality_label"] = insights.get("quality_label") or "mixed"
    item["quality_label_text"] = insights.get("quality_label_text") or item["quality_label"].replace("_", " ").title()
    item["work_pattern"] = insights.get("work_pattern") or "chat_only"
    item["work_pattern_label"] = insights.get("work_pattern_label") or item["work_pattern"].replace("_", " ").title()
    item["outcome_status"] = insights.get("outcome_status") or "unclear"
    item["followup_turns"] = int(insights.get("followup_turns") or 0)
    item["verification_present"] = bool(insights.get("verification_present"))
    item["prompt_score"] = int(insights.get("prompt_score") or 0)
    item["prompt_tags"] = insights.get("prompt_tags") or []
    item["improvement_flags"] = insights.get("improvement_flags") or []
    item["tool_path_text"] = insights.get("tool_path_text") or "chat"
    item["needs_review"] = bool(insights.get("needs_review"))
    item["example_worthy"] = bool(insights.get("example_worthy"))
    return item


def _load_review_items(
    conn: sqlite3.Connection,
    *,
    user_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM sessions"
    params = []
    if user_id:
        sql += " WHERE user_id = ?"
        params.append(user_id)
    sql += " ORDER BY COALESCE(started_at, ended_at) DESC"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_attach_insights(dict(row)) for row in rows]


def _session_row(session: SessionRecord) -> Dict[str, Any]:
    return {
        "session_id": session.session_id,
        "native_session_id": session.native_session_id,
        "source": session.source,
        "user_id": session.user_id,
        "host_name": session.host_name,
        "platform": session.platform,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "duration_sec": session.duration_sec,
        "turn_count": session.turn_count,
        "assistant_turn_count": session.assistant_turn_count,
        "user_turn_count": session.user_turn_count,
        "repo": session.repo,
        "cwd": session.cwd,
        "git_branch": session.git_branch,
        "tool_call_count": session.tool_call_count,
        "tool_diversity": session.tool_diversity,
        "model_primary": session.model_primary,
        "models_json": json.dumps(session.models, ensure_ascii=False),
        "total_input_tokens": session.total_input_tokens,
        "total_output_tokens": session.total_output_tokens,
        "total_cache_creation_tokens": session.total_cache_creation_tokens,
        "total_cache_read_tokens": session.total_cache_read_tokens,
        "total_cost": session.total_cost,
        "category": session.category,
        "category_confidence": session.category_confidence,
        "summary": session.summary,
        "has_skill": int(session.has_skill),
        "has_mcp": int(session.has_mcp),
        "has_subagent": int(session.has_subagent),
        "raw_path": session.raw_path,
        "raw_payload_json": json.dumps(session.raw_payload, ensure_ascii=False),
        "last_message_uuid": session.last_message_uuid,
        "status": session.status,
        "parent_native_session_id": session.parent_native_session_id,
        "root_native_session_id": session.root_native_session_id,
        "extra_json": json.dumps(session.extra_json, ensure_ascii=False),
        "ingested_at": utc_now(),
    }


def upsert_session(conn: sqlite3.Connection, session: SessionRecord) -> None:
    row = _session_row(session)
    placeholders = ", ".join("?" for _ in SESSION_COLUMNS)
    column_sql = ", ".join(SESSION_COLUMNS)
    values = [row[column] for column in SESSION_COLUMNS]
    with conn:
        conn.execute("DELETE FROM sessions WHERE session_id = ?", (session.session_id,))
        conn.execute(f"INSERT INTO sessions ({column_sql}) VALUES ({placeholders})", values)
        conn.execute("DELETE FROM turns WHERE session_id = ?", (session.session_id,))
        conn.execute("DELETE FROM tool_calls WHERE session_id = ?", (session.session_id,))

        for turn in session.turns:
            conn.execute(
                """
                INSERT INTO turns (
                  session_id, turn_idx, started_at, ended_at, user_text, assistant_text,
                  user_prompt_chars, assistant_chars, tool_call_count, duration_sec, category_hint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn.session_id,
                    turn.turn_idx,
                    turn.started_at,
                    turn.ended_at,
                    turn.user_text,
                    turn.assistant_text,
                    turn.user_prompt_chars,
                    turn.assistant_chars,
                    turn.tool_call_count,
                    turn.duration_sec,
                    turn.category_hint,
                ),
            )

        for call in session.tool_calls:
            conn.execute(
                """
                INSERT INTO tool_calls (
                  session_id, turn_idx, call_idx, ts, tool_name, tool_kind,
                  skill_name, mcp_server, subagent_type, success,
                  input_size, output_size, error_text, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    call.session_id,
                    call.turn_idx,
                    call.call_idx,
                    call.ts,
                    call.tool_name,
                    call.tool_kind,
                    call.skill_name,
                    call.mcp_server,
                    call.subagent_type,
                    None if call.success is None else int(call.success),
                    call.input_size,
                    call.output_size,
                    call.error_text,
                    json.dumps(call.raw_json, ensure_ascii=False),
                ),
            )

        try:
            conn.execute("DELETE FROM session_search WHERE session_id = ?", (session.session_id,))
            content = "\n".join(
                filter(
                    None,
                    [
                        session.summary,
                        *(turn.user_text for turn in session.turns),
                        *(turn.assistant_text for turn in session.turns),
                    ],
                )
            )
            conn.execute(
                """
                INSERT INTO session_search (
                  session_id, native_session_id, source, user_id, repo, category, summary, content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.session_id,
                    session.native_session_id,
                    session.source,
                    session.user_id,
                    session.repo or "",
                    session.category,
                    session.summary,
                    content,
                ),
            )
        except sqlite3.OperationalError:
            pass


def _build_filter_sql(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
    clauses = []
    params = []
    simple_fields = {
        "source": "sessions.source = ?",
        "user_id": "sessions.user_id = ?",
        "repo": "sessions.repo = ?",
        "git_branch": "sessions.git_branch = ?",
        "model": "sessions.model_primary = ?",
        "category": "sessions.category = ?",
        "status": "sessions.status = ?",
    }
    for key, sql in simple_fields.items():
        value = filters.get(key)
        if value:
            clauses.append(sql)
            params.append(value)

    if filters.get("has_mcp") is not None:
        clauses.append("sessions.has_mcp = ?")
        params.append(int(bool(filters["has_mcp"])))
    if filters.get("has_skill") is not None:
        clauses.append("sessions.has_skill = ?")
        params.append(int(bool(filters["has_skill"])))
    if filters.get("has_subagent") is not None:
        clauses.append("sessions.has_subagent = ?")
        params.append(int(bool(filters["has_subagent"])))
    if filters.get("date_from"):
        clauses.append("COALESCE(sessions.started_at, sessions.ended_at) >= ?")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        clauses.append("COALESCE(sessions.ended_at, sessions.started_at) <= ?")
        params.append(filters["date_to"])
    if filters.get("tool_name"):
        clauses.append(
            "EXISTS (SELECT 1 FROM tool_calls tc WHERE tc.session_id = sessions.session_id AND tc.tool_name = ?)"
        )
        params.append(filters["tool_name"])
    if filters.get("mcp_server"):
        clauses.append(
            "EXISTS (SELECT 1 FROM tool_calls tc WHERE tc.session_id = sessions.session_id AND tc.mcp_server = ?)"
        )
        params.append(filters["mcp_server"])
    if filters.get("skill_name"):
        clauses.append(
            "EXISTS (SELECT 1 FROM tool_calls tc WHERE tc.session_id = sessions.session_id AND tc.skill_name = ?)"
        )
        params.append(filters["skill_name"])
    if filters.get("text_query"):
        clauses.append(
            """
            (
              sessions.summary LIKE ?
              OR EXISTS (
                SELECT 1 FROM turns tt
                WHERE tt.session_id = sessions.session_id
                AND (tt.user_text LIKE ? OR tt.assistant_text LIKE ?)
              )
            )
            """
        )
        query = f"%{filters['text_query']}%"
        params.extend([query, query, query])

    sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return sql, params


def list_sessions(conn: sqlite3.Connection, filters: Dict[str, Any]) -> Dict[str, Any]:
    sql, params = _build_filter_sql(filters)
    sort_map = {
        "started_at": "COALESCE(sessions.started_at, sessions.ended_at) DESC",
        "duration": "sessions.duration_sec DESC",
        "turns": "sessions.turn_count DESC",
        "tokens": "(sessions.total_input_tokens + sessions.total_output_tokens) DESC",
        "cost": "sessions.total_cost DESC",
    }
    limit = min(int(filters.get("limit", 50)), 500)
    offset = max(int(filters.get("offset", 0)), 0)
    requested_sort = filters.get("sort")
    total = conn.execute(f"SELECT COUNT(*) AS count FROM sessions {sql}", params).fetchone()["count"]

    if requested_sort == "quality":
        rows = conn.execute(f"SELECT * FROM sessions {sql}", params).fetchall()
        items = [_attach_insights(dict(row)) for row in rows]
        items.sort(
            key=lambda item: (
                item.get("quality_score", 0),
                item.get("started_at") or item.get("ended_at") or "",
            ),
            reverse=True,
        )
        page = items[offset : offset + limit]
        return {"total": total, "items": page, "limit": limit, "offset": offset}

    order_sql = sort_map.get(requested_sort, "COALESCE(sessions.started_at, sessions.ended_at) DESC")
    rows = conn.execute(
        f"SELECT * FROM sessions {sql} ORDER BY {order_sql} LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()
    return {"total": total, "items": [_attach_insights(dict(row)) for row in rows], "limit": limit, "offset": offset}


def get_session_detail(conn: sqlite3.Connection, session_id: str) -> Optional[Dict[str, Any]]:
    detail = _raw_session_detail(conn, session_id)
    if not detail:
        return None
    detail = _attach_insights(detail)
    detail["models"] = _json_loads(detail.get("models_json"), [])
    detail["raw_payload"] = _json_loads(detail.get("raw_payload_json"), {})
    detail["raw_payload_pretty"] = _pretty_json(detail["raw_payload"])
    detail["extra_pretty"] = _pretty_json(detail.get("extra"))
    raw_path = compact_ws(detail.get("raw_path"))
    detail["can_export_claude_bundle"] = bool(
        detail.get("source") == "claude-code" and raw_path and Path(raw_path).exists()
    )
    detail["claude_bundle_name"] = "{0}.claude-session.zip".format(detail.get("native_session_id") or "session")
    calls_by_turn = defaultdict(list)
    for call in detail["tool_calls"]:
        call["raw_json"] = _json_loads(call.get("raw_json"), {})
        call["raw_json_pretty"] = _pretty_json(call["raw_json"])
        calls_by_turn[call["turn_idx"]].append(call)
    turn_items = []
    for turn in detail["turns"]:
        turn_calls = calls_by_turn.get(turn["turn_idx"], [])
        user_view = _prepare_transcript_view(turn.get("user_text"))
        assistant_view = _prepare_transcript_view(turn.get("assistant_text"))
        user_view["html"] = _render_message_html(user_view.get("display_text"))
        assistant_view["html"] = _render_message_html(assistant_view.get("display_text"))
        turn["calls"] = turn_calls
        turn["anchor"] = "turn-{0}".format(turn["turn_idx"])
        turn["user_view"] = user_view
        turn["assistant_view"] = assistant_view
        turn["has_user"] = bool(turn.get("user_text"))
        turn["has_assistant"] = bool(turn.get("assistant_text"))
        turn["search_text"] = " ".join(
            filter(
                None,
                [
                    user_view.get("display_text", ""),
                    assistant_view.get("display_text", ""),
                    " ".join(call.get("tool_name", "") for call in turn_calls),
                ],
            )
        ).lower()
        preview = (
            user_view.get("preview_text")
            or assistant_view.get("preview_text")
            or ""
        )
        if not preview and (user_view.get("hidden_segments") or assistant_view.get("hidden_segments")):
            preview = "Setup/context scaffold only"
        if not preview:
            preview = user_view.get("fallback_preview") or assistant_view.get("fallback_preview") or ""
        turn_items.append(
            {
                "turn_idx": turn["turn_idx"],
                "anchor": turn["anchor"],
                "label": "Turn {0}".format(turn["turn_idx"]),
                "preview": preview or "No text",
                "tool_count": len(turn_calls),
                "scaffold_only": bool(
                    (user_view.get("is_scaffold_only") or assistant_view.get("is_scaffold_only"))
                    and not (user_view.get("has_visible_text") or assistant_view.get("has_visible_text"))
                ),
            }
        )
    detail["turn_items"] = turn_items
    return detail


def stats_overview(conn: sqlite3.Connection) -> Dict[str, Any]:
    overview = conn.execute(
        """
        SELECT
          COUNT(*) AS sessions,
          COUNT(DISTINCT user_id) AS users,
          COUNT(DISTINCT repo) AS repos,
          COALESCE(AVG(duration_sec), 0) AS avg_duration_sec,
          COALESCE(AVG(turn_count), 0) AS avg_turn_count,
          COALESCE(SUM(total_input_tokens), 0) AS total_input_tokens,
          COALESCE(SUM(total_output_tokens), 0) AS total_output_tokens,
          COALESCE(SUM(total_cost), 0) AS total_cost,
          COALESCE(AVG(has_mcp), 0) AS mcp_adoption,
          COALESCE(AVG(has_skill), 0) AS skill_adoption,
          COALESCE(AVG(has_subagent), 0) AS subagent_adoption
        FROM sessions
        """
    ).fetchone()
    return dict(overview)


def capability_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    totals = conn.execute(
        """
        SELECT
          COUNT(*) AS sessions,
          COALESCE(SUM(CASE WHEN has_skill = 1 THEN 1 ELSE 0 END), 0) AS skill_sessions,
          COALESCE(SUM(CASE WHEN has_mcp = 1 THEN 1 ELSE 0 END), 0) AS mcp_sessions,
          COALESCE(SUM(CASE WHEN has_subagent = 1 THEN 1 ELSE 0 END), 0) AS subagent_sessions
        FROM sessions
        """
    ).fetchone()
    total_sessions = int(totals["sessions"] or 0)

    calls = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN tool_kind = 'skill' THEN 1 ELSE 0 END), 0) AS skill_calls,
          COALESCE(SUM(CASE WHEN tool_kind = 'mcp' THEN 1 ELSE 0 END), 0) AS mcp_calls,
          COALESCE(SUM(CASE WHEN tool_kind = 'subagent' THEN 1 ELSE 0 END), 0) AS subagent_calls
        FROM tool_calls
        """
    ).fetchone()

    topic = conn.execute(
        """
        SELECT
          COALESCE(
            SUM(
              CASE
                WHEN
                  s.has_skill = 1
                  OR LOWER(s.summary) LIKE '%skill%'
                  OR EXISTS (
                    SELECT 1
                    FROM tool_calls tc
                    WHERE tc.session_id = s.session_id
                      AND (
                        tc.tool_kind = 'skill'
                        OR LOWER(tc.tool_name) LIKE '%skill%'
                      )
                  )
                THEN 1 ELSE 0
              END
            ),
            0
          ) AS skill_topic_sessions,
          COALESCE(
            SUM(
              CASE
                WHEN
                  s.has_mcp = 1
                  OR LOWER(s.summary) LIKE '%mcp%'
                  OR EXISTS (
                    SELECT 1
                    FROM tool_calls tc
                    WHERE tc.session_id = s.session_id
                      AND (
                        tc.tool_kind = 'mcp'
                        OR tc.tool_name = 'load_mcp_documentation'
                        OR tc.tool_name LIKE 'mcp__%'
                      )
                  )
                THEN 1 ELSE 0
              END
            ),
            0
          ) AS mcp_topic_sessions
        FROM sessions s
        """
    ).fetchone()

    availability_rows = conn.execute("SELECT source, extra_json FROM sessions").fetchall()
    mcp_available_sessions = 0
    by_source_available = {}
    for row in availability_rows:
        source = str(row["source"] or "")
        extra = {}
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            extra = {}
        available_servers = extra.get("available_mcp_servers") or []
        if available_servers:
            mcp_available_sessions += 1
            by_source_available[source] = by_source_available.get(source, 0) + 1

    by_source_rows = conn.execute(
        """
        SELECT
          s.source,
          COUNT(*) AS sessions,
          COALESCE(SUM(CASE WHEN s.has_skill = 1 THEN 1 ELSE 0 END), 0) AS skill_sessions,
          COALESCE(SUM(CASE WHEN s.has_mcp = 1 THEN 1 ELSE 0 END), 0) AS mcp_sessions,
          COALESCE(SUM(CASE WHEN s.has_subagent = 1 THEN 1 ELSE 0 END), 0) AS subagent_sessions,
          COALESCE(
            SUM(
              CASE
                WHEN
                  s.has_skill = 1
                  OR LOWER(s.summary) LIKE '%skill%'
                  OR EXISTS (
                    SELECT 1
                    FROM tool_calls tc
                    WHERE tc.session_id = s.session_id
                      AND (
                        tc.tool_kind = 'skill'
                        OR LOWER(tc.tool_name) LIKE '%skill%'
                      )
                  )
                THEN 1 ELSE 0
              END
            ),
            0
          ) AS skill_topic_sessions,
          COALESCE(
            SUM(
              CASE
                WHEN
                  s.has_mcp = 1
                  OR LOWER(s.summary) LIKE '%mcp%'
                  OR EXISTS (
                    SELECT 1
                    FROM tool_calls tc
                    WHERE tc.session_id = s.session_id
                      AND (
                        tc.tool_kind = 'mcp'
                        OR tc.tool_name = 'load_mcp_documentation'
                        OR tc.tool_name LIKE 'mcp__%'
                      )
                  )
                THEN 1 ELSE 0
              END
            ),
            0
          ) AS mcp_topic_sessions
        FROM sessions s
        GROUP BY s.source
        ORDER BY sessions DESC, s.source ASC
        """
    ).fetchall()

    def _rate(count: int) -> float:
        return (count / total_sessions) if total_sessions else 0.0

    skills_actual = int(totals["skill_sessions"] or 0)
    mcp_actual = int(totals["mcp_sessions"] or 0)
    subagent_actual = int(totals["subagent_sessions"] or 0)
    skill_calls = int(calls["skill_calls"] or 0)
    mcp_calls = int(calls["mcp_calls"] or 0)
    subagent_calls = int(calls["subagent_calls"] or 0)
    skill_topic = int(topic["skill_topic_sessions"] or 0)
    mcp_topic = int(topic["mcp_topic_sessions"] or 0)

    by_source = []
    for row in by_source_rows:
        item = dict(row)
        item["mcp_available_sessions"] = by_source_available.get(str(item["source"] or ""), 0)
        by_source.append(item)

    return {
        "skills": {
            "actual_sessions": skills_actual,
            "topic_sessions": skill_topic,
            "call_count": skill_calls,
            "actual_rate": _rate(skills_actual),
            "topic_rate": _rate(skill_topic),
        },
        "mcp": {
            "actual_sessions": mcp_actual,
            "available_sessions": mcp_available_sessions,
            "topic_sessions": mcp_topic,
            "call_count": mcp_calls,
            "actual_rate": _rate(mcp_actual),
            "available_rate": _rate(mcp_available_sessions),
            "topic_rate": _rate(mcp_topic),
        },
        "subagents": {
            "actual_sessions": subagent_actual,
            "call_count": subagent_calls,
            "actual_rate": _rate(subagent_actual),
        },
        "by_source": by_source,
    }


def _timeline_expression(grain: str) -> Tuple[str, str]:
    allowed = {
        "day": ("substr(COALESCE(started_at, ended_at), 1, 10)", "day"),
        "week": ("strftime('%Y-W%W', COALESCE(started_at, ended_at))", "week"),
        "month": ("substr(COALESCE(started_at, ended_at), 1, 7)", "month"),
    }
    try:
        return allowed[grain]
    except KeyError as exc:
        raise ValueError(f"Unsupported grain: {grain}") from exc


def grouped_stats(conn: sqlite3.Connection, dimension: str, limit: int = 50) -> List[Dict[str, Any]]:
    allowed = {
        "users": "user_id",
        "tools": "tool_name",
        "repos": "repo",
        "categories": "category",
        "sources": "source",
    }
    if dimension == "tools":
        rows = conn.execute(
            """
            SELECT
              tool_name AS label,
              COUNT(*) AS calls,
              COUNT(DISTINCT session_id) AS sessions,
              SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS successes,
              SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS failures
            FROM tool_calls
            GROUP BY tool_name
            ORDER BY calls DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    column = allowed[dimension]
    rows = conn.execute(
        f"""
        SELECT
          {column} AS label,
          COUNT(*) AS sessions,
          COALESCE(AVG(duration_sec), 0) AS avg_duration_sec,
          COALESCE(AVG(turn_count), 0) AS avg_turn_count,
          COALESCE(SUM(total_input_tokens + total_output_tokens), 0) AS total_tokens,
          COALESCE(AVG(has_mcp), 0) AS mcp_rate,
          COALESCE(AVG(has_skill), 0) AS skill_rate,
          COALESCE(AVG(has_subagent), 0) AS subagent_rate
        FROM sessions
        WHERE {column} IS NOT NULL AND {column} != ''
        GROUP BY {column}
        ORDER BY sessions DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def timeline_stats(conn: sqlite3.Connection, grain: str, limit: int = 30) -> List[Dict[str, Any]]:
    bucket_sql, label = _timeline_expression(grain)
    rows = conn.execute(
        f"""
        SELECT
          {bucket_sql} AS period,
          COUNT(*) AS sessions,
          COUNT(DISTINCT user_id) AS users,
          COALESCE(SUM(total_input_tokens + total_output_tokens), 0) AS total_tokens,
          COALESCE(SUM(total_cost), 0) AS total_cost,
          COALESCE(AVG(duration_sec), 0) AS avg_duration_sec,
          COALESCE(AVG(turn_count), 0) AS avg_turn_count
        FROM sessions
        WHERE COALESCE(started_at, ended_at) IS NOT NULL
        GROUP BY period
        ORDER BY period DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item[label] = item["period"]
        result.append(item)
    return result


def daily_stats(conn: sqlite3.Connection, limit: int = 30) -> List[Dict[str, Any]]:
    return timeline_stats(conn, "day", limit=limit)


def weekly_stats(conn: sqlite3.Connection, limit: int = 12) -> List[Dict[str, Any]]:
    return timeline_stats(conn, "week", limit=limit)


def monthly_stats(conn: sqlite3.Connection, limit: int = 12) -> List[Dict[str, Any]]:
    return timeline_stats(conn, "month", limit=limit)


def insight_stats(conn: sqlite3.Connection, user_id: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    items = _load_review_items(conn, user_id=user_id, limit=limit)
    if not items:
        return {
            "quality": {
                "avg_quality_score": 0.0,
                "strong_sessions": 0,
                "mixed_sessions": 0,
                "needs_review_sessions": 0,
                "resolved_rate": 0.0,
                "verification_rate": 0.0,
                "avg_followup_turns": 0.0,
            },
            "patterns": [],
            "topics": [],
            "prompt_tags": [],
            "improvement_flags": [],
            "strong_cases": [],
            "needs_review_cases": [],
            "user_status": [],
        }

    quality_counter = Counter(item["quality_label"] for item in items)
    pattern_counter = Counter(item["work_pattern_label"] for item in items)
    prompt_counter = Counter(tag for item in items for tag in item.get("prompt_tags", []))
    improvement_counter = Counter(flag for item in items for flag in item.get("improvement_flags", []))

    topic_groups = {}
    user_groups = {}
    pattern_groups = {}

    for item in items:
        topic_key = item["topic_signature"]
        topic_group = topic_groups.setdefault(
            topic_key,
            {
                "label": item["topic_signature"],
                "sessions": 0,
                "strong_sessions": 0,
                "needs_review_sessions": 0,
            },
        )
        topic_group["sessions"] += 1
        topic_group["strong_sessions"] += int(item["quality_label"] == "strong")
        topic_group["needs_review_sessions"] += int(item["needs_review"])

        pattern_group = pattern_groups.setdefault(
            item["work_pattern_label"],
            {
                "label": item["work_pattern_label"],
                "sessions": 0,
                "quality_total": 0,
                "resolved_sessions": 0,
            },
        )
        pattern_group["sessions"] += 1
        pattern_group["quality_total"] += item["quality_score"]
        pattern_group["resolved_sessions"] += int(item["outcome_status"] == "resolved")

        user_group = user_groups.setdefault(
            item["user_id"],
            {
                "user_id": item["user_id"],
                "sessions": 0,
                "quality_total": 0,
                "strong_sessions": 0,
                "needs_review_sessions": 0,
                "resolved_sessions": 0,
                "verification_sessions": 0,
                "followup_total": 0,
                "patterns": Counter(),
            },
        )
        user_group["sessions"] += 1
        user_group["quality_total"] += item["quality_score"]
        user_group["strong_sessions"] += int(item["quality_label"] == "strong")
        user_group["needs_review_sessions"] += int(item["needs_review"])
        user_group["resolved_sessions"] += int(item["outcome_status"] == "resolved")
        user_group["verification_sessions"] += int(item["verification_present"])
        user_group["followup_total"] += item["followup_turns"]
        user_group["patterns"][item["work_pattern_label"]] += 1

    patterns = []
    for group in pattern_groups.values():
        patterns.append(
            {
                "label": group["label"],
                "sessions": group["sessions"],
                "avg_quality_score": round(group["quality_total"] / group["sessions"], 1),
                "resolved_rate": round(group["resolved_sessions"] / group["sessions"], 3),
            }
        )
    patterns.sort(key=lambda item: (item["sessions"], item["avg_quality_score"]), reverse=True)

    topics = list(topic_groups.values())
    topics.sort(key=lambda item: (item["sessions"], item["strong_sessions"]), reverse=True)

    user_status = []
    for group in user_groups.values():
        sessions = group["sessions"] or 1
        top_pattern = group["patterns"].most_common(1)[0][0] if group["patterns"] else "Chat Only"
        user_status.append(
            {
                "user_id": group["user_id"],
                "sessions": group["sessions"],
                "avg_quality_score": round(group["quality_total"] / sessions, 1),
                "strong_sessions": group["strong_sessions"],
                "needs_review_sessions": group["needs_review_sessions"],
                "resolved_rate": round(group["resolved_sessions"] / sessions, 3),
                "verification_rate": round(group["verification_sessions"] / sessions, 3),
                "avg_followup_turns": round(group["followup_total"] / sessions, 1),
                "top_pattern": top_pattern,
            }
        )
    user_status.sort(key=lambda item: (item["avg_quality_score"], item["sessions"]), reverse=True)

    strong_cases = sorted(
        [item for item in items if item["quality_label"] == "strong"],
        key=lambda item: (item["quality_score"], item.get("started_at") or item.get("ended_at") or ""),
        reverse=True,
    )[:5]
    needs_review_cases = sorted(
        [item for item in items if item["needs_review"]],
        key=lambda item: (item["quality_score"], item.get("started_at") or item.get("ended_at") or ""),
    )[:5]

    return {
        "quality": {
            "avg_quality_score": round(sum(item["quality_score"] for item in items) / len(items), 1),
            "strong_sessions": quality_counter["strong"],
            "mixed_sessions": quality_counter["mixed"],
            "needs_review_sessions": quality_counter["needs_review"],
            "resolved_rate": round(sum(item["outcome_status"] == "resolved" for item in items) / len(items), 3),
            "verification_rate": round(sum(item["verification_present"] for item in items) / len(items), 3),
            "avg_followup_turns": round(sum(item["followup_turns"] for item in items) / len(items), 1),
        },
        "patterns": patterns[:8],
        "topics": topics[:8],
        "prompt_tags": [{"label": label, "sessions": count} for label, count in prompt_counter.most_common(8)],
        "improvement_flags": [{"label": label, "sessions": count} for label, count in improvement_counter.most_common(8)],
        "strong_cases": strong_cases,
        "needs_review_cases": needs_review_cases,
        "user_status": user_status[:10],
    }


def user_patterns(conn: sqlite3.Connection, user_id: str) -> Dict[str, Any]:
    overview = conn.execute(
        """
        SELECT
          COUNT(*) AS sessions,
          COALESCE(AVG(duration_sec), 0) AS avg_duration_sec,
          COALESCE(AVG(turn_count), 0) AS avg_turn_count,
          COALESCE(SUM(total_input_tokens + total_output_tokens), 0) AS total_tokens,
          COALESCE(AVG(has_mcp), 0) AS mcp_rate,
          COALESCE(AVG(has_skill), 0) AS skill_rate,
          COALESCE(AVG(has_subagent), 0) AS subagent_rate
        FROM sessions
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()
    categories = conn.execute(
        """
        SELECT category AS label, COUNT(*) AS sessions
        FROM sessions
        WHERE user_id = ?
        GROUP BY category
        ORDER BY sessions DESC
        LIMIT 10
        """,
        (user_id,),
    ).fetchall()
    tools = conn.execute(
        """
        SELECT tool_name AS label, COUNT(*) AS calls
        FROM tool_calls
        WHERE session_id IN (SELECT session_id FROM sessions WHERE user_id = ?)
        GROUP BY tool_name
        ORDER BY calls DESC
        LIMIT 15
        """,
        (user_id,),
    ).fetchall()
    repos = conn.execute(
        """
        SELECT repo AS label, COUNT(*) AS sessions
        FROM sessions
        WHERE user_id = ? AND repo IS NOT NULL AND repo != ''
        GROUP BY repo
        ORDER BY sessions DESC
        LIMIT 10
        """,
        (user_id,),
    ).fetchall()
    recent = list_sessions(conn, {"user_id": user_id, "limit": 20, "offset": 0})
    insights = insight_stats(conn, user_id=user_id)
    overview_dict = dict(overview)
    overview_dict.update(
        {
            "avg_quality_score": insights["quality"]["avg_quality_score"],
            "resolved_rate": insights["quality"]["resolved_rate"],
            "verification_rate": insights["quality"]["verification_rate"],
            "avg_followup_turns": insights["quality"]["avg_followup_turns"],
            "strong_sessions": insights["quality"]["strong_sessions"],
            "needs_review_sessions": insights["quality"]["needs_review_sessions"],
        }
    )
    return {
        "overview": overview_dict,
        "categories": [dict(row) for row in categories],
        "tools": [dict(row) for row in tools],
        "repos": [dict(row) for row in repos],
        "recent_sessions": recent["items"],
        "quality": insights["quality"],
        "patterns": insights["patterns"],
        "topics": insights["topics"],
        "prompt_tags": insights["prompt_tags"],
        "improvement_flags": insights["improvement_flags"],
        "strong_cases": insights["strong_cases"],
        "needs_review_cases": insights["needs_review_cases"],
    }


def search_sessions(conn: sqlite3.Connection, query: str, limit: int = 25) -> List[Dict[str, Any]]:
    query = query.strip()
    if not query:
        return []
    try:
        rows = conn.execute(
            """
            SELECT s.*
            FROM session_search ss
            JOIN sessions s ON s.session_id = ss.session_id
            WHERE session_search MATCH ?
            ORDER BY COALESCE(s.started_at, s.ended_at) DESC
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()
        return [_attach_insights(dict(row)) for row in rows]
    except sqlite3.OperationalError:
        like = f"%{query}%"
        rows = conn.execute(
            """
            SELECT DISTINCT s.*
            FROM sessions s
            LEFT JOIN turns t ON t.session_id = s.session_id
            WHERE s.summary LIKE ? OR t.user_text LIKE ? OR t.assistant_text LIKE ?
            ORDER BY COALESCE(s.started_at, s.ended_at) DESC
            LIMIT ?
            """,
            (like, like, like, limit),
        ).fetchall()
        return [_attach_insights(dict(row)) for row in rows]


def export_sessions_csv(conn: sqlite3.Connection, filters: Dict[str, Any]) -> str:
    result = list_sessions(conn, {**filters, "limit": 5000, "offset": 0})
    output = io.StringIO()
    fieldnames = [
        "session_id",
        "source",
        "user_id",
        "repo",
        "git_branch",
        "started_at",
        "ended_at",
        "duration_sec",
        "turn_count",
        "tool_call_count",
        "model_primary",
        "category",
        "status",
        "quality_label",
        "quality_score",
        "work_pattern_label",
        "outcome_status",
        "followup_turns",
        "verification_present",
        "total_input_tokens",
        "total_output_tokens",
        "total_cost",
        "case_title",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for item in result["items"]:
        writer.writerow({field: item.get(field) for field in fieldnames})
    return output.getvalue()
