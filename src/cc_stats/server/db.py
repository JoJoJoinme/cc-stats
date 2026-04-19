from __future__ import annotations

import csv
import io
import json
import sqlite3
from pathlib import Path
from typing import Any

from ..config import default_db_path
from ..models import SessionRecord
from ..utils import utc_now


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


def connect_db(db_path: str | None = None) -> sqlite3.Connection:
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


def _session_row(session: SessionRecord) -> dict[str, Any]:
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


def _build_filter_sql(filters: dict[str, Any]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
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


def list_sessions(conn: sqlite3.Connection, filters: dict[str, Any]) -> dict[str, Any]:
    sql, params = _build_filter_sql(filters)
    sort_map = {
        "started_at": "COALESCE(sessions.started_at, sessions.ended_at) DESC",
        "duration": "sessions.duration_sec DESC",
        "turns": "sessions.turn_count DESC",
        "tokens": "(sessions.total_input_tokens + sessions.total_output_tokens) DESC",
        "cost": "sessions.total_cost DESC",
    }
    order_sql = sort_map.get(filters.get("sort"), "COALESCE(sessions.started_at, sessions.ended_at) DESC")
    limit = min(int(filters.get("limit", 50)), 500)
    offset = max(int(filters.get("offset", 0)), 0)
    total = conn.execute(f"SELECT COUNT(*) AS count FROM sessions {sql}", params).fetchone()["count"]
    rows = conn.execute(
        f"SELECT * FROM sessions {sql} ORDER BY {order_sql} LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()
    return {"total": total, "items": [dict(row) for row in rows], "limit": limit, "offset": offset}


def get_session_detail(conn: sqlite3.Connection, session_id: str) -> dict[str, Any] | None:
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


def stats_overview(conn: sqlite3.Connection) -> dict[str, Any]:
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


def capability_stats(conn: sqlite3.Connection) -> dict[str, Any]:
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
    by_source_available: dict[str, int] = {}
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


def _timeline_expression(grain: str) -> tuple[str, str]:
    allowed = {
        "day": ("substr(COALESCE(started_at, ended_at), 1, 10)", "day"),
        "week": ("strftime('%Y-W%W', COALESCE(started_at, ended_at))", "week"),
        "month": ("substr(COALESCE(started_at, ended_at), 1, 7)", "month"),
    }
    try:
        return allowed[grain]
    except KeyError as exc:
        raise ValueError(f"Unsupported grain: {grain}") from exc


def grouped_stats(conn: sqlite3.Connection, dimension: str, limit: int = 50) -> list[dict[str, Any]]:
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


def timeline_stats(conn: sqlite3.Connection, grain: str, limit: int = 30) -> list[dict[str, Any]]:
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


def daily_stats(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    return timeline_stats(conn, "day", limit=limit)


def weekly_stats(conn: sqlite3.Connection, limit: int = 12) -> list[dict[str, Any]]:
    return timeline_stats(conn, "week", limit=limit)


def monthly_stats(conn: sqlite3.Connection, limit: int = 12) -> list[dict[str, Any]]:
    return timeline_stats(conn, "month", limit=limit)


def user_patterns(conn: sqlite3.Connection, user_id: str) -> dict[str, Any]:
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
    return {
        "overview": dict(overview),
        "categories": [dict(row) for row in categories],
        "tools": [dict(row) for row in tools],
        "repos": [dict(row) for row in repos],
        "recent_sessions": recent["items"],
    }


def search_sessions(conn: sqlite3.Connection, query: str, limit: int = 25) -> list[dict[str, Any]]:
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
        return [dict(row) for row in rows]
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
        return [dict(row) for row in rows]


def export_sessions_csv(conn: sqlite3.Connection, filters: dict[str, Any]) -> str:
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
        "total_input_tokens",
        "total_output_tokens",
        "total_cost",
        "summary",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for item in result["items"]:
        writer.writerow({field: item.get(field) for field in fieldnames})
    return output.getvalue()
