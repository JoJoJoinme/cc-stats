from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from ..models import SessionRecord, ToolCallRecord, TurnRecord
from .db import (
    capability_stats,
    connect_db,
    daily_stats,
    export_sessions_csv,
    get_session_detail,
    grouped_stats,
    list_sessions,
    monthly_stats,
    search_sessions,
    stats_overview,
    timeline_stats,
    upsert_session,
    user_patterns,
    weekly_stats,
)


TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _decode_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _session_from_payload(payload: dict[str, Any]) -> SessionRecord:
    turns = [TurnRecord(**turn) for turn in payload.get("turns", [])]
    tool_calls = [ToolCallRecord(**call) for call in payload.get("tool_calls", [])]
    payload = {**payload, "turns": turns, "tool_calls": tool_calls}
    return SessionRecord(**payload)


def _filters_from_request(
    source: str | None,
    user_id: str | None,
    repo: str | None,
    git_branch: str | None,
    model: str | None,
    category: str | None,
    status: str | None,
    has_mcp: str | None,
    has_skill: str | None,
    has_subagent: str | None,
    tool_name: str | None,
    mcp_server: str | None,
    skill_name: str | None,
    date_from: str | None,
    date_to: str | None,
    text_query: str | None,
    sort: str | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    return {
        "source": source,
        "user_id": user_id,
        "repo": repo,
        "git_branch": git_branch,
        "model": model,
        "category": category,
        "status": status,
        "has_mcp": _decode_bool(has_mcp),
        "has_skill": _decode_bool(has_skill),
        "has_subagent": _decode_bool(has_subagent),
        "tool_name": tool_name,
        "mcp_server": mcp_server,
        "skill_name": skill_name,
        "date_from": date_from,
        "date_to": date_to,
        "text_query": text_query,
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }


@lru_cache(maxsize=1)
def get_db_path(default: str | None = None) -> str | None:
    return default


def create_app(db_path: str | None = None, auth_token: str | None = None) -> FastAPI:
    app = FastAPI(title="cc-stats")
    db_target = db_path or get_db_path(None)

    def get_conn():
        conn = connect_db(db_target)
        try:
            yield conn
        finally:
            conn.close()

    def require_ingest_token(authorization: str | None = Header(default=None)) -> None:
        if not auth_token:
            return
        expected = f"Bearer {auth_token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Invalid ingest token")

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return {"ok": True}

    @app.post("/api/v1/ingest/session")
    def ingest_session(
        payload: dict[str, Any],
        _: None = Depends(require_ingest_token),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        session = _session_from_payload(payload)
        upsert_session(conn, session)
        return {"ok": True, "session_id": session.session_id}

    @app.post("/api/v1/ingest/batch")
    def ingest_batch(
        payload: list[dict[str, Any]],
        _: None = Depends(require_ingest_token),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        count = 0
        for item in payload:
            session = _session_from_payload(item)
            upsert_session(conn, session)
            count += 1
        return {"ok": True, "count": count}

    @app.get("/api/v1/stats/overview")
    def api_overview(conn=Depends(get_conn)) -> dict[str, Any]:
        return {
            "overview": stats_overview(conn),
            "capabilities": capability_stats(conn),
            "daily": daily_stats(conn, limit=30),
            "weekly": weekly_stats(conn, limit=16),
            "monthly": monthly_stats(conn, limit=12),
        }

    @app.get("/api/v1/stats/timeline")
    def api_timeline(grain: str = Query(default="day", pattern="^(day|week|month)$"), limit: int = 30, conn=Depends(get_conn)) -> dict[str, Any]:
        return {"grain": grain, "items": timeline_stats(conn, grain, limit=limit)}

    @app.get("/api/v1/stats/users")
    def api_users(limit: int = 50, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return grouped_stats(conn, "users", limit=limit)

    @app.get("/api/v1/stats/tools")
    def api_tools(limit: int = 50, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return grouped_stats(conn, "tools", limit=limit)

    @app.get("/api/v1/stats/repos")
    def api_repos(limit: int = 50, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return grouped_stats(conn, "repos", limit=limit)

    @app.get("/api/v1/stats/categories")
    def api_categories(limit: int = 50, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return grouped_stats(conn, "categories", limit=limit)

    @app.get("/api/v1/stats/sources")
    def api_sources(limit: int = 50, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return grouped_stats(conn, "sources", limit=limit)

    @app.get("/api/v1/stats/capabilities")
    def api_capabilities(conn=Depends(get_conn)) -> dict[str, Any]:
        return capability_stats(conn)

    @app.get("/api/v1/sessions")
    def api_sessions(
        source: str | None = None,
        user_id: str | None = None,
        repo: str | None = None,
        git_branch: str | None = None,
        model: str | None = None,
        category: str | None = None,
        status: str | None = None,
        has_mcp: str | None = None,
        has_skill: str | None = None,
        has_subagent: str | None = None,
        tool_name: str | None = None,
        mcp_server: str | None = None,
        skill_name: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        text_query: str | None = None,
        sort: str | None = None,
        limit: int = Query(default=50, le=500),
        offset: int = Query(default=0, ge=0),
        conn=Depends(get_conn),
    ) -> dict[str, Any]:
        filters = _filters_from_request(
            source,
            user_id,
            repo,
            git_branch,
            model,
            category,
            status,
            has_mcp,
            has_skill,
            has_subagent,
            tool_name,
            mcp_server,
            skill_name,
            date_from,
            date_to,
            text_query,
            sort,
            limit,
            offset,
        )
        return list_sessions(conn, filters)

    @app.get("/api/v1/sessions/{session_id}")
    def api_session_detail(session_id: str, conn=Depends(get_conn)) -> dict[str, Any]:
        detail = get_session_detail(conn, session_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Session not found")
        return detail

    @app.get("/api/v1/users/{user_id}/patterns")
    def api_user_patterns(user_id: str, conn=Depends(get_conn)) -> dict[str, Any]:
        return user_patterns(conn, user_id)

    @app.get("/api/v1/search")
    def api_search(q: str, limit: int = 25, conn=Depends(get_conn)) -> list[dict[str, Any]]:
        return search_sessions(conn, q, limit=limit)

    @app.get("/api/v1/export/sessions.csv")
    def api_export_sessions_csv(
        source: str | None = None,
        user_id: str | None = None,
        repo: str | None = None,
        git_branch: str | None = None,
        model: str | None = None,
        category: str | None = None,
        status: str | None = None,
        has_mcp: str | None = None,
        has_skill: str | None = None,
        has_subagent: str | None = None,
        tool_name: str | None = None,
        mcp_server: str | None = None,
        skill_name: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        text_query: str | None = None,
        sort: str | None = None,
        conn=Depends(get_conn),
    ) -> PlainTextResponse:
        filters = _filters_from_request(
            source,
            user_id,
            repo,
            git_branch,
            model,
            category,
            status,
            has_mcp,
            has_skill,
            has_subagent,
            tool_name,
            mcp_server,
            skill_name,
            date_from,
            date_to,
            text_query,
            sort,
            5000,
            0,
        )
        csv_body = export_sessions_csv(conn, filters)
        return PlainTextResponse(
            csv_body,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sessions.csv"},
        )

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, conn=Depends(get_conn)) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "overview": stats_overview(conn),
                "capabilities": capability_stats(conn),
                "daily": daily_stats(conn, limit=14),
                "weekly": weekly_stats(conn, limit=12),
                "monthly": monthly_stats(conn, limit=12),
                "sources": grouped_stats(conn, "sources", limit=12),
                "users": grouped_stats(conn, "users", limit=12),
                "tools": grouped_stats(conn, "tools", limit=12),
                "repos": grouped_stats(conn, "repos", limit=12),
                "categories": grouped_stats(conn, "categories", limit=12),
            },
        )

    @app.get("/sessions", response_class=HTMLResponse)
    def sessions_page(
        request: Request,
        source: str | None = None,
        user_id: str | None = None,
        repo: str | None = None,
        category: str | None = None,
        has_mcp: str | None = None,
        has_skill: str | None = None,
        text_query: str | None = None,
        sort: str | None = None,
        limit: int = Query(default=100, le=500),
        offset: int = Query(default=0, ge=0),
        conn=Depends(get_conn),
    ) -> HTMLResponse:
        result = list_sessions(
            conn,
            _filters_from_request(
                source,
                user_id,
                repo,
                None,
                None,
                category,
                None,
                has_mcp,
                has_skill,
                None,
                None,
                None,
                None,
                None,
                None,
                text_query,
                sort,
                limit,
                offset,
            ),
        )
        return TEMPLATES.TemplateResponse(
            request,
            "sessions.html",
            {
                "request": request,
                "result": result,
                "filters": {
                    "source": source or "",
                    "user_id": user_id or "",
                    "repo": repo or "",
                    "category": category or "",
                    "has_mcp": has_mcp or "",
                    "has_skill": has_skill or "",
                    "text_query": text_query or "",
                    "sort": sort or "",
                },
            },
        )

    @app.get("/sessions/{session_id}", response_class=HTMLResponse)
    def session_page(request: Request, session_id: str, conn=Depends(get_conn)) -> HTMLResponse:
        detail = get_session_detail(conn, session_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Session not found")
        detail["models"] = json.loads(detail.get("models_json") or "[]")
        detail["raw_payload"] = json.loads(detail.get("raw_payload_json") or "{}")
        detail["extra"] = json.loads(detail.get("extra_json") or "{}")
        return TEMPLATES.TemplateResponse(
            request,
            "session_detail.html",
            {"request": request, "session": detail},
        )

    @app.get("/users/{user_id}", response_class=HTMLResponse)
    def user_page(request: Request, user_id: str, conn=Depends(get_conn)) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            request,
            "user_detail.html",
            {"request": request, "user_id": user_id, "patterns": user_patterns(conn, user_id)},
        )

    return app
