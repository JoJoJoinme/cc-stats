from __future__ import annotations

import time
from collections import Counter
from pathlib import Path
from typing import Any

from ..classification import classify_session, summarize_session
from ..config import watcher_state_path
from ..models import SessionRecord, ToolCallRecord, TurnRecord
from ..paths import candidate_costrict_storage_paths
from ..utils import (
    compact_ws,
    compute_file_signature,
    detect_git_branch,
    detect_host_name,
    detect_platform_name,
    detect_repo_name,
    detect_user_id,
    duration_seconds,
    read_json,
    safe_float,
    safe_int,
    unix_ms_to_iso,
    write_json,
)
from .common import (
    attach_tool_result,
    classify_tool_name,
    distinct_nonempty,
    extract_text_from_content,
    extract_tool_results,
    extract_tool_uses,
    normalize_tool_payload,
    primary_from_counter,
)


def _load_state() -> dict[str, Any]:
    return read_json(watcher_state_path(), default={"tasks": {}}) or {"tasks": {}}


def _save_state(state: dict[str, Any]) -> None:
    write_json(watcher_state_path(), state)


def _task_files(task_dir: Path) -> list[Path]:
    return [
        task_dir / "api_conversation_history.json",
        task_dir / "ui_messages.json",
        task_dir / "history_item.json",
        task_dir / "task_metadata.json",
    ]


def _is_costrict_task_dir(task_dir: Path) -> bool:
    return task_dir.is_dir() and any(path.exists() for path in _task_files(task_dir))


def _parse_api_messages(session_id: str, messages: list[dict[str, Any]]) -> tuple[list[TurnRecord], list[ToolCallRecord], Counter[str], list[str]]:
    turns: list[TurnRecord] = []
    tool_calls: list[ToolCallRecord] = []
    tool_calls_by_id: dict[str, ToolCallRecord] = {}
    current_turn: TurnRecord | None = None
    models = Counter()
    summary_texts: list[str] = []

    for message in messages:
        role = message.get("role")
        timestamp = unix_ms_to_iso(message.get("ts")) or message.get("timestamp")
        if message.get("model"):
            models[str(message["model"])] += 1

        if role == "user":
            for tool_result in extract_tool_results(message.get("content")):
                attach_tool_result(
                    tool_calls_by_id,
                    tool_result.get("tool_use_id"),
                    tool_result.get("content"),
                    tool_result.get("is_error"),
                )
                if current_turn:
                    current_turn.ended_at = timestamp or current_turn.ended_at

            user_text = extract_text_from_content(message.get("content"), include_tool_results=False)
            if user_text:
                current_turn = TurnRecord(
                    session_id=session_id,
                    turn_idx=len(turns) + 1,
                    started_at=timestamp,
                    ended_at=timestamp,
                    user_text=user_text,
                    user_prompt_chars=len(user_text),
                )
                turns.append(current_turn)
                summary_texts.append(user_text)
            continue

        if role != "assistant":
            continue

        if current_turn is None:
            current_turn = TurnRecord(
                session_id=session_id,
                turn_idx=len(turns) + 1,
                started_at=timestamp,
                ended_at=timestamp,
            )
            turns.append(current_turn)

        assistant_text = extract_text_from_content(message.get("content"), include_tool_results=False)
        if assistant_text:
            current_turn.assistant_text = compact_ws("\n".join(filter(None, [current_turn.assistant_text, assistant_text])))
            current_turn.assistant_chars = len(current_turn.assistant_text)
            current_turn.ended_at = timestamp or current_turn.ended_at
            summary_texts.append(assistant_text)

        for tool_use in extract_tool_uses(message.get("content"), message):
            payload = normalize_tool_payload(
                tool_use.get("input")
                or tool_use.get("arguments")
                or tool_use.get("nativeArgs")
                or tool_use.get("params")
                or {}
            )
            kind, skill_name, mcp_server, subagent_type = classify_tool_name(tool_use.get("name"), payload)
            call = ToolCallRecord(
                session_id=session_id,
                turn_idx=current_turn.turn_idx,
                call_idx=current_turn.tool_call_count + 1,
                ts=timestamp,
                tool_name=str(tool_use.get("name") or ""),
                tool_kind=kind,
                skill_name=skill_name,
                mcp_server=mcp_server,
                subagent_type=subagent_type,
                success=None,
                input_size=len(compact_ws(str(payload))),
                raw_json=tool_use,
                tool_use_id=tool_use.get("id"),
            )
            current_turn.tool_call_count += 1
            tool_calls.append(call)
            if call.tool_use_id:
                tool_calls_by_id[call.tool_use_id] = call

    for turn in turns:
        turn.duration_sec = duration_seconds(turn.started_at, turn.ended_at)
    return turns, tool_calls, models, summary_texts


def _parse_ui_messages(session_id: str, messages: list[dict[str, Any]]) -> tuple[list[TurnRecord], list[str]]:
    turns: list[TurnRecord] = []
    current_turn: TurnRecord | None = None
    summary_texts: list[str] = []
    for message in messages:
        timestamp = unix_ms_to_iso(message.get("ts"))
        msg_type = message.get("type")
        ask = message.get("ask")
        say = message.get("say")
        text = compact_ws(message.get("text") or "")

        if msg_type == "say" and say in {"user_feedback", "user_feedback_diff"} and text:
            current_turn = TurnRecord(
                session_id=session_id,
                turn_idx=len(turns) + 1,
                started_at=timestamp,
                ended_at=timestamp,
                user_text=text,
                user_prompt_chars=len(text),
            )
            turns.append(current_turn)
            summary_texts.append(text)
            continue

        if not text:
            continue

        if current_turn is None:
            current_turn = TurnRecord(
                session_id=session_id,
                turn_idx=len(turns) + 1,
                started_at=timestamp,
                ended_at=timestamp,
            )
            turns.append(current_turn)

        if msg_type == "say" and say in {"text", "completion_result", "error", "subtask_result", "reasoning"}:
            current_turn.assistant_text = compact_ws("\n".join(filter(None, [current_turn.assistant_text, text])))
            current_turn.assistant_chars = len(current_turn.assistant_text)
            current_turn.ended_at = timestamp or current_turn.ended_at
            summary_texts.append(text)
        elif msg_type == "ask" and ask in {"followup", "multiple_choice"}:
            current_turn.assistant_text = compact_ws("\n".join(filter(None, [current_turn.assistant_text, text])))
            current_turn.assistant_chars = len(current_turn.assistant_text)
            current_turn.ended_at = timestamp or current_turn.ended_at
            summary_texts.append(text)

    for turn in turns:
        turn.duration_sec = duration_seconds(turn.started_at, turn.ended_at)
    return turns, summary_texts


def parse_costrict_task(task_dir: Path, storage_root: Path) -> SessionRecord:
    native_session_id = task_dir.name
    session_id = f"costrict-ide:{native_session_id}"
    history_item = read_json(task_dir / "history_item.json", default={}) or {}
    task_metadata = read_json(task_dir / "task_metadata.json", default={}) or {}
    api_messages = read_json(task_dir / "api_conversation_history.json", default=[]) or []
    ui_messages = read_json(task_dir / "ui_messages.json", default=[]) or []

    turns, tool_calls, models, summary_texts = _parse_api_messages(session_id, api_messages if isinstance(api_messages, list) else [])
    if not turns:
        turns, summary_texts = _parse_ui_messages(session_id, ui_messages if isinstance(ui_messages, list) else [])
        tool_calls = []
        models = Counter()

    workspace = history_item.get("workspace")
    started_at = None
    ended_at = None
    all_ts = []
    for message in ui_messages if isinstance(ui_messages, list) else []:
        if message.get("ts") is not None:
            all_ts.append(int(message["ts"]))
    for message in api_messages if isinstance(api_messages, list) else []:
        if message.get("ts") is not None:
            all_ts.append(int(message["ts"]))
    if all_ts:
        started_at = unix_ms_to_iso(min(all_ts))
        ended_at = unix_ms_to_iso(max(all_ts))
    if not ended_at and history_item.get("ts") is not None:
        ended_at = unix_ms_to_iso(history_item.get("ts"))
    if not started_at:
        started_at = ended_at

    tool_names = [call.tool_name for call in tool_calls]
    if history_item.get("task"):
        summary_texts.insert(0, str(history_item.get("task")))

    category, confidence = classify_session(summary_texts, tool_names)
    summary = summarize_session(summary_texts, fallback="costrict IDE task")

    session = SessionRecord(
        session_id=session_id,
        native_session_id=native_session_id,
        source="costrict-ide",
        user_id=detect_user_id(workspace),
        host_name=detect_host_name(),
        platform=detect_platform_name(),
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=duration_seconds(started_at, ended_at),
        turn_count=len(turns),
        assistant_turn_count=sum(1 for turn in turns if turn.assistant_text),
        user_turn_count=sum(1 for turn in turns if turn.user_text),
        repo=detect_repo_name(workspace),
        cwd=workspace,
        git_branch=detect_git_branch(workspace),
        tool_call_count=len(tool_calls),
        tool_diversity=len(set(tool_names)),
        model_primary=primary_from_counter(models),
        models=distinct_nonempty(models.keys()),
        total_input_tokens=safe_int(history_item.get("tokensIn")),
        total_output_tokens=safe_int(history_item.get("tokensOut")),
        total_cache_creation_tokens=safe_int(history_item.get("cacheWrites")),
        total_cache_read_tokens=safe_int(history_item.get("cacheReads")),
        total_cost=safe_float(history_item.get("totalCost")),
        category=category,
        category_confidence=confidence,
        summary=summary,
        has_skill=any(call.tool_kind == "skill" for call in tool_calls),
        has_mcp=any(call.tool_kind == "mcp" for call in tool_calls),
        has_subagent=any(call.tool_kind == "subagent" for call in tool_calls),
        raw_path=str(task_dir),
        raw_payload={},
        status=history_item.get("status"),
        parent_native_session_id=history_item.get("parentTaskId"),
        root_native_session_id=history_item.get("rootTaskId"),
        extra_json={
            "storage_root": str(storage_root),
            "task_metadata": task_metadata,
            "history_item": history_item,
        },
        turns=turns,
        tool_calls=tool_calls,
    )
    return session


def discover_costrict_task_dirs() -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    for storage_root in candidate_costrict_storage_paths():
        tasks_dir = storage_root / "tasks"
        if not tasks_dir.exists():
            continue
        for candidate in tasks_dir.iterdir():
            if _is_costrict_task_dir(candidate):
                pairs.append((storage_root, candidate))
    return pairs


def scan_costrict_once(changed_only: bool = False) -> list[SessionRecord]:
    state = _load_state()
    tracked = state.setdefault("tasks", {})
    sessions: list[SessionRecord] = []
    seen_keys: set[str] = set()

    for storage_root, task_dir in discover_costrict_task_dirs():
        signature = compute_file_signature(_task_files(task_dir))
        task_key = f"{storage_root}::{task_dir.name}"
        seen_keys.add(task_key)
        if changed_only and tracked.get(task_key) == signature:
            continue
        try:
            sessions.append(parse_costrict_task(task_dir, storage_root))
            tracked[task_key] = signature
        except Exception:
            continue

    stale = [key for key in tracked if key not in seen_keys]
    for key in stale:
        tracked.pop(key, None)
    _save_state(state)
    return sessions


def watch_costrict_loop(interval: int = 20) -> list[SessionRecord]:
    latest: list[SessionRecord] = []
    while True:
        latest = scan_costrict_once(changed_only=True)
        time.sleep(interval)
    return latest
