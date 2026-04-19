from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

from ..classification import classify_session, summarize_session
from ..models import SessionRecord, ToolCallRecord, TurnRecord
from ..paths import claude_projects_dir, find_claude_transcript
from ..utils import (
    compact_ws,
    detect_git_branch,
    detect_host_name,
    detect_platform_name,
    detect_repo_name,
    detect_user_id,
    duration_seconds,
    load_stdin_json,
    read_jsonl,
)
from .common import (
    attach_tool_result,
    classify_tool_name,
    collect_usage_from_assistant_message,
    distinct_nonempty,
    extract_text_from_content,
    extract_tool_results,
    extract_tool_uses,
    normalize_tool_payload,
    primary_from_counter,
)


def is_local_command_text(text: str) -> bool:
    lowered = compact_ws(text).lower()
    return lowered.startswith("<local-command") or lowered.startswith("<command-name>") or lowered == "[request interrupted by user]"


COMMAND_NAME_RE = re.compile(r"<command-name>\s*([^<\s]+)\s*</command-name>", re.IGNORECASE)
COMMAND_MESSAGE_RE = re.compile(r"<command-message>\s*(.*?)\s*</command-message>", re.IGNORECASE | re.DOTALL)
SKILL_BASE_DIR_RE = re.compile(r"Base directory for this skill:\s*(.+?[\\\\/]skills[\\\\/]([^\\\\/\s]+))", re.IGNORECASE)
SKILL_FORMAT_RE = re.compile(r"<skill-format>\s*true\s*</skill-format>", re.IGNORECASE)


def extract_command_name(text: str) -> str | None:
    match = COMMAND_NAME_RE.search(text)
    if not match:
        return None
    return match.group(1).strip()


def extract_command_message(text: str) -> str | None:
    match = COMMAND_MESSAGE_RE.search(text)
    if not match:
        return None
    return compact_ws(match.group(1))


def extract_skill_name(text: str) -> str | None:
    match = SKILL_BASE_DIR_RE.search(text)
    if not match:
        return None
    return match.group(2).strip()


def has_skill_format_marker(text: str) -> bool:
    return bool(SKILL_FORMAT_RE.search(text))


def extract_mcp_server_from_name(name: str) -> str | None:
    tool_name = compact_ws(name)
    if not tool_name.startswith("mcp__"):
        return None
    parts = tool_name.split("__", 2)
    if len(parts) < 3:
        return None
    return parts[1].strip() or None


def parse_claude_transcript(transcript_path: Path, hook_payload: dict[str, Any] | None = None) -> SessionRecord:
    rows = read_jsonl(transcript_path)
    if not rows:
        raise ValueError(f"No rows found in {transcript_path}")

    first = rows[0]
    native_session_id = (hook_payload or {}).get("session_id") or first.get("sessionId") or transcript_path.stem
    session_id = f"claude-code:{native_session_id}"
    cwd = (hook_payload or {}).get("cwd") or first.get("cwd")
    turns: list[TurnRecord] = []
    tool_calls: list[ToolCallRecord] = []
    tool_calls_by_id: dict[str, ToolCallRecord] = {}
    models = Counter()
    current_turn: TurnRecord | None = None
    summary_texts: list[str] = []
    command_summary_texts: list[str] = []
    last_message_uuid: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    total_input = 0
    total_output = 0
    total_cache_creation = 0
    total_cache_read = 0
    total_cost = 0.0
    seen_usage_ids: set[str] = set()
    pending_command_name: str | None = None
    pending_command_message: str | None = None
    observed_skill_names: set[str] = set()
    available_mcp_tools: set[str] = set()
    available_mcp_servers: set[str] = set()

    for row in rows:
        row_type = row.get("type")
        timestamp = row.get("timestamp")
        if not started_at:
            started_at = timestamp
        ended_at = timestamp or ended_at
        last_message_uuid = row.get("uuid") or last_message_uuid
        message = row.get("message") or {}

        if row_type == "user":
            user_text = extract_text_from_content(message.get("content"), include_tool_results=False)
            command_name = extract_command_name(user_text) if user_text else None
            command_message = extract_command_message(user_text) if user_text else None
            if command_name:
                pending_command_name = command_name.lstrip("/")
                pending_command_message = command_message or pending_command_name
                current_turn = TurnRecord(
                    session_id=session_id,
                    turn_idx=len(turns) + 1,
                    started_at=timestamp,
                    ended_at=timestamp,
                    user_text=f"/{pending_command_name}",
                    user_prompt_chars=len(pending_command_name) + 1,
                )
                turns.append(current_turn)
                command_summary_texts.append(current_turn.user_text)

            for tool_result in extract_tool_results(message.get("content")):
                attach_tool_result(
                    tool_calls_by_id,
                    tool_result.get("tool_use_id"),
                    tool_result.get("content"),
                    tool_result.get("is_error"),
                )
                if current_turn:
                    current_turn.ended_at = timestamp or current_turn.ended_at

            if row.get("isMeta"):
                skill_name = extract_skill_name(user_text or "")
                if not skill_name and has_skill_format_marker(user_text or "") and pending_command_name:
                    skill_name = pending_command_name
                if skill_name and skill_name not in observed_skill_names:
                    if current_turn is None:
                        current_turn = TurnRecord(
                            session_id=session_id,
                            turn_idx=len(turns) + 1,
                            started_at=timestamp,
                            ended_at=timestamp,
                            user_text=f"/{skill_name}",
                            user_prompt_chars=len(skill_name) + 1,
                        )
                        turns.append(current_turn)
                        command_summary_texts.append(f"/{skill_name}")
                    elif not current_turn.user_text:
                        current_turn.user_text = f"/{skill_name}"
                        current_turn.user_prompt_chars = len(skill_name) + 1
                    call = ToolCallRecord(
                        session_id=session_id,
                        turn_idx=current_turn.turn_idx,
                        call_idx=current_turn.tool_call_count + 1,
                        ts=timestamp,
                        tool_name="Skill",
                        tool_kind="skill",
                        skill_name=skill_name,
                        success=True,
                        input_size=len(pending_command_message or skill_name),
                        raw_json={
                            "type": "skill_command",
                            "command_name": pending_command_name or skill_name,
                            "skill_name": skill_name,
                            "skill_prompt": pending_command_message or "",
                            "meta_text": user_text or "",
                        },
                    )
                    current_turn.tool_call_count += 1
                    current_turn.ended_at = timestamp or current_turn.ended_at
                    tool_calls.append(call)
                    observed_skill_names.add(skill_name)
                continue
            if command_name:
                continue
            if not user_text or is_local_command_text(user_text):
                continue
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

        if row_type == "attachment":
            attachment = row.get("attachment") or {}
            if attachment.get("type") == "deferred_tools_delta":
                for tool_name in attachment.get("addedNames") or []:
                    normalized = compact_ws(str(tool_name))
                    server_name = extract_mcp_server_from_name(normalized)
                    if not server_name:
                        continue
                    available_mcp_tools.add(normalized)
                    available_mcp_servers.add(server_name)
            if attachment.get("type") == "invoked_skills":
                target_turn = current_turn or (turns[-1] if turns else None)
                if target_turn is None:
                    target_turn = TurnRecord(
                        session_id=session_id,
                        turn_idx=len(turns) + 1,
                        started_at=timestamp,
                        ended_at=timestamp,
                    )
                    turns.append(target_turn)
                    current_turn = target_turn
                for skill in attachment.get("skills") or []:
                    skill_name = compact_ws(str(skill.get("name") or ""))
                    if not skill_name or skill_name in observed_skill_names:
                        continue
                    call = ToolCallRecord(
                        session_id=session_id,
                        turn_idx=target_turn.turn_idx,
                        call_idx=target_turn.tool_call_count + 1,
                        ts=timestamp,
                        tool_name="Skill",
                        tool_kind="skill",
                        skill_name=skill_name,
                        success=True,
                        input_size=len(skill_name),
                        raw_json={
                            "type": "invoked_skills",
                            "skill_name": skill_name,
                            "path": skill.get("path"),
                        },
                    )
                    target_turn.tool_call_count += 1
                    target_turn.ended_at = timestamp or target_turn.ended_at
                    tool_calls.append(call)
                    observed_skill_names.add(skill_name)
                    if not summary_texts:
                        command_summary_texts.append(f"/{skill_name}")
            continue

        if row_type != "assistant":
            continue

        message_id = message.get("id") or row.get("uuid")
        if message_id and message_id not in seen_usage_ids:
            seen_usage_ids.add(message_id)
            usage = collect_usage_from_assistant_message(message)
            total_input += int(usage["input"])
            total_output += int(usage["output"])
            total_cache_creation += int(usage["cache_creation"])
            total_cache_read += int(usage["cache_read"])
            total_cost += float(usage["cost"])
            if message.get("model"):
                models[str(message["model"])] += 1

        if current_turn is None:
            current_turn = TurnRecord(
                session_id=session_id,
                turn_idx=len(turns) + 1,
                started_at=timestamp,
                ended_at=timestamp,
            )
            turns.append(current_turn)

        assistant_text = extract_text_from_content(message.get("content"), include_tool_results=False)
        if assistant_text and not is_local_command_text(assistant_text):
            current_turn.assistant_text = compact_ws("\n".join(filter(None, [current_turn.assistant_text, assistant_text])))
            current_turn.assistant_chars = len(current_turn.assistant_text)
            current_turn.ended_at = timestamp or current_turn.ended_at
            summary_texts.append(assistant_text)

        for tool_use in extract_tool_uses(message.get("content"), message):
            payload = normalize_tool_payload(tool_use.get("input") or tool_use.get("arguments") or {})
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
            current_turn.ended_at = timestamp or current_turn.ended_at
            tool_calls.append(call)
            if kind == "skill" and skill_name:
                observed_skill_names.add(skill_name)
            if call.tool_use_id:
                tool_calls_by_id[call.tool_use_id] = call

    for turn in turns:
        turn.duration_sec = duration_seconds(turn.started_at, turn.ended_at)

    tool_names = [call.tool_name for call in tool_calls]
    summary_inputs = summary_texts or command_summary_texts
    category, confidence = classify_session(summary_inputs, tool_names)
    summary = summarize_session(summary_inputs, fallback="Claude Code session")

    session = SessionRecord(
        session_id=session_id,
        native_session_id=str(native_session_id),
        source="claude-code",
        user_id=detect_user_id(cwd),
        host_name=detect_host_name(),
        platform=detect_platform_name(),
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=duration_seconds(started_at, ended_at),
        turn_count=len(turns),
        assistant_turn_count=sum(1 for turn in turns if turn.assistant_text),
        user_turn_count=sum(1 for turn in turns if turn.user_text),
        repo=detect_repo_name(cwd),
        cwd=cwd,
        git_branch=first.get("gitBranch") or detect_git_branch(cwd),
        tool_call_count=len(tool_calls),
        tool_diversity=len(set(tool_names)),
        model_primary=primary_from_counter(models),
        models=distinct_nonempty(models.keys()),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cache_creation_tokens=total_cache_creation,
        total_cache_read_tokens=total_cache_read,
        total_cost=total_cost,
        category=category,
        category_confidence=confidence,
        summary=summary,
        has_skill=any(call.tool_kind == "skill" for call in tool_calls),
        has_mcp=any(call.tool_kind == "mcp" for call in tool_calls),
        has_subagent=any(call.tool_kind == "subagent" for call in tool_calls),
        raw_path=str(transcript_path),
        raw_payload=hook_payload or {},
        last_message_uuid=last_message_uuid,
        extra_json={
            "available_mcp_tools": sorted(available_mcp_tools),
            "available_mcp_servers": sorted(available_mcp_servers),
            "available_mcp_tools_count": len(available_mcp_tools),
            "available_mcp_servers_count": len(available_mcp_servers),
        },
        turns=turns,
        tool_calls=tool_calls,
    )
    return session


def build_session_from_claude_hook(payload: dict[str, Any] | None = None) -> SessionRecord:
    payload = payload or load_stdin_json()
    transcript_path = payload.get("transcript_path")
    session_id = payload.get("session_id")
    cwd = payload.get("cwd")
    resolved = Path(transcript_path) if transcript_path else (find_claude_transcript(session_id, cwd) if session_id else None)
    if not resolved or not resolved.exists():
        raise FileNotFoundError(f"Could not resolve Claude transcript for session={session_id!r}, path={transcript_path!r}")
    return parse_claude_transcript(resolved, hook_payload=payload)


def backfill_claude(limit: int | None = None) -> list[SessionRecord]:
    project_dir = claude_projects_dir()
    sessions: list[SessionRecord] = []
    if not project_dir.exists():
        return sessions
    for path in sorted(project_dir.glob("**/*.jsonl")):
        if "subagents" in path.parts:
            continue
        try:
            sessions.append(parse_claude_transcript(path, hook_payload={}))
        except Exception:
            continue
        if limit and len(sessions) >= limit:
            break
    return sessions
