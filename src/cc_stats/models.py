from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ToolCallRecord:
    session_id: str
    turn_idx: int
    call_idx: int
    ts: str | None
    tool_name: str
    tool_kind: str
    skill_name: str | None = None
    mcp_server: str | None = None
    subagent_type: str | None = None
    success: bool | None = None
    input_size: int = 0
    output_size: int = 0
    error_text: str | None = None
    raw_json: dict[str, Any] = field(default_factory=dict)
    tool_use_id: str | None = None

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["raw_json"] = self.raw_json
        return row


@dataclass
class TurnRecord:
    session_id: str
    turn_idx: int
    started_at: str | None = None
    ended_at: str | None = None
    user_text: str = ""
    assistant_text: str = ""
    user_prompt_chars: int = 0
    assistant_chars: int = 0
    tool_call_count: int = 0
    duration_sec: int | None = None
    category_hint: str | None = None

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SessionRecord:
    session_id: str
    native_session_id: str
    source: str
    user_id: str
    host_name: str
    platform: str
    started_at: str | None = None
    ended_at: str | None = None
    duration_sec: int | None = None
    turn_count: int = 0
    assistant_turn_count: int = 0
    user_turn_count: int = 0
    repo: str | None = None
    cwd: str | None = None
    git_branch: str | None = None
    tool_call_count: int = 0
    tool_diversity: int = 0
    model_primary: str | None = None
    models: list[str] = field(default_factory=list)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cache_creation_tokens: int = 0
    total_cache_read_tokens: int = 0
    total_cost: float = 0.0
    category: str = "other"
    category_confidence: float = 0.0
    summary: str = ""
    has_skill: bool = False
    has_mcp: bool = False
    has_subagent: bool = False
    raw_path: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    last_message_uuid: str | None = None
    status: str | None = None
    parent_native_session_id: str | None = None
    root_native_session_id: str | None = None
    extra_json: dict[str, Any] = field(default_factory=dict)
    turns: list[TurnRecord] = field(default_factory=list)
    tool_calls: list[ToolCallRecord] = field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row.pop("turns", None)
        row.pop("tool_calls", None)
        return row
