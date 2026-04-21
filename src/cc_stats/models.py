from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ToolCallRecord:
    session_id: str
    turn_idx: int
    call_idx: int
    ts: Optional[str]
    tool_name: str
    tool_kind: str
    skill_name: Optional[str] = None
    mcp_server: Optional[str] = None
    subagent_type: Optional[str] = None
    success: Optional[bool] = None
    input_size: int = 0
    output_size: int = 0
    error_text: Optional[str] = None
    raw_json: Dict[str, Any] = field(default_factory=dict)
    tool_use_id: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        row = asdict(self)
        row["raw_json"] = self.raw_json
        return row


@dataclass
class TurnRecord:
    session_id: str
    turn_idx: int
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    user_text: str = ""
    assistant_text: str = ""
    user_prompt_chars: int = 0
    assistant_chars: int = 0
    tool_call_count: int = 0
    duration_sec: Optional[int] = None
    category_hint: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SessionRecord:
    session_id: str
    native_session_id: str
    source: str
    user_id: str
    host_name: str
    platform: str
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_sec: Optional[int] = None
    turn_count: int = 0
    assistant_turn_count: int = 0
    user_turn_count: int = 0
    repo: Optional[str] = None
    cwd: Optional[str] = None
    git_branch: Optional[str] = None
    tool_call_count: int = 0
    tool_diversity: int = 0
    model_primary: Optional[str] = None
    models: List[str] = field(default_factory=list)
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
    raw_path: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    last_message_uuid: Optional[str] = None
    status: Optional[str] = None
    parent_native_session_id: Optional[str] = None
    root_native_session_id: Optional[str] = None
    extra_json: Dict[str, Any] = field(default_factory=dict)
    turns: List[TurnRecord] = field(default_factory=list)
    tool_calls: List[ToolCallRecord] = field(default_factory=list)

    def to_row(self) -> Dict[str, Any]:
        row = asdict(self)
        row.pop("turns", None)
        row.pop("tool_calls", None)
        return row
