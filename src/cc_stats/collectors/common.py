from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..models import ToolCallRecord
from ..utils import compact_ws, safe_float, safe_int


def iter_content_blocks(content: Any) -> List[Any]:
    if content is None:
        return []
    if isinstance(content, list):
        return content
    return [content]


def extract_text_from_content(content: Any, include_tool_results: bool = False) -> str:
    parts = []
    for block in iter_content_blocks(content):
        if isinstance(block, str):
            if compact_ws(block):
                parts.append(compact_ws(block))
            continue
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type in {"thinking", "reasoning"}:
            continue
        if block_type == "tool_result" and not include_tool_results:
            continue
        if block_type in {"text", "input_text", "output_text"}:
            text = block.get("text") or block.get("content")
            if isinstance(text, str) and compact_ws(text):
                parts.append(compact_ws(text))
            continue
        if block_type == "tool_result" and include_tool_results:
            nested = extract_text_from_content(block.get("content"), include_tool_results=False)
            if nested:
                parts.append(nested)
            continue
        if isinstance(block.get("content"), str) and compact_ws(block["content"]):
            parts.append(compact_ws(block["content"]))
    return "\n".join(parts).strip()


def extract_tool_results(content: Any) -> List[Dict[str, Any]]:
    results = []
    for block in iter_content_blocks(content):
        if isinstance(block, dict) and block.get("type") == "tool_result":
            results.append(block)
    return results


def extract_tool_uses(content: Any, message: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    tool_uses = []
    for block in iter_content_blocks(content):
        if isinstance(block, dict) and block.get("type") in {"tool_use", "mcp_tool_use"}:
            tool_uses.append(block)
    if message:
        for tool_call in message.get("tool_calls") or []:
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function") or {}
            tool_uses.append(
                {
                    "type": "tool_use",
                    "id": tool_call.get("id"),
                    "name": function.get("name") or tool_call.get("name"),
                    "input": function.get("arguments") or tool_call.get("arguments") or {},
                }
            )
    return tool_uses


def classify_tool_name(name: Optional[str], payload: Optional[Dict[str, Any]] = None) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    tool_name = (name or "").strip()
    lowered = tool_name.lower()
    payload = payload or {}
    if lowered in {"skill", "skills"}:
        return "skill", payload.get("skill") or payload.get("name"), None, None
    if lowered in {"agent", "task", "new_task"}:
        return "subagent", None, None, payload.get("subagent_type") or payload.get("mode")
    if lowered.startswith("mcp__"):
        parts = tool_name.split("__")
        server = parts[1] if len(parts) > 2 else None
        return "mcp", None, server, None
    if lowered == "use_mcp_tool":
        return "mcp", None, payload.get("server_name"), None
    if lowered == "access_mcp_resource":
        return "mcp", None, payload.get("server_name"), None
    if lowered == "mcp_tool_use":
        return "mcp", None, payload.get("serverName"), None
    return "builtin", None, None, None


def normalize_tool_payload(raw_input: Any) -> Dict[str, Any]:
    if isinstance(raw_input, dict):
        return raw_input
    return {"value": raw_input}


def attach_tool_result(tool_calls_by_id: Dict[str, ToolCallRecord], tool_use_id: Optional[str], content: Any, is_error: Optional[bool]) -> None:
    if not tool_use_id or tool_use_id not in tool_calls_by_id:
        return
    tool = tool_calls_by_id[tool_use_id]
    output_text = extract_text_from_content(content, include_tool_results=False)
    tool.output_size += len(output_text)
    if is_error is not None:
        tool.success = not bool(is_error)
        if is_error and output_text:
            tool.error_text = output_text


def collect_usage_from_assistant_message(message: Dict[str, Any]) -> Dict[str, float]:
    usage = message.get("usage") or {}
    cache_creation = usage.get("cache_creation") or {}
    return {
        "input": safe_int(usage.get("input_tokens")),
        "output": safe_int(usage.get("output_tokens")),
        "cache_creation": safe_int(usage.get("cache_creation_input_tokens"))
        or safe_int(cache_creation.get("ephemeral_5m_input_tokens"))
        or safe_int(cache_creation.get("ephemeral_1h_input_tokens")),
        "cache_read": safe_int(usage.get("cache_read_input_tokens")),
        "cost": safe_float(usage.get("cost")),
    }


def primary_from_counter(counter: Counter) -> Optional[str]:
    if not counter:
        return None
    return counter.most_common(1)[0][0]


def distinct_nonempty(values: Iterable[Optional[str]]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
