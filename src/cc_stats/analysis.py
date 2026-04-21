import re
from collections import Counter
from typing import Any, Dict, List

from .models import SessionRecord, ToolCallRecord
from .utils import compact_ws, shorten


ANALYSIS_VERSION = 2

SEARCH_TOOLS = {
    "search_files",
    "grep",
    "glob",
    "find",
    "toolsearch",
}
INSPECT_TOOLS = {
    "read_file",
    "list_files",
    "open",
    "list_code_definition_names",
    "view",
}
EDIT_TOOLS = {
    "write",
    "edit",
    "apply_patch",
    "apply_diff",
    "replace_in_file",
    "write_to_file",
    "insert",
}
EXECUTE_TOOLS = {
    "bash",
    "execute_command",
    "run_terminal_cmd",
    "terminal",
}
ASK_USER_TOOLS = {
    "askuserquestion",
    "ask_user_question",
}
COMPLETE_TOOLS = {
    "attempt_completion",
}

VERIFY_TERMS = {
    "test",
    "tests",
    "pytest",
    "verified",
    "verification",
    "passed",
    "validation",
    "验证",
    "已验证",
    "测试通过",
    "通过测试",
}
RESOLVED_TERMS = {
    "fixed",
    "resolved",
    "updated",
    "implemented",
    "added",
    "completed",
    "done",
    "working",
    "修复",
    "已修复",
    "完成了",
    "已完成",
    "已经可以",
    "搞定",
    "已更新",
    "已添加",
}
BLOCKED_TERMS = {
    "please send",
    "please provide",
    "need ",
    "i need",
    "missing",
    "upload",
    "share",
    "发给我",
    "请提供",
    "请发送",
    "还需要",
    "缺少",
    "上传",
    "告诉我",
}
CONSTRAINT_TERMS = {
    "must",
    "without",
    "only",
    "do not",
    "don't",
    "keep",
    "不要",
    "别",
    "只",
    "仅",
    "必须",
    "保留",
    "兼容",
    "不能",
}
EXPLANATION_TERMS = {
    "why",
    "explain",
    "reason",
    "解释",
    "原因",
    "说明",
}
PROMPT_VERBS = {
    "fix",
    "implement",
    "add",
    "write",
    "build",
    "review",
    "debug",
    "optimize",
    "修复",
    "实现",
    "添加",
    "新增",
    "写",
    "排查",
    "优化",
    "评审",
}
EN_STOPWORDS = {
    "the",
    "a",
    "an",
    "to",
    "for",
    "and",
    "or",
    "of",
    "in",
    "on",
    "with",
    "this",
    "that",
    "please",
    "help",
    "me",
    "we",
    "our",
    "my",
    "it",
}
CN_STOPWORDS = {
    "请",
    "帮我",
    "帮忙",
    "一下",
    "这个",
    "那个",
    "这里",
    "现在",
    "需要",
    "我们",
    "你",
    "我",
}
CATEGORY_LABELS = {
    "debug": "Debug",
    "new_feature": "New Feature",
    "refactor": "Refactor",
    "docs": "Docs",
    "test": "Test",
    "ops": "Ops",
    "exploration": "Exploration",
    "review": "Review",
    "other": "Other",
}
PATTERN_LABELS = {
    "chat_only": "Chat Only",
    "inspect_only": "Inspect Only",
    "inspect_execute": "Inspect + Verify",
    "inspect_edit": "Inspect + Edit",
    "inspect_edit_verify": "Inspect + Edit + Verify",
    "edit_only": "Edit Only",
    "skill_workflow": "Skill Workflow",
    "delegated": "Delegated",
    "mcp_augmented": "MCP Augmented",
    "mixed_tools": "Mixed Tools",
}


def _normalize_texts(values: List[str]) -> List[str]:
    return [compact_ws(value) for value in values if compact_ws(value)]


def _join_lower(values: List[str]) -> str:
    return " ".join(value.lower() for value in _normalize_texts(values))


def _contains_any(texts: List[str], terms: set) -> bool:
    joined = _join_lower(texts)
    return any(term in joined for term in terms)


def _has_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


_PROMPT_SCAFFOLD_PATTERNS = [
    re.compile(r"<environment_details>[\s\S]*$", re.IGNORECASE),
    re.compile(r"# task_progress RECOMMENDED[\s\S]*?(?=<environment_details>|$)", re.IGNORECASE),
    re.compile(r"\[TASK RESUMPTION\][\s\S]*?(?=<environment_details>|$)", re.IGNORECASE),
]


def _clean_prompt_text(text: str) -> str:
    cleaned = text or ""
    for pattern in _PROMPT_SCAFFOLD_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    cleaned = re.sub(r"</?(task|feedback)>", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _first_user_text(session: SessionRecord) -> str:
    user_texts = _normalize_texts([_clean_prompt_text(turn.user_text) for turn in session.turns])
    non_command = [text for text in user_texts if not text.startswith("/")]
    if non_command:
        return non_command[0]
    if user_texts:
        return user_texts[0]
    return ""


def _last_assistant_text(session: SessionRecord) -> str:
    assistant_texts = _normalize_texts([turn.assistant_text for turn in session.turns])
    return assistant_texts[-1] if assistant_texts else ""


def _tool_phase(call: ToolCallRecord) -> str:
    if call.tool_kind == "skill":
        return "skill"
    if call.tool_kind == "mcp":
        return "mcp"
    if call.tool_kind == "subagent":
        return "subagent"
    lowered = compact_ws(call.tool_name).lower()
    if lowered in SEARCH_TOOLS:
        return "search"
    if lowered in INSPECT_TOOLS:
        return "inspect"
    if lowered in EDIT_TOOLS:
        return "edit"
    if lowered in EXECUTE_TOOLS:
        return "execute"
    if lowered in ASK_USER_TOOLS:
        return "ask_user"
    if lowered in COMPLETE_TOOLS:
        return "complete"
    return "other"


def _ordered_unique(values: List[str]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _topic_signature(text: str, category: str) -> str:
    cleaned = compact_ws(text).lower()
    if not cleaned:
        return CATEGORY_LABELS.get(category, category.replace("_", " ").title())
    cleaned = re.sub(r"`[^`]+`", " ", cleaned)
    cleaned = re.sub(r"\b[0-9a-f]{7,}\b", " ", cleaned)
    cleaned = re.sub(r"[^\w\u4e00-\u9fff\s]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if _has_cjk(cleaned):
        tokens = [token for token in cleaned.split() if token not in CN_STOPWORDS]
    else:
        tokens = [token for token in cleaned.split() if token not in EN_STOPWORDS and len(token) > 1]
    if tokens:
        return shorten(" ".join(tokens[:6]), 64)
    if _has_cjk(cleaned):
        return shorten(cleaned.replace(" ", ""), 64)
    return shorten(cleaned, 64)


def _quality_label(score: int) -> str:
    if score >= 78:
        return "strong"
    if score >= 55:
        return "mixed"
    return "needs_review"


def _pattern_from_phases(session: SessionRecord, phase_sequence: List[str]) -> str:
    phase_set = set(phase_sequence)
    if session.has_subagent:
        return "delegated"
    if session.has_skill and len(session.tool_calls) <= 2:
        return "skill_workflow"
    if {"inspect", "edit", "execute"}.issubset(phase_set):
        return "inspect_edit_verify"
    if {"inspect", "edit"}.issubset(phase_set):
        return "inspect_edit"
    if {"inspect", "execute"}.issubset(phase_set):
        return "inspect_execute"
    if phase_set == {"inspect"} or phase_set == {"search"} or phase_set == {"search", "inspect"}:
        return "inspect_only"
    if phase_set == {"edit"}:
        return "edit_only"
    if session.has_mcp:
        return "mcp_augmented"
    if not session.tool_calls:
        return "chat_only"
    return "mixed_tools"


def build_session_insights(session: SessionRecord) -> Dict[str, Any]:
    user_texts = _normalize_texts([_clean_prompt_text(turn.user_text) for turn in session.turns])
    assistant_texts = _normalize_texts([turn.assistant_text for turn in session.turns])
    first_user_text = _first_user_text(session)
    last_assistant_text = _last_assistant_text(session)
    topic_text = first_user_text or _clean_prompt_text(session.summary) or last_assistant_text
    phase_sequence = _ordered_unique(
        [
            phase
            for phase in (_tool_phase(call) for call in session.tool_calls)
            if phase not in {"other", "complete"}
        ]
    )
    phase_counts = Counter(_tool_phase(call) for call in session.tool_calls)
    tool_failures = sum(call.success is False for call in session.tool_calls)
    tool_successes = sum(call.success is True for call in session.tool_calls)
    followup_turns = max(0, session.user_turn_count - 1)
    verification_present = phase_counts["execute"] > 0 or _contains_any(assistant_texts, VERIFY_TERMS)
    completion_present = phase_counts["complete"] > 0 or _contains_any([last_assistant_text], RESOLVED_TERMS)
    waiting_for_input = bool(last_assistant_text) and _contains_any([last_assistant_text], BLOCKED_TERMS)
    prompt_has_constraints = _contains_any(user_texts, CONSTRAINT_TERMS)
    prompt_asks_explanation = _contains_any(user_texts, EXPLANATION_TERMS)
    prompt_has_action = _contains_any(user_texts, PROMPT_VERBS)

    resolved_score = 0
    if compact_ws(session.status).lower() in {"completed", "done", "success"}:
        resolved_score += 2
    if completion_present:
        resolved_score += 1
    if verification_present:
        resolved_score += 1
    if tool_failures:
        resolved_score -= 1
    if waiting_for_input:
        resolved_score -= 2

    if resolved_score >= 2:
        outcome_status = "resolved"
    elif waiting_for_input:
        outcome_status = "blocked"
    elif compact_ws(session.status).lower() in {"running", "in_progress", "started"}:
        outcome_status = "in_progress"
    else:
        outcome_status = "unclear"

    prompt_score = 35
    if first_user_text:
        prompt_score += 18
    if len(first_user_text) >= 24:
        prompt_score += 8
    if len(first_user_text) >= 80:
        prompt_score += 6
    if prompt_has_constraints:
        prompt_score += 10
    if prompt_asks_explanation:
        prompt_score += 6
    if prompt_has_action:
        prompt_score += 8
    if followup_turns:
        prompt_score += min(12, followup_turns * 3)
    if followup_turns >= 6:
        prompt_score -= 6
    prompt_score = max(0, min(100, prompt_score))

    quality_score = 42
    quality_score += round(prompt_score * 0.25)
    if 2 <= session.tool_diversity <= 5:
        quality_score += 8
    elif session.tool_diversity > 5:
        quality_score += 6
    if len(phase_sequence) >= 2:
        quality_score += 6
    if verification_present:
        quality_score += 12
    if outcome_status == "resolved":
        quality_score += 12
    elif outcome_status == "blocked":
        quality_score -= 18
    if tool_failures:
        quality_score -= min(20, tool_failures * 7)
    if not session.tool_calls and session.category in {"debug", "new_feature", "ops", "test"}:
        quality_score -= 8
    if session.assistant_turn_count == 0:
        quality_score -= 20
    quality_score = max(0, min(100, quality_score))
    quality_label = _quality_label(quality_score)

    strengths = []
    gaps = []
    prompt_tags = []

    if first_user_text:
        strengths.append("goal is explicit")
        prompt_tags.append("clear goal")
    else:
        gaps.append("task intent is mostly implicit")
    if len(first_user_text) >= 48:
        strengths.append("prompt carries context")
        prompt_tags.append("context rich")
    if prompt_has_constraints:
        strengths.append("constraints are stated")
        prompt_tags.append("has constraints")
    if prompt_asks_explanation:
        prompt_tags.append("asks explanation")
    if followup_turns:
        strengths.append(f"followed up {followup_turns} time(s)")
        prompt_tags.append("iterative follow-up")
    if len(phase_sequence) >= 2:
        strengths.append(f"used tool path: {' -> '.join(phase_sequence)}")
        prompt_tags.append("tool-backed workflow")
    if verification_present:
        strengths.append("has verification step")
        prompt_tags.append("verified")
    else:
        if session.category in {"debug", "new_feature", "ops", "test"}:
            gaps.append("no obvious verification step")
    if outcome_status == "resolved":
        strengths.append("outcome looks resolved")
    elif outcome_status == "blocked":
        gaps.append("assistant is waiting for missing input")
    if tool_failures:
        gaps.append(f"{tool_failures} tool call(s) failed")
    if session.has_skill:
        prompt_tags.append("skill workflow")
    if session.has_mcp:
        prompt_tags.append("mcp assisted")
    if session.has_subagent:
        prompt_tags.append("delegated")

    work_pattern = _pattern_from_phases(session, phase_sequence)
    improvement_flags = []
    if not verification_present and session.category in {"debug", "new_feature", "ops", "test"}:
        improvement_flags.append("missing_verification")
    if tool_failures:
        improvement_flags.append("tool_failures")
    if outcome_status == "blocked":
        improvement_flags.append("missing_context")
    if followup_turns >= 5:
        improvement_flags.append("long_followup_chain")
    if not first_user_text:
        improvement_flags.append("implicit_goal")

    case_title = shorten(
        f"{CATEGORY_LABELS.get(session.category, session.category.replace('_', ' ').title())}: {topic_text or session.summary or session.session_id}",
        120,
    )

    return {
        "analysis_version": ANALYSIS_VERSION,
        "case_title": case_title,
        "topic_signature": _topic_signature(topic_text, session.category),
        "work_pattern": work_pattern,
        "work_pattern_label": PATTERN_LABELS.get(work_pattern, work_pattern.replace("_", " ").title()),
        "tool_path": phase_sequence,
        "tool_path_text": " -> ".join(phase_sequence) if phase_sequence else "chat",
        "prompt_score": prompt_score,
        "quality_score": quality_score,
        "quality_label": quality_label,
        "quality_label_text": quality_label.replace("_", " ").title(),
        "outcome_status": outcome_status,
        "followup_turns": followup_turns,
        "verification_present": verification_present,
        "tool_failures": tool_failures,
        "tool_successes": tool_successes,
        "prompt_tags": prompt_tags,
        "strengths": strengths,
        "gaps": gaps,
        "improvement_flags": improvement_flags,
        "first_user_text": shorten(first_user_text, 240),
        "last_assistant_text": shorten(last_assistant_text, 240),
        "has_resolution_signal": outcome_status == "resolved",
        "example_worthy": quality_label == "strong" and outcome_status == "resolved",
        "needs_review": quality_label == "needs_review" or outcome_status == "blocked",
    }


def enrich_session(session: SessionRecord) -> SessionRecord:
    insights = build_session_insights(session)
    session.summary = insights["case_title"]
    session.extra_json = {**session.extra_json, "analysis_version": ANALYSIS_VERSION, "insights": insights}
    return session
