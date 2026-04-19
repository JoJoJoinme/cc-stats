from __future__ import annotations

from collections import Counter

from .utils import compact_ws, shorten


CATEGORY_KEYWORDS = {
    "debug": [
        "bug",
        "fix",
        "error",
        "stack trace",
        "exception",
        "crash",
        "broken",
        "not work",
        "failing",
    ],
    "new_feature": [
        "feature",
        "implement",
        "add",
        "build",
        "create",
        "support",
        "endpoint",
        "new page",
    ],
    "refactor": [
        "refactor",
        "cleanup",
        "restructure",
        "rename",
        "simplify",
        "extract",
        "modularize",
    ],
    "docs": [
        "docs",
        "documentation",
        "readme",
        "guide",
        "summarize",
        "explain",
        "wiki",
    ],
    "test": [
        "test",
        "pytest",
        "unit test",
        "integration",
        "e2e",
        "assert",
        "coverage",
    ],
    "ops": [
        "deploy",
        "docker",
        "k8s",
        "kubernetes",
        "ci",
        "cd",
        "build",
        "prod",
        "release",
        "server",
        "nginx",
    ],
    "exploration": [
        "understand",
        "explore",
        "investigate",
        "analyze",
        "inspect",
        "how does",
        "background",
        "look into",
    ],
    "review": [
        "review",
        "audit",
        "security review",
        "code review",
        "pr",
        "diff",
    ],
}


def classify_session(texts: list[str], tool_names: list[str]) -> tuple[str, float]:
    joined = " ".join(compact_ws(text).lower() for text in texts if text).strip()
    scores: Counter[str] = Counter()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in joined:
                scores[category] += 1
    lowered_tools = {tool.lower() for tool in tool_names if tool}
    if any(tool in lowered_tools for tool in {"write", "edit", "write_to_file", "apply_diff"}):
        scores["new_feature"] += 1
    if any(tool in lowered_tools for tool in {"bash", "execute_command"}):
        scores["ops"] += 1
    if any(tool.startswith("mcp__") or tool == "use_mcp_tool" for tool in lowered_tools):
        scores["exploration"] += 1
    if any(tool in lowered_tools for tool in {"new_task", "agent", "task"}):
        scores["new_feature"] += 1
    if not scores:
        return "other", 0.2
    top_category, top_score = scores.most_common(1)[0]
    total = sum(scores.values()) or 1
    return top_category, round(top_score / total, 3)


def summarize_session(texts: list[str], fallback: str = "No summary") -> str:
    for text in texts:
        cleaned = compact_ws(text)
        if cleaned:
            return shorten(cleaned, 180)
    return fallback
