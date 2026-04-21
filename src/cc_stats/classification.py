from collections import Counter
from typing import List, Tuple

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
        "报错",
        "错误",
        "异常",
        "崩溃",
        "失败",
        "修复",
        "修一下",
        "排查",
        "不工作",
        "不能用",
        "无法运行",
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
        "功能",
        "实现",
        "新增",
        "增加",
        "添加",
        "支持",
        "接口",
        "页面",
    ],
    "refactor": [
        "refactor",
        "cleanup",
        "restructure",
        "rename",
        "simplify",
        "extract",
        "modularize",
        "重构",
        "整理",
        "抽取",
        "重命名",
        "简化",
        "优化结构",
    ],
    "docs": [
        "docs",
        "documentation",
        "readme",
        "guide",
        "summarize",
        "explain",
        "wiki",
        "文档",
        "说明",
        "总结",
        "解释",
        "手册",
        "教程",
    ],
    "test": [
        "test",
        "pytest",
        "unit test",
        "integration",
        "e2e",
        "assert",
        "coverage",
        "测试",
        "单测",
        "集成测试",
        "断言",
        "覆盖率",
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
        "部署",
        "发布",
        "构建",
        "服务器",
        "生产环境",
        "容器",
        "流水线",
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
        "理解",
        "调研",
        "研究",
        "看看",
        "分析",
        "原理",
        "怎么实现",
    ],
    "review": [
        "review",
        "audit",
        "security review",
        "code review",
        "pr",
        "diff",
        "评审",
        "审查",
        "代码评审",
        "安全审计",
    ],
}

EDIT_TOOLS = {"write", "edit", "write_to_file", "apply_diff", "replace_in_file", "apply_patch"}
INSPECT_TOOLS = {"read_file", "list_files", "search_files", "list_code_definition_names", "open"}
VERIFY_TOOLS = {"bash", "execute_command"}


def classify_session(texts: List[str], tool_names: List[str]) -> Tuple[str, float]:
    joined = " ".join(compact_ws(text).lower() for text in texts if text).strip()
    scores: Counter[str] = Counter()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in joined:
                scores[category] += 1
    lowered_tools = {tool.lower() for tool in tool_names if tool}
    if any(tool in lowered_tools for tool in EDIT_TOOLS):
        scores["new_feature"] += 1
    if any(tool in lowered_tools for tool in INSPECT_TOOLS):
        scores["exploration"] += 1
    if (
        any(tool in lowered_tools for tool in VERIFY_TOOLS)
        and ("test" in joined or "pytest" in joined or "测试" in joined)
        and scores["debug"] == 0
    ):
        scores["test"] += 1
    if any(tool.startswith("mcp__") or tool == "use_mcp_tool" for tool in lowered_tools):
        scores["exploration"] += 1
    if any(tool in lowered_tools for tool in {"new_task", "agent", "task"}):
        scores["new_feature"] += 1
    if not scores:
        return "other", 0.2
    top_category, top_score = scores.most_common(1)[0]
    total = sum(scores.values()) or 1
    return top_category, round(top_score / total, 3)


def summarize_session(texts: List[str], fallback: str = "No summary") -> str:
    cleaned_texts = [compact_ws(text) for text in texts if compact_ws(text)]
    if len(cleaned_texts) >= 2 and cleaned_texts[0].startswith("/"):
        return shorten(f"{cleaned_texts[0]}: {cleaned_texts[1]}", 180)
    for text in cleaned_texts:
        if text.startswith("/") and len(cleaned_texts) > 1:
            continue
        return shorten(text, 180)
    for text in cleaned_texts:
        return shorten(text, 180)
    return fallback
