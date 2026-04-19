from __future__ import annotations

import os
from pathlib import Path

from .utils import path_slug, read_jsonc, unique_paths


CLAUDE_DIR = Path.home() / ".claude"


def claude_settings_path() -> Path:
    return CLAUDE_DIR / "settings.json"


def claude_projects_dir() -> Path:
    return CLAUDE_DIR / "projects"


def project_claude_settings_path(project_dir: Path | None = None) -> Path:
    root = project_dir or Path.cwd()
    return root / ".claude" / "settings.json"


def find_claude_transcript(session_id: str, cwd: str | None = None) -> Path | None:
    if cwd:
        slug = path_slug(cwd)
        candidate = claude_projects_dir() / slug / f"{session_id}.jsonl"
        if candidate.exists():
            return candidate
    projects_dir = claude_projects_dir()
    if not projects_dir.exists():
        return None
    for candidate in projects_dir.glob(f"**/{session_id}.jsonl"):
        if "subagents" in candidate.parts:
            continue
        return candidate
    return None


def candidate_editor_roots() -> list[tuple[Path, Path]]:
    if os.name == "nt":
        appdata = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
        roots = [
            ("Code", "Code"),
            ("Code - Insiders", "Code - Insiders"),
            ("Cursor", "Cursor"),
            ("Windsurf", "Windsurf"),
            ("VSCodium", "VSCodium"),
        ]
        return [(appdata / editor / "User" / "globalStorage", appdata / editor / "User" / "settings.json") for editor, _ in roots]
    home = Path.home()
    roots = [
        home / ".config" / "Code",
        home / ".config" / "Code - Insiders",
        home / ".config" / "Cursor",
        home / ".config" / "Windsurf",
        home / ".config" / "VSCodium",
    ]
    server_roots = [
        home / ".vscode-server" / "data",
        home / ".cursor-server" / "data",
        home / ".windsurf-server" / "data",
        home / ".antigravity-server" / "data",
    ]
    pairs = [(root / "User" / "globalStorage", root / "User" / "settings.json") for root in roots]
    pairs.extend((root / "User" / "globalStorage", root / "User" / "settings.json") for root in server_roots)
    return unique_paths(pairs)


def candidate_costrict_storage_paths() -> list[Path]:
    results: list[Path] = []
    env_override = os.environ.get("COSTRICT_STORAGE_PATH")
    if env_override:
        results.append(Path(env_override).expanduser())

    for _, settings_path in candidate_editor_roots():
        settings = read_jsonc(settings_path, default={}) or {}
        for key in ("costrict.customStoragePath", "roo-cline.customStoragePath"):
            custom = settings.get(key)
            if isinstance(custom, str) and custom.strip():
                results.append(Path(custom).expanduser())

    extension_dir_names = [
        "zgsm-ai.zgsm",
        "zgsm-ai.costrict",
        "rooveterinaryinc.roo-cline",
        "roo-code.roo-code",
        "saoudrizwan.claude-dev",
    ]

    for global_storage_root, _ in candidate_editor_roots():
        if not global_storage_root.exists():
            continue
        for name in extension_dir_names:
            candidate = global_storage_root / name
            if candidate.exists():
                results.append(candidate)
        for candidate in global_storage_root.iterdir():
            if not candidate.is_dir():
                continue
            name = candidate.name.lower()
            if "costrict" in name or name.startswith("zgsm-ai.") or "roo" in name:
                results.append(candidate)

    expanded: list[Path] = []
    for path in unique_paths(results):
        if path.name == "tasks":
            expanded.append(path.parent)
        else:
            expanded.append(path)
    return unique_paths(expanded)
