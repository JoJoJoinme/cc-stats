from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(read_text(path))
    except Exception:
        return default


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def iso_to_unix(value: str | None) -> float | None:
    dt = parse_iso8601(value)
    return dt.timestamp() if dt else None


def unix_ms_to_iso(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()


def duration_seconds(start: str | None, end: str | None) -> int | None:
    start_dt = parse_iso8601(start)
    end_dt = parse_iso8601(end)
    if not start_dt or not end_dt:
        return None
    return max(0, int((end_dt - start_dt).total_seconds()))


def compact_ws(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def shorten(value: str | None, limit: int = 180) -> str:
    text = compact_ws(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def strip_json_comments(raw: str) -> str:
    result: list[str] = []
    i = 0
    in_string = False
    in_single = False
    in_multi = False
    escape = False
    while i < len(raw):
        ch = raw[i]
        nxt = raw[i + 1] if i + 1 < len(raw) else ""
        if in_single:
            if ch == "\n":
                in_single = False
                result.append(ch)
            i += 1
            continue
        if in_multi:
            if ch == "*" and nxt == "/":
                in_multi = False
                i += 2
            else:
                i += 1
            continue
        if in_string:
            result.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            result.append(ch)
            i += 1
            continue
        if ch == "/" and nxt == "/":
            in_single = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_multi = True
            i += 2
            continue
        result.append(ch)
        i += 1
    return "".join(result)


def read_jsonc(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        raw = read_text(path)
        return json.loads(strip_json_comments(raw))
    except Exception:
        return default


def path_slug(path: str | Path) -> str:
    value = str(path).replace("\\", "/")
    value = value.replace("/", "-")
    return value or "-"


def unique_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[str] = set()
    ordered: list[Path] = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(path)
    return ordered


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def sum_int(values: Iterable[Any]) -> int:
    return sum(safe_int(value, 0) for value in values)


def sum_float(values: Iterable[Any]) -> float:
    return float(sum(safe_float(value, 0.0) for value in values))


def detect_git_root(cwd: str | None) -> str | None:
    if not cwd:
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip() or None
    except Exception:
        return None
    return None


def detect_git_branch(cwd: str | None) -> str | None:
    if not cwd:
        return None
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            branch = result.stdout.strip()
            return branch or None
    except Exception:
        return None
    return None


def detect_repo_name(cwd: str | None) -> str | None:
    root = detect_git_root(cwd) or cwd
    if not root:
        return None
    return Path(root).name


def detect_user_id(cwd: str | None = None) -> str:
    env_override = os.environ.get("CC_STATS_USER_ID")
    if env_override:
        return env_override
    try:
        result = subprocess.run(
            ["git", "config", "user.email"],
            cwd=cwd or None,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    username = os.environ.get("USERNAME") or os.environ.get("USER")
    if username:
        return username
    return socket.gethostname()


def detect_host_name() -> str:
    return socket.gethostname()


def detect_platform_name() -> str:
    return platform.system().lower()


def compute_file_signature(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted((Path(p) for p in paths), key=lambda item: str(item)):
        digest.update(str(path).encode("utf-8"))
        if not path.exists():
            digest.update(b"!missing")
            continue
        stat = path.stat()
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
    return digest.hexdigest()


def load_stdin_json() -> dict[str, Any]:
    raw = ""
    try:
        raw = os.read(0, 10_000_000).decode("utf-8")
    except Exception:
        raw = ""
    raw = raw.strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}
