import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .paths import claude_projects_dir
from .utils import detect_git_branch, ensure_dir, path_slug, utc_now


EXPORT_FORMAT_VERSION = 1


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _dump_jsonl(rows: List[Dict[str, Any]]) -> str:
    return "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"


def _rewrite_transcript_rows(rows: List[Dict[str, Any]], project_dir: Path) -> List[Dict[str, Any]]:
    target_cwd = str(project_dir.resolve())
    target_branch = detect_git_branch(target_cwd)
    rewritten = []
    for row in rows:
        item = dict(row)
        if "cwd" in item:
            item["cwd"] = target_cwd
        if target_branch and "gitBranch" in item:
            item["gitBranch"] = target_branch
        rewritten.append(item)
    return rewritten


def _collect_claude_artifacts(raw_path: Path) -> List[Dict[str, Any]]:
    if not raw_path.exists():
        raise FileNotFoundError("Claude transcript is not available on disk")

    artifacts = [{"archive_path": "session.jsonl", "source_path": raw_path}]
    subagents_dir = raw_path.parent / "subagents"
    if subagents_dir.exists():
        for path in sorted(subagents_dir.rglob("*.jsonl")):
            artifacts.append(
                {
                    "archive_path": str(path.relative_to(raw_path.parent)).replace("\\", "/"),
                    "source_path": path,
                }
            )
    return artifacts


def build_claude_export_bundle(session: Dict[str, Any]) -> bytes:
    if session.get("source") != "claude-code":
        raise ValueError("Only Claude Code sessions support portable export")

    raw_path_value = session.get("raw_path")
    if not raw_path_value:
        raise FileNotFoundError("This Claude session has no raw transcript path")

    raw_path = Path(str(raw_path_value))
    artifacts = _collect_claude_artifacts(raw_path)
    manifest = {
        "format_version": EXPORT_FORMAT_VERSION,
        "source": "claude-code",
        "session_id": session.get("session_id"),
        "native_session_id": session.get("native_session_id"),
        "summary": session.get("case_title") or session.get("summary") or session.get("session_id"),
        "exported_at": utc_now(),
        "original_cwd": session.get("cwd"),
        "original_raw_path": str(raw_path),
        "files": [{"path": item["archive_path"]} for item in artifacts],
    }

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        archive.writestr(
            "session-summary.json",
            json.dumps(
                {
                    "session_id": session.get("session_id"),
                    "native_session_id": session.get("native_session_id"),
                    "case_title": session.get("case_title"),
                    "source": session.get("source"),
                    "user_id": session.get("user_id"),
                    "cwd": session.get("cwd"),
                    "repo": session.get("repo"),
                    "git_branch": session.get("git_branch"),
                    "turn_count": session.get("turn_count"),
                    "tool_call_count": session.get("tool_call_count"),
                    "quality_score": session.get("quality_score"),
                    "quality_label": session.get("quality_label"),
                    "outcome_status": session.get("outcome_status"),
                },
                ensure_ascii=False,
                indent=2,
            ),
        )
        for artifact in artifacts:
            archive.writestr(
                artifact["archive_path"],
                artifact["source_path"].read_text(encoding="utf-8"),
            )
    return buffer.getvalue()


def import_claude_export_bundle(bundle_path: Path, project_dir: Optional[Path] = None, force: bool = False) -> Dict[str, Any]:
    resolved_bundle = bundle_path.expanduser().resolve()
    target_project_dir = (project_dir or Path.cwd()).expanduser().resolve()

    with zipfile.ZipFile(str(resolved_bundle), mode="r") as archive:
        manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
        if manifest.get("source") != "claude-code":
            raise ValueError("Unsupported bundle source")
        if manifest.get("format_version") != EXPORT_FORMAT_VERSION:
            raise ValueError("Unsupported Claude session bundle format")

        native_session_id = str(manifest.get("native_session_id") or "")
        if not native_session_id:
            raise ValueError("Bundle is missing native_session_id")

        target_root = claude_projects_dir() / path_slug(target_project_dir)
        ensure_dir(target_root)

        written = []
        for item in manifest.get("files") or []:
            archive_path = str(item.get("path") or "")
            if not archive_path:
                continue
            if archive_path == "session.jsonl":
                target_path = target_root / "{0}.jsonl".format(native_session_id)
            else:
                target_path = target_root / Path(archive_path)
            if target_path.exists() and not force:
                raise FileExistsError("Target transcript already exists: {0}".format(target_path))

            payload = archive.read(archive_path).decode("utf-8")
            rows = _load_jsonl_from_text(payload)
            rewritten = _rewrite_transcript_rows(rows, target_project_dir)
            ensure_dir(target_path.parent)
            target_path.write_text(_dump_jsonl(rewritten), encoding="utf-8")
            written.append(str(target_path))

    main_path = str((claude_projects_dir() / path_slug(target_project_dir) / "{0}.jsonl".format(native_session_id)).resolve())
    return {
        "ok": True,
        "session_id": "claude-code:{0}".format(native_session_id),
        "native_session_id": native_session_id,
        "project_dir": str(target_project_dir),
        "main_transcript_path": main_path,
        "written_files": written,
    }


def _load_jsonl_from_text(text: str) -> List[Dict[str, Any]]:
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows
