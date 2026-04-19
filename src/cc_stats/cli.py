from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .collectors import backfill_claude, build_session_from_claude_hook, scan_costrict_once
from .config import ClientConfig, load_client_config, save_client_config
from .paths import claude_settings_path, project_claude_settings_path
from .transport import post_json, resolve_server_and_token, send_session
from .utils import ensure_dir, load_stdin_json, read_json, write_json


def _print(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _python_executable() -> str:
    return sys.executable


def _claude_executable() -> str | None:
    return shutil.which("claude")


def _project_hook_dir(project_dir: Path) -> Path:
    return project_dir / ".claude" / "hooks"


def _write_project_hook_wrappers(project_dir: Path) -> dict:
    hook_dir = _project_hook_dir(project_dir)
    ensure_dir(hook_dir)

    shell_script = hook_dir / "cc-stats-ingest.sh"
    shell_body = """#!/usr/bin/env bash
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

if [ -x "$project_dir/.venv/bin/python" ]; then
  exec "$project_dir/.venv/bin/python" -m cc_stats.cli client ingest-claude-hook
fi

if command -v uv >/dev/null 2>&1; then
  exec uv run --project "$project_dir" cc-stats client ingest-claude-hook
fi

echo "cc-stats hook runtime not found: expected $project_dir/.venv/bin/python or uv in PATH" >&2
exit 1
"""
    shell_script.write_text(shell_body, encoding="utf-8")
    shell_script.chmod(0o755)

    powershell_script = hook_dir / "cc-stats-ingest.ps1"
    powershell_body = """$projectDir = if ($env:CLAUDE_PROJECT_DIR) {
    $env:CLAUDE_PROJECT_DIR
} else {
    Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$python = Join-Path $projectDir ".venv\\Scripts\\python.exe"
if (Test-Path $python) {
    & $python -m cc_stats.cli client ingest-claude-hook
    exit $LASTEXITCODE
}

$uv = Get-Command uv -ErrorAction SilentlyContinue
if ($uv) {
    & $uv.Source run --project $projectDir cc-stats client ingest-claude-hook
    exit $LASTEXITCODE
}

Write-Error "cc-stats hook runtime not found: expected $projectDir\\.venv\\Scripts\\python.exe or uv in PATH"
exit 1
"""
    powershell_script.write_text(powershell_body, encoding="utf-8")

    return {"shell": str(shell_script), "powershell": str(powershell_script)}


def _default_hook_command(project_dir: Path | None = None, scope: str = "user") -> str:
    root = (project_dir or Path.cwd()).resolve()
    if scope == "project":
        if os.name == "nt":
            return '& "$env:CLAUDE_PROJECT_DIR\\.claude\\hooks\\cc-stats-ingest.ps1"'
        return '"$CLAUDE_PROJECT_DIR/.claude/hooks/cc-stats-ingest.sh"'
    return f'"{_python_executable()}" -m cc_stats.cli client ingest-claude-hook'


def _install_user_launcher(project_root: Path) -> dict:
    if os.name == "nt":
        base_dir = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming") / "cc-stats" / "bin"
        ensure_dir(base_dir)
        launcher_path = base_dir / "cc-stats.cmd"
        launcher_body = (
            "@echo off\r\n"
            f'"{_python_executable()}" -m cc_stats.cli %*\r\n'
        )
        launcher_path.write_text(launcher_body, encoding="utf-8")
    else:
        base_dir = Path.home() / ".local" / "share" / "cc-stats" / "bin"
        ensure_dir(base_dir)
        launcher_path = base_dir / "cc-stats"
        launcher_body = (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            f'exec "{_python_executable()}" -m cc_stats.cli "$@"\n'
        )
        launcher_path.write_text(launcher_body, encoding="utf-8")
        launcher_path.chmod(0o755)
    return {"path": str(launcher_path), "project_root": str(project_root)}


def _run_command(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False, capture_output=True, text=True)


def _connect_db(db_path: str | None):
    from .server.db import connect_db

    return connect_db(db_path)


def _create_server_app(db_path: str | None, auth_token: str | None):
    try:
        from .server import create_app
    except ModuleNotFoundError as exc:
        raise RuntimeError("server dependencies are not installed; reinstall with '.[server]'") from exc

    try:
        return create_app(db_path=db_path, auth_token=auth_token)
    except ModuleNotFoundError as exc:
        raise RuntimeError("server dependencies are not installed; reinstall with '.[server]'") from exc


def _run_server(app, host: str, port: int) -> None:
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise RuntimeError("server dependencies are not installed; reinstall with '.[server]'") from exc

    uvicorn.run(app, host=host, port=port)


def _install_claude_plugin(project_root: Path, scope: str) -> dict:
    claude = _claude_executable()
    if not claude:
        raise RuntimeError("claude CLI not found in PATH")

    launcher = _install_user_launcher(project_root)
    validation = _run_command([claude, "plugin", "validate", str(project_root)], cwd=project_root)
    if validation.returncode != 0:
        raise RuntimeError(f"claude plugin validate failed: {validation.stderr.strip() or validation.stdout.strip()}")

    marketplace = _run_command([claude, "plugin", "marketplace", "add", str(project_root)], cwd=project_root)
    if marketplace.returncode != 0:
        text = (marketplace.stderr or marketplace.stdout).strip()
        if "already" not in text.lower():
            raise RuntimeError(f"claude plugin marketplace add failed: {text}")

    plugin_id = "cc-stats-telemetry@cc-stats"
    install = _run_command([claude, "plugin", "install", plugin_id, "--scope", scope], cwd=project_root)
    if install.returncode != 0:
        text = (install.stderr or install.stdout).strip()
        if "already" not in text.lower():
            raise RuntimeError(f"claude plugin install failed: {text}")
    enable = _run_command([claude, "plugin", "enable", plugin_id, "--scope", scope], cwd=project_root)
    if enable.returncode != 0:
        enable_text = (enable.stderr or enable.stdout).strip()
        if "already enabled" not in enable_text.lower():
            raise RuntimeError(f"claude plugin enable failed: {enable_text}")

    return {
        "plugin": plugin_id,
        "scope": scope,
        "marketplace": "cc-stats",
        "launcher": launcher,
        "validate_stdout": validation.stdout.strip(),
        "enabled": True,
    }


def _hook_config(command: str, powershell: bool = False) -> dict:
    hook = {
        "type": "command",
        "command": command,
        "async": True,
        "timeout": 30,
    }
    if powershell:
        hook["shell"] = "powershell"
    return {
        "hooks": {
            "Stop": [{"matcher": "", "hooks": [hook]}],
            "SessionEnd": [{"matcher": "", "hooks": [hook]}],
        }
    }


def _merge_hooks(settings: dict, command: str, powershell: bool = False) -> dict:
    settings = settings or {}
    hooks = settings.setdefault("hooks", {})
    for event in ("Stop", "SessionEnd"):
        entries = hooks.setdefault(event, [])
        already_present = False
        for entry in entries:
            for hook in entry.get("hooks", []):
                if hook.get("type") == "command" and hook.get("command") == command:
                    if powershell:
                        hook["shell"] = "powershell"
                    hook["async"] = True
                    hook["timeout"] = 30
                    already_present = True
        if not already_present:
            entry = {"matcher": "", "hooks": [_hook_config(command, powershell)["hooks"][event][0]["hooks"][0]]}
            entries.append(entry)
    return settings


def _install_claude_hooks(scope: str, project_dir: Path, command: str | None = None) -> dict:
    target = claude_settings_path() if scope == "user" else project_claude_settings_path(project_dir)
    ensure_dir(target.parent)
    settings = read_json(target, default={}) or {}
    wrappers = None
    if scope == "project":
        wrappers = _write_project_hook_wrappers(project_dir)
    resolved_command = command or _default_hook_command(project_dir, scope=scope)
    merged = _merge_hooks(settings, resolved_command, powershell=(os.name == "nt"))
    write_json(target, merged)
    result = {"settings_path": str(target), "command": resolved_command}
    if wrappers:
        result["wrappers"] = wrappers
    return result


def cmd_client_config_show(_: argparse.Namespace) -> int:
    config = load_client_config()
    _print(
        {
            "server_url": config.server_url,
            "ingest_token": config.ingest_token,
        }
    )
    return 0


def cmd_client_config_set(args: argparse.Namespace) -> int:
    config = load_client_config()
    if args.server_url is not None:
        config.server_url = args.server_url
    if args.ingest_token is not None:
        config.ingest_token = args.ingest_token
    save_client_config(config)
    _print({"ok": True, "server_url": config.server_url, "has_ingest_token": bool(config.ingest_token)})
    return 0


def _install_linux_autostart(project_root: Path, interval: int) -> dict:
    service_dir = Path.home() / ".config" / "systemd" / "user"
    ensure_dir(service_dir)
    service_path = service_dir / "cc-stats-costrict-client.service"
    python_executable = _python_executable()
    service_body = f"""[Unit]
Description=cc-stats costrict watcher
After=network-online.target

[Service]
WorkingDirectory={project_root}
ExecStart={python_executable} -m cc_stats.cli client watch-costrict --interval {interval}
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
"""
    service_path.write_text(service_body, encoding="utf-8")
    enabled = False
    message = "service file written"
    try:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, capture_output=True, text=True)
        subprocess.run(["systemctl", "--user", "enable", "--now", "cc-stats-costrict-client.service"], check=True, capture_output=True, text=True)
        enabled = True
        message = "enabled with systemctl --user"
    except Exception as exc:
        message = f"service file written, auto-enable skipped: {exc}"
    return {"path": str(service_path), "enabled": enabled, "message": message}


def _install_windows_autostart(project_root: Path, interval: int) -> dict:
    appdata = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    startup_dir = appdata / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    ensure_dir(startup_dir)
    script_path = startup_dir / "cc-stats-costrict-client.cmd"
    python_executable = _python_executable()
    script_body = (
        "@echo off\r\n"
        f'cd /d "{project_root}"\r\n'
        f'start "" /min "{python_executable}" -m cc_stats.cli client watch-costrict --interval {interval}\r\n'
    )
    script_path.write_text(script_body, encoding="utf-8")
    return {"path": str(script_path), "enabled": True, "message": "startup script written"}


def _install_costrict_autostart(project_root: Path, interval: int) -> dict:
    if os.name == "nt":
        return _install_windows_autostart(project_root, interval)
    return _install_linux_autostart(project_root, interval)


def cmd_client_install_claude_hooks(args: argparse.Namespace) -> int:
    result = _install_claude_hooks(args.scope, Path(args.project_dir).resolve(), args.command)
    _print({"ok": True} | result)
    return 0


def cmd_client_install(args: argparse.Namespace) -> int:
    project_root = Path(args.project_dir).resolve()

    config = load_client_config()
    config.server_url = args.server_url
    if args.ingest_token is not None:
        config.ingest_token = args.ingest_token
    save_client_config(config)

    install_result = {
        "config": {"server_url": config.server_url, "has_ingest_token": bool(config.ingest_token)},
        "claude": None,
        "autostart": None,
        "backfill": None,
        "costrict_scan": None,
    }

    if args.claude_mode == "plugin" or (args.claude_mode == "auto" and _claude_executable()):
        try:
            install_result["claude"] = {"mode": "plugin"} | _install_claude_plugin(project_root, args.scope)
        except Exception as exc:
            if args.claude_mode == "plugin":
                raise
            install_result["claude"] = {"mode": "plugin", "ok": False, "error": str(exc), "fallback": "hooks"}
            install_result["claude"] = install_result["claude"] | {
                "hook_settings": _install_claude_hooks(args.scope, project_root)
            }
    elif args.claude_mode == "hooks":
        install_result["claude"] = {"mode": "hooks"} | _install_claude_hooks(args.scope, project_root)

    if not args.no_autostart:
        install_result["autostart"] = _install_costrict_autostart(project_root, args.interval)

    if not args.skip_backfill:
        try:
            sessions = backfill_claude(limit=args.backfill_limit)
            result = _send_sessions(sessions, args.server_url, args.ingest_token)
            install_result["backfill"] = result | {"local_sessions": len(sessions)}
        except Exception as exc:
            install_result["backfill"] = {"ok": False, "error": str(exc)}

    if not args.skip_costrict_scan:
        try:
            sessions = scan_costrict_once(changed_only=False)
            result = _send_sessions(sessions, args.server_url, args.ingest_token)
            install_result["costrict_scan"] = result | {"local_sessions": len(sessions)}
        except Exception as exc:
            install_result["costrict_scan"] = {"ok": False, "error": str(exc)}

    _print({"ok": True, "install": install_result})
    return 0


def cmd_client_ingest_claude_hook(args: argparse.Namespace) -> int:
    payload = load_stdin_json()
    session = build_session_from_claude_hook(payload)
    result = send_session(session.to_row() | {"turns": [turn.to_row() for turn in session.turns], "tool_calls": [call.to_row() for call in session.tool_calls]}, args.server_url, args.ingest_token)
    _print(result)
    return 0


def _send_sessions(sessions, server_url: str | None, ingest_token: str | None) -> dict:
    resolved_server, token = resolve_server_and_token(server_url, ingest_token)
    payload = []
    for session in sessions:
        payload.append(
            session.to_row()
            | {
                "turns": [turn.to_row() for turn in session.turns],
                "tool_calls": [call.to_row() for call in session.tool_calls],
            }
        )
    if not payload:
        return {"ok": True, "count": 0}
    return post_json(f"{resolved_server}/api/v1/ingest/batch", payload, token=token)


def cmd_client_backfill_claude(args: argparse.Namespace) -> int:
    sessions = backfill_claude(limit=args.limit)
    result = _send_sessions(sessions, args.server_url, args.ingest_token)
    _print(result | {"local_sessions": len(sessions)})
    return 0


def cmd_client_scan_costrict(args: argparse.Namespace) -> int:
    sessions = scan_costrict_once(changed_only=args.changed_only)
    result = _send_sessions(sessions, args.server_url, args.ingest_token)
    _print(result | {"local_sessions": len(sessions)})
    return 0


def cmd_client_watch_costrict(args: argparse.Namespace) -> int:
    interval = max(5, int(args.interval))
    while True:
        sessions = scan_costrict_once(changed_only=True)
        if sessions:
            result = _send_sessions(sessions, args.server_url, args.ingest_token)
            _print(result | {"local_sessions": len(sessions)})
        time.sleep(interval)


def cmd_server_init_db(args: argparse.Namespace) -> int:
    conn = _connect_db(args.db_path)
    conn.close()
    resolved = str(Path(args.db_path).resolve()) if args.db_path else str(Path.cwd() / "data" / "cc-stats.db")
    _print({"ok": True, "db_path": resolved})
    return 0


def cmd_server_serve(args: argparse.Namespace) -> int:
    app = _create_server_app(db_path=args.db_path, auth_token=args.auth_token)
    _run_server(app, host=args.host, port=args.port)
    return 0


def cmd_server_install_service(args: argparse.Namespace) -> int:
    if os.name == "nt":
        raise RuntimeError("server install-service only supports Linux")

    project_root = Path(args.project_dir).resolve()
    db_path = Path(args.db_path).resolve() if args.db_path else project_root / "data" / "cc-stats.db"
    ensure_dir(db_path.parent)
    python_executable = _python_executable()
    service_dir = Path("/etc/systemd/system") if args.scope == "system" else Path.home() / ".config" / "systemd" / "user"
    ensure_dir(service_dir)
    service_name = "cc-stats-server.service"
    service_path = service_dir / service_name
    auth_part = f" --auth-token {args.auth_token}" if args.auth_token else ""
    install_target = "multi-user.target" if args.scope == "system" else "default.target"
    service_body = f"""[Unit]
Description=cc-stats server
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory={project_root}
ExecStart={python_executable} -m cc_stats.cli server serve --db-path {db_path} --host {args.host} --port {args.port}{auth_part}
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy={install_target}
"""
    service_path.write_text(service_body, encoding="utf-8")

    command = ["systemctl"]
    if args.scope == "user":
        command.append("--user")
    enabled = False
    message = "service file written"
    try:
        subprocess.run(command + ["daemon-reload"], check=True, capture_output=True, text=True)
        subprocess.run(command + ["enable", "--now", service_name], check=True, capture_output=True, text=True)
        enabled = True
        message = f"enabled with {' '.join(command)}"
    except Exception as exc:
        message = f"service file written, auto-enable skipped: {exc}"

    _print(
        {
            "ok": True,
            "service_path": str(service_path),
            "service_name": service_name,
            "enabled": enabled,
            "message": message,
            "db_path": str(db_path),
            "scope": args.scope,
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cc-stats")
    root_subparsers = parser.add_subparsers(dest="area", required=True)

    client = root_subparsers.add_parser("client")
    client_subparsers = client.add_subparsers(dest="client_command", required=True)

    config_parser = client_subparsers.add_parser("config")
    config_subparsers = config_parser.add_subparsers(dest="config_command", required=True)
    config_show = config_subparsers.add_parser("show")
    config_show.set_defaults(func=cmd_client_config_show)
    config_set = config_subparsers.add_parser("set")
    config_set.add_argument("--server-url")
    config_set.add_argument("--ingest-token")
    config_set.set_defaults(func=cmd_client_config_set)

    install = client_subparsers.add_parser("install")
    install.add_argument("--server-url", required=True)
    install.add_argument("--ingest-token")
    install.add_argument("--scope", choices=["project", "user"], default="user")
    install.add_argument("--project-dir", default=".")
    install.add_argument("--interval", type=int, default=20)
    install.add_argument("--claude-mode", choices=["auto", "plugin", "hooks", "skip"], default="auto")
    install.add_argument("--no-autostart", action="store_true")
    install.add_argument("--skip-backfill", action="store_true")
    install.add_argument("--skip-costrict-scan", action="store_true")
    install.add_argument("--backfill-limit", type=int)
    install.set_defaults(func=cmd_client_install)

    install_hooks = client_subparsers.add_parser("install-claude-hooks")
    install_hooks.add_argument("--scope", choices=["project", "user"], default="project")
    install_hooks.add_argument("--project-dir", default=".")
    install_hooks.add_argument("--command")
    install_hooks.set_defaults(func=cmd_client_install_claude_hooks)

    ingest_hook = client_subparsers.add_parser("ingest-claude-hook")
    ingest_hook.add_argument("--server-url")
    ingest_hook.add_argument("--ingest-token")
    ingest_hook.set_defaults(func=cmd_client_ingest_claude_hook)

    backfill = client_subparsers.add_parser("backfill-claude")
    backfill.add_argument("--server-url")
    backfill.add_argument("--ingest-token")
    backfill.add_argument("--limit", type=int)
    backfill.set_defaults(func=cmd_client_backfill_claude)

    scan_costrict = client_subparsers.add_parser("scan-costrict")
    scan_costrict.add_argument("--server-url")
    scan_costrict.add_argument("--ingest-token")
    scan_costrict.add_argument("--changed-only", action="store_true")
    scan_costrict.set_defaults(func=cmd_client_scan_costrict)

    watch_costrict = client_subparsers.add_parser("watch-costrict")
    watch_costrict.add_argument("--server-url")
    watch_costrict.add_argument("--ingest-token")
    watch_costrict.add_argument("--interval", type=int, default=20)
    watch_costrict.set_defaults(func=cmd_client_watch_costrict)

    server = root_subparsers.add_parser("server")
    server_subparsers = server.add_subparsers(dest="server_command", required=True)

    init_db = server_subparsers.add_parser("init-db")
    init_db.add_argument("--db-path")
    init_db.set_defaults(func=cmd_server_init_db)

    serve = server_subparsers.add_parser("serve")
    serve.add_argument("--db-path")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--auth-token")
    serve.set_defaults(func=cmd_server_serve)

    install_service = server_subparsers.add_parser("install-service")
    install_service.add_argument("--project-dir", default=".")
    install_service.add_argument("--db-path")
    install_service.add_argument("--host", default="0.0.0.0")
    install_service.add_argument("--port", type=int, default=8787)
    install_service.add_argument("--auth-token")
    install_service.add_argument("--scope", choices=["system", "user"], default="system")
    install_service.set_defaults(func=cmd_server_install_service)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
