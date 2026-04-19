# cc-stats

- 中文安装文档: [INSTALL.zh-CN.md](./INSTALL.zh-CN.md)
- English overview: this README

`cc-stats` is a complete MVP for collecting and analyzing team AI usage across:

- `Claude Code` via official hooks and transcript parsing
- `costrict IDE` via its official persisted task storage (`globalStorage/tasks/...`)

The solution is split into two layers:

- Cross-platform client collector for Linux and Windows
- Linux server for ingest, query, and dashboard

Packaging is split the same way:

- client install uses the base package only
- server install uses the `server` extra for `fastapi`, `jinja2`, and `uvicorn`

## Install model

`cc-stats` uses the official Claude Code plugin system for distribution, and Claude hooks under that plugin for runtime events.

- `Claude Code` is packaged as a local marketplace plugin.
- The plugin ships `hooks/hooks.json` plus a small wrapper script.
- The wrapper script calls the locally installed `cc-stats` launcher, so parsing and transport stay in one place.
- `costrict IDE` uses its persisted task files because the open-source IDE extension exposes storage clearly, while its CLI-only plugin system does not map 1:1 to the IDE.
- For installation, the default path is a repo-local one-click script. That keeps Linux and Windows behavior aligned without requiring `uv` on the target machine.

After install:

- `Claude Code` events are handled by the installed Claude plugin
- `costrict IDE` starts automatically after login/reboot
- Linux server can be installed as a `systemd` service

## What it does

- Installs the `cc-stats-telemetry` Claude plugin
- The plugin provides `Stop` and `SessionEnd` hooks through `hooks/hooks.json`
- Parses Claude transcripts from `~/.claude/projects/.../*.jsonl`
- Detects `costrict IDE` storage roots, including `customStoragePath`
- Parses `costrict` task files:
  - `api_conversation_history.json`
  - `ui_messages.json`
  - `history_item.json`
  - `task_metadata.json`
- Normalizes both sources into one session/turn/tool-call schema
- Stores analytics in SQLite
- Exposes ingest and rich query APIs
- Serves an HTML dashboard for team and per-user pattern analysis

## Quick start

### 1. Install

```bash
git clone <this-repo>
cd cc-stats
```

### 2. Install the Linux server

Run once on the server:

```bash
sudo bash install/server.sh --host 0.0.0.0 --port 8787
```

Optional token:

```bash
sudo bash install/server.sh --host 0.0.0.0 --port 8787 --auth-token YOUR_TOKEN
```

This writes and enables `cc-stats-server.service`, so the API and dashboard survive reboot.

### 3. Install a client in one command

Run once on every Linux or Windows client:

```bash
bash install/client.sh --server-url http://YOUR_LINUX_SERVER:8787
```

Optional token:

```bash
bash install/client.sh --server-url http://YOUR_LINUX_SERVER:8787 --ingest-token YOUR_TOKEN
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\install\client.ps1 -ServerUrl http://YOUR_LINUX_SERVER:8787
```

What this does:

- saves the server URL and token
- installs a stable local `cc-stats` launcher
- installs the `cc-stats-telemetry` Claude plugin when `claude` CLI is available
- defaults the Claude plugin to `user` scope and ensures it is enabled
- falls back to standalone Claude hooks only if plugin install is unavailable and `--claude-mode auto` is used
- backfills recent Claude sessions
- scans existing `costrict IDE` tasks
- installs costrict watcher autostart:
  - Linux: `systemd --user`
  - Windows: Startup folder script

Open:

- Dashboard: `http://SERVER:8787/`
- Sessions API: `http://SERVER:8787/api/v1/sessions`
- Overview API: `http://SERVER:8787/api/v1/stats/overview`

## Client commands

```bash
cc-stats client config show
cc-stats client config set --server-url http://server:8787 --ingest-token TOKEN
cc-stats client install --server-url http://server:8787
cc-stats client install --server-url http://server:8787 --claude-mode plugin
cc-stats client install-claude-hooks --scope project --project-dir .
cc-stats client ingest-claude-hook
cc-stats client backfill-claude
cc-stats client scan-costrict
cc-stats client watch-costrict --interval 20
```

## Server commands

```bash
cc-stats server init-db
cc-stats server serve --db-path ./data/cc-stats.db --host 0.0.0.0 --port 8787
cc-stats server install-service --host 0.0.0.0 --port 8787
```

## Architecture

### Client

- `Claude Code`
  - official plugin
  - plugin-local `hooks/hooks.json`
  - parse transcript locally
  - POST normalized session to server
- `costrict IDE`
  - discover `globalStorage` or `costrict.customStoragePath`
  - scan `tasks/<task-id>/...`
  - keep a lightweight JSON state file for changed-task detection
  - POST normalized session to server

### Server

- `FastAPI` app
- `SQLite` storage
- JSON APIs for ingest, filter, search, and aggregation
- HTML dashboard for:
  - team overview
  - user patterns
  - searchable session list
  - session details

## Cross-platform notes

### Claude Code plugin

The repo contains an official local Claude marketplace and plugin:

- marketplace: [.claude-plugin/marketplace.json](./.claude-plugin/marketplace.json)
- plugin manifest: [plugin.json](./plugins/cc-stats-telemetry/.claude-plugin/plugin.json)
- plugin hooks: [hooks.json](./plugins/cc-stats-telemetry/hooks/hooks.json)

Important detail: the default hook file name is `hooks/hooks.json`, not `Hook.json`.

`cc-stats client install` writes a stable local launcher first, then installs the Claude plugin from the local marketplace.

If you do not pass `--scope`, the default is `user`, and the installer explicitly enables the plugin after install.

Manual install is also possible:

```bash
claude plugin marketplace add /path/to/cc-stats
claude plugin install cc-stats-telemetry@cc-stats --scope project
```

Standalone hook install is still available as a fallback:

```bash
cc-stats client install-claude-hooks --scope project --project-dir .
```

### costrict IDE paths

The client automatically checks:

- Linux:
  - `~/.config/Code/User/globalStorage`
  - `~/.config/Code - Insiders/User/globalStorage`
  - `~/.config/Cursor/User/globalStorage`
  - `~/.config/Windsurf/User/globalStorage`
  - `~/.config/VSCodium/User/globalStorage`
- Windows:
  - `%APPDATA%\\Code\\User\\globalStorage`
  - `%APPDATA%\\Code - Insiders\\User\\globalStorage`
  - `%APPDATA%\\Cursor\\User\\globalStorage`
  - `%APPDATA%\\Windsurf\\User\\globalStorage`
  - `%APPDATA%\\VSCodium\\User\\globalStorage`

It also reads editor settings for:

- `costrict.customStoragePath`
- `roo-cline.customStoragePath`

### costrict autostart

The one-command installer configures:

- Linux: `~/.config/systemd/user/cc-stats-costrict-client.service`
- Windows: `%APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\cc-stats-costrict-client.cmd`

## API highlights

- `POST /api/v1/ingest/session`
- `POST /api/v1/ingest/batch`
- `GET /api/v1/health`
- `GET /api/v1/stats/overview`
- `GET /api/v1/stats/users`
- `GET /api/v1/stats/tools`
- `GET /api/v1/stats/repos`
- `GET /api/v1/stats/categories`
- `GET /api/v1/sessions`
- `GET /api/v1/sessions/{session_id}`
- `GET /api/v1/users/{user_id}/patterns`
- `GET /api/v1/search`
- `GET /api/v1/export/sessions.csv`

## Query filters

`/api/v1/sessions` supports:

- `source`
- `user_id`
- `repo`
- `git_branch`
- `model`
- `category`
- `status`
- `has_mcp`
- `has_skill`
- `has_subagent`
- `tool_name`
- `mcp_server`
- `skill_name`
- `date_from`
- `date_to`
- `text_query`
- `sort`
- `limit`
- `offset`

## Official behavior used

### Claude Code

The implementation uses the official Claude Code plugin system plus official hook input fields such as:

- `session_id`
- `transcript_path`
- `cwd`
- `hook_event_name`

The `Stop` event also provides `last_assistant_message`, while `SessionEnd` provides `reason`.

### costrict IDE

The implementation follows the official open-source extension’s persisted task storage model, including:

- `globalStorage/tasks/<task-id>/api_conversation_history.json`
- `globalStorage/tasks/<task-id>/ui_messages.json`
- `globalStorage/tasks/<task-id>/history_item.json`
- optional custom storage base path

## Service examples

See:

- `deploy/systemd/cc-stats-server.service`
- `deploy/systemd/cc-stats-costrict-client.service`
- `deploy/windows/start-costrict-client.ps1`

These are templates. Replace the placeholder paths and server URL before use.

## Recommended install flow

### Linux server

```bash
git clone <this-repo>
cd cc-stats
sudo bash install/server.sh --host 0.0.0.0 --port 8787
```

### Linux client

```bash
git clone <this-repo>
cd cc-stats
bash install/client.sh --server-url http://SERVER:8787
```

### Windows client

```powershell
git clone <this-repo>
cd cc-stats
powershell -ExecutionPolicy Bypass -File .\install\client.ps1 -ServerUrl http://SERVER:8787
```

## Advanced install

If you already manage Python environments with `uv`, the original commands still work:

```bash
uv sync
uv run cc-stats client install --server-url http://SERVER:8787
uv run cc-stats server install-service --host 0.0.0.0 --port 8787
```
