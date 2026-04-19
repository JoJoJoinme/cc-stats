# cc-stats 安装文档

本文档对应当前仓库实现，覆盖：

- Linux 服务端
- Linux 客户端
- Windows 客户端
- `Claude Code + costrict IDE` 同机安装

当前这套安装方式已经按真实机器跑通过：

- 服务端：`systemd` 常驻
- Claude：`user scope` plugin，默认启用
- costrict：Linux 用 `systemd --user`，Windows 用 Startup 自启动

当前打包方式也已经拆开：

- 客户端默认不再安装 `fastapi/jinja2/uvicorn`
- 服务端才安装 `server` 依赖集

## 1. 安装模型

`cc-stats` 分成两层：

- 服务端：Linux 上跑 API、SQLite、Dashboard
- 客户端：部署在开发者机器上，负责采集 `Claude Code` 和 `costrict IDE`

采集方式不是一套逻辑硬吃两边：

- `Claude Code`：官方 plugin + hooks，事件驱动
- `costrict IDE`：扫描本地持久化 task 目录，存储驱动

所以如果一台机器同时装了 `Claude Code` 和 `costrict IDE`，只需要执行一次客户端安装命令。

## 2. 前置条件

### 2.1 服务端

要求：

- Linux
- `systemd`
- `python3` 或 `python`
- 能访问本仓库目录

说明：

- 服务端安装脚本会自己创建 runtime
- 优先用 `python -m venv`
- 如果系统没装 `python3-venv`，会自动 fallback 到 `virtualenv`

### 2.2 客户端

要求：

- Linux 或 Windows
- 本机有 `python3` / `python` / `py -3`
- 如果要采 `Claude Code`，本机还需要有 `claude` CLI
- 如果要采 `costrict IDE`，本机需要已有对应编辑器和 task 落盘目录

说明：

- 不要求预装 `uv`
- 不要求手工建虚拟环境
- 安装脚本会自己创建独立 runtime
- 客户端安装的是基础包，不会把服务端 Web 依赖一起装进去

## 3. 服务端安装

### 3.1 安装命令

在 Linux 服务器执行：

```bash
cd /path/to/cc-stats
sudo bash install/server.sh --host 0.0.0.0 --port 8787
```

如果要加上报鉴权：

```bash
cd /path/to/cc-stats
sudo bash install/server.sh --host 0.0.0.0 --port 8787 --auth-token YOUR_TOKEN
```

默认效果：

- 安装 runtime 到 `/opt/cc-stats/runtime`
- 安装 `cc-stats[server]`
- 创建并启用 `cc-stats-server.service`
- 服务重启后自动恢复

### 3.2 可选参数

`install/server.sh` 最终调用的是：

```bash
cc-stats server install-service --help
```

当前支持：

- `--project-dir`
- `--db-path`
- `--host`
- `--port`
- `--auth-token`
- `--scope {system,user}`

常见例子：

```bash
sudo bash install/server.sh --host 127.0.0.1 --port 8787
sudo bash install/server.sh --db-path /data/cc-stats/cc-stats.db --host 0.0.0.0 --port 8787
```

### 3.3 安装后验证

检查服务状态：

```bash
systemctl status cc-stats-server.service
```

检查接口：

```bash
curl http://127.0.0.1:8787/api/v1/health
curl http://127.0.0.1:8787/api/v1/stats/overview
```

打开页面：

- Dashboard: `http://SERVER:8787/`
- Sessions: `http://SERVER:8787/sessions`

## 4. Linux 客户端安装

### 4.1 一键安装

在开发者机器执行：

```bash
cd /path/to/cc-stats
bash install/client.sh --server-url http://SERVER:8787
```

如果服务端启用了 token：

```bash
cd /path/to/cc-stats
bash install/client.sh --server-url http://SERVER:8787 --ingest-token YOUR_TOKEN
```

### 4.2 这条命令会做什么

默认会一次性完成：

- 创建本地 runtime：`~/.local/share/cc-stats/runtime`
- 安装基础版 `cc-stats`
- 保存 server URL / token
- 安装并启用 Claude plugin
- 默认按 `user scope` 安装 Claude plugin
- 回灌本机已有 Claude 历史 transcript
- 扫描本机已有 costrict task
- 安装 Linux 自启动：
  - `~/.config/systemd/user/cc-stats-costrict-client.service`

对同机同时装了 `Claude Code + costrict IDE` 的用户，不需要拆成两次安装。

### 4.3 常用参数

`install/client.sh` 最终调用的是：

```bash
cc-stats client install --help
```

当前支持：

- `--server-url`
- `--ingest-token`
- `--scope {project,user}`
- `--project-dir`
- `--interval`
- `--claude-mode {auto,plugin,hooks,skip}`
- `--no-autostart`
- `--skip-backfill`
- `--skip-costrict-scan`
- `--backfill-limit`

常见例子：

只装 Claude，不扫 costrict：

```bash
bash install/client.sh --server-url http://SERVER:8787 --skip-costrict-scan
```

跳过 Claude，只装 costrict：

```bash
bash install/client.sh --server-url http://SERVER:8787 --claude-mode skip
```

只装 project scope 的 Claude：

```bash
bash install/client.sh --server-url http://SERVER:8787 --scope project --project-dir /path/to/project
```

### 4.4 安装后验证

检查配置：

```bash
~/.local/share/cc-stats/runtime/bin/python -m cc_stats.cli client config show
```

检查 Claude plugin：

```bash
claude plugin list
```

检查 costrict watcher：

```bash
systemctl --user status cc-stats-costrict-client.service
```

手工触发一次回灌：

```bash
~/.local/share/cc-stats/runtime/bin/python -m cc_stats.cli client backfill-claude --server-url http://SERVER:8787
~/.local/share/cc-stats/runtime/bin/python -m cc_stats.cli client scan-costrict --server-url http://SERVER:8787
```

## 5. Windows 客户端安装

### 5.1 一键安装

在 PowerShell 执行：

```powershell
cd D:\path\to\cc-stats
powershell -ExecutionPolicy Bypass -File .\install\client.ps1 -ServerUrl http://SERVER:8787
```

如果服务端启用了 token：

```powershell
cd D:\path\to\cc-stats
powershell -ExecutionPolicy Bypass -File .\install\client.ps1 -ServerUrl http://SERVER:8787 -IngestToken YOUR_TOKEN
```

### 5.2 默认行为

默认会：

- 创建 runtime：`%APPDATA%\cc-stats\runtime`
- 安装基础版 `cc-stats`
- 保存 server 配置
- 尝试安装并启用 Claude plugin
- 回灌 Claude 历史 transcript
- 扫描本机已有 costrict task
- 写入 Startup 自启动脚本

当前 Windows 自启动文件位置：

```text
%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\cc-stats-costrict-client.cmd
```

### 5.3 验证

检查本地 runtime：

```powershell
& "$env:APPDATA\cc-stats\runtime\Scripts\python.exe" -m cc_stats.cli client config show
```

如果本机装了 Claude CLI，也可以检查：

```powershell
claude plugin list
```

## 6. Claude 和 costrict 分别是怎么接入的

### 6.1 Claude Code

运行时方案：

- 官方 plugin 分发
- plugin 内部 `hooks/hooks.json`
- hook 在 `Stop` / `SessionEnd` 触发本地 `cc-stats`
- 本地解析 `~/.claude/projects/**/*.jsonl`
- 再把归一化 session 上报到服务端

默认安装策略：

- `user scope`
- 安装后显式 `enable`

### 6.2 costrict IDE

运行时方案：

- 不依赖 IDE hook
- 直接读取 IDE 已经持久化的 task 文件
- 做增量扫描后上报

当前支持的 task 文件包括：

- `api_conversation_history.json`
- `ui_messages.json`
- `history_item.json`
- `task_metadata.json`

Linux 上默认通过 `systemd --user` 常驻扫描，Windows 上通过 Startup 脚本自启动。

## 7. 如果一台机器同时装了 Claude 和 costrict

只执行一条客户端安装命令：

Linux：

```bash
bash install/client.sh --server-url http://SERVER:8787
```

Windows：

```powershell
powershell -ExecutionPolicy Bypass -File .\install\client.ps1 -ServerUrl http://SERVER:8787
```

不用拆成两次。

安装器会自动处理：

- Claude plugin
- Claude 历史回灌
- costrict 扫描
- costrict 自启动

## 8. 常见问题

### 8.1 为什么会提示缺少 `python3-venv`

这通常是 Debian / Ubuntu 的打包方式导致的：

- 系统有 `python3`
- 但 `venv` 被拆到了单独的 `python3-venv` 包

当前脚本已经做了 fallback：

- 先尝试 `python -m venv`
- 失败后自动安装并使用 `virtualenv`

所以通常不用手工处理。

### 8.2 重启后还需要重新配置吗

正常不需要。

服务端：

- `cc-stats-server.service` 会自动拉起

Linux 客户端：

- `cc-stats-costrict-client.service` 会在用户登录后自动拉起

Windows 客户端：

- Startup 脚本会在登录后拉起

### 8.3 Claude plugin 没装上怎么办

先确认本机是否有 `claude` CLI：

```bash
claude --version
```

如果没有，安装器在 `auto` 模式下会尽量 fallback 到 standalone hooks；也可以手工指定：

```bash
bash install/client.sh --server-url http://SERVER:8787 --claude-mode hooks
```

### 8.4 怎么看当前数据是否真的上报了

服务端直接查：

```bash
curl http://SERVER:8787/api/v1/stats/overview
curl http://SERVER:8787/api/v1/sessions?limit=20
curl http://SERVER:8787/api/v1/stats/capabilities
```

页面上看：

- `/`
- `/sessions`

## 9. 推荐安装顺序

推荐按这个顺序：

1. 先在 Linux 服务器安装服务端
2. 先在你自己的机器装一个客户端
3. 验证 Dashboard、Sessions、Capability 统计
4. 再推广到组内其他同学

## 10. 文档入口

- 中文安装文档：[INSTALL.zh-CN.md](./INSTALL.zh-CN.md)
- 项目总览：[README.md](./README.md)
