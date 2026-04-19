#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  sudo bash install/server.sh [cc-stats server install-service options]

Examples:
  sudo bash install/server.sh --host 0.0.0.0 --port 8787
  sudo bash install/server.sh --host 0.0.0.0 --port 8787 --auth-token TOKEN

Environment:
  CC_STATS_HOME   Override install home. Default: /opt/cc-stats when root, otherwise ~/.local/share/cc-stats
  PYTHON_BIN      Override bootstrap python executable
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

find_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    printf '%s\n' "${PYTHON_BIN}"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi
  return 1
}

PYTHON="$(find_python || true)"
if [[ -z "${PYTHON}" ]]; then
  echo "python3 or python is required" >&2
  exit 1
fi

if [[ -n "${CC_STATS_HOME:-}" ]]; then
  INSTALL_HOME="${CC_STATS_HOME}"
elif [[ "${EUID}" -eq 0 ]]; then
  INSTALL_HOME="/opt/cc-stats"
else
  INSTALL_HOME="${HOME}/.local/share/cc-stats"
fi

RUNTIME_DIR="${INSTALL_HOME}/runtime"
RUNTIME_PYTHON="${RUNTIME_DIR}/bin/python"

mkdir -p "${INSTALL_HOME}"

create_runtime() {
  if "${PYTHON}" -m venv "${RUNTIME_DIR}" >/dev/null 2>&1; then
    return 0
  fi
  echo "python -m venv unavailable, falling back to virtualenv bootstrap" >&2
  "${PYTHON}" -m pip install --disable-pip-version-check --user virtualenv
  "${PYTHON}" -m virtualenv "${RUNTIME_DIR}"
}

create_runtime
"${RUNTIME_PYTHON}" -m pip install --disable-pip-version-check --upgrade pip setuptools wheel
"${RUNTIME_PYTHON}" -m pip install --disable-pip-version-check --upgrade "${REPO_DIR}[server]"

cd "${REPO_DIR}"
exec "${RUNTIME_PYTHON}" -m cc_stats.cli server install-service "$@"
