#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  bash install/client.sh --server-url http://SERVER:8787 [cc-stats client install options]

Examples:
  bash install/client.sh --server-url http://SERVER:8787
  bash install/client.sh --server-url http://SERVER:8787 --ingest-token TOKEN

Environment:
  CC_STATS_HOME   Override install home. Default: ~/.local/share/cc-stats
  PYTHON_BIN      Override bootstrap python executable
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -eq 0 ]]; then
  usage
  exit 1
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

INSTALL_HOME="${CC_STATS_HOME:-${HOME}/.local/share/cc-stats}"
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
"${RUNTIME_PYTHON}" -m pip install --disable-pip-version-check --upgrade "${REPO_DIR}"

cd "${REPO_DIR}"
exec "${RUNTIME_PYTHON}" -m cc_stats.cli client install "$@"
