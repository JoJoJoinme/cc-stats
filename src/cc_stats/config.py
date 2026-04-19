from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .utils import ensure_dir, read_json, write_json


@dataclass
class ClientConfig:
    server_url: str | None = None
    ingest_token: str | None = None

    def resolve_server_url(self) -> str | None:
        return os.environ.get("CC_STATS_SERVER_URL") or self.server_url

    def resolve_ingest_token(self) -> str | None:
        return os.environ.get("CC_STATS_INGEST_TOKEN") or self.ingest_token


def config_dir() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
        return ensure_dir(base / "cc-stats")
    return ensure_dir(Path.home() / ".config" / "cc-stats")


def state_dir() -> Path:
    return ensure_dir(config_dir() / "state")


def client_config_path() -> Path:
    return config_dir() / "client.json"


def default_db_path() -> Path:
    return ensure_dir(Path.cwd() / "data") / "cc-stats.db"


def watcher_state_path() -> Path:
    return state_dir() / "watcher-state.json"


def load_client_config() -> ClientConfig:
    payload = read_json(client_config_path(), default={}) or {}
    return ClientConfig(
        server_url=payload.get("server_url"),
        ingest_token=payload.get("ingest_token"),
    )


def save_client_config(config: ClientConfig) -> None:
    write_json(
        client_config_path(),
        {
            "server_url": config.server_url,
            "ingest_token": config.ingest_token,
        },
    )
