import json
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

from .config import load_client_config


def resolve_server_and_token(server_url: Optional[str] = None, ingest_token: Optional[str] = None) -> Tuple[str, Optional[str]]:
    config = load_client_config()
    resolved_server = server_url or config.resolve_server_url()
    resolved_token = ingest_token or config.resolve_ingest_token()
    if not resolved_server:
        raise RuntimeError("Server URL is not configured. Run `cc-stats client config set --server-url ...` first.")
    return resolved_server.rstrip("/"), resolved_token


def post_json(url: str, payload: Any, token: Optional[str] = None, timeout: int = 20) -> Dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = request.Request(url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to reach {url}: {exc}") from exc


def send_session(payload: Dict[str, Any], server_url: Optional[str] = None, ingest_token: Optional[str] = None) -> Dict[str, Any]:
    resolved_server, token = resolve_server_and_token(server_url, ingest_token)
    return post_json(f"{resolved_server}/api/v1/ingest/session", payload, token=token)
