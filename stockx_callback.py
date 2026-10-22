"""Secure local OAuth utility for the StockX Authorization Code flow."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_TOKEN_FILE = PROJECT_ROOT / ".stockx_oauth_tokens.json"
AUTHORIZE_URL = "https://accounts.stockx.com/authorize"
TOKEN_URL = "https://accounts.stockx.com/oauth/token"
CATALOG_SEARCH_URL = "https://api.stockx.com/v2/catalog/search"
REQUEST_TIMEOUT_SECONDS = 30


def load_local_env() -> None:
    """Load simple KEY=VALUE entries from the Git-ignored local .env file."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.is_file():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip("\"'")
        if name and name not in os.environ:
            os.environ[name] = value


def required_env(name: str) -> str:
    """Get a required credential without including its value in errors."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def token_file_path() -> Path:
    configured_path = os.getenv("STOCKX_TOKEN_FILE", "").strip()
    return Path(configured_path) if configured_path else DEFAULT_TOKEN_FILE


def read_token_file() -> dict[str, Any]:
    """Read locally stored OAuth tokens without printing their contents."""
    path = token_file_path()
    if not path.is_file():
        return {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RuntimeError("Unable to read the local StockX token file") from error
    return data if isinstance(data, dict) else {}


def store_tokens(token_response: dict[str, Any]) -> None:
    """Persist only API tokens in a Git-ignored local file."""
    access_token = token_response.get("access_token")
    refresh_token = token_response.get("refresh_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("StockX token response did not include access_token")

    previous_tokens = read_token_file()
    token_data = {
        "access_token": access_token,
        "refresh_token": (
            refresh_token
            if isinstance(refresh_token, str) and refresh_token
            else previous_tokens.get("refresh_token")
        ),
        "expires_in": token_response.get("expires_in"),
        "received_at": int(time.time()),
    }
    path = token_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(token_data), encoding="utf-8")
    os.chmod(temporary_path, 0o600)
    temporary_path.replace(path)


def authorization_url(state: str) -> str:
    """Build the registered StockX OAuth authorization URL."""
    params = {
        "response_type": "code",
        "client_id": required_env("STOCKX_CLIENT_ID"),
        "redirect_uri": required_env("STOCKX_REDIRECT_URI"),
        "scope": "offline_access openid",
        "audience": "gateway.stockx.com",
        "state": state,
    }
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def token_request(data: dict[str, str], action: str) -> None:
    try:
        response = requests.post(
            TOKEN_URL,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        raise RuntimeError(f"StockX {action} failed: network error") from None

    if not response.ok:
        raise RuntimeError(f"StockX {action} failed: HTTP {response.status_code}")

    try:
        token_response = response.json()
    except ValueError as error:
        raise RuntimeError(f"StockX {action} returned invalid JSON") from error
    if not isinstance(token_response, dict):
        raise RuntimeError(f"StockX {action} returned an unexpected response")
    store_tokens(token_response)


def exchange_authorization_code(code: str) -> None:
    """Exchange a one-time authorization code without logging it."""
    token_request(
        {
            "grant_type": "authorization_code",
            "client_id": required_env("STOCKX_CLIENT_ID"),
            "client_secret": required_env("STOCKX_CLIENT_SECRET"),
            "code": code,
            "redirect_uri": required_env("STOCKX_REDIRECT_URI"),
        },
        "token exchange",
    )


def refresh_access_token() -> None:
    """Use the stored refresh token to obtain a new access token."""
    refresh_token = (
        os.getenv("STOCKX_REFRESH_TOKEN", "").strip()
        or read_token_file().get("refresh_token", "")
    )
    if not isinstance(refresh_token, str) or not refresh_token:
        raise RuntimeError("No StockX refresh token is available")

    token_request(
        {
            "grant_type": "refresh_token",
            "client_id": required_env("STOCKX_CLIENT_ID"),
            "client_secret": required_env("STOCKX_CLIENT_SECRET"),
            "audience": "gateway.stockx.com",
            "refresh_token": refresh_token,
        },
        "token refresh",
    )


def active_access_token() -> str:
    """Use an explicit environment token first, then the local token file."""
    token = os.getenv("STOCKX_ACCESS_TOKEN", "").strip()
    if token:
        return token
    token = read_token_file().get("access_token", "")
    if isinstance(token, str) and token:
        return token
    raise RuntimeError("No StockX access token is available")


def catalog_search_once(query: str) -> None:
    """Perform exactly one, page-one Catalog Search request for validation."""
    try:
        response = requests.get(
            CATALOG_SEARCH_URL,
            headers={
                "Authorization": f"Bearer {active_access_token()}",
                "x-api-key": required_env("STOCKX_API_KEY"),
                "Accept": "application/json",
            },
            params={"query": query, "pageNumber": 1, "pageSize": 1},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        raise RuntimeError("StockX Catalog Search failed: network error") from None

    if response.status_code in {401, 403, 429}:
        print(f"Catalog Search HTTP status: {response.status_code}")
        return
    if not response.ok:
        raise RuntimeError(f"StockX Catalog Search failed: HTTP {response.status_code}")

    try:
        payload = response.json()
    except ValueError as error:
        raise RuntimeError("StockX Catalog Search returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise RuntimeError("StockX Catalog Search returned an unexpected response")

    products = payload.get("products") or []
    if not isinstance(products, list):
        raise RuntimeError("StockX Catalog Search returned an unexpected product list")
    print("Catalog Search HTTP status: 200")
    print(f"Catalog Search products in page: {len(products)}")
    print(f"Catalog Search total matches: {payload.get('count', 'unknown')}")


def authorization_callback_handler(
    expected_state: str,
    result: dict[str, bool],
    auth_url: str,
):
    """Create a callback handler bound to the state for this OAuth run."""

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - required callback method name
            parsed = urlparse(self.path)
            if parsed.path == "/start":
                self.send_response(302)
                self.send_header("Location", auth_url)
                self.end_headers()
                return
            if parsed.path != "/callback":
                self.send_error(404)
                return
            params = parse_qs(parsed.query)
            received_state = params.get("state", [""])[0]
            code = params.get("code", [""])[0]
            if not received_state or not secrets.compare_digest(received_state, expected_state):
                self.send_error(400, "Invalid OAuth state")
                return
            if not code:
                self.send_error(400, "Authorization was not completed")
                return
            try:
                exchange_authorization_code(code)
            except RuntimeError:
                self.send_error(502, "Token exchange failed")
                result["done"] = True
                return

            body = b"StockX authorization completed. You can close this tab."
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            result["success"] = True
            result["done"] = True

        def log_message(self, _format: str, *_args: Any) -> None:
            """Never log callback URLs, authorization codes, or tokens."""

    return CallbackHandler


def run_authorization_server(timeout_seconds: int) -> None:
    """Open the browser authorization flow and wait for one valid callback."""
    redirect_uri = required_env("STOCKX_REDIRECT_URI")
    if urlparse(redirect_uri).path != "/callback":
        raise RuntimeError("STOCKX_REDIRECT_URI must end with /callback")

    state = secrets.token_urlsafe(32)
    result = {"done": False, "success": False}
    auth_url = authorization_url(state)
    callback_host = os.getenv("STOCKX_CALLBACK_HOST", "127.0.0.1").strip()
    if callback_host not in {"127.0.0.1", "localhost", "0.0.0.0"}:
        raise RuntimeError("STOCKX_CALLBACK_HOST must be a local bind address")
    server = HTTPServer(
        (callback_host, 8000),
        authorization_callback_handler(state, result, auth_url),
    )
    server.timeout = 1
    print("StockX OAuth callback server is listening on http://127.0.0.1:8000/callback")
    print("Opening the StockX login and consent page in your browser...")
    print("If it does not open, browse to http://127.0.0.1:8000/start")
    webbrowser.open("http://127.0.0.1:8000/start", new=1)

    deadline = time.monotonic() + timeout_seconds
    while not result["done"] and time.monotonic() < deadline:
        server.handle_request()
    server.server_close()
    if not result["done"]:
        raise RuntimeError("Timed out waiting for the StockX OAuth callback")
    if not result["success"]:
        raise RuntimeError("StockX OAuth token exchange failed")
    print("StockX OAuth token exchange completed; tokens were stored locally.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StockX OAuth utility")
    subparsers = parser.add_subparsers(dest="command", required=True)
    authorize_parser = subparsers.add_parser("authorize")
    authorize_parser.add_argument("--timeout-seconds", type=int, default=300)
    subparsers.add_parser("refresh")
    catalog_parser = subparsers.add_parser("catalog-test")
    catalog_parser.add_argument("--query", default="sneaker")
    return parser.parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    if args.command == "authorize":
        run_authorization_server(args.timeout_seconds)
    elif args.command == "refresh":
        refresh_access_token()
        print("StockX access token refreshed and stored locally.")
    elif args.command == "catalog-test":
        catalog_search_once(args.query)


if __name__ == "__main__":
    main()
