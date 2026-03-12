"""Minimal data.gouv.fr MCP client used by tests and future source discovery."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx


MCP_URL = "https://mcp.data.gouv.fr/mcp"
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


@dataclass(slots=True)
class DataGouvMCP:
    """Tiny wrapper around the public data.gouv.fr MCP endpoint."""

    base_url: str = MCP_URL
    timeout: float = 30.0
    headers: dict[str, str] = field(default_factory=lambda: dict(MCP_HEADERS))

    def client(self) -> httpx.Client:
        return httpx.Client(
            timeout=self.timeout,
            headers=self.headers,
            follow_redirects=True,
        )
