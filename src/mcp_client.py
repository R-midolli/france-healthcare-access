import httpx
import json
import pandas as pd
from typing import Any

MCP_URL = "https://mcp.data.gouv.fr/mcp"

# MCP data.gouv.fr uses Streamable HTTP transport (SSE).
# The client MUST send Accept: application/json, text/event-stream
# and parse SSE "data:" lines from the response stream.
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


def _parse_sse_response(response: httpx.Response) -> Any:
    """
    Parse a Streamable HTTP MCP response.
    If Content-Type is text/event-stream, extract JSON from 'data:' lines.
    If Content-Type is application/json, parse directly.
    """
    ct = response.headers.get("content-type", "")

    if "text/event-stream" in ct:
        # SSE format: lines like "event: message\ndata: {json}\n\n"
        data_payload = None
        for line in response.iter_lines():
            line = line.strip()
            if line.startswith("data: "):
                data_payload = line[6:]
        if data_payload:
            return json.loads(data_payload)
        raise RuntimeError("SSE response contained no data: lines")
    else:
        # Standard JSON-RPC response
        return response.json()


class DataGouvMCP:
    """
    JSON-RPC 2.0 client for the official data.gouv.fr MCP endpoint.
    Endpoint: https://mcp.data.gouv.fr/mcp
    Transport: Streamable HTTP (SSE)
    No authentication. Read-only. No LLM intermediary.
    """

    def __init__(self, timeout: int = 60):
        self._http = httpx.Client(timeout=timeout)
        self._req_id = 0

    def _call(self, tool: str, args: dict) -> Any:
        self._req_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": args},
        }
        with self._http.stream(
            "POST", MCP_URL, json=payload, headers=MCP_HEADERS
        ) as resp:
            resp.raise_for_status()
            body = _parse_sse_response(resp)

        if "error" in body:
            raise RuntimeError(f"MCP [{tool}] error: {body['error']}")
        # Result in result.content[0].text as JSON string
        text = body.get("result", {}).get("content", [{}])[0].get("text", "[]")
        if not text:
            text = "[]"
        try:
            return json.loads(text) if isinstance(text, str) else text
        except json.JSONDecodeError:
            print(f"DEBUG text: {text}")
            return []

    def search_datasets(self, query: str, page_size: int = 5) -> list[dict]:
        return self._call("search_datasets", {
            "query": query, "page_size": page_size
        })

    def list_resources(self, dataset_id: str) -> list[dict]:
        return self._call("list_dataset_resources", {
            "dataset_id": dataset_id
        })

    def query_page(self, resource_id: str, question: str, page: int = 1) -> dict:
        """200 rows per page. Use get_all_rows() for complete datasets."""
        return self._call("query_resource_data", {
            "resource_id": resource_id,
            "question": question,
            "page": page,
        })

    def get_all_rows(self, resource_id: str, question: str) -> pd.DataFrame:
        """
        Automatically paginates until all data is consumed.
        APL ~35k communes → ~175 pages of 200 rows.
        """
        frames, page = [], 1
        while True:
            result = self.query_page(resource_id, question, page=page)
            rows = result.get("rows", [])
            cols = result.get("columns", [])
            if not rows:
                break
            frames.append(pd.DataFrame(rows, columns=cols))
            print(f"  MCP p.{page}: {len(rows)} rows", end="\r")
            if len(rows) < 200:
                break
            page += 1
        print()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def discover(self, query: str, max_results: int = 3) -> None:
        """Helper to find dataset_id and resource_id. Use in discover_ids.py."""
        print(f"\n{'='*60}\n🔍 '{query}'")
        for d in self.search_datasets(query, page_size=max_results):
            print(f"\n  📦 {d.get('title','?')[:70]}")
            print(f"     org        : {d.get('organization',{}).get('name','?')[:50]}")
            print(f"     dataset_id : {d.get('id','?')}")
            for r in self.list_resources(d["id"])[:5]:
                print(f"     └─ [{r.get('format','?'):5s}] {r.get('title','?')[:45]}")
                print(f"              resource_id: {r.get('id','?')}")
