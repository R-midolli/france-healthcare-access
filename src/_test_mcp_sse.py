"""Debug: inspect raw response for list_dataset_resources and query_resource_data."""
import httpx
import json

MCP_URL = "https://mcp.data.gouv.fr/mcp"
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


def mcp_call(tool, args):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
    }
    with httpx.stream(
        "POST", MCP_URL, json=payload, timeout=60,
        headers=MCP_HEADERS, follow_redirects=True,
    ) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            line = line.strip()
            if line.startswith("data: "):
                return json.loads(line[6:])
    return None


# Test 1: list_dataset_resources for the APL dataset found earlier
print("=== list_dataset_resources ===")
dataset_id = "697956ff4ba57932dd986c05"
resp = mcp_call("list_dataset_resources", {"dataset_id": dataset_id})
if resp:
    content = resp.get("result", {}).get("content", [{}])[0]
    print(f"type: {content.get('type')}")
    print(f"text (first 500):\n{content.get('text', '')[:500]}")
    sc = resp.get("result", {}).get("structuredContent")
    if sc:
        print(f"\nstructuredContent keys: {list(sc.keys()) if isinstance(sc, dict) else type(sc)}")
        print(f"structuredContent (first 500): {json.dumps(sc, ensure_ascii=False)[:500]}")

# Test 2: search with more keywords for DREES APL
print("\n\n=== search_datasets (DREES APL) ===")
resp2 = mcp_call("search_datasets", {
    "query": "APL accessibilité potentielle localisée médecins généralistes DREES",
    "page_size": 5,
})
if resp2:
    text = resp2.get("result", {}).get("content", [{}])[0].get("text", "")
    print(text[:2000])

# Test 3: search RPPS
print("\n\n=== search_datasets (RPPS département) ===")
resp3 = mcp_call("search_datasets", {
    "query": "effectifs médecins généralistes département RPPS DREES atlas démographie",
    "page_size": 5,
})
if resp3:
    text = resp3.get("result", {}).get("content", [{}])[0].get("text", "")
    print(text[:2000])
