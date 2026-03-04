# python src/test_sources.py — run before any extraction
import httpx

# MCP data.gouv.fr requires dual Accept header (Streamable HTTP transport)
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}

SOURCES = {
    "MCP data.gouv.fr": (
        "POST",
        "https://mcp.data.gouv.fr/mcp",
    ),
    "GeoAPI communes": (
        "GET",
        "https://geo.api.gouv.fr/communes?fields=code&format=json&geometry=centre",
    ),
    "INSEE RP2021": (
        "GET",
        "https://www.insee.fr/fr/statistiques/7739582",
    ),
}


def main():
    print("🔍 Testing data sources...")
    all_ok = True
    for name, (method, url) in SOURCES.items():
        try:
            if method == "POST":
                # MCP: use streaming with SSE Accept header
                with httpx.stream(
                    "POST",
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {},
                    },
                    timeout=15,
                    headers=MCP_HEADERS,
                ) as r:
                    ok = r.status_code == 200
                    print(f"  {'✅' if ok else '❌'} {name} (HTTP {r.status_code})")
                    if not ok:
                        all_ok = False
            else:
                r = httpx.get(url, timeout=15, follow_redirects=True)
                ok = r.status_code == 200
                print(f"  {'✅' if ok else '❌'} {name} (HTTP {r.status_code})")
                if not ok:
                    all_ok = False
        except Exception as e:
            print(f"  ❌ {name} — {type(e).__name__}: {e}")
            all_ok = False
    print()
    if all_ok:
        print("✅ All sources accessible — pipeline ready.")
    else:
        print("❌ Source(s) inaccessible — check connectivity before continuing.")


if __name__ == "__main__":
    main()
