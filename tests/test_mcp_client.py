from src.mcp_client import DataGouvMCP, MCP_URL

def test_mcp_client_init():
    client = DataGouvMCP()
    assert client is not None
    assert MCP_URL == "https://mcp.data.gouv.fr/mcp"
