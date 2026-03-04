from src.mcp_client import DataGouvMCP

def test_mcp_client_init():
    client = DataGouvMCP()
    assert client.base_url == "https://www.data.gouv.fr/api/1"
