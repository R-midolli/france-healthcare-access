from mcp_client import DataGouvMCP

mcp = DataGouvMCP()
print("Running official discovery from SKILL-data.md")
mcp.discover("accessibilité potentielle localisée médecins généralistes commune APL")
mcp.discover("atlas démographie médicale médecins généralistes département DREES")
