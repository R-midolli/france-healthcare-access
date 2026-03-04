---
name: data
description: >
  Use este skill para tudo relacionado a acesso de dados:
  client MCP JSON-RPC para data.gouv.fr (APL e RPPS),
  httpx direto para GeoJSON communes e ZIP INSEE.
  Cobre paginação MCP, schemas esperados, validações e anti-patterns.
  Referência para src/mcp_client.py, src/discover_ids.py e src/extract.py.
---

# SKILL: Acesso a Dados — MCP + httpx

## Decisão de client por fonte

| Fonte | Client | Motivo |
|---|---|---|
| APL DREES — data.gouv.fr | **MCP** | CSV estruturado, query_resource_data |
| RPPS médecins — data.gouv.fr | **MCP** | CSV estruturado, query_resource_data |
| GeoJSON communes — geo.api.gouv.fr | **httpx** | 150 MB — fora do suporte MCP |
| Population INSEE RP2021 — insee.fr | **httpx** | ZIP binário — MCP não suporta |

---

## `src/mcp_client.py`

```python
import httpx
import json
import pandas as pd
from typing import Any

MCP_URL = "https://mcp.data.gouv.fr/mcp"


class DataGouvMCP:
    """
    Client JSON-RPC 2.0 para o MCP oficial do data.gouv.fr.
    Endpoint: https://mcp.data.gouv.fr/mcp
    Sem autenticação. Read-only. Sem LLM intermediário.
    """

    def __init__(self, timeout: int = 60):
        self._http = httpx.Client(timeout=timeout)
        self._req_id = 0

    def _call(self, tool: str, args: dict) -> Any:
        self._req_id += 1
        resp = self._http.post(MCP_URL, json={
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": args},
        })
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise RuntimeError(f"MCP [{tool}] erro: {body['error']}")
        # Resultado em result.content[0].text como JSON string
        text = body.get("result", {}).get("content", [{}])[0].get("text", "[]")
        return json.loads(text) if isinstance(text, str) else text

    def search_datasets(self, query: str, page_size: int = 5) -> list[dict]:
        return self._call("search_datasets", {
            "query": query, "page_size": page_size
        })

    def list_resources(self, dataset_id: str) -> list[dict]:
        return self._call("list_dataset_resources", {
            "dataset_id": dataset_id
        })

    def query_page(self, resource_id: str, question: str, page: int = 1) -> dict:
        """200 linhas por página. Usar get_all_rows() para datasets completos."""
        return self._call("query_resource_data", {
            "resource_id": resource_id,
            "question": question,
            "page": page,
        })

    def get_all_rows(self, resource_id: str, question: str) -> pd.DataFrame:
        """
        Pagina automaticamente até esgotar os dados.
        APL ~35k communes → ~175 páginas de 200 linhas.
        """
        frames, page = [], 1
        while True:
            result = self.query_page(resource_id, question, page=page)
            rows = result.get("rows", [])
            cols = result.get("columns", [])
            if not rows:
                break
            frames.append(pd.DataFrame(rows, columns=cols))
            print(f"  MCP p.{page}: {len(rows)} linhas", end="\r")
            if len(rows) < 200:
                break
            page += 1
        print()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def discover(self, query: str, max_results: int = 3) -> None:
        """Helper para encontrar dataset_id e resource_id. Usar em discover_ids.py."""
        print(f"\n{'='*60}\n🔍 '{query}'")
        for d in self.search_datasets(query, page_size=max_results):
            print(f"\n  📦 {d.get('title','?')[:70]}")
            print(f"     org        : {d.get('organization',{}).get('name','?')[:50]}")
            print(f"     dataset_id : {d.get('id','?')}")
            for r in self.list_resources(d["id"])[:5]:
                print(f"     └─ [{r.get('format','?'):5s}] {r.get('title','?')[:45]}")
                print(f"              resource_id: {r.get('id','?')}")
```

---

## `src/discover_ids.py`

```python
# Rodar UMA VEZ antes de extract.py
# python src/discover_ids.py
# Copiar os IDs e colar em src/extract.py como constantes documentadas

from mcp_client import DataGouvMCP

mcp = DataGouvMCP()
mcp.discover("accessibilité potentielle localisée médecins généralistes commune APL")
mcp.discover("atlas démographie médicale médecins généralistes département DREES")
```

---

## `src/extract.py`

```python
from pathlib import Path
import httpx
import pandas as pd
import zipfile
import io
from mcp_client import DataGouvMCP

mcp = DataGouvMCP()

# ── Preencher após rodar src/discover_ids.py ──────────────────────────────────
# Verificado em YYYY-MM-DD — dataset: "Titre exact du dataset"
APL_RESOURCE_ID  = "PREENCHER_APÓS_DISCOVER"
RPPS_RESOURCE_ID = "PREENCHER_APÓS_DISCOVER"
# ─────────────────────────────────────────────────────────────────────────────


def extract_apl(output_dir: Path) -> pd.DataFrame:
    # Recebe : Path para data/raw/
    # Retorna: DataFrame — codgeo (str 5ch), apl_mg (float), dept, reg
    # Valida : len > 30k, codgeo 5 chars, apl_mg numérico em [0,200]
    # Risco  : nomes de colunas mudam por millésime — normalizar sempre

    print("⏳ [MCP] Extraindo APL par commune...")
    df = mcp.get_all_rows(
        APL_RESOURCE_ID,
        "Toutes les communes avec APL médecins généralistes"
    )

    # Normalizar nomes (variam: "codgeo"/"com"/"code_commune" etc.)
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    for alias, target in [("com", "codgeo"), ("code_commune", "codgeo"),
                          ("commune", "codgeo"), ("apl_medecin", "apl_mg")]:
        if alias in df.columns and target not in df.columns:
            df = df.rename(columns={alias: target})

    df["codgeo"] = df["codgeo"].astype(str).str.strip().str.zfill(5)
    df["apl_mg"] = pd.to_numeric(
        df["apl_mg"].astype(str).str.replace(",", ".").str.strip(),
        errors="coerce",
    )

    assert "codgeo" in df.columns, f"codgeo ausente — colunas: {df.columns.tolist()}"
    assert "apl_mg" in df.columns, f"apl_mg ausente — colunas: {df.columns.tolist()}"
    assert len(df) > 30_000,       f"Esperado >30k communes, obtido {len(df)}"
    assert df["codgeo"].str.len().eq(5).all(), "codgeo com tamanho ≠ 5"
    assert df["apl_mg"].notna().mean() > 0.8,  "Mais de 20% de APL nulos"

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "apl_communes.csv", index=False)
    print(f"✅ APL: {len(df):,} communes | médiane APL: {df['apl_mg'].median():.2f}")
    return df


def extract_rpps(output_dir: Path) -> pd.DataFrame:
    # Recebe : Path para data/raw/
    # Retorna: DataFrame — dept (str), colunas de médecins
    # Valida : len ≥ 95 (96 depts + DOM-TOM)
    # Risco  : pode conter múltiples spécialités — inspecionar colunas ao usar

    print("⏳ [MCP] Extraindo RPPS médecins par département...")
    df = mcp.get_all_rows(
        RPPS_RESOURCE_ID,
        "Nombre médecins généralistes par département"
    )
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    assert len(df) >= 95, f"Esperado ≥95 départements, obtido {len(df)}"

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "rpps_departements.csv", index=False)
    print(f"✅ RPPS: {len(df)} départements | colunas: {df.columns.tolist()}")
    return df


def extract_geodata(output_dir: Path) -> None:
    # Recebe : Path para data/raw/
    # Retorna: None — salva communes.geojson (~150 MB)
    # Valida : arquivo > 50 MB no disco após download
    # Risco  : timeout em conexões lentas — usar streaming com chunks

    GEO_URL = (
        "https://geo.api.gouv.fr/communes"
        "?fields=code,nom,codeDepartement,codeRegion,centre,surface"
        "&format=geojson&geometry=contour"
    )
    out = output_dir / "communes.geojson"
    print("⏳ [httpx] Download GeoJSON communes (~150 MB)...")
    with httpx.stream("GET", GEO_URL, timeout=300, follow_redirects=True) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=65_536):
                f.write(chunk)

    size_mb = out.stat().st_size / 1_048_576
    assert size_mb > 50, f"GeoJSON suspeito: apenas {size_mb:.1f} MB"
    print(f"✅ GeoJSON: {size_mb:.0f} MB")


def extract_population(output_dir: Path) -> pd.DataFrame:
    # Recebe : Path para data/raw/
    # Retorna: DataFrame — codgeo (str 5ch), population (int)
    # Valida : soma total ≈ 68M (France entière)
    # Risco  : URL INSEE muda a cada millésime — verificar 404 antes de prosseguir

    POP_URL = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"
    print("⏳ [httpx] Download Population INSEE RP2021...")
    resp = httpx.get(POP_URL, timeout=120, follow_redirects=True)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = next((f for f in z.namelist() if f.endswith(".csv")), None)
        assert csv_name, f"Nenhum CSV no ZIP. Conteúdo: {z.namelist()}"
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep=";", dtype={"CODGEO": str}, decimal=",")

    df = df.rename(columns={"CODGEO": "codgeo", "PTOT": "population"})
    df = df[["codgeo", "population"]].copy()
    df["codgeo"]     = df["codgeo"].str.strip().str.zfill(5)
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.dropna(subset=["population"])
    df["population"] = df["population"].astype(int)

    total = df["population"].sum()
    assert 65_000_000 < total < 72_000_000, \
        f"Population totale incohérente: {total:,.0f} (attendu 65M–72M)"

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "population_communes.csv", index=False)
    print(f"✅ Population: {len(df):,} communes | total: {total:,.0f}")
    return df
```

---

## `src/test_sources.py`

```python
# python src/test_sources.py — rodar antes de qualquer extração
import httpx

SOURCES = {
    "MCP data.gouv.fr" : ("POST", "https://mcp.data.gouv.fr/mcp"),
    "GeoAPI communes"  : ("GET",  "https://geo.api.gouv.fr/communes?fields=code&format=json&geometry=centre"),
    "INSEE RP2021"     : ("GET",  "https://www.insee.fr/fr/statistiques/7739582"),
}

def main():
    print("🔍 Test des sources de données...")
    all_ok = True
    for name, (method, url) in SOURCES.items():
        try:
            if method == "POST":
                r = httpx.post(url, json={"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}, timeout=15)
            else:
                r = httpx.get(url, timeout=15, follow_redirects=True)
            ok = r.status_code in (200, 405)  # 405 = MCP existe mas método errado = OK
            print(f"  {'✅' if ok else '❌'} {name} (HTTP {r.status_code})")
            if not ok:
                all_ok = False
        except Exception as e:
            print(f"  ❌ {name} — {type(e).__name__}: {e}")
            all_ok = False
    print()
    if all_ok:
        print("✅ Toutes les sources accessibles — pipeline prêt.")
    else:
        print("❌ Source(s) inaccessible(s) — vérifier connectivité avant de continuer.")

if __name__ == "__main__":
    main()
```

---

## Anti-patterns ❌

```python
# ❌ MCP para arquivo > 100 MB ou binário
mcp.get_all_rows(geojson_rid, "geometries")  # timeout ou resposta truncada

# ❌ query_page sem paginar — 35k communes não cabem em 200 linhas
df = mcp.query_page(APL_RESOURCE_ID, "tudo")  # obtém só 200 de 35.000

# ❌ codgeo como inteiro — Ain "01001" → 1001, join falha 100%
df["codgeo"] = df["codgeo"].astype(int)

# ❌ decimal vírgula sem conversão explícita
df["apl_mg"].mean()  # TypeError se string "2,4" não foi convertida

# ❌ resource_id hardcoded sem comentário de data de verificação
APL_RESOURCE_ID = "abc123"  # IDs mudam — sempre rodar discover_ids.py primeiro
```
