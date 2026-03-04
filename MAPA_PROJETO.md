# MAPA DO PROJETO — France Healthcare Access
> Guia humano: o que é, onde vai cada arquivo, o que você faz vs o que o agente faz
> Rafael Midolli · Março 2026
> GitHub: https://github.com/R-midolli/france-healthcare-access

---

## O projeto em 2 frases

Pipeline ETL que lê 4 fontes oficiais do governo francês (via MCP + httpx),
processa com DuckDB e publica um dashboard Streamlit com mapa interativo
dos déserts médicaux na França, commune par commune.

Deploy gratuito no Streamlit Cloud. Refresh automático mensal via GitHub Actions.

---

## Onde vai cada arquivo no seu computador

```
~/workspace/france-healthcare-access/     ← workspace do Antigravity
│
├── AGENT_PROMPT.md          ← COLAR NO ANTIGRAVITY (entrada do agente)
├── MAPA_PROJETO.md          ← este arquivo
│
├── .agent/
│   └── skills/
│       ├── SKILL-data.md        ← regras de extração (MCP + httpx)
│       ├── SKILL-pipeline.md    ← regras de transformação DuckDB
│       └── SKILL-output.md      ← regras do app Streamlit + deploy
│
├── app/
│   └── streamlit_app.py         ← dashboard FR/EN (Folium + Plotly)
│
├── src/
│   ├── __init__.py
│   ├── mcp_client.py            ← client JSON-RPC para MCP data.gouv.fr
│   ├── discover_ids.py          ← encontra resource_ids reais (rodar 1x)
│   ├── extract.py               ← extração das 4 fontes
│   ├── transform.py             ← DuckDB: joins + APL_cat + agregações
│   ├── validate.py              ← quality checks + quality_report.json
│   ├── pipeline.py              ← orquestrador: extract→transform→validate
│   └── test_sources.py          ← verifica acesso HTTP às 4 fontes
│
├── tests/
│   ├── __init__.py
│   ├── test_transform.py        ← testa SQL DuckDB e APL_cat
│   ├── test_validate.py         ← testa quality checks
│   └── test_mcp_client.py       ← testa client com mock HTTP
│
├── data/
│   ├── raw/                     ← ❌ NÃO vai pro git
│   │   └── .gitkeep             ← mantém a pasta vazia no repo
│   └── processed/               ← ✅ VAI pro git
│       ├── communes_enriched.parquet
│       ├── departements_summary.parquet
│       └── quality_report.json
│
├── .github/
│   └── workflows/
│       ├── ci.yml               ← testes em todo push
│       └── refresh.yml          ← repipeline dia 1 de cada mês
│
├── .gitignore
├── .env.example                 ← vazio (sem secrets neste projeto)
├── requirements.txt             ← versões fixas (Streamlit Cloud lê este)
├── requirements-dev.txt
└── Makefile
```

---

## O que você faz vs o que o agente faz

| Quem | O quê |
|---|---|
| **Você** | Criar o repo GitHub ✅ (já feito) |
| **Você** | Colar o AGENT_PROMPT.md no Antigravity |
| **Você** | Confirmar checkpoints quando o agente pedir |
| **Você** | Deploy no Streamlit Cloud (2 cliques, 1x) |
| **Agente** | Criar todos os arquivos Python |
| **Agente** | Rodar o pipeline e gerar os Parquets |
| **Agente** | Montar o app Streamlit |
| **Agente** | Fazer os commits e push no GitHub |
| **Agente** | Criar a página do portfólio HTML |

---

## Setup em 1 comando

Cole no Git Bash antes de abrir o Antigravity:

```bash
cd ~/workspace/france-healthcare-access && \
mkdir -p app src tests data/raw data/processed .agent/skills .github/workflows && \
touch data/raw/.gitkeep && \
echo "✅ Estrutura pronta"
```

Depois copie os 5 arquivos (.md) para os lugares indicados acima
e abra a pasta no Antigravity.

---

## Como avaliar o projeto (para recrutadores)

**Opção rápida — ver o dashboard:**
```
URL Streamlit Cloud: [preencher após deploy]
```

**Opção técnica — rodar localmente:**
```bash
git clone https://github.com/R-midolli/france-healthcare-access.git
cd france-healthcare-access
pip install -r requirements.txt
streamlit run app/streamlit_app.py
# Os Parquets já estão no repo — o dashboard abre direto
```

**Opção completa — replicar o pipeline do zero:**
```bash
pip install -r requirements-dev.txt
python src/test_sources.py   # confirmar acesso às 4 fontes
python src/discover_ids.py   # encontrar resource_ids atuais
# preencher APL_RESOURCE_ID e RPPS_RESOURCE_ID em src/extract.py
python src/pipeline.py       # extract → transform → validate
streamlit run app/streamlit_app.py
```

---

## Commits esperados (sequência do agente)

```
feat(src): add DataGouvMCP client with auto-pagination
feat(src): add discover_ids script for APL and RPPS
feat(src): add 4-source extraction pipeline (MCP + httpx)
feat(src): add DuckDB transform with DREES APL seuils
feat(src): add quality validation and quality_report.json
feat(app): add Streamlit dashboard with Folium choropleth
feat(app): add FR/EN toggle, department filter, 3 tabs
feat(ci): add CI and monthly refresh GitHub Actions
docs: add bilingual README with Streamlit Cloud badge
feat(portfolio): add France Healthcare Access project page
```

---

## Narrativa do portfólio

**FR:**
> "En France, des millions de personnes vivent dans un désert médical.
> Ce projet croise 4 sources officielles via le MCP data.gouv.fr et des
> API publiques, puis transforme ces données avec DuckDB pour produire
> un observatoire interactif — localisant les zones sous-dotées commune
> par commune et quantifiant l'impact réel sur la population."

**EN:**
> "Millions of people in France live in medical deserts.
> This project cross-references 4 official government sources via the
> data.gouv.fr MCP and public APIs, processes them with DuckDB, and
> produces an interactive observatory — mapping under-served areas
> commune by commune with real population impact data."

**KPIs** (valores reais após rodar o pipeline — buscar em quality_report.json):
- `nb_communes` communes analysées
- `nb_communes_desert` communes en désert médical
- `population_en_desert` habitants concernés
- `apl_mediane_france` APL médiane nationale
- 4 sources officielles croisées · MCP data.gouv.fr · Streamlit Cloud

---

*v3.0 · france-healthcare-access · Rafael Midolli · Março 2026*
*Otimizado para Claude Opus 4.6 / Antigravity IDE*
