---
name: pipeline
description: >
  Use este skill para transformações DuckDB: JOIN APL + population + RPPS,
  calcular APL_cat com seuils DREES, agregar por département, gerar Parquets,
  rodar quality checks e produzir quality_report.json.
  Referência para src/transform.py, src/validate.py e src/pipeline.py.
---

# SKILL: Pipeline ETL — DuckDB → Parquet

## Camadas de dados

```
Bronze  data/raw/                     ← pipeline gera, .gitignore exclui
  apl_communes.csv        ~35k lignes
  population_communes.csv ~35k lignes
  rpps_departements.csv   ~100 lignes
  communes.geojson        ~150 MB (usado só pelo app Streamlit)

Gold    data/processed/               ← commitado no repo GitHub
  communes_enriched.parquet     join APL+pop + APL_cat + is_desert
  departements_summary.parquet  agregação département
  quality_report.json           stats reais para o portfólio
```

---

## `src/transform.py`

```python
import duckdb
import pandas as pd
from pathlib import Path


def build_communes_enriched(raw_dir: Path, processed_dir: Path) -> pd.DataFrame:
    # Recebe : CSVs apl_communes + population_communes em raw_dir
    # Retorna: DataFrame com APL_cat pelos seuils DREES oficiais
    # Valida : unicidade codgeo, APL_cat enum válido, range APL [0,200]
    # Risco  : LEFT JOIN mantém communes sem pop INSEE (population = NULL)
    #          → correto: não eliminar communes — manter com pop nula

    con = duckdb.connect()
    processed_dir.mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        CREATE TABLE apl AS
        SELECT
            LPAD(CAST(codgeo AS VARCHAR), 5, '0')             AS codgeo,
            TRY_CAST(
                REPLACE(CAST(apl_mg AS VARCHAR), ',', '.')
            AS DOUBLE)                                         AS apl_mg,
            CAST(dept AS VARCHAR)                             AS dept,
            CAST(reg  AS VARCHAR)                             AS reg
        FROM read_csv_auto(
            '{raw_dir}/apl_communes.csv',
            dtypes={{'codgeo':'VARCHAR','dept':'VARCHAR','reg':'VARCHAR'}}
        )
        WHERE TRY_CAST(REPLACE(CAST(apl_mg AS VARCHAR),',','.') AS DOUBLE)
              IS NOT NULL
    """)

    con.execute(f"""
        CREATE TABLE pop AS
        SELECT
            LPAD(CAST(codgeo AS VARCHAR), 5, '0') AS codgeo,
            CAST(population AS INTEGER)            AS population
        FROM read_csv_auto(
            '{raw_dir}/population_communes.csv',
            dtypes={{'codgeo':'VARCHAR'}}
        )
        WHERE population IS NOT NULL
          AND TRY_CAST(population AS INTEGER) > 0
    """)

    # LEFT JOIN — conservar communes APL même sans données de population
    # Seuils DREES officiels — NE PAS MODIFIER
    df = con.execute("""
        SELECT
            a.codgeo,
            a.dept,
            a.reg,
            ROUND(a.apl_mg, 4)      AS apl_mg,
            p.population,
            CASE
                WHEN a.apl_mg < 1.5 THEN 'désert_critique'
                WHEN a.apl_mg < 2.5 THEN 'sous-doté'
                WHEN a.apl_mg < 4.0 THEN 'correct'
                ELSE                     'bien_doté'
            END                     AS apl_cat,
            CASE WHEN a.apl_mg < 2.5 THEN 1 ELSE 0 END AS is_desert
        FROM apl a
        LEFT JOIN pop p ON a.codgeo = p.codgeo
        ORDER BY a.dept, a.codgeo
    """).df()

    # Validações obrigatórias
    assert len(df) > 30_000, f"Communes insuficientes: {len(df)}"
    assert df["codgeo"].is_unique, \
        f"codgeo duplicado: {df[df['codgeo'].duplicated()]['codgeo'].head().tolist()}"
    valid = {"désert_critique", "sous-doté", "correct", "bien_doté"}
    invalid = set(df["apl_cat"].unique()) - valid
    assert not invalid, f"APL_cat inválida: {invalid}"
    assert df["apl_mg"].between(0, 200).all(), \
        f"APL fora de [0,200]: {df[~df['apl_mg'].between(0,200)]['apl_mg'].describe()}"

    n = df["is_desert"].sum()
    print(f"✅ communes_enriched: {len(df):,} communes | "
          f"{n:,} déserts ({n/len(df)*100:.1f}%) | "
          f"APL médiane: {df['apl_mg'].median():.2f}")

    df.to_parquet(processed_dir / "communes_enriched.parquet", index=False)
    con.close()
    return df


def build_departements_summary(raw_dir: Path, processed_dir: Path) -> pd.DataFrame:
    # Recebe : communes_enriched.parquet + rpps_departements.csv
    # Retorna: DataFrame avec métriques par département + med_pour_10k
    # Valida : 90 ≤ nb départements ≤ 110
    # Risco  : colunas RPPS têm nomes variáveis — inspecionar antes do join

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE communes AS
        SELECT * FROM read_parquet('{processed_dir}/communes_enriched.parquet')
    """)

    df = con.execute("""
        SELECT
            dept,
            COUNT(*)                                        AS nb_communes,
            COALESCE(SUM(population), 0)                   AS population_totale,
            ROUND(MEDIAN(apl_mg), 3)                       AS apl_mediane,
            MIN(apl_mg)                                    AS apl_min,
            MAX(apl_mg)                                    AS apl_max,
            SUM(is_desert)                                 AS nb_communes_desert,
            ROUND(SUM(is_desert)*100.0/COUNT(*), 2)        AS pct_desert,
            SUM(CASE WHEN apl_cat='désert_critique' THEN 1 ELSE 0 END)
                                                           AS nb_desert_critique
        FROM communes
        GROUP BY dept
        ORDER BY pct_desert DESC
    """).df()

    # Tenter join RPPS pour médecins/10k — robuste aux noms de colonnes variables
    try:
        rpps = pd.read_csv(raw_dir / "rpps_departements.csv", dtype=str)
        print(f"  RPPS colonnes: {rpps.columns.tolist()}")
        dept_col = next(
            (c for c in rpps.columns if any(k in c.lower() for k in ["dept","dep","code"])),
            None
        )
        med_col = next(
            (c for c in rpps.columns if any(k in c.lower() for k in ["gen","med","omni"])),
            None
        )
        if dept_col and med_col:
            slim = rpps[[dept_col, med_col]].copy()
            slim.columns = ["dept", "nb_medecins"]
            slim["dept"]        = slim["dept"].astype(str).str.strip().str.zfill(2)
            slim["nb_medecins"] = pd.to_numeric(slim["nb_medecins"], errors="coerce")
            df = df.merge(slim, on="dept", how="left")
            df["med_pour_10k"] = (
                df["nb_medecins"] / df["population_totale"] * 10_000
            ).round(2)
            print(f"  ✅ RPPS joiné (dept={dept_col}, médecins={med_col})")
        else:
            print(f"  ⚠️ Colonnes RPPS non reconnues — med_pour_10k absent")
    except Exception as e:
        print(f"  ⚠️ RPPS join ignoré: {e}")

    assert 90 <= len(df) <= 110, f"Nb départements anormal: {len(df)}"

    df.to_parquet(processed_dir / "departements_summary.parquet", index=False)
    print(f"✅ departements_summary: {len(df)} depts | "
          f"top 3: {df.head(3)['dept'].tolist()}")
    con.close()
    return df


def run_transform(raw_dir: Path, processed_dir: Path) -> None:
    print("\n📦 Transformation DuckDB...")
    build_communes_enriched(raw_dir, processed_dir)
    build_departements_summary(raw_dir, processed_dir)
    print("✅ Transformation terminée\n")
```

---

## `src/validate.py`

```python
import pandas as pd
import json
from pathlib import Path


def validate(processed_dir: Path) -> dict:
    """
    Quality checks. Salva quality_report.json com stats reais para o portfólio.
    errors[]   → bloqueantes (falha o pipeline)
    warnings[] → informativos (registrar e continuar)
    stats{}    → valores reais para KPI cards do portfólio
    """
    report: dict = {"errors": [], "warnings": [], "stats": {}}

    communes = pd.read_parquet(processed_dir / "communes_enriched.parquet")
    depts    = pd.read_parquet(processed_dir / "departements_summary.parquet")

    # ── Errors (bloqueantes) ─────────────────────────────────────────────────
    if communes["codgeo"].duplicated().any():
        n = communes["codgeo"].duplicated().sum()
        report["errors"].append(f"{n} codgeo dupliqués")

    if not communes["apl_mg"].between(0, 200).all():
        n = (~communes["apl_mg"].between(0, 200)).sum()
        report["errors"].append(f"{n} valeurs APL hors [0,200]")

    valid = {"désert_critique", "sous-doté", "correct", "bien_doté"}
    bad = set(communes["apl_cat"].unique()) - valid
    if bad:
        report["errors"].append(f"APL_cat invalide: {bad}")

    # ── Warnings (informativos) ──────────────────────────────────────────────
    pop_total = communes["population"].sum()
    if not (65_000_000 < pop_total < 72_000_000):
        report["warnings"].append(
            f"Population totale: {pop_total:,.0f} (attendu 65M–72M)"
        )

    top_dept = depts.iloc[0]["dept"]
    ruraux = {"23","58","61","52","55","36","48","70","03","87","19"}
    if top_dept not in ruraux:
        report["warnings"].append(
            f"Dept le plus touché = {top_dept} (attendu zone rurale)"
        )

    apl_med = communes["apl_mg"].median()
    if not (1.5 < apl_med < 6.0):
        report["warnings"].append(f"APL médiane nationale anormale: {apl_med:.2f}")

    # ── Stats pour le portfólio ──────────────────────────────────────────────
    pop_desert = int(communes.loc[communes["is_desert"]==1, "population"].sum())
    report["stats"] = {
        "nb_communes"          : int(len(communes)),
        "nb_communes_desert"   : int(communes["is_desert"].sum()),
        "pct_communes_desert"  : round(float(communes["is_desert"].mean()*100), 1),
        "population_en_desert" : pop_desert,
        "population_totale"    : int(pop_total),
        "apl_mediane_france"   : round(float(apl_med), 2),
        "dept_le_plus_touche"  : str(top_dept),
        "pct_desert_dept_max"  : round(float(depts.iloc[0]["pct_desert"]), 1),
        "nb_departements"      : int(len(depts)),
        "sources"              : [
            "APL DREES via MCP data.gouv.fr",
            "INSEE RP2021",
            "geo.api.gouv.fr",
            "RPPS DREES via MCP data.gouv.fr",
        ],
    }

    with open(processed_dir / "quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    if report["errors"]:
        print(f"❌ {len(report['errors'])} erreur(s):")
        for e in report["errors"]: print(f"   • {e}")
    else:
        print("✅ Qualité OK")

    if report["warnings"]:
        print(f"⚠️  {len(report['warnings'])} warning(s):")
        for w in report["warnings"]: print(f"   • {w}")

    print("\n📊 Stats pour le portfólio:")
    for k, v in report["stats"].items():
        print(f"   {k}: {v}")

    return report
```

---

## `src/pipeline.py` — orquestrador

```python
# python src/pipeline.py
from pathlib import Path
from extract   import extract_apl, extract_rpps, extract_geodata, extract_population
from transform import run_transform
from validate  import validate

RAW  = Path("data/raw")
PROC = Path("data/processed")


def run() -> None:
    print("="*60)
    print("🚀 France Healthcare Access — Pipeline")
    print("="*60)

    print("\n[1/3] Extraction")
    extract_apl(RAW)
    extract_rpps(RAW)
    extract_geodata(RAW)
    extract_population(RAW)

    print("\n[2/3] Transformation")
    run_transform(RAW, PROC)

    print("\n[3/3] Validation")
    report = validate(PROC)
    if report["errors"]:
        raise SystemExit(
            f"\n❌ Pipeline échoué — {len(report['errors'])} erreur(s) critique(s)"
        )

    print("\n✅ Pipeline terminé avec succès")
    print(f"   Parquets: {[p.name for p in PROC.glob('*.parquet')]}")


if __name__ == "__main__":
    run()
```

---

## Anti-patterns ❌

```python
# ❌ INNER JOIN — perd communes isolées sans données INSEE
FROM apl INNER JOIN pop ON apl.codgeo = pop.codgeo  # usar LEFT JOIN

# ❌ Modifier les seuils DREES — valeurs réglementaires officielles
WHEN apl_mg < 3.0 THEN 'sous-doté'  # seuil officiel est 2.5

# ❌ Commitar data/raw/ — arquivos grandes, reproduzíveis pelo pipeline
git add data/raw/  # PROIBIDO

# ❌ Lancer le Streamlit sans @st.cache_data
def load_communes(): return pd.read_parquet(...)  # recarrega a cada clique
```
