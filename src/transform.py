import duckdb
import pandas as pd
from pathlib import Path


def build_communes_enriched(raw_dir: Path, processed_dir: Path) -> pd.DataFrame:
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
