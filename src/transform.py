"""
transform.py — Transform layer (T in ELT).

Reads from raw.* tables, cleans/normalises into staging.*, then aggregates
and joins into mart.* tables that the dashboard consumes.

Stage 1 — raw → staging (clean, type, validate)
Stage 2 — staging → mart  (join, aggregate, derive business metrics)
"""

import pandas as pd
from sqlalchemy import text

from db import get_engine, ensure_schemas


# ────────────────────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────────────────────

def _find_col(df: pd.DataFrame, keywords: list[str]) -> str | None:
    """Find first column whose name contains any of the keywords."""
    for kw in keywords:
        hits = [c for c in df.columns if kw in c.lower()]
        if hits:
            return hits[0]
    return None


# ────────────────────────────────────────────────────────────────────────────
# STAGE 1: raw → staging
# ────────────────────────────────────────────────────────────────────────────

def _stg_communes(engine) -> pd.DataFrame:
    """raw.apl_by_commune + raw.population_by_commune → staging.stg_communes"""
    apl_raw = pd.read_sql("SELECT * FROM raw.apl_by_commune", engine)
    pop_raw = pd.read_sql("SELECT * FROM raw.population_by_commune", engine)

    # --- APL: identify and normalise key columns ---
    codgeo_col = _find_col(apl_raw, ["codgeo", "code_commune", "com"])
    apl_col    = _find_col(apl_raw, ["apl_mg", "apl"])
    dept_col   = _find_col(apl_raw, ["dept", "dep"])
    reg_col    = _find_col(apl_raw, ["reg"])

    if not codgeo_col or not apl_col:
        raise ValueError(
            f"Cannot identify required APL columns. Found: {apl_raw.columns.tolist()}"
        )

    apl = apl_raw[[codgeo_col, apl_col]
                  + ([dept_col] if dept_col else [])
                  + ([reg_col]  if reg_col  else [])
                  ].copy()
    apl.columns = (
        ["codgeo", "apl_mg"]
        + (["dept"] if dept_col else [])
        + (["reg"]  if reg_col  else [])
    )
    apl["codgeo"] = apl["codgeo"].astype(str).str.strip().str.zfill(5)
    apl["apl_mg"] = pd.to_numeric(
        apl["apl_mg"].astype(str).str.replace(",", "."), errors="coerce"
    )
    if "dept" in apl.columns:
        apl["dept"] = apl["dept"].astype(str).str.strip().str.zfill(2)
    if "reg" in apl.columns:
        apl["reg"] = apl["reg"].astype(str).str.strip()

    apl = apl.dropna(subset=["codgeo", "apl_mg"])
    apl = apl[apl["apl_mg"].between(0, 200)]
    apl = apl.drop_duplicates(subset=["codgeo"])

    # --- Population: identify and normalise key columns ---
    codgeo_pop = _find_col(pop_raw, ["codgeo", "com"])
    pop_col    = _find_col(pop_raw, ["ptot", "population", "pop"])

    if not codgeo_pop or not pop_col:
        raise ValueError(
            f"Cannot identify population columns. Found: {pop_raw.columns.tolist()}"
        )

    pop = pop_raw[[codgeo_pop, pop_col]].copy()
    pop.columns = ["codgeo", "population"]
    pop["codgeo"]     = pop["codgeo"].astype(str).str.strip().str.zfill(5)
    pop["population"] = pd.to_numeric(pop["population"], errors="coerce")
    pop = pop.dropna()
    pop["population"] = pop["population"].astype(int)
    pop = pop[pop["population"] > 0]
    pop = pop.groupby("codgeo", as_index=False)["population"].sum()

    # Join
    df = apl.merge(pop, on="codgeo", how="left")

    with engine.begin() as conn:
        df.to_sql("stg_communes", conn, schema="staging", if_exists="replace", index=False)

    print(f"  ✅ staging.stg_communes: {len(df):,} rows")
    return df


def _stg_departments(engine) -> pd.DataFrame:
    """raw.doctors_by_dept → staging.stg_departments"""
    rpps_raw = pd.read_sql("SELECT * FROM raw.doctors_by_dept", engine)

    dept_col = _find_col(rpps_raw, ["dept", "dep", "code_dep"])
    med_col  = _find_col(rpps_raw, ["gen", "med", "omni"])

    if not dept_col or not med_col:
        raise ValueError(
            f"Cannot identify RPPS columns. Found: {rpps_raw.columns.tolist()}"
        )

    df = rpps_raw[[dept_col, med_col]].copy()
    df.columns = ["dept", "nb_medecins"]
    df["dept"]        = df["dept"].astype(str).str.strip().str.zfill(2)
    df["nb_medecins"] = pd.to_numeric(df["nb_medecins"], errors="coerce")
    df = df.dropna()
    df["nb_medecins"] = df["nb_medecins"].astype(int)
    df = df.drop_duplicates(subset=["dept"])

    with engine.begin() as conn:
        df.to_sql("stg_departments", conn, schema="staging", if_exists="replace", index=False)

    print(f"  ✅ staging.stg_departments: {len(df)} rows")
    return df


# ────────────────────────────────────────────────────────────────────────────
# STAGE 2: staging → mart
# ────────────────────────────────────────────────────────────────────────────

APL_THRESHOLD = 2.5   # Default medical desert threshold

APL_CATEGORIES = {
    "critical_desert": (0.0,   1.5),
    "under_served":    (1.5,   APL_THRESHOLD),
    "adequate":        (APL_THRESHOLD, 4.0),
    "well_served":     (4.0,   float("inf")),
}


def _categorise_apl(apl: float) -> str:
    for cat, (lo, hi) in APL_CATEGORIES.items():
        if lo <= apl < hi:
            return cat
    return "well_served"


def _mart_fact_communes(engine) -> pd.DataFrame:
    """staging.stg_communes → mart.fact_communes"""
    df = pd.read_sql("SELECT * FROM staging.stg_communes", engine)

    df["apl_category"] = df["apl_mg"].apply(_categorise_apl)
    df["is_desert"]    = (df["apl_mg"] < APL_THRESHOLD).astype(int)

    with engine.begin() as conn:
        df.to_sql("fact_communes", conn, schema="mart", if_exists="replace", index=False)

    print(f"  ✅ mart.fact_communes: {len(df):,} rows | "
          f"{df['is_desert'].sum():,} deserts ({df['is_desert'].mean()*100:.1f}%)")
    return df


def _mart_dim_departments(engine, communes: pd.DataFrame) -> pd.DataFrame:
    """staging.stg_departments + mart.fact_communes → mart.dim_departments"""
    docs = pd.read_sql("SELECT * FROM staging.stg_departments", engine)

    agg = (
        communes.groupby("dept", as_index=False)
        .agg(
            nb_communes      = ("codgeo",       "count"),
            total_population = ("population",   "sum"),
            apl_median       = ("apl_mg",       "median"),
            apl_min          = ("apl_mg",       "min"),
            apl_max          = ("apl_mg",       "max"),
            nb_desert        = ("is_desert",    "sum"),
            nb_critical      = ("apl_category", lambda x: (x == "critical_desert").sum()),
        )
    )
    agg["pct_desert"] = (agg["nb_desert"] / agg["nb_communes"] * 100).round(2)
    agg["apl_median"] = agg["apl_median"].round(3)

    df = agg.merge(docs, on="dept", how="left")
    df["doctors_per_10k"] = (
        df["nb_medecins"] / df["total_population"] * 10_000
    ).round(2)

    df = df.sort_values("pct_desert", ascending=False).reset_index(drop=True)

    with engine.begin() as conn:
        df.to_sql("dim_departments", conn, schema="mart", if_exists="replace", index=False)

    print(f"  ✅ mart.dim_departments: {len(df)} departments | "
          f"top 3 worst: {df.head(3)['dept'].tolist()}")
    return df


# ────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ────────────────────────────────────────────────────────────────────────────

def run_transform() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Execute all transform stages and return (fact_communes, dim_departments)."""
    engine = get_engine()
    ensure_schemas(engine)

    print("\n  [Stage 1] raw → staging")
    communes = _stg_communes(engine)
    _stg_departments(engine)

    print("\n  [Stage 2] staging → mart")
    fact = _mart_fact_communes(engine)
    dims = _mart_dim_departments(engine, fact)

    return fact, dims
