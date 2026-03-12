"""
extract.py — Extract layer (E in ELT).

Downloads data from public APIs and writes it as-is to the raw.* schema
in PostgreSQL. No transformations here — raw data is preserved exactly
as received from the source.

Sources:
  - DREES APL data   → raw.apl_by_commune
  - AMELI RPPS data  → raw.doctors_by_dept
  - INSEE Population → raw.population_by_commune
  - Dept GeoJSON     → data/raw/departements.geojson (file, used by Plotly map)
"""

import io
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

try:
    from src.db import get_engine, ensure_schemas
except ImportError:
    from db import get_engine, ensure_schemas

# ── Source URLs (verified 2026-03) ───────────────────────────────────────────
APL_XLSX_URL = "https://data.drees.solidarites-sante.gouv.fr/api/v2/catalog/datasets/530_l-accessibilite-potentielle-localisee-apl/attachments/indicateur_d_accessibilite_potentielle_localisee_apl_aux_medecins_generalistes_xlsx"
AMELI_RPPS_URL = "https://data.ameli.fr/api/explore/v2.1/catalog/datasets/demographie-exercices-liberaux/exports/csv?use_labels=true&limit=-1"
INSEE_POP_URL = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"

DEPT_GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master"
    "/departements-version-simplifiee.geojson"
)
# ──────────────────────────────────────────────────────────────────────────────


def _latest_apl_sheet_name(content: bytes) -> str:
    workbook = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    dated_sheets: list[tuple[int, str]] = []
    for name in workbook.sheet_names:
        match = re.match(r"^APL\s+(\d{4})$", str(name).strip(), flags=re.IGNORECASE)
        if match:
            dated_sheets.append((int(match.group(1)), name))

    if dated_sheets:
        return max(dated_sheets)[1]

    fallback = next((name for name in workbook.sheet_names if "apl" in str(name).lower()), None)
    if fallback is None:
        raise ValueError(f"No APL worksheet found. Sheets: {workbook.sheet_names}")
    return fallback


def extract_apl(raw_geojson_dir: Path) -> None:
    """Download DREES APL per commune (XLSX) → raw.apl_by_commune."""
    print("  ⏳ APL data from DREES (XLSX)…")
    # We use a custom User-Agent just in case
    headers = {"User-Agent": "Mozilla/5.0 (Healthcare Access Pipeline)"}
    resp = httpx.get(APL_XLSX_URL, timeout=120, follow_redirects=True, headers=headers)
    resp.raise_for_status()

    sheet_name = _latest_apl_sheet_name(resp.content)
    df = pd.read_excel(
        io.BytesIO(resp.content),
        engine="openpyxl",
        sheet_name=sheet_name,
        header=8,
        dtype=str,
    )
    df.columns = [str(c).lower().strip() for c in df.columns]

    codgeo_col = next((c for c in df.columns if "code commune" in c), None)
    apl_col = next((c for c in df.columns if c.startswith("apl aux médecins généralistes")), None)
    commune_col = next((c for c in df.columns if c == "commune"), None)
    pop_col = next((c for c in df.columns if "population totale" in c), None)
    if not codgeo_col or not apl_col:
        raise ValueError(f"Cannot identify APL columns from sheet {sheet_name!r}: {df.columns.tolist()}")

    keep_cols = [codgeo_col, apl_col]
    if commune_col:
        keep_cols.append(commune_col)
    if pop_col:
        keep_cols.append(pop_col)

    df = df[keep_cols].copy()
    df = df[df[codgeo_col].astype(str).str.match(r"^(\d{5}|2A\d{3}|2B\d{3}|97\d{3})$", na=False)]
    rename_map = {codgeo_col: "codgeo", apl_col: "apl_mg"}
    if commune_col:
        rename_map[commune_col] = "commune"
    if pop_col:
        rename_map[pop_col] = "population_source"
    df = df.rename(columns=rename_map)
    df["codgeo"] = df["codgeo"].astype(str).str.strip().str.upper().str.zfill(5)
    df["apl_mg"] = pd.to_numeric(df["apl_mg"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    if "population_source" in df.columns:
        df["population_source"] = pd.to_numeric(df["population_source"], errors="coerce")
    df = df.dropna(subset=["codgeo", "apl_mg"]).drop_duplicates(subset=["codgeo"])

    # Record extraction timestamp
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    raw_geojson_dir.mkdir(parents=True, exist_ok=True)
    df.drop(columns=["_extracted_at"], errors="ignore").to_csv(raw_geojson_dir / "apl_communes.csv", index=False)

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("apl_by_commune", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.apl_by_commune: {len(df):,} rows | columns: {df.columns.tolist()[:6]}")


def extract_rpps(raw_geojson_dir: Path | None = None) -> None:
    """Download RPPS GP headcount per département (AMELI CSV) → raw.doctors_by_dept."""
    print("  ⏳ RPPS data from AMELI (CSV)…")
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = httpx.get(AMELI_RPPS_URL, timeout=120, follow_redirects=True, headers=headers)
    resp.raise_for_status()

    df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str)
    df.columns = [c.lower().strip().replace("\ufeff", "") for c in df.columns]
    latest_year = df["annee"].dropna().astype(int).max()
    df = df[
        (df["annee"].astype(str) == str(latest_year))
        & (df["profession_sante"] == "Ensemble des médecins généralistes")
        & (df["departement"].astype(str) != "999")
    ].copy()
    df["effectif"] = pd.to_numeric(df["effectif"], errors="coerce")
    df = (
        df.groupby("departement", as_index=False)["effectif"]
        .sum()
        .rename(columns={"departement": "dept", "effectif": "medecins_generalistes"})
    )
    df["dept"] = df["dept"].astype(str).str.strip().str.upper().str.zfill(2)
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    if raw_geojson_dir is not None:
        raw_geojson_dir.mkdir(parents=True, exist_ok=True)
        df.drop(columns=["_extracted_at"], errors="ignore").to_csv(raw_geojson_dir / "rpps_departements.csv", index=False)

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("doctors_by_dept", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.doctors_by_dept: {len(df):,} rows | columns: {df.columns.tolist()[:6]}")


def extract_population(output_dir: Path | None = None) -> None:
    """Download INSEE RP2021 commune population → raw.population_by_commune."""
    print("  ⏳ Population data from INSEE RP2021 (ZIP CSV)…")
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = httpx.get(INSEE_POP_URL, timeout=120, follow_redirects=True, headers=headers)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = next((f for f in z.namelist() if f.endswith("donnees_communes.csv")), None)
        if not csv_name:
            raise ValueError(f"No CSV in ZIP. Contents: {z.namelist()}")
        with z.open(csv_name) as f:
            data = f.read()

    # INSEE CSV is usually delimited by semicolon
    df = pd.read_csv(io.BytesIO(data), sep=";", dtype=str)
    df.columns = [c.lower().strip() for c in df.columns]
    df = df.rename(columns={"com": "codgeo", "dep": "dept", "reg": "reg", "ptot": "population"})
    required = ["codgeo", "dept", "reg", "population"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected population columns: {missing}; found {df.columns.tolist()}")
    df = df[required].copy()
    df["codgeo"] = df["codgeo"].astype(str).str.strip().str.upper().str.zfill(5)
    df["dept"] = df["dept"].astype(str).str.strip().str.upper().str.zfill(2)
    df["reg"] = df["reg"].astype(str).str.strip().str.zfill(2)
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.dropna(subset=["codgeo", "population"])
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.drop(columns=["_extracted_at"], errors="ignore").to_csv(output_dir / "population_communes.csv", index=False)

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("population_by_commune", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.population_by_commune: {len(df):,} rows")


def extract_dept_geojson(output_dir: Path) -> None:
    """Download departements GeoJSON (~300 KB) to file for Plotly map."""
    out = output_dir / "departements.geojson"
    print("  ⏳ Departements GeoJSON from CDN…")
    output_dir.mkdir(parents=True, exist_ok=True)

    r = httpx.get(DEPT_GEOJSON_URL, timeout=30, follow_redirects=True)
    r.raise_for_status()

    geo = r.json()
    if geo.get("type") != "FeatureCollection" or not geo.get("features"):
        raise ValueError("Downloaded file is not a valid GeoJSON FeatureCollection")

    out.write_bytes(r.content)
    size_kb = out.stat().st_size / 1024
    print(f"  ✅ {out.name}: {size_kb:.0f} KB | {len(geo['features'])} features")


def extract_geodata(output_dir: Path) -> None:
    """Backward-compatible alias used by older tests/scripts."""
    extract_dept_geojson(output_dir)
