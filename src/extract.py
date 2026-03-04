"""
extract.py — Extract layer (E in ELT).

Downloads data from public APIs and writes it as-is to the raw.* schema
in PostgreSQL. No transformations here — raw data is preserved exactly
as received from the source.

Sources:
  - DREES APL data   → raw.apl_by_commune
  - RPPS GP data     → raw.doctors_by_dept
  - INSEE Population → raw.population_by_commune
  - Dept GeoJSON     → data/raw/departements.geojson (file, used by Plotly)
"""

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

from db import get_engine, ensure_schemas

# ── Source resource IDs — data.gouv.fr (verified 2026-03) ────────────────────
APL_RESOURCE_ID  = "5ae24fb0-4966-4d02-ad85-3fe56bad8309"
RPPS_RESOURCE_ID = "fd0aac1f-14a6-46fe-abcf-aabc7dffbfe1"
DATAGOUV_BASE    = "https://www.data.gouv.fr/fr/datasets/r"

DEPT_GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master"
    "/departements-version-simplifiee.geojson"
)
# ──────────────────────────────────────────────────────────────────────────────


def _read_response(resp: httpx.Response) -> pd.DataFrame:
    """Parse an httpx response (CSV or ZIP containing CSV) into a DataFrame."""
    content_type = resp.headers.get("content-type", "")
    data = resp.content

    if "zip" in content_type or data[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            csv_name = next((f for f in z.namelist() if f.endswith(".csv")), None)
            if not csv_name:
                raise ValueError(f"No CSV in ZIP. Contents: {z.namelist()}")
            with z.open(csv_name) as f:
                data = f.read()

    # Try semicolon-delimited first (common in French open data)
    for sep, dec in ((";", ","), (",", ".")):
        try:
            df = pd.read_csv(io.BytesIO(data), sep=sep, dtype=str, decimal=dec)
            if len(df.columns) > 1:
                return df
        except Exception:
            continue
    raise ValueError("Could not parse response as CSV (tried ';' and ',')")


def extract_apl(raw_geojson_dir: Path) -> None:
    """Download DREES APL per commune → raw.apl_by_commune."""
    url = f"{DATAGOUV_BASE}/{APL_RESOURCE_ID}"
    print(f"  ⏳ APL data from data.gouv.fr ({APL_RESOURCE_ID[:8]}…)")

    resp = httpx.get(url, timeout=120, follow_redirects=True)
    resp.raise_for_status()

    df = _read_response(resp)
    df.columns = [c.lower().strip() for c in df.columns]

    # Record extraction timestamp
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("apl_by_commune", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.apl_by_commune: {len(df):,} rows | columns: {df.columns.tolist()[:6]}")


def extract_rpps(raw_geojson_dir: Path) -> None:
    """Download RPPS GP headcount per département → raw.doctors_by_dept."""
    url = f"{DATAGOUV_BASE}/{RPPS_RESOURCE_ID}"
    print(f"  ⏳ RPPS data from data.gouv.fr ({RPPS_RESOURCE_ID[:8]}…)")

    resp = httpx.get(url, timeout=120, follow_redirects=True)
    resp.raise_for_status()

    df = _read_response(resp)
    df.columns = [c.lower().strip() for c in df.columns]
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("doctors_by_dept", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.doctors_by_dept: {len(df):,} rows | columns: {df.columns.tolist()[:6]}")


def extract_population() -> None:
    """Download INSEE RP2021 commune population → raw.population_by_commune."""
    url = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"
    print("  ⏳ Population data from INSEE RP2021…")

    resp = httpx.get(url, timeout=120, follow_redirects=True)
    resp.raise_for_status()

    df = _read_response(resp)
    df.columns = [c.lower().strip() for c in df.columns]
    df["_extracted_at"] = datetime.now(timezone.utc).isoformat()

    engine = get_engine()
    ensure_schemas(engine)
    with engine.begin() as conn:
        df.to_sql("population_by_commune", conn, schema="raw", if_exists="replace", index=False)

    print(f"  ✅ raw.population_by_commune: {len(df):,} rows")


def extract_dept_geojson(output_dir: Path) -> None:
    """
    Download departements GeoJSON (~300 KB) to file.

    Stored as a file (not in DB) because Plotly reads it directly as a dict.
    Source: github.com/gregoiredavid/france-geojson
    Output: data/raw/departements.geojson
    """
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
