"""
pipeline.py — ELT Orchestrator.

Executes the full data pipeline:
  Step 1 — Extract: APIs → raw.* (source data, no transformations)
  Step 2 — Transform: raw.* → staging.* → mart.* (clean, join, aggregate)
  Step 3 — Validate: mart.* quality checks + record pipeline run

Run monthly or on demand:
    uv run python src/pipeline.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv  # optional: loads .env if present

# Load .env if available (ignored if file doesn't exist)
try:
    load_dotenv()
except ImportError:
    pass

try:
    from src.extract import extract_apl, extract_rpps, extract_population, extract_dept_geojson
    from src.transform import run_transform
    from src.validate import validate
except ImportError:
    from extract import extract_apl, extract_rpps, extract_population, extract_dept_geojson
    from transform import run_transform
    from validate import validate

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _write_local_snapshots(fact, dims, report: dict) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    fact_out = fact.rename(columns={"apl_category": "apl_cat"}).copy()
    dims_out = dims.rename(
        columns={
            "apl_median": "apl_mediane",
            "doctors_per_10k": "med_pour_10k",
            "total_population": "population_totale",
            "nb_desert": "nb_communes_desert",
            "nb_critical": "nb_desert_critique",
        }
    ).copy()

    fact_out.to_parquet(PROCESSED_DIR / "communes_enriched.parquet", index=False)
    dims_out.to_parquet(PROCESSED_DIR / "departements_summary.parquet", index=False)

    stats = report["stats"]
    quality_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": report.get("errors", []),
        "warnings": report.get("warnings", []),
        "stats": {
            "nb_communes": stats.get("nb_communes"),
            "nb_communes_desert": stats.get("nb_desert_communes"),
            "pct_communes_desert": stats.get("pct_desert"),
            "population_en_desert": stats.get("pop_in_desert"),
            "apl_mediane_france": stats.get("national_apl_median"),
            "dept_le_plus_touche": stats.get("worst_dept"),
            "pct_desert_dept_max": stats.get("worst_dept_pct"),
            "nb_departements": stats.get("nb_departments"),
            "sources": [
                "DREES APL (latest GP workbook)",
                "INSEE RP2021 commune population",
                "AMELI demographie-exercices-liberaux",
                "france-geojson",
            ],
        },
    }
    (PROCESSED_DIR / "quality_report.json").write_text(
        json.dumps(quality_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run() -> None:
    print("=" * 60)
    print("🚀  France Healthcare Access — ELT Pipeline")
    print("=" * 60)

    # ── Step 1: Extract → raw.* ───────────────────────────────────────────────
    print("\n[1/3] Extract — loading raw data into PostgreSQL (raw.*)")
    extract_apl(RAW_DIR)
    extract_rpps(RAW_DIR)
    extract_population(RAW_DIR)
    extract_dept_geojson(RAW_DIR)      # File only (used directly by Plotly)

    # ── Step 2: Transform → staging.* → mart.* ───────────────────────────────
    print("\n[2/3] Transform — staging & mart layers")
    run_transform()

    # ── Step 3: Validate ─────────────────────────────────────────────────────
    print("\n[3/3] Validate — quality checks & pipeline run record")
    report = validate()
    if report["errors"]:
        raise SystemExit(
            f"\n❌ Pipeline failed — {len(report['errors'])} critical error(s)\n"
            + "\n".join(f"   • {e}" for e in report["errors"])
        )

    _write_local_snapshots(fact, dims, report)

    s = report["stats"]
    print(
        f"\n✅  Pipeline completed successfully (run_id: {report['run_id'][:8]}…)\n"
        f"    Communes: {s['nb_communes']:,} | Deserts: {s['pct_desert']:.1f}% "
        f"| Pop. affected: {s['pop_in_desert']:,.0f}\n"
        f"    Departments: {s['nb_departments']} | National APL median: {s['national_apl_median']}"
    )


if __name__ == "__main__":
    run()
