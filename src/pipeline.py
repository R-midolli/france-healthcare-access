"""
pipeline.py — ELT Orchestrator.

Executes the full data pipeline:
  Step 1 — Extract: APIs → raw.* (source data, no transformations)
  Step 2 — Transform: raw.* → staging.* → mart.* (clean, join, aggregate)
  Step 3 — Validate: mart.* quality checks + record pipeline run

Run monthly or on demand:
    uv run python src/pipeline.py
"""

from pathlib import Path

from dotenv import load_dotenv  # optional: loads .env if present

# Load .env if available (ignored if file doesn't exist)
try:
    load_dotenv()
except ImportError:
    pass

from extract   import extract_apl, extract_rpps, extract_population, extract_dept_geojson
from transform import run_transform
from validate  import validate

RAW_DIR = Path("data/raw")


def run() -> None:
    print("=" * 60)
    print("🚀  France Healthcare Access — ELT Pipeline")
    print("=" * 60)

    # ── Step 1: Extract → raw.* ───────────────────────────────────────────────
    print("\n[1/3] Extract — loading raw data into PostgreSQL (raw.*)")
    extract_apl(RAW_DIR)
    extract_rpps(RAW_DIR)
    extract_population()
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

    s = report["stats"]
    print(
        f"\n✅  Pipeline completed successfully (run_id: {report['run_id'][:8]}…)\n"
        f"    Communes: {s['nb_communes']:,} | Deserts: {s['pct_desert']:.1f}% "
        f"| Pop. affected: {s['pop_in_desert']:,.0f}\n"
        f"    Departments: {s['nb_departments']} | National APL median: {s['national_apl_median']}"
    )


if __name__ == "__main__":
    run()
