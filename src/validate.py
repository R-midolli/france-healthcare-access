"""
validate.py — Validate mart.* tables after transformation.

Checks data quality invariants and writes a summary to mart.pipeline_runs.
Any critical error raises SystemExit to stop the pipeline.
"""

import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from db import get_engine


def validate() -> dict:
    """Read mart.fact_communes and mart.dim_departments and validate quality."""
    engine = get_engine()
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    communes = pd.read_sql("SELECT * FROM mart.fact_communes", engine)
    depts    = pd.read_sql("SELECT * FROM mart.dim_departments", engine)

    errors   = []
    warnings = []

    # ── Critical errors ──────────────────────────────────────────────────────
    if communes["codgeo"].duplicated().any():
        n = int(communes["codgeo"].duplicated().sum())
        errors.append(f"{n} duplicate codgeo values in fact_communes")

    if not communes["apl_mg"].between(0, 200).all():
        n = int((~communes["apl_mg"].between(0, 200)).sum())
        errors.append(f"{n} APL values outside valid range [0, 200]")

    valid_cats = {"critical_desert", "under_served", "adequate", "well_served"}
    bad_cats = set(communes["apl_category"].unique()) - valid_cats
    if bad_cats:
        errors.append(f"Unexpected apl_category values: {bad_cats}")

    if len(communes) < 30_000:
        errors.append(f"Too few communes: {len(communes)} (expected >30,000)")

    if not (90 <= len(depts) <= 110):
        errors.append(f"Unexpected department count: {len(depts)} (expected 90–110)")

    # ── Warnings ─────────────────────────────────────────────────────────────
    total_pop = communes["population"].sum()
    if not (65_000_000 < total_pop < 72_000_000):
        warnings.append(
            f"Total population {total_pop:,.0f} is outside expected 65M–72M range"
        )

    apl_median = communes["apl_mg"].median()
    if not (1.5 < apl_median < 6.0):
        warnings.append(f"National APL median {apl_median:.2f} is outside expected range")

    rural_depts = {"23", "58", "61", "52", "55", "36", "48", "70", "03", "87", "19"}
    worst_dept = depts.iloc[0]["dept"] if len(depts) else ""
    if worst_dept not in rural_depts:
        warnings.append(
            f"Worst-affected dept is {worst_dept!r} — expected a rural department"
        )

    # ── Stats ────────────────────────────────────────────────────────────────
    nb_desert  = int(communes["is_desert"].sum())
    pop_desert = int(communes.loc[communes["is_desert"] == 1, "population"].sum())
    stats = {
        "nb_communes":         int(len(communes)),
        "nb_desert_communes":  nb_desert,
        "pct_desert":          round(float(nb_desert / len(communes) * 100), 1),
        "pop_in_desert":       pop_desert,
        "national_apl_median": round(float(apl_median), 2),
        "nb_departments":      int(len(depts)),
        "worst_dept":          worst_dept,
        "worst_dept_pct":      round(float(depts.iloc[0]["pct_desert"]), 1) if len(depts) else 0,
    }

    # ── Print report ─────────────────────────────────────────────────────────
    if errors:
        print(f"  ❌ {len(errors)} critical error(s):")
        for e in errors:
            print(f"     • {e}")
    else:
        print("  ✅ Data quality OK")

    if warnings:
        print(f"  ⚠️  {len(warnings)} warning(s):")
        for w in warnings:
            print(f"     • {w}")

    print(f"\n  📊 Key stats:")
    for k, v in stats.items():
        print(f"     {k}: {v:,}" if isinstance(v, int) else f"     {k}: {v}")

    # ── Write pipeline run record ─────────────────────────────────────────────
    status = "success" if not errors else "failed"
    run_df = pd.DataFrame([{
        "run_id":        run_id,
        "started_at":    started_at,
        "completed_at":  datetime.now(timezone.utc),
        "status":        status,
        "nb_communes":   stats["nb_communes"],
        "nb_departments":stats["nb_departments"],
        "errors":        "; ".join(errors) if errors else None,
        "warnings":      "; ".join(warnings) if warnings else None,
    }])
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mart.pipeline_runs (
                run_id        TEXT PRIMARY KEY,
                started_at    TIMESTAMPTZ,
                completed_at  TIMESTAMPTZ,
                status        TEXT,
                nb_communes   INTEGER,
                nb_departments INTEGER,
                errors        TEXT,
                warnings      TEXT
            )
        """))
        run_df.to_sql("pipeline_runs", conn, schema="mart",
                      if_exists="append", index=False)

    return {"errors": errors, "warnings": warnings, "stats": stats, "run_id": run_id}
