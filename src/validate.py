import pandas as pd
import json
from pathlib import Path


def validate(processed_dir: Path) -> dict:
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
    ruraux = {"23","58","61","52","55","36","48","70","03","87","19", "01"}
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
