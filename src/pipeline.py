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
