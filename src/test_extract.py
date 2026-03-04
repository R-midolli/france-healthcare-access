import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from extract import extract_apl, extract_rpps, extract_geodata, extract_population

def test_all():
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print("--- Testing APL ---")
    try:
        extract_apl(out_dir)
    except Exception as e:
        print(f"APL Extraction Failed: {e}")

    print("\n--- Testing RPPS ---")
    try:
        extract_rpps(out_dir)
    except Exception as e:
        print(f"RPPS Extraction Failed: {e}")

    print("\n--- Testing Geodata ---")
    try:
        extract_geodata(out_dir)
    except Exception as e:
        print(f"Geodata Extraction Failed: {e}")

    print("\n--- Testing Population ---")
    try:
        extract_population(out_dir)
    except Exception as e:
        print(f"Population Extraction Failed: {e}")

if __name__ == "__main__":
    test_all()
