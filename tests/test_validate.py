import pandas as pd
from src.validate import validate

def test_validate_warnings(tmp_path):
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    
    pd.DataFrame({
        "codgeo": ["00001", "00002"],
        "apl_mg": [3.5, 1.0],
        "apl_cat": ["correct", "désert_critique"],
        "population": [1500, 2000],
        "dept": ["01", "02"],
        "is_desert": [0, 1]
    }).to_parquet(proc_dir / "communes_enriched.parquet")
    
    pd.DataFrame({
        "dept": ["01", "02"],
        "pct_desert": [0.0, 100.0],
        "apl_mediane": [3.5, 1.0],
        "med_pour_10k": [2.5, 1.0]
    }).to_parquet(proc_dir / "departements_summary.parquet")
    
    report = validate(proc_dir)
    assert len(report["warnings"]) > 0
    assert "stats" in report
