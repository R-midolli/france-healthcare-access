"""
France Healthcare Access — Premium BI Dashboard
================================================
Data: DREES APL · INSEE RP2021 · RPPS
DB  : PostgreSQL — mart.fact_communes, mart.dim_departments
"""

import json
import os
from pathlib import Path

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="France Healthcare Access",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS — Premium dark BI theme ────────────────────────────────────────
st.markdown("""
<style>
  /* Import font */
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Cormorant+Garamond:wght@600;700&display=swap');

  :root {
      --bg-primary:    #08111f;
      --bg-card:       #101a2b;
      --bg-card2:      #162338;
      --accent:        #6ea8ff;
      --accent2:       #8b7dff;
    --danger:        #ef4444;
    --warning:       #f59e0b;
    --success:       #10b981;
    --info:          #3b82f6;
      --text-primary:  #f8fafc;
            --text-muted:    #dbe7f5;
      --border:        #334155;
  }

  html, body, [class*="css"] {
        font-family: 'Manrope', sans-serif;
    color: var(--text-primary);
  }

  /* App background */
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(110,168,255,0.16), transparent 26%),
            radial-gradient(circle at top right, rgba(139,125,255,0.12), transparent 22%),
            linear-gradient(180deg, #08111f 0%, #091423 100%);
    }
  section[data-testid="stSidebar"] {
    background: var(--bg-card);
    border-right: 1px solid var(--border);
  }

  /* Header banner */
  .header-banner {
        background: linear-gradient(135deg, rgba(18,26,47,0.98) 0%, rgba(23,33,59,0.98) 50%, rgba(18,24,50,0.98) 100%);
    border: 1px solid var(--border);
        border-radius: 18px;
        padding: 30px 38px;
    margin-bottom: 24px;
    position: relative;
    overflow: hidden;
        box-shadow: 0 18px 50px rgba(0, 0, 0, 0.22);
  }
  .header-banner::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: radial-gradient(ellipse at 70% 50%, rgba(79,127,255,0.08) 0%, transparent 70%);
    pointer-events: none;
  }
  .header-title {
        font-family: 'Cormorant Garamond', serif;
        font-size: 2.35rem;
    font-weight: 700;
        letter-spacing: -0.02em;
        background: linear-gradient(135deg, #f8fafc 0%, #c7d2fe 55%, #93c5fd 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0 0 6px 0;
  }
  .header-sub {
    font-size: 0.85rem;
    color: var(--text-muted);
        font-weight: 500;
    letter-spacing: 0.01em;
  }
  .badge {
    display: inline-block;
        background: rgba(110,168,255,0.18);
        border: 1px solid rgba(110,168,255,0.35);
        color: #dbeafe;
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 0.72rem;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    margin-left: 10px;
  }

  /* KPI cards */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 14px;
    margin-bottom: 24px;
  }
  .kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
        border-radius: 14px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
        transition: border-color 0.2s, transform 0.2s ease, box-shadow 0.2s ease;
        box-shadow: 0 12px 32px rgba(5, 8, 18, 0.22);
  }
  .kpi-card::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: var(--accent-color, var(--accent));
    border-radius: 12px 12px 0 0;
  }
    .kpi-card:hover {
        border-color: rgba(91,140,255,0.48);
        transform: translateY(-1px);
        box-shadow: 0 18px 36px rgba(5, 8, 18, 0.28);
    }
  .kpi-label {
    font-size: 0.72rem;
        font-weight: 700;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 6px;
  }
  .kpi-value {
    font-size: 1.75rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    color: var(--text-primary);
    line-height: 1;
  }
  .kpi-sub {
    font-size: 0.78rem;
    margin-top: 5px;
    font-weight: 500;
  }
  .kpi-sub.danger { color: var(--danger); }
  .kpi-sub.warning { color: var(--warning); }
  .kpi-sub.success { color: var(--success); }

  /* Tab styling */
  button[data-baseweb="tab"] {
        font-size: 0.9rem !important;
        font-weight: 700 !important;
    letter-spacing: 0.02em !important;
        color: #d6e2f2 !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: var(--text-primary) !important;
  }

  /* Chart containers */
  .chart-container {
    background: var(--bg-card);
    border: 1px solid var(--border);
        border-radius: 14px;
    padding: 20px;
    margin-bottom: 16px;
  }
  .chart-title {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 4px;
  }
  .chart-sub {
    font-size: 0.75rem;
    color: var(--text-muted);
    margin-bottom: 12px;
  }

    .insight-panel {
        background: linear-gradient(180deg, rgba(16,26,43,0.96) 0%, rgba(12,22,38,0.98) 100%);
        border: 1px solid rgba(148,163,184,0.22);
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 14px 32px rgba(2, 8, 23, 0.24);
    }
    .insight-panel h3 {
        margin: 0 0 12px 0;
        font-size: 1.35rem;
        color: #f8fafc;
    }
    .insight-list {
        display: grid;
        gap: 10px;
        margin-bottom: 16px;
    }
    .insight-item {
        background: rgba(37, 99, 235, 0.12);
        border: 1px solid rgba(96, 165, 250, 0.22);
        border-radius: 12px;
        padding: 10px 12px;
    }
    .insight-item strong {
        display: block;
        font-size: 0.78rem;
        color: #93c5fd;
        margin-bottom: 2px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .insight-item span {
        color: #f8fafc;
        font-size: 0.95rem;
        font-weight: 600;
    }
    .mini-ranking {
        display: grid;
        gap: 10px;
        margin-top: 14px;
    }
    .mini-ranking-row {
        display: grid;
        grid-template-columns: 42px 1fr auto auto;
        gap: 10px;
        align-items: center;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(148,163,184,0.14);
        border-radius: 12px;
        padding: 10px 12px;
    }
    .mini-rank-badge {
        width: 32px;
        height: 32px;
        border-radius: 999px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(59,130,246,0.18);
        color: #bfdbfe;
        font-weight: 800;
        font-size: 0.82rem;
    }
    .mini-rank-name {
        color: #f8fafc;
        font-weight: 600;
        line-height: 1.2;
    }
    .mini-rank-metric {
        color: #fca5a5;
        font-weight: 700;
        font-size: 0.88rem;
    }
    .mini-rank-sub {
        color: #cbd5e1;
        font-size: 0.82rem;
    }

    .compare-panel {
        background: linear-gradient(180deg, rgba(16,26,43,0.98) 0%, rgba(12,22,38,0.98) 100%);
        border: 1px solid rgba(148,163,184,0.20);
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 14px 32px rgba(2, 8, 23, 0.22);
        min-height: 500px;
        overflow: hidden;
        box-sizing: border-box;
    }
    .compare-panel-title {
        color: #f8fafc;
        font-size: 1rem;
        font-weight: 700;
        margin-bottom: 12px;
    }
    .compare-grid {
        display: grid;
        gap: 10px;
        width: 100%;
        box-sizing: border-box;
    }
    .compare-row {
        display: grid;
        grid-template-columns: minmax(140px, 1.45fr) minmax(86px, 0.92fr) minmax(72px, 0.72fr) minmax(72px, 0.72fr) minmax(86px, 0.82fr) minmax(88px, 0.88fr);
        gap: 8px;
        align-items: center;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(148,163,184,0.12);
        border-radius: 12px;
        padding: 10px 12px;
        width: 100%;
        box-sizing: border-box;
        overflow: hidden;
    }
    .compare-row.header {
        background: rgba(59,130,246,0.12);
        border-color: rgba(96,165,250,0.18);
        color: #bfdbfe;
        font-weight: 700;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .compare-cell {
        color: #f8fafc;
        font-size: 0.86rem;
        font-weight: 500;
        min-width: 0;
        word-break: break-word;
        overflow-wrap: anywhere;
    }
    .compare-cell.muted {
        color: #cbd5e1;
    }
    .compare-cell.danger {
        color: #fca5a5;
        font-weight: 700;
    }

    .profile-summary-panel {
        background: linear-gradient(180deg, rgba(16,26,43,0.98) 0%, rgba(12,22,38,0.98) 100%);
        border: 1px solid rgba(148,163,184,0.20);
        border-radius: 16px;
        padding: 14px;
        box-shadow: 0 14px 32px rgba(2, 8, 23, 0.20);
    }
    .profile-summary-title {
        color: #f8fafc;
        font-size: 0.98rem;
        font-weight: 700;
        margin-bottom: 10px;
    }
    .profile-summary-grid {
        display: grid;
        gap: 10px;
    }
    .profile-summary-row {
        display: grid;
        grid-template-columns: minmax(120px, 1.2fr) minmax(90px, 0.9fr) minmax(90px, 0.9fr);
        gap: 10px;
        align-items: center;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(148,163,184,0.12);
        border-radius: 12px;
        padding: 10px 12px;
    }
    .profile-summary-row.header {
        background: rgba(59,130,246,0.12);
        border-color: rgba(96,165,250,0.18);
        color: #bfdbfe;
        font-weight: 700;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .profile-summary-cell {
        color: #f8fafc;
        font-size: 0.9rem;
        font-weight: 500;
    }
    .profile-summary-cell.muted {
        color: #cbd5e1;
    }

  /* Sidebar */
  .sidebar-section {
        background: rgba(255,255,255,0.06);
        border-radius: 12px;
    padding: 14px;
    margin-bottom: 12px;
  }

    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li,
    [data-testid="stMarkdownContainer"] span,
    label,
    .stCaption,
    [data-testid="stCaptionContainer"] {
        color: var(--text-primary) !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"],
    [data-testid="stSidebar"] label {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
        color: #bfd0e3 !important;
    }

    [data-baseweb="select"] > div,
    .stSelectbox [data-baseweb="select"] > div,
    .stMultiSelect [data-baseweb="select"] > div {
        background: #f8fafc !important;
        color: #0f172a !important;
        border-color: #94a3b8 !important;
    }

    .stRadio [role="radiogroup"] label,
    .stSlider label,
    .stSelectbox label {
        font-weight: 700 !important;
        color: #e2e8f0 !important;
    }

    .stRadio [role="radiogroup"] label span {
        color: #f1f5f9 !important;
        font-size: 0.85rem !important;
    }

    .stAlert {
        border: 1px solid var(--border) !important;
    }

    [role="tooltip"],
    [data-baseweb="tooltip"] {
        background: #f8fafc !important;
        color: #0f172a !important;
        border: 1px solid #cbd5e1 !important;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.28) !important;
    }

    [role="tooltip"] *,
    [data-baseweb="tooltip"] * {
        color: #0f172a !important;
    }

    [data-testid="stWidgetLabelHelp"] {
        color: #cbd5e1 !important;
    }

    .stDataFrame, [data-testid="stTable"] {
        color: #f8fafc !important;
    }

    .stPlotlyChart {
        border-radius: 12px;
    }

  /* Divider */
  hr { border-color: var(--border) !important; }

  /* Streamlit metric override (kept for comp.) */
  [data-testid="metric-container"] {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 16px;
  }

  /* Footer */
  .footer-note {
    text-align: center;
    color: var(--text-muted);
    font-size: 0.72rem;
    padding: 16px 0 8px;
    letter-spacing: 0.04em;
  }
  a { color: #93c5fd; text-decoration: none; }
  a:hover { text-decoration: underline; }
  /* Navigation Buttons Style - Dark & Premium */
  .stButton > button {
      width: 100% !important;
      border-radius: 12px !important;
      padding: 12px 24px !important;
      font-weight: 800 !important;
      font-size: 1rem !important;
      transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
      text-transform: uppercase !important;
      letter-spacing: 0.05em !important;
      background-color: #101a2b !important; /* Dark like the cards */
      color: #ffffff !important;           /* Pure white text */
      border: 1px solid #334155 !important;
  }

  .stButton > button:hover {
      background-color: #162338 !important;
      border-color: #3b82f6 !important;
      box-shadow: 0 4px 15px rgba(59, 130, 246, 0.2) !important;
      transform: translateY(-1px) !important;
  }

  /* Primary Button (The Active Tab / Highlighted) */
  button[kind="primary"] {
      background-color: #1e293b !important; /* Slightly lighter to stand out */
      color: #ffffff !important;
      border: 2px solid #3b82f6 !important; /* Vivid blue border for active state */
  }
  button[kind="primary"]:hover {
      background-color: #2563eb !important;
      border-color: #60a5fa !important;
      box-shadow: 0 6px 20px rgba(59, 130, 246, 0.4) !important;
  }

  /* Hide sidebar toggle menu button strictly */
  [data-testid="stSidebarNav"],
  button[kind="header"] {
      display: none !important;
  }
  section[data-testid="stSidebar"] {
      display: none !important;
  }
</style>
""", unsafe_allow_html=True)

# ── Paths & DB ───────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
DEPT_GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/"
    "departements-version-simplifiee.geojson"
)

_POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://healthcare:healthcare@localhost:5433/healthcare",
)

COMMUNE_CATEGORY_LABELS = {
    "critical_desert": {"fr": "Désert critique", "en": "Critical desert"},
    "under_served": {"fr": "Sous-doté", "en": "Under-served"},
    "adequate": {"fr": "Couverture correcte", "en": "Adequate"},
    "well_served": {"fr": "Bien couvert", "en": "Well served"},
}
CATEGORY_COLORS = {
    "critical_desert": "#ef4444",
    "under_served": "#f59e0b",
    "adequate": "#3b82f6",
    "well_served": "#10b981",
}
REGION_NAMES = {
    "11": "Île-de-France",
    "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté",
    "28": "Normandie",
    "32": "Hauts-de-France",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "53": "Bretagne",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur",
    "94": "Corse",
    "01": "Guadeloupe",
    "02": "Martinique",
    "03": "Guyane",
    "04": "La Réunion",
    "06": "Mayotte",
}

# ── Translation strings ────────────────────────────────────────────────────────
T = {
    "title":      {"fr": "Déserts Médicaux en France",    "en": "Medical Deserts in France"},
    "subtitle":   {"fr": "Observatoire de l'accessibilité aux soins de premier recours",
                   "en": "Primary healthcare accessibility observatory"},
    "k1":         {"fr": "Communes analysées",    "en": "Communes analysed"},
    "k2":         {"fr": "En désert médical",     "en": "In medical desert"},
    "k3":         {"fr": "Population concernée",  "en": "Affected population"},
    "k4":         {"fr": "APL médiane France",    "en": "France median APL"},
    "k5":         {"fr": "Déserts critiques",     "en": "Critical deserts"},
    "f_dept":     {"fr": "Filtrer par département", "en": "Filter by department"},
    "threshold":  {"fr": "Seuil désert médical (APL)",   "en": "Medical desert threshold (APL)"},
    "t1":         {"fr": "🗺️ Carte", "en": "🗺️ Map"},
    "t2":         {"fr": "📊 Analyse nationale", "en": "📊 National Analysis"},
    "t3":         {"fr": "🏆 Classement",         "en": "🏆 Ranking"},
    "t4":         {"fr": "📋 Fiche département",  "en": "📋 Department Profile"},
    "source":     {"fr": "Sources : DREES · INSEE RP2021 · AMELI · france-geojson",
                   "en": "Sources: DREES · INSEE RP2021 · AMELI · france-geojson"},
    "map_metric": {"fr": "APL médiane par département",     "en": "Median APL by department"},
    "map_pct":    {"fr": "% communes en désert",            "en": "% desert communes"},
    "no_map":     {"fr": "Fichier GeoJSON introuvable. Exécutez d'abord le pipeline.",
                   "en": "GeoJSON not found. Run the pipeline first."},
}

# ── Plotly layout defaults (dark theme) ───────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="#101a2b",
    plot_bgcolor="#101a2b",
    font=dict(family="Manrope", color="#f8fafc", size=13),
    xaxis=dict(
        gridcolor="#334155",
        linecolor="#475569",
        zerolinecolor="#64748b",
        tickfont=dict(color="#e2e8f0"),
        titlefont=dict(color="#f8fafc"),
    ),
    yaxis=dict(
        gridcolor="#334155",
        linecolor="#475569",
        zerolinecolor="#64748b",
        tickfont=dict(color="#e2e8f0"),
        titlefont=dict(color="#f8fafc"),
    ),
    legend=dict(font=dict(color="#f8fafc")),
    margin=dict(t=40, r=20, b=40, l=20),
)
COLOR_SCALE  = "RdYlGn_r"      # red=bad (desert), green=good
COLOR_SCALE2 = [[0,"#ef4444"], [0.5,"#f59e0b"], [1,"#10b981"]]


def _categorise_apl(apl: float) -> str:
    if apl < 1.5:
        return "critical_desert"
    if apl < 2.5:
        return "under_served"
    if apl < 4.0:
        return "adequate"
    return "well_served"


def _normalize_communes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"apl_cat": "apl_category"}).copy()
    if "apl_category" in df.columns:
        df["apl_category"] = df["apl_category"].replace(
            {
                "désert_critique": "critical_desert",
                "sous-doté": "under_served",
                "correct": "adequate",
                "bien_doté": "well_served",
            }
        )
    else:
        df["apl_category"] = df["apl_mg"].apply(_categorise_apl)

    if "dept" not in df.columns:
        df["dept"] = df["codgeo"].astype(str).str[:2]

    df["dept"] = df["dept"].astype(str).str.strip().str.zfill(2)
    df["codgeo"] = df["codgeo"].astype(str).str.strip().str.zfill(5)
    df["apl_mg"] = pd.to_numeric(df["apl_mg"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0)
    df["is_desert"] = (df["apl_mg"] < 2.5).astype(int)
    return df.dropna(subset=["apl_mg", "codgeo"])


def _normalize_departments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "apl_mediane": "apl_median",
            "med_pour_10k": "doctors_per_10k",
            "population_totale": "total_population",
            "nb_communes_desert": "nb_desert",
            "nb_desert_critique": "nb_critical",
        }
    ).copy()
    df["dept"] = df["dept"].astype(str).str.strip().str.zfill(2)
    numeric_cols = [
        "apl_median",
        "apl_min",
        "apl_max",
        "pct_desert",
        "nb_communes",
        "nb_desert",
        "nb_critical",
        "nb_medecins",
        "doctors_per_10k",
        "total_population",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_parquet(name: str) -> pd.DataFrame:
    path = PROCESSED_DIR / name
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def _format_int(value: float | int) -> str:
    return f"{int(round(value)):,}"


def _format_pct(value: float) -> str:
    return f"{value:.1f}%"


def _apl_status(apl: float, language: str) -> str:
    if apl < 1.5:
        return "Très insuffisant" if language == "fr" else "Very low access"
    if apl < 2.5:
        return "Sous le seuil" if language == "fr" else "Below threshold"
    if apl < 4.0:
        return "Correct" if language == "fr" else "Acceptable"
    return "Confortable" if language == "fr" else "Comfortable"


def _risk_status(pct: float, language: str) -> str:
    if pct >= 40:
        return "Alerte élevée" if language == "fr" else "High alert"
    if pct >= 20:
        return "Sous surveillance" if language == "fr" else "Watch closely"
    return "Situation plutôt stable" if language == "fr" else "Relatively stable"


def _format_apl_gap(value: float, language: str) -> str:
    if value >= 0:
        return f"+{value:.2f} {'au-dessus' if language == 'fr' else 'above'}"
    return f"{value:.2f} {'sous le seuil' if language == 'fr' else 'below threshold'}"


def _format_gap_short(value: float) -> str:
    return f"{value:+.2f}"


def _category_breakdown(frame: pd.DataFrame, language: str) -> pd.DataFrame:
    order = ["critical_desert", "under_served", "adequate", "well_served"]
    counts = frame["apl_category"].value_counts().reindex(order, fill_value=0)
    total = max(int(len(frame)), 1)
    return pd.DataFrame(
        {
            "category": order,
            "label": [COMMUNE_CATEGORY_LABELS[key][language] for key in order],
            "count": counts.values,
            "share": counts.values / total * 100,
            "color": [CATEGORY_COLORS[key] for key in order],
        }
    )


def _build_department_view(departments: pd.DataFrame, communes_frame: pd.DataFrame) -> pd.DataFrame:
    dept_desert_live = (
        communes_frame.groupby("dept")
        .agg(
            nb_desert=("is_desert", "sum"),
            nb_communes=("is_desert", "count"),
            pct_desert=("is_desert", lambda x: round(x.mean() * 100, 2)),
            pop_desert=("population", lambda x: (x * communes_frame.loc[x.index, "is_desert"]).sum()),
        )
        .reset_index()
    )
    merged = departments.drop(columns=["nb_desert", "pct_desert"], errors="ignore").merge(
        dept_desert_live,
        on="dept",
        how="left",
    )
    merged["pop_desert"] = pd.to_numeric(merged["pop_desert"], errors="coerce").fillna(0)
    return merged.sort_values("pct_desert", ascending=False).reset_index(drop=True)


def _build_department_reference(communes_frame: pd.DataFrame, geojson: dict | None) -> pd.DataFrame:
    name_rows: list[dict[str, str]] = []
    if geojson and geojson.get("features"):
        for feature in geojson["features"]:
            props = feature.get("properties", {})
            dept = str(props.get("code", "")).strip().zfill(2)
            if dept:
                name_rows.append({"dept": dept, "dept_name": props.get("nom", dept)})

    names = pd.DataFrame(name_rows).drop_duplicates(subset=["dept"]) if name_rows else pd.DataFrame(columns=["dept", "dept_name"])
    region_ref = (
        communes_frame[["dept", "reg"]]
        .dropna()
        .assign(reg=lambda frame: frame["reg"].astype(str).str.strip().str.zfill(2))
        .drop_duplicates(subset=["dept"])
        .rename(columns={"reg": "region_code"})
    )
    ref = region_ref.merge(names, on="dept", how="left")
    ref["dept_name"] = ref["dept_name"].fillna(ref["dept"])
    ref["region_name"] = ref["region_code"].map(REGION_NAMES).fillna(ref["region_code"])
    ref["dept_label"] = ref["dept"] + " · " + ref["dept_name"]
    # Sort numerically by dept code (01, 02, ..., 2A, 2B, ..., 97)
    ref["_sort_key"] = ref["dept"].apply(
        lambda d: (0, int(d)) if d.isdigit() else (1, ord(d[0]) * 1000 + ord(d[-1]))
    )
    return ref.sort_values("_sort_key").drop(columns=["_sort_key"]).reset_index(drop=True)

# ── Data loading — reads exclusively from mart.* in PostgreSQL ────────────────
@st.cache_resource
def _get_engine():
    return create_engine(_POSTGRES_URL, pool_pre_ping=True)


@st.cache_data(ttl=3600)
def load_communes() -> pd.DataFrame:
    try:
        return _normalize_communes(pd.read_sql("SELECT * FROM mart.fact_communes", _get_engine()))
    except Exception as e:
        try:
            return _normalize_communes(_load_parquet("communes_enriched.parquet"))
        except Exception:
            st.error(
                f"❌ Cannot load commune data from PostgreSQL or local parquet: {e}\n\n"
                "Start PostgreSQL and run the pipeline, or keep data/processed populated."
            )
            st.stop()


@st.cache_data(ttl=3600)
def load_depts() -> pd.DataFrame:
    try:
        return _normalize_departments(pd.read_sql("SELECT * FROM mart.dim_departments", _get_engine()))
    except Exception as e:
        try:
            return _normalize_departments(_load_parquet("departements_summary.parquet"))
        except Exception:
            st.error(f"❌ Cannot load department data from PostgreSQL or local parquet: {e}")
            st.stop()


@st.cache_data(ttl=3600)
def load_last_refresh() -> str:
    try:
        result = pd.read_sql(
            "SELECT completed_at FROM mart.pipeline_runs "
            "WHERE status = 'success' ORDER BY completed_at DESC LIMIT 1",
            _get_engine(),
        )
        if not result.empty:
            return str(result.iloc[0, 0])[:16]
    except Exception:
        report_path = PROCESSED_DIR / "quality_report.json"
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
                return report.get("generated_at") or report.get("stats", {}).get("source_date") or "local file"
            except Exception:
                pass
    return "unknown"


@st.cache_data(ttl=3600)
def load_dept_geojson() -> dict | None:
    p = RAW_DIR / "departements.geojson"
    if p.exists():
        with open(p, encoding="utf-8") as f:
            geo = json.load(f)
        if geo.get("features"):
            return geo
    try:
        response = httpx.get(DEPT_GEOJSON_URL, timeout=20, follow_redirects=True)
        response.raise_for_status()
        geo = response.json()
        if geo.get("features"):
            return geo
    except Exception:
        return None
    return None

# ── Sidebar (Collapsed/Info only) ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style='padding:8px 0 16px'>
          <div style='font-size:1.1rem;font-weight:700;color:#f1f5f9'>🏥 Accès aos cuidados</div>
          <div style='font-size:0.72rem;color:#94a3b8;margin-top:2px'>França · DREES · INSEE</div>
        </div>
    """, unsafe_allow_html=True)
    st.divider()
    st.caption(T["source"]["fr"])

L = "fr"

# ── Data Loading ──────────────────────────────────────────────────────────────
communes_raw = load_communes()
depts_raw    = load_depts()
geo_ref = load_dept_geojson()
dept_reference = _build_department_reference(communes_raw, geo_ref)

communes_raw = communes_raw.merge(
    dept_reference[["dept", "dept_name", "dept_label", "region_code", "region_name"]],
    on="dept",
    how="left",
)
depts_raw = depts_raw.merge(
    dept_reference[["dept", "dept_name", "dept_label", "region_code", "region_name"]],
    on="dept",
    how="left",
)

# ── Filter Section (Moved to Main) ───────────────────────────────────────────
st.markdown("### Filtrar dados" if L == "fr" else "### Filter data")
filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 0.8])

with filter_col1:
    all_regions_label = "Toutes les régions" if L == "fr" else "All regions"
    region_options = dept_reference["region_name"].dropna().drop_duplicates().sort_values().tolist()
    selected_region_names = st.multiselect(
        "Régions" if L == "fr" else "Regions",
        options=region_options,
        default=[],
        placeholder=all_regions_label,
    )

with filter_col2:
    all_departments_label = "Tous les départements" if L == "fr" else "All departments"
    dept_options_df = dept_reference.copy()
    if selected_region_names:
        dept_options_df = dept_options_df[dept_options_df["region_name"].isin(selected_region_names)]

    dept_options_df = dept_options_df.copy()
    dept_options_df["_sort_key"] = dept_options_df["dept"].apply(
        lambda d: (0, int(d)) if d.isdigit() else (1, ord(d[0]) * 1000 + ord(d[-1]))
    )
    dept_options_df = dept_options_df.sort_values("_sort_key").drop(columns=["_sort_key"])

    dept_option_labels = dept_options_df["dept_label"].tolist()
    selected_dept_labels = st.multiselect(
        "Départements" if L == "fr" else "Departments",
        options=dept_option_labels,
        default=[],
        placeholder=all_departments_label,
    )

with filter_col3:
    with st.expander("⚙️ Paramètres" if L == "fr" else "⚙️ Parameters"):
        use_custom_threshold = st.checkbox(
            "Seuil APL" if L == "fr" else "APL threshold",
            value=False,
        )
        threshold = 2.5
        if use_custom_threshold:
            threshold = st.slider(
                "APL",
                min_value=1.5, max_value=4.0, value=2.5, step=0.1,
            )
        else:
            st.caption("Fixo DREES: 2.5")

# ── Data filtering + threshold recomputation ──────────────────────────────────
communes = communes_raw.copy()
communes["is_desert"] = (communes["apl_mg"] < threshold).astype(int)
departments_live = _build_department_view(depts_raw, communes)

selected_region_codes = (
    dept_reference.loc[dept_reference["region_name"].isin(selected_region_names), "region_code"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
    if selected_region_names else []
)

selected_depts = (
    dept_reference.loc[dept_reference["dept_label"].isin(selected_dept_labels), "dept"]
    .dropna()
    .astype(str)
    .tolist()
    if selected_dept_labels else []
)

cf = communes.copy()
df = departments_live.copy()
if selected_region_codes:
    cf = cf[cf["region_code"].isin(selected_region_codes)].copy()
    df = df[df["region_code"].isin(selected_region_codes)].copy()
if selected_depts:
    cf = cf[cf["dept"].isin(selected_depts)].copy()
    df = df[df["dept"].isin(selected_depts)].copy()

dept_list = dept_reference["dept"].drop_duplicates().tolist()
profile_dept = selected_depts[0] if selected_depts else dept_list[0]
profile_index = dept_list.index(profile_dept) if profile_dept in dept_list else 0

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_communes  = len(cf)
nb_desert       = int(cf["is_desert"].sum())
pct_desert_nat  = cf["is_desert"].mean() * 100
pop_desert      = cf.loc[cf["is_desert"] == 1, "population"].sum()
pop_total       = cf["population"].sum()
apl_median      = cf["apl_mg"].median()
nb_critique     = int((cf["apl_mg"] < 1.5).sum())
pct_critique    = (cf["apl_mg"] < 1.5).mean() * 100
pop_desert_share = (pop_desert / pop_total * 100) if pop_total else 0
df = df.copy()
df["apl_gap"] = df["apl_median"] - threshold
departments_live = departments_live.copy()
departments_live["apl_gap"] = departments_live["apl_median"] - threshold
scope_label = (
    selected_dept_labels[0]
    if len(selected_dept_labels) == 1
    else (
        (f"{len(selected_dept_labels)} départements" if L == "fr" else f"{len(selected_dept_labels)} departments")
        if len(selected_dept_labels) > 1
        else (
            selected_region_names[0]
            if len(selected_region_names) == 1
            else (
                (f"{len(selected_region_names)} régions" if L == "fr" else f"{len(selected_region_names)} regions")
                if len(selected_region_names) > 1
                else ("France entière" if L == "fr" else "Whole France")
            )
        )
    )
)
if df.empty:
    st.warning("Aucune donnée pour la sélection actuelle." if L == "fr" else "No data for current selection.")
    st.stop()
worst_row = df.sort_values("pct_desert", ascending=False).iloc[0]
best_row = df.sort_values("pct_desert", ascending=True).iloc[0]
lowest_apl_row = df.sort_values("apl_median", ascending=True).iloc[0]
category_df = _category_breakdown(cf, L)
quality_notes = []
if len(communes_raw) > 10_000 and communes_raw["apl_mg"].nunique(dropna=True) < 100:
    quality_notes.append(
        "Le snapshot courant semble trop compressé pour une lecture fiable. Relancez le pipeline ou régénérez les fichiers de fallback."
        if L == "fr"
        else "The current snapshot looks overly compressed for reliable reading. Re-run the pipeline or refresh the fallback files."
    )
if len(depts_raw) > 50 and depts_raw["pct_desert"].nunique(dropna=True) < 5:
    quality_notes.append(
        "Les taux départementaux manquent de variation, ce qui indique souvent une source ou un snapshot incohérent."
        if L == "fr"
        else "Department rates show too little variation, which usually indicates a broken source parse or stale snapshot."
    )

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-banner">
  <div class="header-title">
    🏥 {T['title'][L]}
    <span class="badge">APL 2023</span>
  </div>
  <div class="header-sub">{T['subtitle'][L]} · {scope_label} · MAJ: {load_last_refresh()}</div>
</div>
""", unsafe_allow_html=True)

for note in quality_notes:
    st.warning(note)

# ── KPI CARDS ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #4f7fff">
      <div class="kpi-label">{"Communes analysées" if L=="fr" else "Communes analysed"}</div>
      <div class="kpi-value">{total_communes:,}</div>
      <div class="kpi-sub" style="color:#94a3b8">{len(df)} {"départements visibles" if L=="fr" else "departments in view"}</div>
    </div>""", unsafe_allow_html=True)

with c2:
    sub_class = "danger" if pct_desert_nat > 30 else ("warning" if pct_desert_nat > 20 else "success")
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #ef4444">
      <div class="kpi-label">{T['k2'][L]}</div>
      <div class="kpi-value">{pct_desert_nat:.1f}%</div>
      <div class="kpi-sub {sub_class}">{nb_desert:,} {"communes concernées" if L=="fr" else "communes concerned"}</div>
    </div>""", unsafe_allow_html=True)

with c3:
    pop_m = pop_desert / 1e6
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #f59e0b">
      <div class="kpi-label">{T['k3'][L]}</div>
      <div class="kpi-value">{pop_m:.1f}M</div>
      <div class="kpi-sub warning">{pop_desert_share:.1f}% {"de la pop." if L=="fr" else "of population"}</div>
    </div>""", unsafe_allow_html=True)

with c4:
    apl_class = "danger" if apl_median < 2.5 else ("warning" if apl_median < 3.5 else "success")
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #7c6af7">
      <div class="kpi-label">{T['k4'][L]}</div>
      <div class="kpi-value">{apl_median:.2f}</div>
      <div class="kpi-sub {apl_class}">{nb_critique:,} {"critiques" if L=="fr" else "critical"} · seuil {threshold:.1f}</div>
    </div>""", unsafe_allow_html=True)

# Only show comparison insights if there are multiple departments
if len(df) > 1:
    insight_left, insight_right = st.columns([1.4, 1])
    with insight_left:
        st.info(
            (
                f"Lecture rapide : {lowest_apl_row['dept_label']} a l'APL médiane la plus faible ({lowest_apl_row['apl_median']:.2f}), soit {_format_apl_gap(float(lowest_apl_row['apl_gap']), L)}."
                if L == "fr"
                else f"Quick read: {lowest_apl_row['dept_label']} has the lowest median APL ({lowest_apl_row['apl_median']:.2f}), {_format_apl_gap(float(lowest_apl_row['apl_gap']), L)}."
            )
        )
    with insight_right:
        st.success(
            (
                f"{best_row['dept_label']} reste le plus confortable · {_risk_status(float(best_row['pct_desert']), L)}."
                if L == "fr"
                else f"{best_row['dept_label']} is the most comfortable case · {_risk_status(float(best_row['pct_desert']), L)}."
            )
        )
else:
    st.info(
        (
            f"APL médiane : {lowest_apl_row['apl_median']:.2f} · {_apl_status(float(lowest_apl_row['apl_median']), L)} · {_format_apl_gap(float(lowest_apl_row['apl_gap']), L)}"
            if L == "fr"
            else f"Median APL: {lowest_apl_row['apl_median']:.2f} · {_apl_status(float(lowest_apl_row['apl_median']), L)} · {_format_apl_gap(float(lowest_apl_row['apl_gap']), L)}"
        )
    )

with st.expander("Comment lire les métriques" if L == "fr" else "How to read the metrics"):
    st.markdown(
        (
            "- **APL médiane**: accessibilité potentielle localisée médiane du périmètre affiché. Plus c'est élevé, meilleure est l'accessibilité.\n"
            "- **Écart au seuil APL**: différence entre l'APL médiane d'un département et le seuil sélectionné. En dessous de `0`, le département passe sous le seuil d'alerte.\n"
            "- **% communes en désert**: part des communes dont l'APL est inférieure au seuil configuré.\n"
            "- **Population concernée**: population vivant dans les communes sous le seuil.\n"
            "- **Communes critiques**: communes avec `APL < 1.5`, c'est le niveau le plus fragile du modèle."
            if L == "fr"
            else
            "- **Median APL**: median local potential accessibility for the displayed scope. Higher is better.\n"
            "- **APL gap to threshold**: difference between a department median APL and the selected threshold. Below `0`, the department falls under the alert line.\n"
            "- **% communes in desert**: share of communes with APL below the configured threshold.\n"
            "- **Affected population**: population living in communes below the threshold.\n"
            "- **Critical communes**: communes with `APL < 1.5`, the most fragile level in the model."
        )
    )

st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)

# ── CUSTOM NAVIGATION ────────────────────────────────────────────────────────
if "current_tab" not in st.session_state:
    st.session_state.current_tab = "overview"

nav_col1, nav_col2, nav_col3, nav_spacer = st.columns([1, 1.1, 1.4, 3])

with nav_col1:
    if st.button("🧭 Vue d'ensemble", use_container_width=True, 
                 type="primary" if st.session_state.current_tab == "overview" else "secondary"):
        st.session_state.current_tab = "overview"
        st.rerun()

with nav_col2:
    if st.button("📊 Comparer", use_container_width=True,
                 type="primary" if st.session_state.current_tab == "compare" else "secondary"):
        st.session_state.current_tab = "compare"
        st.rerun()

with nav_col3:
    if st.button("🎯 Focus département", use_container_width=True,
                 type="primary" if st.session_state.current_tab == "focus" else "secondary"):
        st.session_state.current_tab = "focus"
        st.rerun()

st.markdown("<div style='margin-bottom:20px'></div>", unsafe_allow_html=True)

active_tab = st.session_state.current_tab

# ════════════════════════════════════════════════════════════════════════════════
# VIEWS
# ════════════════════════════════════════════════════════════════════════════════
if active_tab == "overview":
    geo = load_dept_geojson()

    overview_left, overview_right = st.columns([1.7, 1])

    with overview_left:
        if geo is None:
            st.warning(T["no_map"][L])
        else:
            map_metric = st.radio(
                "La carte montre" if L == "fr" else "Map shows",
                [T["map_metric"][L], T["map_pct"][L]],
                horizontal=True,
            )
            use_pct = map_metric == T["map_pct"][L]
            color_col = "pct_desert" if use_pct else "apl_median"
            color_label = T["map_pct"][L] if use_pct else T["map_metric"][L]
            geo_key = "code"
            if geo["features"] and "properties" in geo["features"][0]:
                props = geo["features"][0]["properties"]
                geo_key = next(
                    (k for k in props if k in ("code", "CODE_DEPT", "DEP", "codgeo")),
                    list(props.keys())[0]
                )

            fig_map = px.choropleth_mapbox(
                df,
                geojson=geo,
                locations="dept",
                featureidkey=f"properties.{geo_key}",
                color=color_col,
                color_continuous_scale="RdYlGn_r" if use_pct else "RdYlGn",
                mapbox_style="carto-darkmatter",
                zoom=4.7,
                center={"lat": 46.5, "lon": 2.3},
                opacity=0.86,
                hover_data={
                    "dept": True,
                    "apl_median": ":.2f",
                    "pct_desert": ":.1f",
                    "doctors_per_10k": ":.1f" if "doctors_per_10k" in df.columns else False,
                    "total_population": ":,.0f",
                },
                labels={
                    "dept": "Dép.",
                    "apl_median": "APL médiane",
                    "pct_desert": "% désert",
                    "doctors_per_10k": "Médecins/10k",
                    "total_population": "Population",
                },
            )
            fig_map.update_layout(
                **DARK_LAYOUT,
                height=520,
                coloraxis_colorbar=dict(
                    title=color_label,
                    tickfont=dict(color="#f8fafc", size=12),
                    titlefont=dict(color="#f8fafc", size=12),
                    bgcolor="rgba(16,26,43,0.82)",
                    outlinecolor="#475569",
                ),
            )
            fig_map.update_layout(margin=dict(t=10, r=10, b=10, l=10))
            st.plotly_chart(fig_map, use_container_width=True)

    with overview_right:
        top5 = df.nlargest(min(5, len(df)), "pct_desert")[["dept_label", "pct_desert", "apl_median"]].copy()
        insights = [
            (
                "APL la plus faible" if L == "fr" else "Lowest APL",
                f"{lowest_apl_row['dept_label']} ({lowest_apl_row['apl_median']:.2f})",
            ),
            (
                "Écart au seuil" if L == "fr" else "Gap to threshold",
                _format_apl_gap(float(lowest_apl_row['apl_gap']), L),
            ),
            (
                "Référence haute" if L == "fr" else "Best reference",
                f"{best_row['dept_label']} · {best_row['apl_median']:.2f} APL",
            ),
            (
                "Communes critiques" if L == "fr" else "Critical communes",
                f"{nb_critique:,}",
            ),
            (
                "Seuil actif" if L == "fr" else "Active threshold",
                f"{threshold:.1f}",
            ),
        ]
        insight_html = "".join(
            f"<div class='insight-item'><strong>{label}</strong><span>{value}</span></div>"
            for label, value in insights
        )
        ranking_html = "".join(
            f"<div class='mini-ranking-row'>"
            f"<div class='mini-rank-badge'>{idx}</div>"
            f"<div class='mini-rank-name'>{row.dept_label}</div>"
            f"<div class='mini-rank-metric'>{_format_pct(float(row.pct_desert))}</div>"
            f"<div class='mini-rank-sub'>{row.apl_median:.2f} APL</div>"
            f"</div>"
            for idx, row in enumerate(top5.itertuples(index=False), start=1)
        )
        st.markdown(
            f"<div class='insight-panel'>"
            f"<h3>{'À retenir' if L == 'fr' else 'Key takeaways'}</h3>"
            f"<div class='insight-list'>{insight_html}</div>"
            f"<div class='chart-sub'>{'Top 5 des départements les plus exposés' if L == 'fr' else 'Top 5 most exposed departments'}</div>"
            f"<div class='mini-ranking'>{ranking_html}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "Ici, le tableau classe les départements selon la part de communes sous le seuil. Cela peut différer du classement par APL médiane."
            if L == "fr"
            else "This table ranks departments by the share of communes below the threshold. It can differ from the ranking based on median APL."
        )

    row_left, row_right = st.columns([1.2, 1])

    with row_left:
        bar_data = df.nsmallest(min(12, len(df)), "apl_gap").sort_values("apl_gap", ascending=True)
        fig_bar = px.bar(
            bar_data,
            x="apl_gap",
            y="dept_label",
            orientation="h",
            color="apl_gap",
            color_continuous_scale="RdYlGn",
            text=bar_data["apl_gap"].map(lambda v: _format_gap_short(float(v))),
        )
        fig_bar.update_traces(
            textposition="outside",
            cliponaxis=False,
            textfont=dict(color="#f8fafc", size=12),
            hovertemplate="%{y}<br>Écart au seuil: %{x:.2f}<extra></extra>",
        )
        fig_bar.update_layout(
            **DARK_LAYOUT,
            height=420,
            title=dict(
                text="Départements les plus proches ou sous le seuil" if L == "fr" else "Departments closest to or below the threshold",
                x=0,
                font=dict(size=14),
            ),
            coloraxis_showscale=False,
        )
        fig_bar.update_layout(margin=dict(t=60, r=48, b=20, l=20))
        fig_bar.add_vline(x=0, line_dash="dash", line_color="#ef4444")
        fig_bar.update_xaxes(title="Écart APL vs seuil" if L == "fr" else "APL gap vs threshold")
        fig_bar.update_yaxes(title="")
        st.plotly_chart(fig_bar, use_container_width=True)
        st.caption(
            "Lecture: ce graphique est trié par APL médiane relative au seuil. Un département peut être très bas en APL médiane sans être le pire en % de communes sous le seuil."
            if L == "fr"
            else "Reading note: this chart is ranked by median APL relative to the threshold. A department can have the lowest median APL without having the highest share of communes below threshold."
        )

    with row_right:
        fig_categories = px.bar(
            category_df.sort_values("share"),
            x="share",
            y="label",
            orientation="h",
            color="label",
            text=category_df.sort_values("share")["share"].map(_format_pct),
            color_discrete_map={row["label"]: row["color"] for _, row in category_df.iterrows()},
        )
        fig_categories.update_traces(
            textposition="outside",
            cliponaxis=False,
            textfont=dict(color="#f8fafc", size=12),
            hovertemplate="%{y}<br>%{x:.1f}% des communes<extra></extra>",
        )
        fig_categories.update_layout(
            **DARK_LAYOUT,
            height=420,
            title=dict(
                text="Répartition des communes par niveau d’accès" if L == "fr" else "Commune split by access level",
                x=0,
                font=dict(size=14),
            ),
            showlegend=False,
        )
        fig_categories.update_layout(margin=dict(t=60, r=48, b=20, l=20))
        fig_categories.update_xaxes(title="Part des communes" if L == "fr" else "Share of communes")
        fig_categories.update_yaxes(title="")
        st.plotly_chart(fig_categories, use_container_width=True)


elif active_tab == "compare":
    compare_controls_left, compare_controls_right = st.columns([1, 1])
    with compare_controls_left:
        metric_label = st.selectbox(
            "Classement sur" if L == "fr" else "Rank by",
            options=[
                "Écart au seuil APL" if L == "fr" else "APL gap to threshold",
                "% communes en désert",
                "APL médiane",
                "Médecins / 10k",
                "Population concernée",
            ],
        )
    with compare_controls_right:
        n_depts = len(df)
        if n_depts <= 1:
            top_n = n_depts
        else:
            slider_max = min(30, n_depts)
            slider_default = min(12, slider_max)
            top_n = st.slider(
                "Nombre de départements" if L == "fr" else "Number of departments",
                1, slider_max, slider_default,
            )

    metric_map = {
        "Écart au seuil APL" if L == "fr" else "APL gap to threshold": "apl_gap",
        "% communes en désert": "pct_desert",
        "APL médiane": "apl_median",
        "Médecins / 10k": "doctors_per_10k",
        "Population concernée": "pop_desert",
    }
    metric_col = metric_map[metric_label]
    ascending = metric_col in {"apl_gap", "apl_median"}
    ranked = df.dropna(subset=[metric_col]).sort_values(metric_col, ascending=ascending).head(top_n).copy()
    if ascending:
        ranked = ranked.sort_values(metric_col, ascending=False)

    compare_left, compare_right = st.columns([1.1, 0.9])

    with compare_left:
        fig_compare = px.bar(
            ranked.sort_values(metric_col, ascending=True),
            x=metric_col,
            y="dept_label",
            orientation="h",
            color=metric_col,
            color_continuous_scale="RdYlGn" if metric_col in {"apl_gap", "apl_median"} else "RdYlGn_r",
        )
        fig_compare.update_layout(
            **DARK_LAYOUT,
            height=500,
            title=dict(
                text=(f"Lecture par {metric_label}" if L == "fr" else f"View by {metric_label}"),
                x=0,
                font=dict(size=14),
            ),
            coloraxis_showscale=False,
        )
        fig_compare.update_layout(margin=dict(t=60, r=20, b=20, l=20))
        if metric_col == "apl_gap":
            fig_compare.add_vline(x=0, line_dash="dash", line_color="#ef4444")
        fig_compare.update_yaxes(title="")
        fig_compare.update_xaxes(title=metric_label)
        st.plotly_chart(fig_compare, use_container_width=True)
        compare_note = (
            "Le classement actuel repose sur l'écart entre APL médiane et seuil. Changez la métrique ci-dessus pour comparer plutôt le % désert, la densité médicale ou la population concernée."
            if L == "fr"
            else "The current ranking is driven by the gap between median APL and the threshold. Change the metric above to compare desert rate, doctor density, or affected population instead."
        )
        if metric_col == "apl_gap":
            st.caption(compare_note)

    with compare_right:
        compare_rows = "".join(
            f"<div class='compare-row'>"
            f"<div class='compare-cell'>{row.dept_label}</div>"
            f"<div class='compare-cell danger'>{_format_gap_short(float(row.apl_gap))}</div>"
            f"<div class='compare-cell'>{_format_pct(float(row.pct_desert))}</div>"
            f"<div class='compare-cell'>{float(row.apl_median):.2f}</div>"
            f"<div class='compare-cell'>{('N/A' if pd.isna(row.doctors_per_10k) else f'{float(row.doctors_per_10k):.1f}')}</div>"
            f"<div class='compare-cell muted'>{_format_int(float(row.total_population))}</div>"
            f"</div>"
            for row in ranked.itertuples(index=False)
        )
        st.markdown(
            "<div class='compare-panel'>"
            "<div class='compare-panel-title'>Lecture détaillée</div>"
            "<div class='compare-grid'>"
            "<div class='compare-row header'>"
            "<div>Département</div>"
            "<div>Écart seuil</div>"
            "<div>% désert</div>"
            "<div>APL médiane</div>"
            "<div>Médecins/10k</div>"
            "<div>Population</div>"
            "</div>"
            f"{compare_rows}"
            "</div>"
            "</div>",
            unsafe_allow_html=True,
        )


elif active_tab == "focus":
    available_depts = [dept for dept in dept_list if dept in set(df["dept"].dropna().astype(str))] or dept_list
    dept_sel = st.selectbox(
        "Département",
        options=available_depts,
        index=(available_depts.index(profile_dept) if profile_dept in available_depts else 0),
        format_func=lambda d: dept_reference.loc[dept_reference["dept"] == d, "dept_label"].iloc[0],
    )
    row  = departments_live[departments_live["dept"] == dept_sel]
    comm = communes[communes["dept"] == dept_sel]

    if row.empty or comm.empty:
        st.info("Aucune donnée pour ce département." if L == "fr" else "No data for this department.")
    else:
        r = row.iloc[0]

        # Profile header
        pct_d = df[df["dept"] == dept_sel]["pct_desert"].values
        pct_d = pct_d[0] if len(pct_d) else r.get("pct_desert", 0)

        color_accent = "#ef4444" if pct_d > 40 else "#f59e0b" if pct_d > 20 else "#10b981"
        st.markdown(f"""
        <div style="background:#1a1d27;border:1px solid #2d3148;border-left:4px solid {color_accent};
                    border-radius:12px;padding:20px 24px;margin-bottom:20px">
                    <div style="font-size:1.4rem;font-weight:700;color:#f1f5f9">{r.get('dept_label', dept_sel)}</div>
          <div style="font-size:0.8rem;color:#94a3b8;margin-top:4px">
            {len(comm):,} {"communes analysées" if L=="fr" else "communes analysed"} ·
                        Population: {r.get('total_population', 0):,.0f}
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Metric row
        m1, m2, m3, m4 = st.columns(4)
        nat_apl = communes["apl_mg"].median()

        with m1:
            delta_apl = r["apl_median"] - nat_apl
            st.metric("APL médiane", f"{r['apl_median']:.2f}",
                      delta=f"{delta_apl:+.2f} vs France", delta_color="normal")
        with m2:
            st.metric("% désert", f"{pct_d:.1f}%")
        with m3:
            nb_d = int(comm["is_desert"].sum())
            st.metric("Communes désert", f"{nb_d:,}")
        with m4:
            med_v = r.get("doctors_per_10k")
            if pd.notna(med_v):
                st.metric("Médecins / 10k hab", f"{med_v:.1f}")
            else:
                st.metric("Médecins / 10k hab", "N/A")

        col_p1, col_p2 = st.columns([1, 1])

        with col_p1:
            profile_categories = _category_breakdown(comm, L).sort_values("share")
            fig_profile_categories = px.bar(
                profile_categories,
                x="share",
                y="label",
                orientation="h",
                color="label",
                text=profile_categories["share"].map(_format_pct),
                color_discrete_map={row["label"]: row["color"] for _, row in profile_categories.iterrows()},
            )
            fig_profile_categories.update_traces(
                textposition="outside",
                cliponaxis=False,
                textfont=dict(color="#f8fafc", size=12),
            )
            fig_profile_categories.update_layout(
                **DARK_LAYOUT,
                title=dict(text="Structure du département" if L == "fr" else "Department structure",
                           font=dict(size=13), x=0),
                showlegend=False,
                height=380,
            )
            fig_profile_categories.update_xaxes(title="Part des communes" if L == "fr" else "Share of communes")
            fig_profile_categories.update_yaxes(title="")
            st.plotly_chart(fig_profile_categories, use_container_width=True)

        with col_p2:
            benchmark_df = pd.DataFrame(
                {
                    "label": ["Département" if L == "fr" else "Department", "France", "Seuil" if L == "fr" else "Threshold"],
                    "value": [r["apl_median"], departments_live["apl_median"].median(), threshold],
                    "color": [color_accent, "#94a3b8", "#ef4444"],
                }
            )
            fig_box = go.Figure()
            fig_box.add_trace(
                go.Scatter(
                    x=benchmark_df["value"],
                    y=benchmark_df["label"],
                    mode="markers+text",
                    text=[f"{v:.2f}" for v in benchmark_df["value"]],
                    textposition="middle right",
                    marker=dict(size=16, color=benchmark_df["color"], line=dict(width=1, color="#ffffff")),
                    hovertemplate="%{y}: %{x:.2f}<extra></extra>",
                )
            )
            fig_box.add_shape(
                type="line",
                x0=0,
                x1=max(5.5, float(benchmark_df["value"].max()) + 0.5),
                y0=0,
                y1=0,
                line=dict(color="rgba(255,255,255,0.08)", width=1),
            )
            fig_box.update_layout(
                **DARK_LAYOUT,
                title=dict(text=(f"Repères APL — {r.get('dept_label', dept_sel)}" if L == "fr" else f"APL benchmarks — {r.get('dept_label', dept_sel)}"),
                           font=dict(size=13), x=0),
                xaxis_title="APL",
                yaxis_title="",
                height=380,
                showlegend=False,
            )
            fig_box.update_xaxes(range=[0, max(5.5, float(benchmark_df["value"].max()) + 0.7)])
            st.plotly_chart(fig_box, use_container_width=True)

        insight_col1, insight_col2 = st.columns([1, 1])
        with insight_col1:
            st.markdown(
                f"### {'Lecture du département' if L == 'fr' else 'Department read'}\n"
                f"- **Rang national** : {int((departments_live['pct_desert'] > pct_d).sum()) + 1} / {len(departments_live)}\n"
                f"- **Lecture risque** : {_risk_status(float(pct_d), L)}\n"
                f"- **Communes critiques** : {_format_pct(float((comm['apl_mg'] < 1.5).mean() * 100))}\n"
                f"- **APL médiane** : {r['apl_median']:.2f} · {_apl_status(float(r['apl_median']), L)}\n"
                f"- **Population exposée** : {_format_int(float(r.get('pop_desert', 0)))}"
            )
        with insight_col2:
            benchmark_rows = [
                ("APL médiane", f"{r['apl_median']:.2f}", f"{departments_live['apl_median'].median():.2f}"),
                ("% désert", _format_pct(float(pct_d)), _format_pct(float(departments_live['pct_desert'].mean()))),
                (
                    "Médecins/10k",
                    "N/A" if pd.isna(r.get('doctors_per_10k')) else f"{r['doctors_per_10k']:.1f}",
                    f"{departments_live['doctors_per_10k'].median():.1f}" if departments_live['doctors_per_10k'].notna().any() else "N/A",
                ),
            ]
            benchmark_html = "".join(
                f"<div class='profile-summary-row'>"
                f"<div class='profile-summary-cell'>{label}</div>"
                f"<div class='profile-summary-cell'>{dept_val}</div>"
                f"<div class='profile-summary-cell muted'>{fr_val}</div>"
                f"</div>"
                for label, dept_val, fr_val in benchmark_rows
            )
            st.markdown(
                "<div class='profile-summary-panel'>"
                "<div class='profile-summary-title'>Comparatif rapide</div>"
                "<div class='profile-summary-grid'>"
                "<div class='profile-summary-row header'>"
                "<div>Indicateur</div>"
                "<div>Département</div>"
                "<div>France</div>"
                "</div>"
                f"{benchmark_html}"
                "</div>"
                "</div>",
                unsafe_allow_html=True,
            )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    f"<div class='footer-note'>"
    f"{'Données' if L=='fr' else 'Data'}: "
    f"<a href='https://data.gouv.fr' target='_blank'>data.gouv.fr</a> · "
    f"DREES APL · INSEE RP2021 · RPPS · "
    f"{'Carte' if L=='fr' else 'Map'}: france-geojson (gregoiredavid)"
    f"</div>",
    unsafe_allow_html=True,
)
