"""
France Healthcare Access — Premium BI Dashboard
================================================
Data: DREES APL · INSEE RP2021 · RPPS
DB  : PostgreSQL — mart.fact_communes, mart.dim_departments
"""

import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

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
    initial_sidebar_state="expanded",
)

# ── Custom CSS — Premium dark BI theme ────────────────────────────────────────
st.markdown("""
<style>
  /* Import font */
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  :root {
    --bg-primary:    #0f1117;
    --bg-card:       #1a1d27;
    --bg-card2:      #232637;
    --accent:        #4f7fff;
    --accent2:       #7c6af7;
    --danger:        #ef4444;
    --warning:       #f59e0b;
    --success:       #10b981;
    --info:          #3b82f6;
    --text-primary:  #f1f5f9;
    --text-muted:    #94a3b8;
    --border:        #2d3148;
  }

  html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    color: var(--text-primary);
  }

  /* App background */
  .stApp { background: var(--bg-primary); }
  section[data-testid="stSidebar"] {
    background: var(--bg-card);
    border-right: 1px solid var(--border);
  }

  /* Header banner */
  .header-banner {
    background: linear-gradient(135deg, #1a1d27 0%, #232637 50%, #1a1f3a 100%);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 28px 36px;
    margin-bottom: 24px;
    position: relative;
    overflow: hidden;
  }
  .header-banner::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: radial-gradient(ellipse at 70% 50%, rgba(79,127,255,0.08) 0%, transparent 70%);
    pointer-events: none;
  }
  .header-title {
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: -0.03em;
    background: linear-gradient(135deg, #e2e8f0 0%, #93c5fd 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0 0 6px 0;
  }
  .header-sub {
    font-size: 0.85rem;
    color: var(--text-muted);
    font-weight: 400;
    letter-spacing: 0.01em;
  }
  .badge {
    display: inline-block;
    background: rgba(79,127,255,0.15);
    border: 1px solid rgba(79,127,255,0.3);
    color: #93c5fd;
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
    border-radius: 12px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
  }
  .kpi-card::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: var(--accent-color, var(--accent));
    border-radius: 12px 12px 0 0;
  }
  .kpi-card:hover { border-color: rgba(79,127,255,0.4); }
  .kpi-label {
    font-size: 0.72rem;
    font-weight: 500;
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
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em !important;
  }

  /* Chart containers */
  .chart-container {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
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

  /* Sidebar */
  .sidebar-section {
    background: rgba(255,255,255,0.04);
    border-radius: 10px;
    padding: 14px;
    margin-bottom: 12px;
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
</style>
""", unsafe_allow_html=True)

# ── Paths & DB ───────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")

_POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://healthcare:healthcare@localhost:5432/healthcare",
)

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
    "source":     {"fr": "Sources : DREES · INSEE RP2021 · data.gouv.fr",
                   "en": "Sources: DREES · INSEE RP2021 · data.gouv.fr"},
    "map_metric": {"fr": "APL médiane par département",     "en": "Median APL by department"},
    "map_pct":    {"fr": "% communes en désert",            "en": "% desert communes"},
    "no_map":     {"fr": "Fichier GeoJSON introuvable. Exécutez d'abord le pipeline.",
                   "en": "GeoJSON not found. Run the pipeline first."},
}

# ── Plotly layout defaults (dark theme) ───────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="#1a1d27",
    plot_bgcolor="#1a1d27",
    font=dict(family="Inter", color="#f1f5f9", size=12),
    xaxis=dict(gridcolor="#2d3148", linecolor="#2d3148", zerolinecolor="#2d3148"),
    yaxis=dict(gridcolor="#2d3148", linecolor="#2d3148", zerolinecolor="#2d3148"),
    margin=dict(t=40, r=20, b=40, l=20),
)
COLOR_SCALE  = "RdYlGn_r"      # red=bad (desert), green=good
COLOR_SCALE2 = [[0,"#ef4444"], [0.5,"#f59e0b"], [1,"#10b981"]]

# ── Data loading — reads exclusively from mart.* in PostgreSQL ────────────────
@st.cache_resource
def _get_engine():
    return create_engine(_POSTGRES_URL, pool_pre_ping=True)


@st.cache_data(ttl=3600)
def load_communes() -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM mart.fact_communes", _get_engine())
    except Exception as e:
        st.error(
            f"❌ Cannot connect to PostgreSQL: {e}\n\n"
            "Run `docker compose up -d` then `uv run python src/pipeline.py` first."
        )
        st.stop()


@st.cache_data(ttl=3600)
def load_depts() -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM mart.dim_departments", _get_engine())
    except Exception as e:
        st.error(f"❌ Cannot read mart.dim_departments: {e}")
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
        pass
    return "unknown"


@st.cache_data(ttl=3600)
def load_dept_geojson() -> dict | None:
    p = RAW_DIR / "departements.geojson"
    if not p.exists():
        return None
    with open(p, encoding="utf-8") as f:
        geo = json.load(f)
    if not geo.get("features"):
        return None
    return geo

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style='padding:8px 0 16px'>
          <div style='font-size:1.1rem;font-weight:700;color:#f1f5f9'>🏥 Healthcare Access</div>
          <div style='font-size:0.72rem;color:#64748b;margin-top:2px'>France · DREES · INSEE</div>
        </div>
    """, unsafe_allow_html=True)

    en = st.toggle("🇬🇧 English", value=False)
    L  = "en" if en else "fr"

    st.divider()

    communes_raw = load_communes()
    depts_raw    = load_depts()

    dept_list = sorted(communes_raw["dept"].dropna().unique().tolist())
    selected  = st.multiselect(
        T["f_dept"][L], options=dept_list, default=[],
        placeholder="Tous" if L == "fr" else "All",
    )

    st.divider()

    threshold = st.slider(
        T["threshold"][L],
        min_value=1.0, max_value=4.5, value=2.5, step=0.1,
        help="APL < threshold → commune considered a medical desert. Default: 2.5"
    )

    st.divider()
    st.caption(T["source"][L])

# ── Data filtering + threshold recomputation ──────────────────────────────────
communes = communes_raw.copy()
communes["is_desert"] = (communes["apl_mg"] < threshold).astype(int)

cf = communes[communes["dept"].isin(selected)].copy() if selected else communes.copy()
df = depts_raw[depts_raw["dept"].isin(selected)].copy() if selected else depts_raw.copy()

# Recompute desert stats at dept level with the live threshold
dept_desert_live = (
    cf.groupby("dept")
    .agg(
        nb_communes_desert=("is_desert", "sum"),
        nb_communes=("is_desert", "count"),
        pct_desert=("is_desert", lambda x: round(x.mean() * 100, 2)),
        pop_desert=("population", lambda x: (x * cf.loc[x.index, "is_desert"]).sum()),
    )
    .reset_index()
)
df = df.drop(columns=["nb_communes_desert", "pct_desert"], errors="ignore")
df = df.merge(dept_desert_live[["dept", "pct_desert", "nb_communes_desert"]], on="dept", how="left")

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_communes  = len(cf)
nb_desert       = int(cf["is_desert"].sum())
pct_desert_nat  = cf["is_desert"].mean() * 100
pop_desert      = cf.loc[cf["is_desert"] == 1, "population"].sum()
pop_total       = cf["population"].sum()
apl_median      = cf["apl_mg"].median()
nb_critique     = int((cf["apl_mg"] < 1.5).sum())
pct_critique    = (cf["apl_mg"] < 1.5).mean() * 100

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-banner">
  <div class="header-title">
    🏥 {T['title'][L]}
    <span class="badge">2021</span>
  </div>
  <div class="header-sub">{T['subtitle'][L]} · DREES · INSEE · data.gouv.fr</div>
</div>
""", unsafe_allow_html=True)

# ── KPI CARDS ─────────────────────────────────────────────────────────────────
def _color(val, green_below=None, red_above=None):
    if red_above and val > red_above:
        return "danger"
    if green_below and val < green_below:
        return "success"
    return "warning"

danger_pct = pct_desert_nat > 30
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #4f7fff">
      <div class="kpi-label">{"Communes analysées" if L=="fr" else "Communes analysed"}</div>
      <div class="kpi-value">{total_communes:,}</div>
      <div class="kpi-sub" style="color:#64748b">{len(df)} {"départements" if L=="fr" else "departments"}</div>
    </div>""", unsafe_allow_html=True)

with c2:
    sub_class = "danger" if pct_desert_nat > 30 else ("warning" if pct_desert_nat > 20 else "success")
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #ef4444">
      <div class="kpi-label">{T['k2'][L]}</div>
      <div class="kpi-value">{nb_desert:,}</div>
      <div class="kpi-sub {sub_class}">{pct_desert_nat:.1f}% {"des communes" if L=="fr" else "of communes"}</div>
    </div>""", unsafe_allow_html=True)

with c3:
    pop_m = pop_desert / 1e6
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #f59e0b">
      <div class="kpi-label">{T['k3'][L]}</div>
      <div class="kpi-value">{pop_m:.1f}M</div>
      <div class="kpi-sub warning">{pop_desert/pop_total*100:.1f}% {"de la pop." if L=="fr" else "of pop."}</div>
    </div>""", unsafe_allow_html=True)

with c4:
    apl_class = "danger" if apl_median < 2.5 else ("warning" if apl_median < 3.5 else "success")
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #7c6af7">
      <div class="kpi-label">{T['k4'][L]}</div>
      <div class="kpi-value">{apl_median:.2f}</div>
      <div class="kpi-sub {apl_class}">{"Seuil désert" if L=="fr" else "Desert threshold"}: {threshold}</div>
    </div>""", unsafe_allow_html=True)

with c5:
    st.markdown(f"""
    <div class="kpi-card" style="--accent-color: #ef4444">
      <div class="kpi-label">{T['k5'][L]}</div>
      <div class="kpi-value">{nb_critique:,}</div>
      <div class="kpi-sub danger">{pct_critique:.1f}% (APL &lt; 1.5)</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<div style='margin-bottom:20px'></div>", unsafe_allow_html=True)

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([T["t1"][L], T["t2"][L], T["t3"][L], T["t4"][L]])


# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — MAP
# ════════════════════════════════════════════════════════════════════════════════
with tab1:
    geo = load_dept_geojson()

    if geo is None:
        st.warning(T["no_map"][L])
    else:
        map_metric = st.radio(
            "Métrique" if L == "fr" else "Metric",
            [T["map_metric"][L], T["map_pct"][L]],
            horizontal=True,
        )
        use_pct = map_metric == T["map_pct"][L]
        color_col   = "pct_desert" if use_pct else "apl_mediane"
        color_label = T["map_pct"][L] if use_pct else T["map_metric"][L]

        # Build geojson property key — features use 'code' field
        geo_key = "code"
        if geo["features"] and "properties" in geo["features"][0]:
            props = geo["features"][0]["properties"]
            geo_key = next(
                (k for k in props if k in ("code", "CODE_DEPT", "DEP", "codgeo")),
                list(props.keys())[0]
            )

        scale = COLOR_SCALE if use_pct else "RdYlGn"

        fig_map = px.choropleth_mapbox(
            df,
            geojson=geo,
            locations="dept",
            featureidkey=f"properties.{geo_key}",
            color=color_col,
            color_continuous_scale=scale,
            mapbox_style="carto-darkmatter",
            zoom=4.7,
            center={"lat": 46.5, "lon": 2.3},
            opacity=0.82,
            hover_data={
                "dept": True,
                "apl_mediane": ":.2f",
                "pct_desert": ":.1f",
                "med_pour_10k": ":.1f" if "med_pour_10k" in df.columns else False,
                "population_totale": ":,.0f",
            },
            labels={
                "dept": "Dép.",
                "apl_mediane": "APL médiane",
                "pct_desert": "% désert",
                "med_pour_10k": "Médecins/10k",
                "population_totale": "Population",
            },
        )
        fig_map.update_layout(
            **DARK_LAYOUT,
            height=580,
            coloraxis_colorbar=dict(
                title=color_label,
                tickfont=dict(color="#94a3b8"),
                titlefont=dict(color="#94a3b8"),
            ),
        )
        st.plotly_chart(fig_map, use_container_width=True)

        # Context note under map
        worst = df.nlargest(3, "pct_desert")["dept"].tolist()
        st.markdown(
            f"<div class='footer-note'>"
            f"{'Départements les plus touchés' if L=='fr' else 'Most affected departments'}: "
            f"<strong>{'  ·  '.join(worst)}</strong> · "
            f"{'APL < ' + str(threshold) + ' → désert médical' if L=='fr' else 'APL < ' + str(threshold) + ' → medical desert'}"
            f"</div>",
            unsafe_allow_html=True,
        )


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — NATIONAL ANALYSIS
# ════════════════════════════════════════════════════════════════════════════════
with tab2:
    col_a, col_b = st.columns([1, 1])

    with col_a:
        # Horizontal bar: worst departments
        bar_data = df.nlargest(20, "pct_desert")
        fig_bar = go.Figure(go.Bar(
            x=bar_data["pct_desert"],
            y=bar_data["dept"],
            orientation="h",
            marker=dict(
                color=bar_data["pct_desert"],
                colorscale="RdYlGn_r",
                cmin=0,
                cmax=df["pct_desert"].max(),
                showscale=False,
            ),
            text=bar_data["pct_desert"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
            textfont=dict(size=11, color="#94a3b8"),
            hovertemplate="Dept %{y}<br>% désert: %{x:.1f}%<extra></extra>",
        ))
        fig_bar.update_layout(
            **DARK_LAYOUT,
            title=dict(
                text="🔴 Top 20 — " + ("Départements les plus touchés" if L == "fr" else "Most affected departments"),
                font=dict(size=13), x=0
            ),
            height=500,
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            xaxis=dict(title="% communes en désert"),
            bargap=0.25,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_b:
        # Treemap: departments sized by pop in desert, colored by %
        tree_df = df.dropna(subset=["pct_desert", "nb_communes_desert", "nb_communes"])
        tree_df = tree_df.copy()
        tree_df["pop_desert_est"] = (
            tree_df["pct_desert"] / 100 * tree_df.get("population_totale", tree_df["nb_communes"] * 1500)
        ).astype(int).clip(lower=1)

        fig_tree = px.treemap(
            tree_df,
            path=["dept"],
            values="pop_desert_est",
            color="pct_desert",
            color_continuous_scale="RdYlGn_r",
            hover_data={"pct_desert": ":.1f", "nb_communes_desert": True},
            labels={"pct_desert": "% désert", "nb_communes_desert": "Communes désert"},
        )
        fig_tree.update_traces(
            textinfo="label+percent root",
            textfont=dict(size=12, family="Inter"),
            hovertemplate="<b>%{label}</b><br>% désert: %{color:.1f}%<br>Pop. en désert: %{value:,.0f}<extra></extra>",
        )
        fig_tree.update_layout(
            **DARK_LAYOUT,
            title=dict(
                text="🌍 " + ("Population en désert par département" if L == "fr" else "Population in desert by department"),
                font=dict(size=13), x=0
            ),
            height=500,
            coloraxis_colorbar=dict(
                title="% désert",
                tickfont=dict(color="#94a3b8"),
                titlefont=dict(color="#94a3b8"),
            ),
        )
        st.plotly_chart(fig_tree, use_container_width=True)

    # Scatter: doctors vs APL
    if "med_pour_10k" in df.columns and df["med_pour_10k"].notna().any():
        sc_df = df.dropna(subset=["med_pour_10k", "apl_mediane", "population_totale"])

        fig_sc = go.Figure()
        fig_sc.add_trace(go.Scatter(
            x=sc_df["med_pour_10k"],
            y=sc_df["apl_mediane"],
            mode="markers+text",
            text=sc_df["dept"],
            textposition="top center",
            textfont=dict(size=9, color="#94a3b8"),
            marker=dict(
                size=sc_df["population_totale"] / sc_df["population_totale"].max() * 40 + 8,
                color=sc_df["pct_desert"],
                colorscale="RdYlGn_r",
                showscale=True,
                colorbar=dict(
                    title="% désert",
                    tickfont=dict(color="#94a3b8"),
                    titlefont=dict(color="#94a3b8"),
                ),
                line=dict(color="rgba(255,255,255,0.15)", width=1),
            ),
            hovertemplate="<b>Dept %{text}</b><br>Médecins/10k: %{x:.1f}<br>APL médiane: %{y:.2f}<extra></extra>",
        ))
        fig_sc.add_hline(
            y=threshold, line_dash="dash", line_color="#ef4444",
            annotation_text=f"Seuil désert (APL {threshold})",
            annotation_font_color="#ef4444",
            annotation_position="bottom right",
        )
        fig_sc.update_layout(
            **DARK_LAYOUT,
            title=dict(
                text="💊 " + ("Médecins/10k hab vs APL médiane — taille = population" if L == "fr"
                               else "Doctors/10k pop vs median APL — bubble size = population"),
                font=dict(size=13), x=0
            ),
            xaxis_title="Médecins généralistes / 10 000 hab",
            yaxis_title="APL médiane",
            height=480,
        )
        st.plotly_chart(fig_sc, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — RANKING TABLE
# ════════════════════════════════════════════════════════════════════════════════
with tab3:
    col_r1, col_r2 = st.columns([3, 2])

    with col_r1:
        # Styled ranking dataframe
        display_cols = ["dept", "apl_mediane", "pct_desert", "nb_communes", "nb_communes_desert"]
        if "med_pour_10k" in df.columns:
            display_cols.append("med_pour_10k")
        if "population_totale" in df.columns:
            display_cols.append("population_totale")

        table_df = df[display_cols].copy().sort_values("pct_desert", ascending=False)
        table_df.columns = [
            "Département", "APL médiane", "% désert",
            "Communes", "Communes désert",
        ] + (["Médecins/10k"] if "med_pour_10k" in display_cols else []) \
          + (["Population"] if "population_totale" in display_cols else [])

        st.dataframe(
            table_df.style
            .background_gradient(subset=["% désert"], cmap="RdYlGn_r", vmin=0, vmax=100)
            .background_gradient(subset=["APL médiane"], cmap="RdYlGn", vmin=0, vmax=6)
            .format({"APL médiane": "{:.2f}", "% désert": "{:.1f}%",
                     "Médecins/10k": "{:.1f}" if "Médecins/10k" in table_df.columns else "{}",
                     "Population": "{:,.0f}" if "Population" in table_df.columns else "{}"}),
            use_container_width=True,
            height=480,
        )

    with col_r2:
        # APL distribution histogram (national)
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=cf["apl_mg"],
            nbinsx=50,
            marker=dict(
                color=cf["apl_mg"].apply(
                    lambda v: "#ef4444" if v < 1.5
                    else "#f59e0b" if v < threshold
                    else "#10b981"
                ),
                line=dict(width=0.3, color="#1a1d27"),
            ),
            hovertemplate="APL: %{x:.1f}<br>Communes: %{y}<extra></extra>",
        ))
        fig_hist.add_vline(
            x=threshold, line_dash="dash", line_color="#ef4444",
            annotation_text=f"Seuil: {threshold}",
            annotation_font_color="#ef4444",
        )
        fig_hist.add_vline(
            x=1.5, line_dash="dot", line_color="#f97316",
            annotation_text="Critique: 1.5",
            annotation_font_color="#f97316",
            annotation_position="top left",
        )
        fig_hist.update_layout(
            **DARK_LAYOUT,
            title=dict(text="📊 " + ("Distribution APL — toutes communes" if L == "fr" else "APL Distribution — all communes"),
                       font=dict(size=13), x=0),
            xaxis_title="APL (consultations/hab/an)",
            yaxis_title="Nombre de communes",
            height=480,
            bargap=0.02,
            showlegend=False,
        )
        st.plotly_chart(fig_hist, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 — DEPARTMENT PROFILE
# ════════════════════════════════════════════════════════════════════════════════
with tab4:
    dept_sel = st.selectbox(
        "Département",
        options=dept_list,
        format_func=lambda d: f"Dept {d}",
    )
    row  = depts_raw[depts_raw["dept"] == dept_sel]
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
          <div style="font-size:1.4rem;font-weight:700;color:#f1f5f9">Département {dept_sel}</div>
          <div style="font-size:0.8rem;color:#64748b;margin-top:4px">
            {len(comm):,} {"communes analysées" if L=="fr" else "communes analysed"} ·
            Population: {r.get('population_totale', 0):,.0f}
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Metric row
        m1, m2, m3, m4 = st.columns(4)
        nat_apl = communes["apl_mg"].median()

        with m1:
            delta_apl = r["apl_mediane"] - nat_apl
            st.metric("APL médiane", f"{r['apl_mediane']:.2f}",
                      delta=f"{delta_apl:+.2f} vs France", delta_color="normal")
        with m2:
            st.metric("% désert", f"{pct_d:.1f}%")
        with m3:
            nb_d = int(comm["is_desert"].sum())
            st.metric("Communes désert", f"{nb_d:,}")
        with m4:
            med_v = r.get("med_pour_10k")
            if pd.notna(med_v):
                st.metric("Médecins / 10k hab", f"{med_v:.1f}")
            else:
                st.metric("Médecins / 10k hab", "N/A")

        col_p1, col_p2 = st.columns([1, 1])

        with col_p1:
            # Donut chart — APL category distribution
            cat_order = ["désert_critique", "sous-doté", "correct", "bien_doté"]
            cat_colors = {"désert_critique": "#ef4444", "sous-doté": "#f59e0b",
                          "correct": "#3b82f6", "bien_doté": "#10b981"}
            cat_counts = comm["apl_cat"].value_counts().reindex(cat_order).fillna(0)

            fig_donut = go.Figure(go.Pie(
                labels=cat_counts.index.tolist(),
                values=cat_counts.values.tolist(),
                hole=0.58,
                marker=dict(
                    colors=[cat_colors[c] for c in cat_counts.index],
                    line=dict(color="#0f1117", width=2),
                ),
                textfont=dict(family="Inter", size=12),
                hovertemplate="%{label}<br>%{value:,} communes (%{percent})<extra></extra>",
            ))
            fig_donut.update_layout(
                **DARK_LAYOUT,
                title=dict(text="Répartition des communes par catégorie APL",
                           font=dict(size=13), x=0),
                legend=dict(font=dict(color="#94a3b8", size=11),
                            bgcolor="rgba(0,0,0,0)", x=0.7, y=0.5),
                annotations=[dict(
                    text=f"<b>{pct_d:.0f}%</b><br><span style='font-size:10px'>désert</span>",
                    x=0.5, y=0.5, font_size=18, font_color="#f1f5f9", showarrow=False,
                )],
                height=380,
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_p2:
            # APL histogram for this dept vs national overlay
            fig_dept_hist = go.Figure()

            # National reference (lighter)
            fig_dept_hist.add_trace(go.Histogram(
                x=communes["apl_mg"],
                nbinsx=60,
                name="France",
                marker_color="rgba(100,116,139,0.3)",
                histnorm="probability density",
                hovertemplate="France — APL: %{x:.1f}<extra></extra>",
            ))
            # Dept highlight
            fig_dept_hist.add_trace(go.Histogram(
                x=comm["apl_mg"],
                nbinsx=30,
                name=f"Dept {dept_sel}",
                marker_color=color_accent,
                opacity=0.8,
                histnorm="probability density",
                hovertemplate=f"Dept {dept_sel} — APL: %{{x:.1f}}<extra></extra>",
            ))
            fig_dept_hist.add_vline(
                x=threshold, line_dash="dash", line_color="#ef4444",
                annotation_text=f"Seuil {threshold}",
                annotation_font_color="#ef4444",
            )
            fig_dept_hist.add_vline(
                x=nat_apl, line_dash="dot", line_color="#94a3b8",
                annotation_text=f"Médiane France ({nat_apl:.2f})",
                annotation_font_color="#94a3b8",
                annotation_position="top left",
            )
            fig_dept_hist.update_layout(
                **DARK_LAYOUT,
                title=dict(text=f"Distribution APL — Dept {dept_sel} vs France",
                           font=dict(size=13), x=0),
                xaxis_title="APL",
                yaxis_title="Densité",
                legend=dict(font=dict(color="#94a3b8"), bgcolor="rgba(0,0,0,0)"),
                barmode="overlay",
                height=380,
            )
            st.plotly_chart(fig_dept_hist, use_container_width=True)

        # APL gauge
        apl_val = r["apl_mediane"]
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=apl_val,
            delta={"reference": threshold, "suffix": " vs seuil",
                   "font": {"color": "#94a3b8"}},
            number={"suffix": " APL", "font": {"size": 28, "color": "#f1f5f9",
                                                "family": "Inter"}},
            gauge=dict(
                axis=dict(range=[0, 8], tickcolor="#64748b",
                          tickfont=dict(color="#64748b")),
                bar=dict(color="#4f7fff", thickness=0.25),
                bgcolor="#1a1d27",
                borderwidth=0,
                steps=[
                    dict(range=[0, 1.5],    color="#ef4444"),
                    dict(range=[1.5, threshold], color="#f59e0b"),
                    dict(range=[threshold, 4.0], color="#3b82f6"),
                    dict(range=[4.0, 8.0],  color="#10b981"),
                ],
                threshold=dict(line=dict(color="#ffffff", width=2),
                               thickness=0.75, value=threshold),
            ),
        ))
        fig_gauge.update_layout(
            **DARK_LAYOUT,
            title=dict(text=f"Score APL médiane — Dept {dept_sel}",
                       font=dict(size=13), x=0),
            height=280,
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

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
