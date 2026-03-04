import streamlit as st
import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import json
from pathlib import Path

st.set_page_config(
    page_title="France Healthcare Access",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path("data/processed")
RAW_DIR  = Path("data/raw")

# ── Cache — OBRIGATÓRIO em toda função que lê arquivo ─────────────────────────

@st.cache_data
def load_communes() -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / "communes_enriched.parquet")

@st.cache_data
def load_depts() -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / "departements_summary.parquet")

@st.cache_data
def load_quality() -> dict:
    with open(DATA_DIR / "quality_report.json", encoding="utf-8") as f:
        return json.load(f)

@st.cache_data
def load_geo_simplified() -> gpd.GeoDataFrame:
    """
    Simplifica geometrias para performance Folium.
    35k communes completo → mapa trava > 30s.
    tolerance=0.01 reduz ~70% do tamanho com impacto visual mínimo.
    Documentado como decisão de performance intencional.
    """
    try:
        gdf = gpd.read_file(RAW_DIR / "communes.geojson")
        # Handle empty mock geojson 
        if len(gdf) == 0:
            return pd.DataFrame(columns=["codgeo", "nom", "codeDepartement", "geometry"])
            
        if "code" in gdf.columns:
            gdf = gdf.rename(columns={"code": "codgeo"})
        if "geometry" in gdf.columns:
            gdf["geometry"] = gdf["geometry"].simplify(
                tolerance=0.01, preserve_topology=True
            )
        cols = [c for c in ["codgeo", "nom", "codeDepartement", "geometry"] if c in gdf.columns]
        return gdf[cols]
    except Exception as e:
        print(f"Error loading geojson: {e}")
        return pd.DataFrame(columns=["codgeo", "nom", "codeDepartement", "geometry"])

# ── Textos FR/EN ──────────────────────────────────────────────────────────────

T = {
    "title"    : {"fr": "🏥 Déserts Médicaux France",
                  "en": "🏥 France Healthcare Access"},
    "subtitle" : {"fr": "Observatoire de l'accessibilité aux soins — DREES · INSEE · data.gouv.fr",
                  "en": "Healthcare accessibility observatory — DREES · INSEE · data.gouv.fr"},
    "k1"       : {"fr": "Communes analysées",   "en": "Communes analyzed"},
    "k2"       : {"fr": "En désert médical",    "en": "In medical desert"},
    "k3"       : {"fr": "Population concernée", "en": "Affected population"},
    "k4"       : {"fr": "APL médiane France",   "en": "France median APL"},
    "f_dept"   : {"fr": "Filtrer par département", "en": "Filter by department"},
    "t1"       : {"fr": "🗺️ Carte",       "en": "🗺️ Map"},
    "t2"       : {"fr": "📊 Classement",  "en": "📊 Ranking"},
    "t3"       : {"fr": "📋 Département", "en": "📋 Department"},
    "map_leg"  : {"fr": "APL médecins généralistes",
                  "en": "GP accessibility (APL)"},
    "rank_h"   : {"fr": "Départements les plus touchés (% communes en désert)",
                  "en": "Most affected departments (% desert communes)"},
    "sc_h"     : {"fr": "Médecins / 10 000 hab vs APL médiane",
                  "en": "Doctors per 10k pop vs median APL"},
    "seuil"    : {"fr": "Seuil désert (APL 2.5)", "en": "Desert threshold (APL 2.5)"},
    "d_apl"    : {"fr": "APL médiane",          "en": "Median APL"},
    "d_pct"    : {"fr": "% communes désert",    "en": "% desert communes"},
    "d_med"    : {"fr": "Médecins / 10k hab",   "en": "Doctors per 10k pop"},
    "source"   : {"fr": "Source : DREES / INSEE / geo.api.gouv.fr · MCP data.gouv.fr",
                  "en": "Source: DREES / INSEE / geo.api.gouv.fr · MCP data.gouv.fr"},
}

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    en = st.toggle("🇬🇧 English", value=False)
    L  = "en" if en else "fr"

    st.title(T["title"][L])
    st.caption(T["subtitle"][L])
    st.divider()

    communes = load_communes()
    depts    = load_depts()
    quality  = load_quality()

    dept_list = sorted(communes["dept"].dropna().unique().tolist())
    selected  = st.multiselect(
        T["f_dept"][L], options=dept_list, default=[],
        placeholder="Tous" if L == "fr" else "All",
    )
    st.divider()
    st.caption(T["source"][L])

# ── Filtrage ──────────────────────────────────────────────────────────────────

cf = communes[communes["dept"].isin(selected)].copy() if selected else communes.copy()
df = depts[depts["dept"].isin(selected)].copy()       if selected else depts.copy()

# ── KPI Cards ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
pop_desert = cf.loc[cf["is_desert"]==1, "population"].sum()

c1.metric(T["k1"][L], f"{len(cf):,}")
c2.metric(T["k2"][L], f"{cf['is_desert'].sum():,}",
          delta=f"{cf['is_desert'].mean()*100:.1f}%", delta_color="inverse")
c3.metric(T["k3"][L], f"{pop_desert/1e6:.1f}M")
c4.metric(T["k4"][L], f"{cf['apl_mg'].median():.2f}",
          help="Consultations/habitant/an — seuil désert: 2.5")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs([T["t1"][L], T["t2"][L], T["t3"][L]])

# TAB 1 — Carte Folium choropleth
with tab1:
    gdf     = load_geo_simplified()
    m = folium.Map(location=[46.5, 2.3], zoom_start=6, tiles="CartoDB positron")
    
    if len(gdf) > 0 and "codgeo" in gdf.columns:
        merged  = gdf.merge(
            cf[["codgeo","apl_mg","is_desert","apl_cat"]],
            on="codgeo", how="left"
        )
        folium.Choropleth(
            geo_data   = merged.__geo_interface__,
            data       = cf,
            columns    = ["codgeo", "apl_mg"],
            key_on     = "feature.properties.codgeo",
            fill_color = "RdYlGn",
            fill_opacity  = 0.75,
            line_opacity  = 0.05,
            legend_name   = T["map_leg"][L],
            nan_fill_color= "#cccccc",
            nan_fill_opacity= 0.3,
            bins = [0, 1.5, 2.5, 4.0, 6.0, 200],
        ).add_to(m)
    else:
        st.warning("⚠️ Les données géographiques (geo.api.gouv.fr) sont indisponibles pour le moment. La carte Choropleth ne peut pas être générée.")

    st_folium(m, width="100%", height=560, returned_objects=[])

# TAB 2 — Classement + scatter
with tab2:
    st.subheader(T["rank_h"][L])
    fig_bar = px.bar(
        df.head(20), x="pct_desert", y="dept", orientation="h",
        color="pct_desert", color_continuous_scale="RdYlGn_r",
        labels={"pct_desert": "% en désert", "dept": "Département"},
        template="plotly_white",
    )
    fig_bar.update_layout(showlegend=False, height=480,
                          coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

    if "med_pour_10k" in df.columns and df["med_pour_10k"].notna().any():
        st.subheader(T["sc_h"][L])
        sc_df = df.dropna(subset=["med_pour_10k", "population_totale"])
        fig_sc = px.scatter(
            sc_df,
            x="med_pour_10k", y="apl_mediane",
            size="population_totale", color="pct_desert",
            hover_name="dept", color_continuous_scale="RdYlGn_r",
            labels={"med_pour_10k":"Médecins/10k","apl_mediane":"APL médiane",
                    "pct_desert":"% déserts"},
            template="plotly_white",
        )
        fig_sc.add_hline(
            y=2.5, line_dash="dash", line_color="#e74c3c",
            annotation_text=T["seuil"][L],
            annotation_position="bottom right",
        )
        fig_sc.update_layout(height=440)
        st.plotly_chart(fig_sc, use_container_width=True)

# TAB 3 — Fiche département
with tab3:
    dept_sel = st.selectbox("Département", options=dept_list)
    row = depts[depts["dept"]==dept_sel]
    if not row.empty:
        r = row.iloc[0]
        m1, m2, m3 = st.columns(3)
        m1.metric(T["d_apl"][L], f"{r['apl_mediane']:.2f}")
        m2.metric(T["d_pct"][L], f"{r['pct_desert']}%")
        med_v = f"{r['med_pour_10k']:.1f}" \
            if "med_pour_10k" in r.index and pd.notna(r.get("med_pour_10k")) \
            else "N/D"
        m3.metric(T["d_med"][L], med_v)

        dist = communes[communes["dept"]==dept_sel]["apl_cat"].value_counts()
        fig_pie = px.pie(
            values=dist.values, names=dist.index,
            color=dist.index,
            color_discrete_map={
                "désert_critique": "#e74c3c", "sous-doté": "#e67e22",
                "correct": "#f1c40f",         "bien_doté": "#27ae60",
            },
            template="plotly_white",
        )
        fig_pie.update_layout(height=380)
        st.plotly_chart(fig_pie, use_container_width=True)
