"""
NYC Mobility — Tableau de bord analytique (couche Gold).

Trois vues alimentées par les datamarts Gold (Parquet S3), avec filtres croisés
et sélecteurs de mesure :
  1. Rentabilité — borough, Pareto, classement zones, Yellow/Green, heatmap, efficacité
  2. Météo       — heatmap pluie×température, sensibilité par borough, lien 311/revenu
  3. Anomalies   — sévérité distance/tarif, trajets touchés, distribution, profil horaire

Lancement :
    pip install -r requirements.txt
    streamlit run dashboard/app.py
"""

import os
import datetime as dt
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BUCKET = "nyc-mobility-gold-am2024"
DATAMARTS = {k: f"s3://{BUCKET}/{k}/" for k in
             ("rentabilite", "meteo_demande", "anomalies")}

INK = "#14110F"
PAPER = "#FFFFFF"
CANVAS = "#F3F0EA"
HAIRLINE = "#E4DFD5"
ACCENT = "#1F5C3D"
ACCENT_SOFT = "#A9C3B3"
ALERT = "#B23A2E"
GOLD = "#C28A2B"
MUTED = "#7A736B"
GREEN_SCALE = [[0.0, "#EDF3EE"], [0.5, "#5E8A6F"], [1.0, ACCENT]]
SEV_SCALE = [[0.0, ACCENT_SOFT], [0.5, GOLD], [1.0, ALERT]]

st.set_page_config(page_title="NYC Mobility — Analytics", page_icon="▦",
                   layout="wide")
CFG = {"displayModeBar": False}


# ── Données ───────────────────────────────────────────────────
def _ensure_aws_credentials():
    paths = [Path.home() / ".streamlit" / "secrets.toml",
             Path.cwd() / ".streamlit" / "secrets.toml"]
    if not any(p.exists() for p in paths):
        return
    try:
        if "AWS_ACCESS_KEY_ID" in st.secrets:
            os.environ.setdefault("AWS_ACCESS_KEY_ID", st.secrets["AWS_ACCESS_KEY_ID"])
            os.environ.setdefault("AWS_SECRET_ACCESS_KEY", st.secrets["AWS_SECRET_ACCESS_KEY"])
            os.environ.setdefault("AWS_DEFAULT_REGION",
                                  st.secrets.get("AWS_DEFAULT_REGION", "eu-west-3"))
    except Exception:
        pass


@st.cache_data(ttl=3600, show_spinner="Lecture des datamarts…")
def load_datamart(name):
    _ensure_aws_credentials()
    return pd.read_parquet(DATAMARTS[name], engine="pyarrow")


# ── Formatage ─────────────────────────────────────────────────
def fmt_money(x):
    return f"$ {x:,.0f}".replace(",", " ")


def fmt_int(x):
    return f"{int(x):,}".replace(",", " ")


def money_short(x):
    if x >= 1e6:
        return f"${x / 1e6:.1f}M"
    if x >= 1e3:
        return f"${x / 1e3:.0f}k"
    return f"${x:.0f}"


# ── Style ─────────────────────────────────────────────────────
def inject_css():
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Spectral:wght@400;500;600&family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
    .stApp {{ background:{CANVAS}; }}
    [data-testid="stHeader"] {{ background:transparent; }}
    .block-container {{ padding-top:1.2rem; max-width:1220px; }}
    html, body, [class*="css"] {{ font-family:'Inter',sans-serif; color:{INK}; }}
    .masthead {{ border-bottom:2px solid {INK}; padding-bottom:.5rem;
        margin-bottom:1.5rem; display:flex; justify-content:space-between;
        align-items:baseline; }}
    .masthead .title {{ font-family:'Spectral',serif; font-weight:600;
        font-size:1.5rem; letter-spacing:-.01em; }}
    .masthead .stamp {{ font-family:'IBM Plex Mono',monospace; font-size:.72rem;
        color:{MUTED}; text-transform:uppercase; letter-spacing:.12em; }}
    .eyebrow {{ font-family:'IBM Plex Mono',monospace; font-size:.7rem;
        text-transform:uppercase; letter-spacing:.18em; color:{ACCENT};
        margin-bottom:.3rem; }}
    .headline {{ font-family:'Spectral',serif; font-weight:500; font-size:1.65rem;
        line-height:1.15; margin:0 0 1.1rem 0; letter-spacing:-.01em; }}
    .tearsheet {{ display:flex; border-top:1px solid {INK};
        border-bottom:1px solid {INK}; margin-bottom:1.6rem; }}
    .kpi {{ flex:1; padding:.85rem 1rem; border-right:1px solid {HAIRLINE}; }}
    .kpi:last-child {{ border-right:none; }}
    .kpi .label {{ font-family:'IBM Plex Mono',monospace; font-size:.62rem;
        text-transform:uppercase; letter-spacing:.14em; color:{MUTED};
        margin-bottom:.35rem; }}
    .kpi .value {{ font-family:'IBM Plex Mono',monospace; font-weight:500;
        font-size:1.5rem; font-variant-numeric:tabular-nums; line-height:1; }}
    .kpi .value.alert {{ color:{ALERT}; }}
    .kpi .sub {{ font-size:.71rem; color:{MUTED}; margin-top:.3rem; }}
    .charttitle {{ font-family:'Spectral',serif; font-size:1.05rem; font-weight:500;
        margin:.7rem 0 .1rem 0; }}
    .chartnote {{ font-size:.78rem; color:{MUTED}; margin-bottom:.5rem;
        line-height:1.4; }}
    .sectionrule {{ border:0; border-top:1px solid {HAIRLINE}; margin:1.4rem 0 .2rem; }}
    [data-testid="stSidebar"] {{ background:{PAPER}; border-right:1px solid {HAIRLINE}; }}
    [data-testid="stDataFrame"] {{ border:1px solid {HAIRLINE}; }}
    </style>
    """, unsafe_allow_html=True)


def masthead(n):
    today = dt.date.today().strftime("%d %b %Y").upper()
    st.markdown(
        f'<div class="masthead"><div class="title">NYC&nbsp;Mobility&nbsp;'
        f'<span style="color:{ACCENT}">/</span>&nbsp;Analytics</div>'
        f'<div class="stamp">Gold layer · {fmt_int(n)} lignes · {today}</div></div>',
        unsafe_allow_html=True)


def tearsheet(items):
    cells = "".join(
        f'<div class="kpi"><div class="label">{lab}</div>'
        f'<div class="value{" alert" if al else ""}">{val}</div>'
        f'<div class="sub">{sub}</div></div>' for lab, val, sub, al in items)
    st.markdown(f'<div class="tearsheet">{cells}</div>', unsafe_allow_html=True)


def page_header(eyebrow, headline):
    st.markdown(f'<div class="eyebrow">{eyebrow}</div>'
                f'<div class="headline">{headline}</div>', unsafe_allow_html=True)


def chart_title(title, note=""):
    st.markdown(f'<div class="charttitle">{title}</div>'
                + (f'<div class="chartnote">{note}</div>' if note else ""),
                unsafe_allow_html=True)


def rule():
    st.markdown('<hr class="sectionrule">', unsafe_allow_html=True)


def base_layout(fig, height=360):
    fig.update_layout(
        height=height, paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        font=dict(family="Inter, sans-serif", size=12, color=INK),
        margin=dict(l=10, r=30, t=18, b=10), showlegend=False,
        hoverlabel=dict(bgcolor=INK, font=dict(color=PAPER, family="IBM Plex Mono")),
        xaxis=dict(showgrid=False, linecolor=HAIRLINE, ticks="outside",
                   tickcolor=HAIRLINE),
        yaxis=dict(showgrid=True, gridcolor="#ECE8E0", zeroline=False,
                   linecolor=PAPER))
    return fig


def heat_layout(fig, height=380):
    fig.update_layout(height=height, paper_bgcolor=PAPER, plot_bgcolor=PAPER,
                      font=dict(family="Inter", size=11, color=INK),
                      margin=dict(l=10, r=10, t=10, b=10))
    return fig


# ══════════════════════════════════════════════════════════════
# PAGE 1 — RENTABILITE
# ══════════════════════════════════════════════════════════════
def page_rentabilite():
    df = load_datamart("rentabilite")
    masthead(len(df))
    page_header("Problématique 01 — Rentabilité",
                "Où et quand un chauffeur maximise-t-il ses revenus ?")

    with st.sidebar:
        st.markdown("**Filtres — Rentabilité**")
        boroughs = sorted(df["pickup_borough"].unique())
        sel_b = st.multiselect("Borough de départ", boroughs, default=boroughs)
        types = sorted(df["taxi_type"].unique())
        sel_t = st.multiselect("Type de taxi", types, default=types)
        h0, h1 = st.select_slider("Plage horaire", options=list(range(24)),
                                  value=(0, 23))
        st.markdown("---")
        mesure = st.radio("Mesure du classement des zones",
                          ["Revenu total", "Panier moyen", "Nb courses"])
        topn = st.slider("Zones affichées", 5, 20, 12)

    d = df[df["pickup_borough"].isin(sel_b) & df["taxi_type"].isin(sel_t)
           & df["heure"].between(h0, h1)]
    if d.empty:
        st.info("Aucune donnée pour ces filtres.")
        return

    rev_total = d["revenu_total"].sum()
    courses = d["nb_courses"].sum()
    rev_moy = rev_total / courses if courses else 0
    zsum = d.groupby("pickup_zone")["revenu_total"].sum().sort_values(ascending=False)
    n_zones_80 = int((zsum.cumsum() / rev_total <= 0.8).sum()) + 1

    tearsheet([
        ("Revenu total", fmt_money(rev_total), "sur la sélection", False),
        ("Courses", fmt_int(courses), "nombre de trajets", False),
        ("Panier moyen", f"$ {rev_moy:,.2f}".replace(",", " "), "par course", False),
        ("Concentration", f"{n_zones_80} zones", "= 80 % du revenu", False),
    ])

    # Borough + Pareto
    c1, c2 = st.columns(2)
    with c1:
        chart_title("Revenu par borough",
                    "Revenu cumulé par arrondissement, classé du plus au moins rémunérateur.")
        b = d.groupby("pickup_borough")["revenu_total"].sum().sort_values()
        colors = [ACCENT if x == b.index[-1] else ACCENT_SOFT for x in b.index]
        fig = go.Figure(go.Bar(
            x=b.values, y=b.index, orientation="h", marker_color=colors,
            marker_line_width=0,
            text=[f"{money_short(v)} · {v / rev_total * 100:.0f}%" for v in b.values],
            textposition="outside",
            textfont=dict(family="IBM Plex Mono", size=11, color=INK),
            hovertemplate="%{y}<br>%{x:,.0f} $<extra></extra>"))
        base_layout(fig, 360)
        fig.update_xaxes(visible=False)
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c2:
        chart_title("Courbe de concentration (Pareto)",
                    "Part cumulée du revenu à mesure qu'on ajoute les zones, "
                    "des plus rentables aux moins rentables.")
        cum = (zsum.cumsum() / rev_total * 100).reset_index(drop=True)
        cum.index = cum.index + 1
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=cum.index, y=cum.values, mode="lines",
                                 line=dict(color=ACCENT, width=2.5), fill="tozeroy",
                                 fillcolor="rgba(31,92,61,.08)",
                                 hovertemplate="%{x} zones → %{y:.0f} %<extra></extra>"))
        fig.add_hline(y=80, line_dash="dot", line_color=MUTED)
        base_layout(fig, 360)
        fig.update_xaxes(title="nombre de zones (classées)")
        fig.update_yaxes(title="% cumulé du revenu", range=[0, 101])
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    # Classement zones (mesure dynamique) + Yellow/Green
    c3, c4 = st.columns([3, 2])
    with c3:
        chart_title(f"Top {topn} zones — {mesure.lower()}",
                    "Classement des zones de prise en charge selon la mesure choisie "
                    "dans le panneau de gauche.")
        if mesure == "Revenu total":
            s = d.groupby("pickup_zone")["revenu_total"].sum()
        elif mesure == "Panier moyen":
            g = d.groupby("pickup_zone").agg(r=("revenu_total", "sum"),
                                             c=("nb_courses", "sum"))
            s = g["r"] / g["c"]
        else:
            s = d.groupby("pickup_zone")["nb_courses"].sum()
        s = s.sort_values(ascending=False).head(topn).sort_values()
        if mesure == "Panier moyen":
            txt = [f"${v:.0f}" for v in s.values]
        elif mesure == "Nb courses":
            txt = [fmt_int(v) for v in s.values]
        else:
            txt = [money_short(v) for v in s.values]
        colors = [ACCENT if x == s.index[-1] else ACCENT_SOFT for x in s.index]
        fig = go.Figure(go.Bar(
            x=s.values, y=s.index, orientation="h", marker_color=colors,
            marker_line_width=0, text=txt, textposition="outside",
            textfont=dict(family="IBM Plex Mono", size=11, color=INK),
            hovertemplate="%{y}<br>%{x:,.0f}<extra></extra>"))
        base_layout(fig, max(360, 26 * topn))
        fig.update_xaxes(visible=False)
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c4:
        chart_title("Yellow vs Green",
                    "Revenu total et panier moyen comparés par type de taxi.")
        g = d.groupby("taxi_type").agg(rev=("revenu_total", "sum"),
                                       crs=("nb_courses", "sum")).reset_index()
        g["panier"] = g["rev"] / g["crs"]
        cmap = {"yellow": GOLD, "green": ACCENT}
        fig = go.Figure(go.Bar(
            x=g["taxi_type"], y=g["rev"],
            marker_color=[cmap.get(t, ACCENT_SOFT) for t in g["taxi_type"]],
            marker_line_width=0,
            text=[f"{money_short(v)}<br>panier ${p:.0f}"
                  for v, p in zip(g["rev"], g["panier"])],
            textposition="outside",
            textfont=dict(family="IBM Plex Mono", size=11, color=INK),
            hovertemplate="%{x}<br>%{y:,.0f} $<extra></extra>"))
        base_layout(fig, max(360, 26 * topn))
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    # Heatmap zone × heure + efficacité
    chart_title("Intensité du revenu — zone × heure",
                "Revenu par couple (zone, créneau horaire) : plus la cellule est "
                "foncée, plus elle est rémunératrice.")
    top_zones = zsum.head(12).index.tolist()
    piv = (d[d["pickup_zone"].isin(top_zones)]
           .pivot_table(index="pickup_zone", columns="heure",
                        values="revenu_total", aggfunc="sum", fill_value=0)
           .reindex(top_zones))
    fig = go.Figure(go.Heatmap(z=piv.values, x=piv.columns, y=piv.index,
                               colorscale=GREEN_SCALE, xgap=2, ygap=2,
                               colorbar=dict(thickness=9, len=.8),
                               hovertemplate="%{y}<br>%{x}h → %{z:,.0f} $<extra></extra>"))
    heat_layout(fig, 420)
    fig.update_xaxes(title="heure", dtick=2)
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True, config=CFG)

    chart_title("Efficacité — panier moyen vs distance moyenne (par zone)",
                "Chaque point est une zone ; la taille reflète le nombre de courses. "
                "En haut à gauche : zones les plus rentables au mile.")
    d2 = d.assign(_dw=d["distance_moyenne"] * d["nb_courses"])
    eff = d2.groupby("pickup_zone").agg(rev=("revenu_total", "sum"),
                                        crs=("nb_courses", "sum"),
                                        dw=("_dw", "sum"))
    eff = eff[eff["crs"] >= 300]
    eff["dist"] = eff["dw"] / eff["crs"]
    eff["panier"] = eff["rev"] / eff["crs"]
    fig = go.Figure(go.Scatter(
        x=eff["dist"], y=eff["panier"], mode="markers", text=eff.index,
        marker=dict(size=(eff["crs"] ** 0.5) / 12 + 5, color=ACCENT, opacity=.5,
                    line=dict(color=PAPER, width=.5)),
        hovertemplate="%{text}<br>dist %{x:.1f} mi<br>panier $%{y:.0f}<extra></extra>"))
    base_layout(fig, 360)
    fig.update_xaxes(title="distance moyenne (miles)")
    fig.update_yaxes(title="panier moyen ($)")
    st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    chart_title("Détail (trié par revenu cumulé)")
    st.dataframe(d.sort_values("revenu_total", ascending=False)[
        ["pickup_borough", "pickup_zone", "taxi_type", "heure",
         "nb_courses", "revenu_moyen", "revenu_total"]].head(50),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE 2 — METEO
# ══════════════════════════════════════════════════════════════
def page_meteo():
    df = load_datamart("meteo_demande")
    masthead(len(df))
    page_header("Problématique 02 — Météo & 311",
                "La météo fait-elle varier la demande et les revenus ?")

    with st.sidebar:
        st.markdown("**Filtres — Météo**")
        boroughs = sorted(df["pickup_borough"].unique())
        sel_b = st.multiselect("Borough", boroughs, default=boroughs)
        st.markdown("---")
        mesure = st.radio("Mesure de la heatmap", ["Panier moyen", "Nb courses"])

    d = df[df["pickup_borough"].isin(sel_b)]
    if d.empty:
        st.info("Aucune donnée pour ces filtres.")
        return

    def wavg(frame):
        w = frame["nb_courses"]
        return (frame["revenu_moyen"] * w).sum() / w.sum() if w.sum() else 0

    r_sec = wavg(d[d["tranche_pluie"] == "1_sec"])
    r_forte = wavg(d[d["tranche_pluie"] == "3_pluie_forte"])
    delta = (r_forte - r_sec) / r_sec * 100 if r_sec else 0
    share_pluie = (d[d["tranche_pluie"] != "1_sec"]["nb_courses"].sum()
                   / d["nb_courses"].sum() * 100)
    tearsheet([
        ("Panier — sec", f"$ {r_sec:,.2f}".replace(",", " "), "moy. pondérée", False),
        ("Panier — pluie forte", f"$ {r_forte:,.2f}".replace(",", " "),
         "moy. pondérée", False),
        ("Effet pluie", f"{delta:+.1f} %", "sur le panier", delta < 0),
        ("Courses sous pluie", f"{share_pluie:.0f} %", "du volume", False),
    ])

    plabels = {"1_sec": "Sec", "2_pluie_faible": "Pluie faible",
               "3_pluie_forte": "Pluie forte"}
    tlabels = {"1_gel": "Gel", "2_froid": "Froid", "3_doux": "Doux"}
    porder = ["1_sec", "2_pluie_faible", "3_pluie_forte"]
    torder = ["1_gel", "2_froid", "3_doux"]

    c1, c2 = st.columns([3, 2])
    with c1:
        chart_title(f"{mesure} — pluie × température",
                    "Croisement des conditions météo selon la mesure choisie à gauche.")
        if mesure == "Panier moyen":
            cell = (d.groupby(["tranche_pluie", "tranche_temp"]).apply(wavg)
                    .unstack().reindex(index=porder, columns=torder))
            txt = [[f"${v:.1f}" if pd.notna(v) else "" for v in row]
                   for row in cell.values]
        else:
            cell = (d.groupby(["tranche_pluie", "tranche_temp"])["nb_courses"].sum()
                    .unstack().reindex(index=porder, columns=torder))
            txt = [[fmt_int(v) if pd.notna(v) else "" for v in row]
                   for row in cell.values]
        fig = go.Figure(go.Heatmap(
            z=cell.values, x=[tlabels[c] for c in cell.columns],
            y=[plabels[i] for i in cell.index], colorscale=GREEN_SCALE,
            xgap=3, ygap=3, text=txt, texttemplate="%{text}",
            textfont=dict(family="IBM Plex Mono", size=12),
            colorbar=dict(thickness=9, len=.8)))
        heat_layout(fig, 360)
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c2:
        chart_title("Panier moyen selon la pluie",
                    "Évolution du panier moyen quand l'intensité de pluie augmente.")
        g = d.groupby("tranche_pluie").apply(wavg).reindex(porder)
        colors = [ALERT if k == "3_pluie_forte" else ACCENT_SOFT for k in g.index]
        fig = go.Figure(go.Bar(
            x=[plabels[k] for k in g.index], y=g.values, marker_color=colors,
            marker_line_width=0, text=[f"${v:.1f}" for v in g.values],
            textposition="outside",
            textfont=dict(family="IBM Plex Mono", size=12, color=INK)))
        base_layout(fig, 360)
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    chart_title("Sensibilité à la pluie par borough",
                "Comparaison du panier moyen entre temps sec et pluie forte, "
                "arrondissement par arrondissement.")
    sec = d[d["tranche_pluie"] == "1_sec"].groupby("pickup_borough").apply(wavg)
    forte = d[d["tranche_pluie"] == "3_pluie_forte"].groupby("pickup_borough").apply(wavg)
    commun = [b for b in sec.index if b in forte.index]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=commun, y=[sec[b] for b in commun], name="Sec",
                         marker_color=ACCENT_SOFT, marker_line_width=0))
    fig.add_trace(go.Bar(x=commun, y=[forte[b] for b in commun], name="Pluie forte",
                         marker_color=ALERT, marker_line_width=0))
    base_layout(fig, 340)
    fig.update_layout(barmode="group", showlegend=True,
                      legend=dict(orientation="h", y=1.1, x=0))
    fig.update_yaxes(title="panier moyen ($)")
    st.plotly_chart(fig, use_container_width=True, config=CFG)

    chart_title("Relation signalements 311 ↔ revenu",
                "Chaque point représente un borough sous une condition météo donnée.")
    fig = go.Figure(go.Scatter(
        x=d["moy_signalements_311"], y=d["revenu_moyen"], mode="markers",
        marker=dict(color=ACCENT, opacity=.55, size=9, line_width=0),
        text=d["pickup_borough"],
        hovertemplate="%{text}<br>311=%{x:.0f}<br>$ %{y:.1f}<extra></extra>"))
    base_layout(fig, 320)
    fig.update_xaxes(title="signalements 311 moyens / heure")
    fig.update_yaxes(title="panier moyen ($)")
    st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    chart_title("Détail par borough et conditions")
    st.dataframe(d.sort_values(["pickup_borough", "tranche_pluie", "tranche_temp"]),
                 use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE 3 — ANOMALIES
# ══════════════════════════════════════════════════════════════
def page_anomalies():
    df = load_datamart("anomalies")
    masthead(len(df))
    page_header("Problématique 03 — Anomalies tarifaires",
                "Quelles courses ont un tarif statistiquement aberrant ?")

    with st.sidebar:
        st.markdown("**Filtres — Anomalies**")
        zmin = st.slider("Z-score minimum (|écart|)", 3.0, 50.0, 5.0, 0.5)
        types = sorted(df["taxi_type"].unique())
        sel_t = st.multiselect("Type de taxi", types, default=types)
        zone_q = st.text_input("Filtrer une zone", "")

    d = df[(df["z_score"].abs() >= zmin) & df["taxi_type"].isin(sel_t)].copy()
    if zone_q:
        m = (d["pickup_zone"].str.contains(zone_q, case=False, na=False)
             | d["dropoff_zone"].str.contains(zone_q, case=False, na=False))
        d = d[m]
    if d.empty:
        st.info("Aucune anomalie pour ces filtres.")
        return

    d["absz"] = d["z_score"].abs()
    worst = d.loc[d["absz"].idxmax()]
    short_share = d[d["trip_distance"] <= 3].shape[0] / len(d) * 100
    tearsheet([
        ("Anomalies", fmt_int(len(d)), f"|z| ≥ {zmin}", False),
        ("Z-score max", f"{d['absz'].max():.0f}", "écarts-types", True),
        ("Tarif extrême", fmt_money(d['total_amount'].max()), "course unique", False),
        ("Trajets courts", f"{short_share:.0f} %", "≤ 3 miles", False),
    ])

    c1, c2 = st.columns([3, 2])
    with c1:
        chart_title("Sévérité — distance vs tarif",
                    "Chaque point est une course anormale ; la couleur indique "
                    "l'amplitude de l'écart (z-score). En haut à gauche : trajets "
                    "courts au tarif élevé.")
        fig = go.Figure(go.Scatter(
            x=d["trip_distance"], y=d["total_amount"], mode="markers",
            marker=dict(color=d["absz"], colorscale=SEV_SCALE, size=7, opacity=.6,
                        line_width=0, colorbar=dict(title="|z|", thickness=9, len=.7)),
            hovertemplate="dist %{x:.1f} mi<br>%{y:.0f} $<extra></extra>"))
        fig.add_trace(go.Scatter(
            x=[worst["trip_distance"]], y=[worst["total_amount"]], mode="markers",
            marker=dict(color=ALERT, size=14, line=dict(color=PAPER, width=1.5))))
        base_layout(fig, 420)
        fig.update_xaxes(title="distance (miles)")
        fig.update_yaxes(title="tarif ($)")
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c2:
        chart_title("Top 10 trajets touchés",
                    "Couples départ → arrivée comptant le plus d'anomalies.")
        tj = (d.assign(trajet=d["pickup_zone"] + " → " + d["dropoff_zone"])
              .groupby("trajet").size().sort_values().tail(10))
        fig = go.Figure(go.Bar(
            x=tj.values, y=tj.index, orientation="h", marker_color=ACCENT,
            marker_line_width=0, text=tj.values, textposition="outside",
            textfont=dict(family="IBM Plex Mono", size=11, color=INK)))
        base_layout(fig, 420)
        fig.update_xaxes(visible=False)
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    c3, c4 = st.columns(2)
    with c3:
        chart_title("Distribution des z-scores",
                    "Répartition de l'amplitude des écarts détectés.")
        fig = go.Figure(go.Histogram(x=d["z_score"], nbinsx=40,
                                     marker_color=ACCENT, marker_line_width=0))
        base_layout(fig, 320)
        fig.update_xaxes(title="z-score")
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c4:
        chart_title("Profil horaire des anomalies",
                    "Nombre d'anomalies selon l'heure de prise en charge.")
        hours = pd.to_datetime(d["pickup_datetime"], errors="coerce").dt.hour
        hc = hours.value_counts().reindex(range(24), fill_value=0)
        fig = go.Figure(go.Bar(x=hc.index, y=hc.values, marker_color=ACCENT_SOFT,
                               marker_line_width=0))
        base_layout(fig, 320)
        fig.update_xaxes(title="heure", dtick=2)
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    rule()
    chart_title("Courses les plus anormales")
    st.dataframe(d.sort_values("absz", ascending=False)[
        ["taxi_type", "pickup_zone", "dropoff_zone", "pickup_datetime",
         "total_amount", "moy_tarif", "z_score"]].head(30),
        use_container_width=True, hide_index=True)


PAGES = {
    "Rentabilité chauffeur": page_rentabilite,
    "Météo & demande": page_meteo,
    "Anomalies tarifaires": page_anomalies,
}


def main():
    inject_css()
    with st.sidebar:
        st.markdown(
            f"<div style='font-family:Spectral,serif;font-size:1.2rem;"
            f"font-weight:600;margin-bottom:.2rem'>NYC Mobility</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:.62rem;"
            f"letter-spacing:.14em;color:{MUTED};text-transform:uppercase;"
            f"margin-bottom:1.2rem'>Gold · Athena · S3</div>", unsafe_allow_html=True)
        choice = st.radio("Vue", list(PAGES.keys()), label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption("Source : datamarts Gold (Parquet S3).")
    try:
        PAGES[choice]()
    except Exception as e:
        st.error("Lecture des données impossible. Vérifie tes identifiants AWS "
                 f"et l'accès au bucket `{BUCKET}`.\n\nDétail : {e}")


if __name__ == "__main__":
    main()