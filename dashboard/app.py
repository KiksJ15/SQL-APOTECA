"""
Dashboard interactif APOTECA - Simulation du robot de chimiothérapie
====================================================================
Lance avec: streamlit run dashboard/app.py
"""

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ajouter le dossier dashboard au path pour les imports simulation
sys.path.insert(0, str(Path(__file__).parent))
from simulation import (
    compute_molecule_stats,
    compute_dose_time_regression,
    compute_dose_time_heatmap,
    compute_correlation_table,
    compute_inter_arrival_times,
    estimate_start_times,
    compute_batch_patterns,
    compute_transition_matrix,
    compute_hourly_rhythm,
    fit_production_time_distribution,
    fit_inter_arrival_distribution,
    APOTECASimulator,
    SimulationConfig,
)

# --- Configuration ---
DB_PATH = Path(__file__).parent.parent / "apoteca.db"


@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def query(sql, params=None):
    conn = get_connection()
    return pd.read_sql_query(sql, conn, params=params or [])


# --- Helper: convertir temps HH:MM:SS en secondes ---
def time_to_seconds(col: pd.Series) -> pd.Series:
    def parse_one(val):
        if pd.isna(val) or val == "":
            return None
        parts = str(val).split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return None
    return col.apply(parse_one)


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="APOTECA - Simulation Robot Chimio",
    page_icon="🤖",
    layout="wide",
)

st.title("APOTECA - Simulation Robot de Chimiothérapie")
st.caption("Dashboard interactif pour simuler et analyser l'utilisation du robot APOTECA")

# ============================================================
# SIDEBAR - FILTRES
# ============================================================
st.sidebar.header("Filtres")

# Plage de dates
dates = query("SELECT MIN(date(date_fin)) as d_min, MAX(date(date_fin)) as d_max FROM preparations WHERE date_fin IS NOT NULL")
date_min = pd.to_datetime(dates["d_min"][0]).date()
date_max = pd.to_datetime(dates["d_max"][0]).date()

date_range = st.sidebar.date_input(
    "Période",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    d_start, d_end = date_range
else:
    d_start, d_end = date_min, date_max

# Filtre médicament
meds = query("SELECT DISTINCT m.nom FROM medicaments m JOIN preparations p ON p.medicament_id = m.id ORDER BY m.nom")
selected_meds = st.sidebar.multiselect("Médicaments", meds["nom"].tolist(), default=[])

# Construction clause WHERE
where = "WHERE date(p.date_fin) BETWEEN ? AND ?"
params = [str(d_start), str(d_end)]
if selected_meds:
    placeholders = ",".join(["?"] * len(selected_meds))
    where += f" AND m.nom IN ({placeholders})"
    params.extend(selected_meds)


# ============================================================
# TAB 1 - VUE D'ENSEMBLE
# ============================================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Vue d'ensemble",
    "Simulation journée",
    "Médicaments",
    "Qualité & Erreurs",
    "Productivité",
    "Stocks & Température",
    "Temps par Molécule",
    "Séquences de Production",
    "Simulation DES",
])

with tab1:
    st.header("Vue d'ensemble de l'activité")

    # KPIs
    kpis = query(f"""
        SELECT
            COUNT(*) AS nb_preps,
            COUNT(DISTINCT p.patient_code) AS nb_patients,
            COUNT(DISTINCT p.medicament_id) AS nb_meds,
            ROUND(AVG(p.dosage_mg), 1) AS dosage_moyen,
            COUNT(DISTINCT date(p.date_fin)) AS nb_jours
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
    """, params)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Préparations", f"{kpis['nb_preps'][0]:,}")
    c2.metric("Patients", f"{kpis['nb_patients'][0]:,}")
    c3.metric("Médicaments", kpis["nb_meds"][0])
    c4.metric("Dosage moyen", f"{kpis['dosage_moyen'][0]} mg")
    c5.metric("Jours actifs", kpis["nb_jours"][0])

    # Graphique mensuel
    monthly = query(f"""
        SELECT strftime('%Y-%m', p.date_fin) AS mois, COUNT(*) AS nb
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        GROUP BY mois ORDER BY mois
    """, params)

    fig_monthly = px.bar(
        monthly, x="mois", y="nb",
        title="Préparations par mois",
        labels={"mois": "Mois", "nb": "Nombre de préparations"},
        color_discrete_sequence=["#1f77b4"],
    )
    st.plotly_chart(fig_monthly, width="stretch")

    # Distribution par jour de la semaine et par heure
    col_left, col_right = st.columns(2)

    with col_left:
        weekday = query(f"""
            SELECT
                CASE CAST(strftime('%w', p.date_fin) AS INTEGER)
                    WHEN 0 THEN 'Dim' WHEN 1 THEN 'Lun' WHEN 2 THEN 'Mar'
                    WHEN 3 THEN 'Mer' WHEN 4 THEN 'Jeu' WHEN 5 THEN 'Ven' WHEN 6 THEN 'Sam'
                END AS jour,
                CAST(strftime('%w', p.date_fin) AS INTEGER) AS jour_num,
                COUNT(*) AS nb
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            {where}
            GROUP BY jour_num ORDER BY jour_num
        """, params)

        fig_wd = px.bar(
            weekday, x="jour", y="nb",
            title="Préparations par jour de la semaine",
            labels={"jour": "", "nb": "Préparations"},
            color_discrete_sequence=["#2ca02c"],
        )
        st.plotly_chart(fig_wd, width="stretch")

    with col_right:
        hourly = query(f"""
            SELECT CAST(strftime('%H', p.date_fin) AS INTEGER) AS heure, COUNT(*) AS nb
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            {where}
            GROUP BY heure ORDER BY heure
        """, params)

        fig_hr = px.bar(
            hourly, x="heure", y="nb",
            title="Préparations par heure de la journée",
            labels={"heure": "Heure", "nb": "Préparations"},
            color_discrete_sequence=["#ff7f0e"],
        )
        st.plotly_chart(fig_hr, width="stretch")


# ============================================================
# TAB 2 - SIMULATION D'UNE JOURNÉE
# ============================================================
with tab2:
    st.header("Simulation d'une journée de production")

    # Sélection de date
    available_dates = query(f"""
        SELECT DISTINCT date(p.date_fin) AS jour, COUNT(*) AS nb
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        GROUP BY jour HAVING nb > 0 ORDER BY jour DESC
    """, params)

    if available_dates.empty:
        st.warning("Aucune donnée pour les filtres sélectionnés.")
    else:
        sim_date = st.selectbox(
            "Choisir une journée à simuler",
            available_dates["jour"].tolist(),
            index=0,
        )

        # Détails de la journée
        day_data = query("""
            SELECT
                p.date_fin AS heure,
                m.nom_complet AS medicament,
                p.dosage_mg,
                c.nom AS conteneur,
                p.temps_production,
                p.temps_confirmation,
                p.temps_queue,
                p.temps_final_check,
                p.patient_code
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            LEFT JOIN conteneurs c ON p.conteneur_id = c.id
            WHERE date(p.date_fin) = ?
            ORDER BY p.date_fin
        """, [sim_date])

        # KPIs de la journée
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Préparations", len(day_data))
        k2.metric("Patients", day_data["patient_code"].nunique())
        k3.metric("Médicaments", day_data["medicament"].nunique())

        prod_sec = time_to_seconds(day_data["temps_production"])
        avg_prod = prod_sec.mean()
        if pd.notna(avg_prod):
            k4.metric("Temps prod. moyen", f"{int(avg_prod // 60)}m {int(avg_prod % 60)}s")

        # Timeline de la journée
        st.subheader("Timeline de production")
        day_data["prod_sec"] = prod_sec
        day_data["heure_str"] = pd.to_datetime(day_data["heure"]).dt.strftime("%H:%M:%S")

        fig_timeline = px.scatter(
            day_data, x="heure", y="dosage_mg",
            color="medicament",
            size="prod_sec",
            hover_data=["conteneur", "temps_production"],
            title=f"Productions du {sim_date}",
            labels={"heure": "Heure", "dosage_mg": "Dosage (mg)", "medicament": "Médicament"},
        )
        st.plotly_chart(fig_timeline, width="stretch")

        # Répartition des médicaments de la journée
        col1, col2 = st.columns(2)
        with col1:
            med_counts = day_data["medicament"].value_counts().reset_index()
            med_counts.columns = ["medicament", "nb"]
            fig_pie = px.pie(
                med_counts, names="medicament", values="nb",
                title="Répartition des médicaments",
            )
            st.plotly_chart(fig_pie, width="stretch")

        with col2:
            # Temps par étape
            steps_data = pd.DataFrame({
                "Étape": ["Confirmation", "Queue", "Production", "Final Check"],
                "Durée moy. (sec)": [
                    time_to_seconds(day_data["temps_confirmation"]).mean(),
                    time_to_seconds(day_data["temps_queue"]).mean(),
                    time_to_seconds(day_data["temps_production"]).mean(),
                    time_to_seconds(day_data["temps_final_check"]).mean(),
                ],
            })
            steps_data = steps_data.dropna()
            fig_steps = px.bar(
                steps_data, x="Étape", y="Durée moy. (sec)",
                title="Temps moyen par étape du robot",
                color_discrete_sequence=["#d62728"],
            )
            st.plotly_chart(fig_steps, width="stretch")

        # Tableau détaillé
        with st.expander("Tableau détaillé des préparations"):
            display_cols = ["heure_str", "medicament", "dosage_mg", "conteneur",
                          "temps_production", "temps_confirmation"]
            st.dataframe(
                day_data[display_cols].rename(columns={
                    "heure_str": "Heure",
                    "medicament": "Médicament",
                    "dosage_mg": "Dosage (mg)",
                    "conteneur": "Conteneur",
                    "temps_production": "T. Production",
                    "temps_confirmation": "T. Confirmation",
                }),
                width="stretch",
                hide_index=True,
            )


# ============================================================
# TAB 3 - MÉDICAMENTS
# ============================================================
with tab3:
    st.header("Analyse des médicaments")

    # Top médicaments
    top_meds = query(f"""
        SELECT
            m.nom_complet AS medicament,
            COUNT(*) AS nb_preparations,
            ROUND(SUM(p.dosage_mg), 1) AS dose_totale_mg,
            ROUND(AVG(p.dosage_mg), 1) AS dose_moyenne_mg,
            ROUND(MIN(p.dosage_mg), 1) AS dose_min_mg,
            ROUND(MAX(p.dosage_mg), 1) AS dose_max_mg
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        GROUP BY m.nom_complet
        ORDER BY nb_preparations DESC
    """, params)

    fig_top = px.bar(
        top_meds.head(15), x="nb_preparations", y="medicament",
        orientation="h",
        title="Top 15 médicaments (nombre de préparations)",
        labels={"nb_preparations": "Préparations", "medicament": ""},
        color="dose_moyenne_mg",
        color_continuous_scale="Viridis",
    )
    fig_top.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_top, width="stretch")

    # Consommation par service
    st.subheader("Consommation par service hospitalier")
    service_data = query("""
        SELECT s.nom AS service, m.nom AS medicament, um.nb_preparations, um.dose_totale
        FROM utilisation_medicaments um
        JOIN services s ON um.service_id = s.id
        JOIN medicaments m ON um.medicament_id = m.id
        ORDER BY um.nb_preparations DESC
    """)

    if not service_data.empty:
        fig_service = px.treemap(
            service_data, path=["service", "medicament"],
            values="nb_preparations",
            title="Consommation par service et médicament",
            color="dose_totale",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig_service, width="stretch")

    # Tableau complet
    with st.expander("Tableau détaillé des médicaments"):
        st.dataframe(top_meds, width="stretch", hide_index=True)


# ============================================================
# TAB 4 - QUALITÉ & ERREURS
# ============================================================
with tab4:
    st.header("Qualité et gestion des erreurs")

    col_q1, col_q2 = st.columns(2)

    with col_q1:
        # Précision de dosage
        st.subheader("Précision du dosage")
        precision = query("""
            SELECT precision_pct, SUM(nombre) AS nb
            FROM distribution_precision_dosage
            GROUP BY precision_pct
            ORDER BY precision_pct
        """)

        if not precision.empty:
            fig_prec = px.bar(
                precision, x="precision_pct", y="nb",
                title="Distribution de la précision de dosage",
                labels={"precision_pct": "Écart (%)", "nb": "Nombre de préparations"},
                color="nb",
                color_continuous_scale="RdYlGn_r",
            )
            st.plotly_chart(fig_prec, width="stretch")

            # Calcul du taux dans +-1%
            total = precision["nb"].sum()
            within_1 = precision[precision["precision_pct"].abs() <= 1]["nb"].sum()
            st.success(f"Taux de précision ±1% : **{within_1/total*100:.1f}%** ({within_1}/{total} préparations)")

    with col_q2:
        # Erreurs
        st.subheader("Erreurs du robot")
        erreurs = query("""
            SELECT message AS type_erreur, COUNT(*) AS nb,
                   GROUP_CONCAT(description, ' | ') AS details
            FROM erreurs
            GROUP BY message
            ORDER BY nb DESC
        """)

        if not erreurs.empty:
            # Raccourcir les labels pour le camembert
            erreurs["label"] = erreurs["type_erreur"].str[:50].where(
                erreurs["type_erreur"].str.len() <= 50,
                erreurs["type_erreur"].str[:50] + "...",
            )
            fig_err = px.bar(
                erreurs, x="nb", y="label",
                orientation="h",
                title="Types d'erreurs",
                labels={"nb": "Nombre", "label": ""},
                color="nb",
                color_continuous_scale="Reds",
            )
            fig_err.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
            st.plotly_chart(fig_err, width="stretch")

            st.dataframe(
                erreurs[["type_erreur", "nb", "details"]].rename(columns={
                    "type_erreur": "Type d'erreur",
                    "nb": "Nombre",
                    "details": "Détails",
                }),
                width="stretch", hide_index=True,
            )

    # Taux d'erreur global
    nb_err = query("SELECT COUNT(*) AS n FROM erreurs")["n"][0]
    nb_prep = query("SELECT COUNT(*) AS n FROM preparations")["n"][0]
    taux = nb_err / nb_prep * 100 if nb_prep > 0 else 0
    st.info(f"Taux d'erreur global : **{taux:.3f}%** ({nb_err} erreurs / {nb_prep} préparations)")

    # Précision par médicament
    st.subheader("Précision par médicament")
    prec_med = query("""
        SELECT
            m.nom AS medicament,
            SUM(d.nombre) AS total,
            SUM(CASE WHEN ABS(d.precision_pct) <= 1 THEN d.nombre ELSE 0 END) AS dans_1pct,
            ROUND(SUM(CASE WHEN ABS(d.precision_pct) <= 1 THEN d.nombre ELSE 0 END) * 100.0 / SUM(d.nombre), 1) AS pct_1pct
        FROM distribution_precision_dosage d
        JOIN medicaments m ON d.medicament_id = m.id
        GROUP BY m.nom
        ORDER BY total DESC
    """)
    if not prec_med.empty:
        fig_pm = px.bar(
            prec_med, x="medicament", y="pct_1pct",
            title="Taux de précision ±1% par médicament",
            labels={"medicament": "", "pct_1pct": "% dans ±1%"},
            color="pct_1pct",
            color_continuous_scale="RdYlGn",
            range_color=[80, 100],
        )
        st.plotly_chart(fig_pm, width="stretch")


# ============================================================
# TAB 5 - PRODUCTIVITÉ
# ============================================================
with tab5:
    st.header("Productivité des opérateurs")

    # Résumé par opérateur
    prod_summary = query("""
        SELECT
            u.nom AS operateur,
            COUNT(*) AS nb_jours,
            SUM(pu.nb_preparations) AS total_preps,
            ROUND(AVG(pu.nb_preparations), 1) AS preps_jour_moy,
            ROUND(AVG(pu.preparations_par_heure), 1) AS preps_heure_moy,
            MAX(pu.nb_preparations) AS record_jour
        FROM productivite_utilisateurs pu
        JOIN utilisateurs u ON pu.utilisateur_id = u.id
        WHERE pu.nb_preparations > 0
        GROUP BY u.nom
        ORDER BY total_preps DESC
    """)

    if not prod_summary.empty:
        st.dataframe(prod_summary.rename(columns={
            "operateur": "Opérateur",
            "nb_jours": "Jours travaillés",
            "total_preps": "Total préparations",
            "preps_jour_moy": "Preps/jour (moy)",
            "preps_heure_moy": "Preps/heure (moy)",
            "record_jour": "Record journalier",
        }), width="stretch", hide_index=True)

    # Évolution dans le temps
    prod_time = query("""
        SELECT pu.date, u.nom AS operateur, pu.nb_preparations, pu.preparations_par_heure
        FROM productivite_utilisateurs pu
        JOIN utilisateurs u ON pu.utilisateur_id = u.id
        WHERE pu.nb_preparations > 0
        ORDER BY pu.date
    """)

    if not prod_time.empty:
        prod_time["date"] = pd.to_datetime(prod_time["date"])
        fig_prod = px.line(
            prod_time, x="date", y="preparations_par_heure",
            color="operateur",
            title="Évolution de la productivité par opérateur",
            labels={"date": "Date", "preparations_par_heure": "Preps/heure", "operateur": "Opérateur"},
        )
        st.plotly_chart(fig_prod, width="stretch")

    # Simulation de charge
    st.subheader("Simulation de montée en charge")
    pct_increase = st.slider("Augmentation du volume (%)", 0, 100, 20, 5)

    charge = query(f"""
        SELECT
            strftime('%Y-%m', p.date_fin) AS mois,
            COUNT(*) AS preps_actuelles,
            ROUND(SUM(
                CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
                CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
                CAST(substr(p.temps_production,7,2) AS INTEGER)
            ) / 3600.0, 1) AS heures_prod
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        AND p.temps_production != ''
        GROUP BY mois ORDER BY mois
    """, params)

    if not charge.empty:
        factor = 1 + pct_increase / 100
        charge["preps_simulation"] = (charge["preps_actuelles"] * factor).round().astype(int)
        charge["heures_simulation"] = (charge["heures_prod"] * factor).round(1)

        fig_charge = go.Figure()
        fig_charge.add_trace(go.Bar(
            x=charge["mois"], y=charge["preps_actuelles"],
            name="Actuel", marker_color="#1f77b4",
        ))
        fig_charge.add_trace(go.Bar(
            x=charge["mois"], y=charge["preps_simulation"],
            name=f"Simulation +{pct_increase}%", marker_color="#ff7f0e",
        ))
        fig_charge.update_layout(
            title=f"Simulation de charge : +{pct_increase}% de volume",
            xaxis_title="Mois",
            yaxis_title="Nombre de préparations",
            barmode="group",
        )
        st.plotly_chart(fig_charge, width="stretch")

        # Capacité max estimée
        daily_cap = query(f"""
            SELECT date(p.date_fin) AS jour, COUNT(*) AS nb
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            {where}
            GROUP BY jour
        """, params)
        if not daily_cap.empty:
            max_day = daily_cap["nb"].max()
            avg_day = daily_cap["nb"].mean()
            st.info(
                f"Capacité observée : **max {max_day} preps/jour**, "
                f"moyenne {avg_day:.0f} preps/jour. "
                f"Avec +{pct_increase}%, la moyenne passerait à **{avg_day * factor:.0f} preps/jour**."
            )


# ============================================================
# TAB 6 - STOCKS & TEMPÉRATURE
# ============================================================
with tab6:
    st.header("Stocks, Consommation & Monitoring")

    # --- Consommation de stock par médicament et par mois ---
    st.subheader("Consommation de stock par médicament")

    conso_mensuelle = query(f"""
        SELECT
            m.nom AS medicament,
            strftime('%Y-%m', p.date_fin) AS mois,
            COUNT(*) AS nb_preparations,
            ROUND(SUM(p.dosage_mg), 1) AS consommation_mg
        FROM preparations p
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        AND p.dosage_mg IS NOT NULL
        GROUP BY m.nom, mois
        ORDER BY mois, consommation_mg DESC
    """, params)

    if not conso_mensuelle.empty:
        # KPIs consommation
        total_conso_mg = conso_mensuelle["consommation_mg"].sum()
        nb_meds_used = conso_mensuelle["medicament"].nunique()
        nb_mois = conso_mensuelle["mois"].nunique()
        conso_moy_mois = total_conso_mg / nb_mois if nb_mois > 0 else 0

        ck1, ck2, ck3, ck4 = st.columns(4)
        ck1.metric("Consommation totale", f"{total_conso_mg / 1000:,.1f} g")
        ck2.metric("Molécules utilisées", nb_meds_used)
        ck3.metric("Conso. moy./mois", f"{conso_moy_mois / 1000:,.1f} g")
        ck4.metric("Période couverte", f"{nb_mois} mois")

        # Top 10 médicaments par consommation totale
        top_conso = conso_mensuelle.groupby("medicament").agg(
            total_mg=("consommation_mg", "sum"),
            total_preps=("nb_preparations", "sum"),
        ).sort_values("total_mg", ascending=False).head(10).reset_index()

        fig_top_conso = px.bar(
            top_conso, x="total_mg", y="medicament",
            orientation="h",
            title="Top 10 - Consommation totale (mg)",
            labels={"total_mg": "Consommation (mg)", "medicament": ""},
            color="total_preps",
            color_continuous_scale="Oranges",
        )
        fig_top_conso.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_top_conso, width="stretch")

        # Évolution mensuelle des top 5
        top5_meds = top_conso["medicament"].head(5).tolist()
        conso_top5 = conso_mensuelle[conso_mensuelle["medicament"].isin(top5_meds)]

        fig_evol = px.line(
            conso_top5, x="mois", y="consommation_mg",
            color="medicament",
            title="Évolution mensuelle de la consommation (Top 5)",
            labels={"mois": "Mois", "consommation_mg": "Consommation (mg)", "medicament": "Médicament"},
            markers=True,
        )
        st.plotly_chart(fig_evol, width="stretch")

    # --- Consommation dispositifs (poches, seringues, etc.) ---
    st.subheader("Consommation de dispositifs (poches, seringues)")

    conteneurs_usage = query(f"""
        SELECT
            c.nom AS conteneur,
            COUNT(*) AS nb_utilises
        FROM preparations p
        JOIN conteneurs c ON p.conteneur_id = c.id
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        GROUP BY c.nom
        ORDER BY nb_utilises DESC
    """, params)

    if not conteneurs_usage.empty:
        col_disp1, col_disp2 = st.columns(2)
        with col_disp1:
            fig_cont = px.bar(
                conteneurs_usage, x="nb_utilises", y="conteneur",
                orientation="h",
                title="Conteneurs utilisés (total)",
                labels={"nb_utilises": "Nombre", "conteneur": ""},
                color_discrete_sequence=["#17becf"],
            )
            fig_cont.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_cont, width="stretch")

        with col_disp2:
            fig_cont_pie = px.pie(
                conteneurs_usage, names="conteneur", values="nb_utilises",
                title="Répartition des conteneurs",
            )
            fig_cont_pie.update_traces(textposition="inside", textinfo="percent+label")
            fig_cont_pie.update_layout(showlegend=False)
            st.plotly_chart(fig_cont_pie, width="stretch")

    # Évolution mensuelle des conteneurs
    conteneurs_mois = query(f"""
        SELECT
            strftime('%Y-%m', p.date_fin) AS mois,
            c.nom AS conteneur,
            COUNT(*) AS nb
        FROM preparations p
        JOIN conteneurs c ON p.conteneur_id = c.id
        JOIN medicaments m ON p.medicament_id = m.id
        {where}
        GROUP BY mois, c.nom
        ORDER BY mois
    """, params)

    if not conteneurs_mois.empty:
        # Top 5 conteneurs pour lisibilité
        top5_cont = conteneurs_usage["conteneur"].head(5).tolist()
        cont_top5 = conteneurs_mois[conteneurs_mois["conteneur"].isin(top5_cont)]
        fig_cont_evol = px.line(
            cont_top5, x="mois", y="nb",
            color="conteneur",
            title="Évolution mensuelle des conteneurs (Top 5)",
            labels={"mois": "Mois", "nb": "Nombre", "conteneur": "Conteneur"},
            markers=True,
        )
        st.plotly_chart(fig_cont_evol, width="stretch")

    # Composants consommés - séparés entre captifs et chimiothérapies
    composants = query("""
        SELECT m.nom_complet AS composant, cu.quantite
        FROM composants_utilisation cu
        JOIN medicaments m ON cu.medicament_id = m.id
        ORDER BY cu.quantite DESC
    """)

    if not composants.empty:
        # Classification : captifs = poches, seringues, aiguilles, eau PPI
        motifs_captifs = ["POCHE", "SYRINGE", "NaCl", "Glucose", "EAU PPI", "NEEDLE", "PUMP", "FOLFUSOR"]
        composants["type"] = composants["composant"].apply(
            lambda x: "Captif" if any(m in x.upper() for m in [s.upper() for s in motifs_captifs]) else "Chimiothérapie"
        )

        captifs = composants[composants["type"] == "Captif"].copy()
        chimios = composants[composants["type"] == "Chimiothérapie"].copy()

        st.subheader("Composants consommés")
        col_cap, col_chi = st.columns(2)

        with col_cap:
            if not captifs.empty:
                fig_cap = px.bar(
                    captifs, x="quantite", y="composant",
                    orientation="h",
                    title="Captifs (poches, seringues, aiguilles)",
                    labels={"quantite": "Quantité", "composant": ""},
                    color_discrete_sequence=["#17becf"],
                )
                fig_cap.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_cap, width="stretch")

        with col_chi:
            if not chimios.empty:
                fig_chi = px.bar(
                    chimios, x="quantite", y="composant",
                    orientation="h",
                    title="Flacons de chimiothérapie",
                    labels={"quantite": "Quantité", "composant": ""},
                    color_discrete_sequence=["#e377c2"],
                )
                fig_chi.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_chi, width="stretch")

        with st.expander("Tableau complet des composants"):
            st.dataframe(
                composants[["composant", "quantite", "type"]].rename(columns={
                    "composant": "Composant",
                    "quantite": "Quantité",
                    "type": "Type",
                }),
                width="stretch", hide_index=True,
            )

    # --- Consommation par service ---
    st.subheader("Consommation par service hospitalier")
    conso_service = query("""
        SELECT
            s.nom AS service,
            m.nom AS medicament,
            um.nb_preparations,
            um.dose_totale,
            um.unite_mesure
        FROM utilisation_medicaments um
        JOIN services s ON um.service_id = s.id
        JOIN medicaments m ON um.medicament_id = m.id
        ORDER BY um.dose_totale DESC
    """)

    if not conso_service.empty:
        col_srv1, col_srv2 = st.columns(2)
        with col_srv1:
            service_totals = conso_service.groupby("service").agg(
                total_dose=("dose_totale", "sum"),
                total_preps=("nb_preparations", "sum"),
            ).sort_values("total_dose", ascending=False).reset_index()

            fig_srv = px.pie(
                service_totals, names="service", values="total_dose",
                title="Répartition de la consommation par service (mg)",
            )
            st.plotly_chart(fig_srv, width="stretch")

        with col_srv2:
            fig_srv_bar = px.bar(
                service_totals, x="service", y="total_preps",
                title="Nombre de préparations par service",
                labels={"service": "", "total_preps": "Préparations"},
                color="total_dose",
                color_continuous_scale="Blues",
            )
            st.plotly_chart(fig_srv_bar, width="stretch")

        with st.expander("Détail consommation par service et médicament"):
            st.dataframe(
                conso_service.rename(columns={
                    "service": "Service",
                    "medicament": "Médicament",
                    "nb_preparations": "Préparations",
                    "dose_totale": "Dose totale",
                    "unite_mesure": "Unité",
                }),
                width="stretch", hide_index=True,
            )

    # --- État des stocks et péremption ---
    st.subheader("État des stocks et dates de péremption")

    stocks = query("""
        SELECT
            m.nom AS medicament,
            sm.lot,
            sm.quantite_totale AS quantite,
            sm.date_expiration,
            CASE
                WHEN sm.date_expiration < date('now') THEN 'EXPIRÉ'
                WHEN sm.date_expiration < date('now', '+90 days') THEN 'EXPIRE BIENTÔT'
                ELSE 'OK'
            END AS statut
        FROM statistiques_medicaments sm
        JOIN medicaments m ON sm.medicament_id = m.id
        WHERE sm.date_expiration IS NOT NULL
        ORDER BY sm.date_expiration
    """)

    if not stocks.empty:
        def color_status(val):
            if val == "EXPIRÉ":
                return "background-color: #ffcccc"
            elif val == "EXPIRE BIENTÔT":
                return "background-color: #fff3cd"
            return "background-color: #d4edda"

        styled = stocks.style.map(color_status, subset=["statut"])
        st.dataframe(styled, width="stretch", hide_index=True)

        nb_expired = len(stocks[stocks["statut"] == "EXPIRÉ"])
        nb_soon = len(stocks[stocks["statut"] == "EXPIRE BIENTÔT"])
        nb_ok = len(stocks[stocks["statut"] == "OK"])
        if nb_expired > 0:
            st.error(f"{nb_expired} lot(s) expiré(s)")
        if nb_soon > 0:
            st.warning(f"{nb_soon} lot(s) expire(nt) dans moins de 90 jours")
        if nb_ok > 0 and nb_expired == 0 and nb_soon == 0:
            st.success(f"Tous les {nb_ok} lots sont en date")

    # --- Température ---
    st.subheader("Température de la chambre robot")
    temps = query("""
        SELECT date_heure, temperature
        FROM temperatures
        ORDER BY date_heure
    """)

    if not temps.empty:
        fig_temp = px.line(
            temps, x="date_heure", y="temperature",
            title="Température du robot APOTECA",
            labels={"date_heure": "Date/Heure", "temperature": "Température (°C)"},
        )
        fig_temp.add_hline(y=25, line_dash="dash", line_color="red", annotation_text="Limite haute 25°C")
        fig_temp.add_hline(y=18, line_dash="dash", line_color="blue", annotation_text="Limite basse 18°C")
        st.plotly_chart(fig_temp, width="stretch")

        t_min = temps["temperature"].min()
        t_max = temps["temperature"].max()
        t_avg = temps["temperature"].mean()
        alerts = temps[(temps["temperature"] > 25) | (temps["temperature"] < 18)]
        if len(alerts) > 0:
            st.warning(f"Alertes température : {len(alerts)} relevés hors plage")
        else:
            st.success(f"Température OK : min={t_min}°C, max={t_max}°C, moy={t_avg:.1f}°C")

    # --- Nettoyages ---
    st.subheader("Historique nettoyages")
    nettoyages = query("""
        SELECT
            tn.debut, tn.fin, tn.type_nettoyage AS type,
            u.nom AS operateur, tn.commentaire
        FROM taches_nettoyage tn
        LEFT JOIN utilisateurs u ON tn.utilisateur_id = u.id
        ORDER BY tn.debut DESC
    """)
    if not nettoyages.empty:
        st.dataframe(nettoyages, width="stretch", hide_index=True)


# ============================================================
# TAB 7 - TEMPS PAR MOLÉCULE
# ============================================================
with tab7:
    st.header("Analyse des temps de préparation par molécule")

    # Requête brute : molecule, dosage_mg, prod_sec
    @st.cache_data(ttl=300)
    def load_dose_time_data(_d_start, _d_end, _selected_meds):
        med_filter = ""
        p = [str(_d_start), str(_d_end)]
        if _selected_meds:
            placeholders = ",".join(["?"] * len(_selected_meds))
            med_filter = f" AND m.nom IN ({placeholders})"
            p.extend(_selected_meds)

        return query(f"""
            SELECT
                m.nom AS molecule,
                p.dosage_mg,
                CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
                CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
                CAST(substr(p.temps_production,7,2) AS INTEGER) AS prod_sec
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            WHERE p.temps_production IS NOT NULL AND p.temps_production <> ''
              AND p.dosage_mg IS NOT NULL
              AND date(p.date_fin) BETWEEN ? AND ?
              {med_filter}
        """, p)

    dt_data = load_dose_time_data(d_start, d_end, tuple(selected_meds) if selected_meds else ())

    if dt_data.empty:
        st.warning("Aucune donnée de temps de production pour les filtres sélectionnés.")
    else:
        # --- KPIs globaux ---
        mol_stats = compute_molecule_stats(dt_data)

        avg_global = dt_data["prod_sec"].mean()
        median_global = dt_data["prod_sec"].median()
        fastest = mol_stats.loc[mol_stats["temps_median"].idxmin()]
        slowest = mol_stats.loc[mol_stats["temps_median"].idxmax()]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Temps prod. moyen", f"{int(avg_global // 60)}m {int(avg_global % 60)}s")
        k2.metric("Temps prod. médian", f"{int(median_global // 60)}m {int(median_global % 60)}s")
        k3.metric("Plus rapide (médian)", f"{fastest['molecule']}: {int(fastest['temps_median'])}s")
        k4.metric("Plus lente (médian)", f"{slowest['molecule']}: {int(slowest['temps_median'])}s")

        # --- Sélecteur de molécules dans le tab ---
        top_molecules = mol_stats.head(15)["molecule"].tolist()
        selected_tab7 = st.multiselect(
            "Molécules à afficher (défaut: top 15 par volume)",
            mol_stats["molecule"].tolist(),
            default=top_molecules,
            key="tab7_mol_select",
        )

        if selected_tab7:
            filtered_stats = mol_stats[mol_stats["molecule"].isin(selected_tab7)]
            filtered_data = dt_data[dt_data["molecule"].isin(selected_tab7)]

            # --- Tableau de stats ---
            st.subheader("Statistiques par molécule")
            st.dataframe(
                filtered_stats.rename(columns={
                    "molecule": "Molécule",
                    "count": "Nb preps",
                    "dose_moy": "Dose moy. (mg)",
                    "dose_min": "Dose min",
                    "dose_max": "Dose max",
                    "temps_moy": "T. moy (sec)",
                    "temps_median": "T. médian (sec)",
                    "temps_min": "T. min (sec)",
                    "temps_max": "T. max (sec)",
                    "temps_std": "Ecart-type (sec)",
                }),
                width="stretch", hide_index=True,
            )

            # --- Box plots ---
            col_box, col_scatter_global = st.columns(2)

            with col_box:
                st.subheader("Distribution du temps de production")
                fig_box = px.box(
                    filtered_data, x="molecule", y="prod_sec",
                    title="Temps de production par molécule",
                    labels={"molecule": "", "prod_sec": "Temps (sec)"},
                    color="molecule",
                )
                fig_box.update_layout(showlegend=False, xaxis_tickangle=-45)
                st.plotly_chart(fig_box, width="stretch")

            with col_scatter_global:
                st.subheader("Dose vs Temps (toutes molécules)")
                fig_global = px.scatter(
                    filtered_data, x="dosage_mg", y="prod_sec",
                    color="molecule",
                    opacity=0.5,
                    title="Corrélation dose-temps (global)",
                    labels={"dosage_mg": "Dose (mg)", "prod_sec": "Temps prod. (sec)"},
                    trendline="ols",
                )
                st.plotly_chart(fig_global, width="stretch")

            # --- Scatter plots par molécule (top 6) ---
            st.subheader("Dose vs Temps par molécule (détail)")
            top6 = selected_tab7[:6]
            cols_per_row = 3
            for row_start in range(0, len(top6), cols_per_row):
                cols = st.columns(cols_per_row)
                for i, col in enumerate(cols):
                    idx = row_start + i
                    if idx >= len(top6):
                        break
                    mol = top6[idx]
                    mol_data = filtered_data[filtered_data["molecule"] == mol]
                    with col:
                        reg = compute_dose_time_regression(dt_data, mol)
                        title_suffix = f" (R²={reg['r_squared']:.3f})" if reg else ""
                        fig_mol = px.scatter(
                            mol_data, x="dosage_mg", y="prod_sec",
                            title=f"{mol}{title_suffix}",
                            labels={"dosage_mg": "Dose (mg)", "prod_sec": "Temps (sec)"},
                            trendline="ols",
                            color_discrete_sequence=["#1f77b4"],
                        )
                        fig_mol.update_layout(height=300)
                        st.plotly_chart(fig_mol, width="stretch")

            # --- Heatmap dose x molecule x temps ---
            st.subheader("Heatmap : dose vs temps moyen par molécule")
            heatmap_df = compute_dose_time_heatmap(dt_data, n_bins=5, min_count=10)
            if not heatmap_df.empty:
                fig_heat = px.imshow(
                    heatmap_df,
                    title="Temps de production moyen (sec) par plage de dose",
                    labels=dict(x="Plage de dose", y="Molécule", color="Temps (sec)"),
                    color_continuous_scale="YlOrRd",
                    aspect="auto",
                )
                st.plotly_chart(fig_heat, width="stretch")
            else:
                st.info("Pas assez de données pour la heatmap.")

            # --- Table de corrélations ---
            with st.expander("Corrélations dose-temps (Pearson)"):
                corr_table = compute_correlation_table(dt_data)
                if not corr_table.empty:
                    st.dataframe(
                        corr_table.rename(columns={
                            "molecule": "Molécule",
                            "correlation": "Corrélation (r)",
                            "p_value": "p-value",
                            "n_samples": "N échantillons",
                            "significatif": "Significatif (p<0.05)",
                        }),
                        width="stretch", hide_index=True,
                    )
                else:
                    st.info("Pas assez de données pour les corrélations.")


# ============================================================
# TAB 8 - SÉQUENCES DE PRODUCTION
# ============================================================
with tab8:
    st.header("Analyse des séquences de production")

    # Requête pour données séquentielles
    @st.cache_data(ttl=300)
    def load_sequence_data(_d_start, _d_end, _selected_meds):
        med_filter = ""
        p = [str(_d_start), str(_d_end)]
        if _selected_meds:
            placeholders = ",".join(["?"] * len(_selected_meds))
            med_filter = f" AND m.nom IN ({placeholders})"
            p.extend(_selected_meds)

        return query(f"""
            SELECT
                p.date_fin,
                date(p.date_fin) AS jour,
                m.nom AS molecule,
                p.dosage_mg,
                CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
                CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
                CAST(substr(p.temps_production,7,2) AS INTEGER) AS prod_sec
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            WHERE p.temps_production IS NOT NULL AND p.temps_production <> ''
              AND p.date_fin IS NOT NULL
              AND date(p.date_fin) BETWEEN ? AND ?
              {med_filter}
            ORDER BY p.date_fin
        """, p)

    seq_data = load_sequence_data(d_start, d_end, tuple(selected_meds) if selected_meds else ())

    if seq_data.empty:
        st.warning("Aucune donnée de séquence pour les filtres sélectionnés.")
    else:
        # --- Inter-arrivées ---
        st.subheader("Temps inter-arrivées entre préparations")

        seq_with_ia = compute_inter_arrival_times(seq_data)
        ia_values = seq_with_ia["inter_arrival_sec"].dropna()
        ia_positive = ia_values[ia_values > 0]

        if not ia_positive.empty:
            col_ia1, col_ia2 = st.columns([2, 1])

            with col_ia1:
                fig_ia = px.histogram(
                    ia_positive, x=ia_positive,
                    nbins=50,
                    title="Distribution des temps inter-arrivées",
                    labels={"x": "Temps inter-arrivée (sec)", "count": "Fréquence"},
                    color_discrete_sequence=["#1f77b4"],
                )
                fig_ia.update_layout(xaxis_range=[0, min(ia_positive.quantile(0.95), 3600)])
                st.plotly_chart(fig_ia, width="stretch")

            with col_ia2:
                ia_fit = fit_inter_arrival_distribution(ia_positive.values)
                st.metric("Moyenne", f"{ia_positive.mean():.0f} sec ({ia_positive.mean()/60:.1f} min)")
                st.metric("Médiane", f"{ia_positive.median():.0f} sec")
                st.metric("Écart-type", f"{ia_positive.std():.0f} sec")
                st.metric("Distribution fittée", ia_fit.get("name", "N/A"))
                if ia_fit.get("ks_statistic"):
                    st.metric("KS statistic", f"{ia_fit['ks_statistic']:.4f}")

        # --- Rythme journalier ---
        st.subheader("Rythme journalier de production")

        col_rhythm1, col_rhythm2 = st.columns(2)

        with col_rhythm1:
            hourly = compute_hourly_rhythm(seq_data)
            fig_rhythm = px.bar(
                hourly, x="heure", y="nb_moyen",
                error_y="nb_std",
                title="Nombre moyen de préparations par heure",
                labels={"heure": "Heure", "nb_moyen": "Preps/heure (moy)"},
                color_discrete_sequence=["#2ca02c"],
            )
            st.plotly_chart(fig_rhythm, width="stretch")

        with col_rhythm2:
            # Volume journalier par jour de semaine
            daily_counts = seq_data.groupby("jour").size().reset_index(name="nb")
            daily_counts["jour_dt"] = pd.to_datetime(daily_counts["jour"])
            daily_counts["jour_sem"] = daily_counts["jour_dt"].dt.day_name()
            jour_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            daily_counts["jour_sem"] = pd.Categorical(daily_counts["jour_sem"], categories=jour_order, ordered=True)
            fig_weekday = px.box(
                daily_counts.sort_values("jour_sem"), x="jour_sem", y="nb",
                title="Volume journalier par jour de semaine",
                labels={"jour_sem": "", "nb": "Préparations/jour"},
                color_discrete_sequence=["#ff7f0e"],
            )
            st.plotly_chart(fig_weekday, width="stretch")

        # --- Batches ---
        st.subheader("Patterns de batch (séries consécutives)")

        batches = compute_batch_patterns(seq_data)
        if not batches.empty:
            col_batch1, col_batch2 = st.columns(2)

            with col_batch1:
                fig_batch_dist = px.histogram(
                    batches, x="batch_size",
                    title="Distribution des tailles de batch",
                    labels={"batch_size": "Taille du batch", "count": "Fréquence"},
                    color_discrete_sequence=["#9467bd"],
                    nbins=int(max(batches["batch_size"].max(), 10)),
                )
                st.plotly_chart(fig_batch_dist, width="stretch")

            with col_batch2:
                batch_avg = batches.groupby("molecule").agg(
                    taille_moy=("batch_size", "mean"),
                    nb_batches=("batch_size", "size"),
                ).sort_values("taille_moy", ascending=False).head(15).reset_index()
                batch_avg["taille_moy"] = batch_avg["taille_moy"].round(1)

                fig_batch_mol = px.bar(
                    batch_avg, x="taille_moy", y="molecule",
                    orientation="h",
                    title="Taille moyenne de batch par molécule (top 15)",
                    labels={"taille_moy": "Taille moy.", "molecule": ""},
                    color="nb_batches",
                    color_continuous_scale="Viridis",
                )
                fig_batch_mol.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_batch_mol, width="stretch")

        # --- Matrice de transition ---
        st.subheader("Matrice de transition entre molécules")

        n_mol_transition = st.slider(
            "Nombre de molécules pour la matrice", 5, 20, 12, key="transition_slider"
        )
        trans_matrix = compute_transition_matrix(seq_data, top_n=n_mol_transition)

        if not trans_matrix.empty:
            fig_trans = px.imshow(
                trans_matrix,
                title=f"P(suivant | courant) - Top {n_mol_transition} molécules",
                labels=dict(x="Molécule suivante", y="Molécule courante", color="Probabilité"),
                color_continuous_scale="Blues",
                aspect="auto",
            )
            fig_trans.update_layout(height=500)
            st.plotly_chart(fig_trans, width="stretch")

            st.caption(
                "Lecture : chaque ligne somme à 1.0. Les valeurs sur la diagonale "
                "indiquent les auto-transitions (même molécule préparée consécutivement)."
            )
        else:
            st.info("Pas assez de données pour la matrice de transition.")

        # --- Distributions fittées des temps de production ---
        st.subheader("Distributions des temps de production par molécule")
        top_mols_fit = seq_data["molecule"].value_counts().head(8).index.tolist()

        fit_results = []
        cols_per_row = 4
        for row_start in range(0, len(top_mols_fit), cols_per_row):
            cols = st.columns(cols_per_row)
            for i, col in enumerate(cols):
                idx = row_start + i
                if idx >= len(top_mols_fit):
                    break
                mol = top_mols_fit[idx]
                mol_times = seq_data[seq_data["molecule"] == mol]["prod_sec"].dropna().values
                fit = fit_production_time_distribution(mol_times)
                fit_results.append({"molecule": mol, **fit})

                with col:
                    fig_fit = px.histogram(
                        x=mol_times, nbins=30,
                        title=f"{mol} ({fit.get('name', 'N/A')})",
                        labels={"x": "Temps (sec)", "count": ""},
                        color_discrete_sequence=["#17becf"],
                    )
                    fig_fit.update_layout(height=250, showlegend=False)
                    st.plotly_chart(fig_fit, width="stretch")

        with st.expander("Paramètres des distributions fittées"):
            fit_df = pd.DataFrame(fit_results)
            if not fit_df.empty:
                display_cols = ["molecule", "name", "mean", "std", "ks_statistic", "p_value"]
                available = [c for c in display_cols if c in fit_df.columns]
                st.dataframe(
                    fit_df[available].rename(columns={
                        "molecule": "Molécule",
                        "name": "Distribution",
                        "mean": "Moyenne (sec)",
                        "std": "Écart-type (sec)",
                        "ks_statistic": "KS stat",
                        "p_value": "p-value",
                    }),
                    width="stretch", hide_index=True,
                )


# ============================================================
# TAB 9 - SIMULATION DES
# ============================================================
with tab9:
    st.header("Simulation à Événements Discrets (DES)")
    st.caption(
        "Simule une journée de production du robot APOTECA en utilisant "
        "les distributions statistiques fittées sur les données historiques."
    )

    # Charger les données pour le simulateur
    @st.cache_data(ttl=300)
    def load_sim_historical(_d_start, _d_end):
        return query("""
            SELECT
                p.date_fin,
                date(p.date_fin) AS jour,
                m.nom AS molecule,
                p.dosage_mg,
                CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
                CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
                CAST(substr(p.temps_production,7,2) AS INTEGER) AS prod_sec
            FROM preparations p
            JOIN medicaments m ON p.medicament_id = m.id
            WHERE p.temps_production IS NOT NULL AND p.temps_production <> ''
              AND p.date_fin IS NOT NULL
              AND date(p.date_fin) BETWEEN ? AND ?
            ORDER BY p.date_fin
        """, [str(_d_start), str(_d_end)])

    sim_hist = load_sim_historical(d_start, d_end)

    if sim_hist.empty:
        st.warning("Pas assez de données historiques pour la simulation.")
    else:
        # Construire le simulateur
        @st.cache_resource
        def build_simulator(_data_hash):
            return APOTECASimulator.from_historical_data(sim_hist)

        data_hash = hash(sim_hist.to_json())
        simulator = build_simulator(data_hash)

        # Calculer les stats historiques
        daily_counts = sim_hist.groupby("jour").size()
        avg_daily = int(daily_counts.mean())
        max_daily = int(daily_counts.max())

        # --- Contrôles ---
        st.subheader("Paramètres de simulation")

        col_ctrl1, col_ctrl2, col_ctrl3, col_ctrl4 = st.columns(4)
        with col_ctrl1:
            n_preps = st.slider(
                "Nombre de préparations",
                min_value=10, max_value=150, value=avg_daily,
                help=f"Historique : moy={avg_daily}, max={max_daily}",
            )
        with col_ctrl2:
            n_robots = st.selectbox("Nombre de robots", [1, 2, 3], index=0)
        with col_ctrl3:
            start_hour = st.time_input("Heure de début", value=pd.Timestamp("09:00").time())
            start_h = start_hour.hour + start_hour.minute / 60
        with col_ctrl4:
            volume_factor = st.slider("Facteur de volume", 0.5, 3.0, 1.0, 0.1)

        col_seed, col_run = st.columns([1, 1])
        with col_seed:
            random_seed = st.number_input("Seed aléatoire", value=42, min_value=0, max_value=99999)
        with col_run:
            st.write("")  # spacer
            st.write("")
            run_sim = st.button("Lancer la simulation", type="primary", use_container_width=True)

        # --- Scénarios pré-configurés ---
        st.subheader("Scénarios rapides")
        sc1, sc2, sc3, sc4 = st.columns(4)
        scenario = None
        with sc1:
            if st.button("Journée standard", use_container_width=True):
                scenario = SimulationConfig(n_preparations=avg_daily, n_robots=1, start_time_hour=9.0, random_seed=random_seed)
        with sc2:
            if st.button("+20% volume", use_container_width=True):
                scenario = SimulationConfig(n_preparations=avg_daily, volume_factor=1.2, n_robots=1, start_time_hour=9.0, random_seed=random_seed)
        with sc3:
            if st.button("+50% volume", use_container_width=True):
                scenario = SimulationConfig(n_preparations=avg_daily, volume_factor=1.5, n_robots=1, start_time_hour=9.0, random_seed=random_seed)
        with sc4:
            if st.button("2 robots", use_container_width=True):
                scenario = SimulationConfig(n_preparations=avg_daily, n_robots=2, start_time_hour=9.0, random_seed=random_seed)

        # Déterminer la config à utiliser
        config = None
        if run_sim:
            config = SimulationConfig(
                n_preparations=n_preps,
                n_robots=n_robots,
                start_time_hour=start_h,
                volume_factor=volume_factor,
                random_seed=int(random_seed),
            )
        elif scenario:
            config = scenario

        if config is not None:
            # Exécuter la simulation
            results = simulator.run(config)
            sim_df = APOTECASimulator.to_dataframe(results)
            metrics = APOTECASimulator.compute_metrics(sim_df)

            # --- KPIs de la simulation ---
            st.subheader("Résultats de la simulation")

            mk1, mk2, mk3, mk4, mk5 = st.columns(5)
            mk1.metric("Préparations", metrics.get("n_preparations", 0))
            mk2.metric("Durée totale", f"{metrics.get('duree_totale_min', 0):.0f} min")
            mk3.metric("Débit", f"{metrics.get('debit_preps_heure', 0):.1f} preps/h")
            mk4.metric("Utilisation robot", f"{metrics.get('taux_utilisation_pct', 0):.1f}%")
            mk5.metric("Attente moyenne", f"{metrics.get('temps_attente_moyen_sec', 0):.0f} sec")

            # --- Gantt chart ---
            st.subheader("Timeline de production (Gantt)")

            # Créer les données pour le Gantt
            gantt_data = sim_df.copy()
            ref_date = pd.Timestamp("2025-01-01")
            gantt_data["Start"] = ref_date + pd.to_timedelta(gantt_data["start_time"], unit="s")
            gantt_data["End"] = ref_date + pd.to_timedelta(gantt_data["end_time"], unit="s")
            gantt_data["Robot"] = gantt_data["robot_id"].apply(lambda x: f"Robot {x + 1}")

            fig_gantt = px.timeline(
                gantt_data,
                x_start="Start", x_end="End",
                y="Robot", color="molecule",
                title="Timeline simulée de production",
                labels={"molecule": "Molécule"},
                hover_data=["dose_mg", "production_time"],
            )
            fig_gantt.update_layout(
                xaxis_title="Heure",
                yaxis_title="",
                height=200 + 100 * config.n_robots,
                xaxis=dict(tickformat="%H:%M"),
            )
            st.plotly_chart(fig_gantt, width="stretch")

            # --- Courbe de débit ---
            col_debit, col_compare = st.columns(2)

            with col_debit:
                sim_df["heure"] = (sim_df["end_time"] // 3600).astype(int)
                sim_hourly = sim_df.groupby("heure").size().reset_index(name="nb_sim")

                # Historique horaire
                hist_hourly = compute_hourly_rhythm(sim_hist)

                fig_debit = go.Figure()
                fig_debit.add_trace(go.Bar(
                    x=sim_hourly["heure"], y=sim_hourly["nb_sim"],
                    name="Simulation", marker_color="#ff7f0e",
                ))
                if not hist_hourly.empty:
                    fig_debit.add_trace(go.Scatter(
                        x=hist_hourly["heure"], y=hist_hourly["nb_moyen"],
                        name="Historique (moy)", mode="lines+markers",
                        line=dict(color="#1f77b4", width=2),
                    ))
                fig_debit.update_layout(
                    title="Débit horaire : simulation vs historique",
                    xaxis_title="Heure",
                    yaxis_title="Préparations",
                    barmode="group",
                )
                st.plotly_chart(fig_debit, width="stretch")

            with col_compare:
                # Tableau de comparaison
                comparison = APOTECASimulator.compare_with_historical(metrics, sim_hist)
                st.subheader("Comparaison simulation vs historique")
                st.dataframe(comparison, width="stretch", hide_index=True)

            # --- Distribution des molécules simulées ---
            with st.expander("Détail de la simulation"):
                col_detail1, col_detail2 = st.columns(2)
                with col_detail1:
                    mol_sim_counts = sim_df["molecule"].value_counts().reset_index()
                    mol_sim_counts.columns = ["molecule", "nb"]
                    fig_mol_sim = px.pie(
                        mol_sim_counts.head(15), names="molecule", values="nb",
                        title="Répartition des molécules simulées",
                    )
                    st.plotly_chart(fig_mol_sim, width="stretch")

                with col_detail2:
                    fig_wait = px.histogram(
                        sim_df, x="wait_time",
                        title="Distribution des temps d'attente",
                        labels={"wait_time": "Temps d'attente (sec)"},
                        color_discrete_sequence=["#d62728"],
                    )
                    st.plotly_chart(fig_wait, width="stretch")

                st.dataframe(
                    sim_df[["prep_id", "molecule", "dose_mg", "start_time_str",
                            "end_time_str", "production_time", "wait_time", "robot_id"]].rename(columns={
                        "prep_id": "#",
                        "molecule": "Molécule",
                        "dose_mg": "Dose (mg)",
                        "start_time_str": "Début",
                        "end_time_str": "Fin",
                        "production_time": "T. prod (sec)",
                        "wait_time": "Attente (sec)",
                        "robot_id": "Robot",
                    }),
                    width="stretch", hide_index=True,
                )

        # --- Comparaison multi-scénarios ---
        st.subheader("Comparaison multi-scénarios")
        if st.button("Lancer la comparaison (4 scénarios)", key="multi_scenario"):
            scenarios = {
                "Standard": SimulationConfig(n_preparations=avg_daily, n_robots=1, start_time_hour=9.0, random_seed=int(random_seed)),
                "+20% vol.": SimulationConfig(n_preparations=avg_daily, volume_factor=1.2, n_robots=1, start_time_hour=9.0, random_seed=int(random_seed)),
                "+50% vol.": SimulationConfig(n_preparations=avg_daily, volume_factor=1.5, n_robots=1, start_time_hour=9.0, random_seed=int(random_seed)),
                "2 robots": SimulationConfig(n_preparations=avg_daily, n_robots=2, start_time_hour=9.0, random_seed=int(random_seed)),
            }

            scenario_metrics = []
            for name, sc_config in scenarios.items():
                sc_results = simulator.run(sc_config)
                sc_df = APOTECASimulator.to_dataframe(sc_results)
                sc_metrics = APOTECASimulator.compute_metrics(sc_df)
                sc_metrics["Scénario"] = name
                scenario_metrics.append(sc_metrics)

            compare_df = pd.DataFrame(scenario_metrics)
            display_cols = {
                "Scénario": "Scénario",
                "n_preparations": "Préparations",
                "duree_totale_min": "Durée (min)",
                "debit_preps_heure": "Débit (preps/h)",
                "taux_utilisation_pct": "Utilisation (%)",
                "temps_attente_moyen_sec": "Attente moy. (sec)",
                "n_robots": "Robots",
            }
            available_cols = [c for c in display_cols.keys() if c in compare_df.columns]
            st.dataframe(
                compare_df[available_cols].rename(columns=display_cols),
                width="stretch", hide_index=True,
            )


# ============================================================
# FOOTER
# ============================================================
st.divider()
st.caption("APOTECA Simulation Dashboard | Données: base SQLite apoteca.db | 11 273 préparations")
