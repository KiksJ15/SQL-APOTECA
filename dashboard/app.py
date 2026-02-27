"""
Dashboard interactif APOTECA - Simulation du robot de chimiothérapie
====================================================================
Lance avec: streamlit run dashboard/app.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Vue d'ensemble",
    "Simulation journée",
    "Médicaments",
    "Qualité & Erreurs",
    "Productivité",
    "Stocks & Température",
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
            fig_err = px.pie(
                erreurs, names="type_erreur", values="nb",
                title="Types d'erreurs",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig_err, width="stretch")

            st.dataframe(erreurs, width="stretch", hide_index=True)

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
        fig_prod = px.scatter(
            prod_time, x="date", y="preparations_par_heure",
            color="operateur",
            size="nb_preparations",
            title="Évolution de la productivité par opérateur",
            labels={"date": "Date", "preparations_par_heure": "Preps/heure", "operateur": "Opérateur"},
            trendline="lowess",
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

    col_s1, col_s2 = st.columns(2)

    with col_s1:
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

    with col_s2:
        # Composants utilisation (flacons consommés)
        composants = query("""
            SELECT m.nom AS medicament, cu.quantite AS flacons_utilises
            FROM composants_utilisation cu
            JOIN medicaments m ON cu.medicament_id = m.id
            ORDER BY cu.quantite DESC
        """)

        if not composants.empty:
            fig_comp = px.bar(
                composants.head(15), x="flacons_utilises", y="medicament",
                orientation="h",
                title="Flacons consommés par médicament",
                labels={"flacons_utilises": "Flacons", "medicament": ""},
                color_discrete_sequence=["#e377c2"],
            )
            fig_comp.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_comp, width="stretch")

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
# FOOTER
# ============================================================
st.divider()
st.caption("APOTECA Simulation Dashboard | Données: base SQLite apoteca.db | 11 273 préparations")
