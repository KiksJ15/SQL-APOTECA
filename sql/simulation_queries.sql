-- ============================================================
-- REQUÊTES SQL DE SIMULATION - Robot APOTECA
-- Simuler et analyser l'utilisation du robot de chimiothérapie
-- ============================================================

-- ============================================================
-- 1. SIMULATION D'UNE JOURNÉE TYPE
-- ============================================================

-- 1a. Résumé d'une journée de production (choisir la date)
-- Simule le rapport de fin de journée du robot
SELECT
    date(p.date_fin) AS jour,
    COUNT(*) AS nb_preparations,
    COUNT(DISTINCT p.patient_code) AS nb_patients,
    COUNT(DISTINCT p.medicament_id) AS nb_medicaments_diff,
    ROUND(AVG(p.dosage_mg), 1) AS dosage_moyen_mg,
    MIN(p.date_fin) AS premiere_prep,
    MAX(p.date_fin) AS derniere_prep,
    -- Temps moyen de production en secondes
    ROUND(AVG(
        CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
        CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
        CAST(substr(p.temps_production,7,2) AS INTEGER)
    )) AS temps_prod_moyen_sec
FROM preparations p
WHERE date(p.date_fin) = '2025-09-01'  -- ← Modifier la date ici
GROUP BY date(p.date_fin);

-- 1b. Détail chronologique d'une journée (timeline du robot)
SELECT
    p.date_fin AS heure,
    m.nom AS medicament,
    p.dosage_mg || ' mg' AS dosage,
    c.nom AS conteneur,
    p.temps_production AS duree_production,
    p.temps_confirmation AS duree_confirmation
FROM preparations p
JOIN medicaments m ON p.medicament_id = m.id
LEFT JOIN conteneurs c ON p.conteneur_id = c.id
WHERE date(p.date_fin) = '2025-09-01'
ORDER BY p.date_fin;

-- ============================================================
-- 2. PERFORMANCE DU ROBOT
-- ============================================================

-- 2a. Performance mensuelle (nombre de preps, tendance)
SELECT
    strftime('%Y-%m', date_fin) AS mois,
    COUNT(*) AS nb_preparations,
    COUNT(DISTINCT patient_code) AS nb_patients,
    ROUND(AVG(dosage_mg), 1) AS dosage_moyen_mg,
    ROUND(AVG(
        CAST(substr(temps_production,1,2) AS INTEGER)*3600 +
        CAST(substr(temps_production,4,2) AS INTEGER)*60 +
        CAST(substr(temps_production,7,2) AS INTEGER)
    )) AS temps_prod_moyen_sec
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY mois
ORDER BY mois;

-- 2b. Performance par jour de la semaine
SELECT
    CASE CAST(strftime('%w', date_fin) AS INTEGER)
        WHEN 0 THEN 'Dimanche'
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
    END AS jour_semaine,
    COUNT(*) AS nb_preparations,
    ROUND(AVG(dosage_mg), 1) AS dosage_moyen_mg,
    ROUND(AVG(
        CAST(substr(temps_production,1,2) AS INTEGER)*3600 +
        CAST(substr(temps_production,4,2) AS INTEGER)*60 +
        CAST(substr(temps_production,7,2) AS INTEGER)
    )) AS temps_prod_moyen_sec
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY CAST(strftime('%w', date_fin) AS INTEGER)
ORDER BY CAST(strftime('%w', date_fin) AS INTEGER);

-- 2c. Distribution horaire (à quelle heure le robot travaille le plus)
SELECT
    CAST(strftime('%H', date_fin) AS INTEGER) AS heure,
    COUNT(*) AS nb_preparations,
    ROUND(AVG(dosage_mg), 1) AS dosage_moyen_mg
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY heure
ORDER BY heure;

-- ============================================================
-- 3. ANALYSE DES MÉDICAMENTS
-- ============================================================

-- 3a. Top 10 médicaments les plus préparés
SELECT
    m.nom_complet AS medicament,
    COUNT(*) AS nb_preparations,
    ROUND(SUM(p.dosage_mg), 1) AS dose_totale_mg,
    ROUND(AVG(p.dosage_mg), 1) AS dose_moyenne_mg,
    ROUND(MIN(p.dosage_mg), 1) AS dose_min_mg,
    ROUND(MAX(p.dosage_mg), 1) AS dose_max_mg
FROM preparations p
JOIN medicaments m ON p.medicament_id = m.id
GROUP BY m.nom_complet
ORDER BY nb_preparations DESC
LIMIT 10;

-- 3b. Médicaments par type de conteneur (quelle poche pour quel médicament)
SELECT
    m.nom AS medicament,
    c.nom AS conteneur,
    COUNT(*) AS nb_preparations,
    ROUND(AVG(p.dosage_mg), 1) AS dosage_moyen_mg
FROM preparations p
JOIN medicaments m ON p.medicament_id = m.id
JOIN conteneurs c ON p.conteneur_id = c.id
GROUP BY m.nom, c.nom
ORDER BY nb_preparations DESC
LIMIT 20;

-- ============================================================
-- 4. ANALYSE DES ERREURS (SIMULATION INCIDENTS)
-- ============================================================

-- 4a. Types d'erreurs et fréquence
SELECT
    message AS type_erreur,
    COUNT(*) AS nb_occurrences,
    GROUP_CONCAT(DISTINCT description) AS details
FROM erreurs
WHERE message != 'Message'
GROUP BY message
ORDER BY nb_occurrences DESC;

-- 4b. Taux d'erreur simulé par rapport aux préparations
SELECT
    'Taux erreur' AS indicateur,
    (SELECT COUNT(*) FROM erreurs WHERE message != 'Message') AS nb_erreurs,
    (SELECT COUNT(*) FROM preparations) AS nb_preparations_total,
    ROUND(
        CAST((SELECT COUNT(*) FROM erreurs WHERE message != 'Message') AS FLOAT) /
        (SELECT COUNT(*) FROM preparations) * 100, 3
    ) || '%' AS taux_erreur;

-- ============================================================
-- 5. SIMULATION DE CHARGE - CAPACITÉ DU ROBOT
-- ============================================================

-- 5a. Capacité journalière (combien de preps par jour)
SELECT
    date(date_fin) AS jour,
    COUNT(*) AS nb_preparations,
    COUNT(DISTINCT patient_code) AS nb_patients,
    MIN(time(date_fin)) AS debut_journee,
    MAX(time(date_fin)) AS fin_journee,
    -- Durée active en heures
    ROUND(
        (julianday(MAX(date_fin)) - julianday(MIN(date_fin))) * 24, 1
    ) AS heures_actives,
    -- Débit: préparations par heure
    ROUND(
        CAST(COUNT(*) AS FLOAT) /
        NULLIF((julianday(MAX(date_fin)) - julianday(MIN(date_fin))) * 24, 0), 1
    ) AS preps_par_heure
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY date(date_fin)
HAVING COUNT(*) > 5
ORDER BY nb_preparations DESC
LIMIT 20;

-- 5b. Pic de charge : journées les plus chargées
SELECT
    date(date_fin) AS jour,
    COUNT(*) AS nb_preparations,
    COUNT(DISTINCT medicament_id) AS nb_medicaments,
    ROUND(SUM(dosage_mg), 0) AS dose_totale_mg
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY date(date_fin)
ORDER BY nb_preparations DESC
LIMIT 10;

-- 5c. Simulation de montée en charge : si on augmente de 20%
SELECT
    strftime('%Y-%m', date_fin) AS mois,
    COUNT(*) AS preps_actuelles,
    ROUND(COUNT(*) * 1.2) AS preps_simulation_plus20pct,
    -- Temps total de production actuel en heures
    ROUND(SUM(
        CAST(substr(temps_production,1,2) AS INTEGER)*3600 +
        CAST(substr(temps_production,4,2) AS INTEGER)*60 +
        CAST(substr(temps_production,7,2) AS INTEGER)
    ) / 3600.0, 1) AS heures_prod_actuelles,
    -- Temps estimé avec +20%
    ROUND(SUM(
        CAST(substr(temps_production,1,2) AS INTEGER)*3600 +
        CAST(substr(temps_production,4,2) AS INTEGER)*60 +
        CAST(substr(temps_production,7,2) AS INTEGER)
    ) * 1.2 / 3600.0, 1) AS heures_prod_simulation
FROM preparations
WHERE date_fin IS NOT NULL AND temps_production != ''
GROUP BY mois
ORDER BY mois;

-- ============================================================
-- 6. PRÉCISION DU DOSAGE (QUALITÉ)
-- ============================================================

-- 6a. Distribution de précision globale
SELECT
    precision_pct AS ecart_pourcent,
    SUM(nombre) AS nb_preparations,
    ROUND(SUM(nombre) * 100.0 / (SELECT SUM(nombre) FROM distribution_precision_dosage), 1) AS pourcentage
FROM distribution_precision_dosage
GROUP BY precision_pct
ORDER BY precision_pct;

-- 6b. Précision par médicament
SELECT
    m.nom AS medicament,
    SUM(CASE WHEN d.precision_pct = 0 THEN d.nombre ELSE 0 END) AS parfait_0pct,
    SUM(CASE WHEN ABS(d.precision_pct) <= 1 THEN d.nombre ELSE 0 END) AS dans_1pct,
    SUM(CASE WHEN ABS(d.precision_pct) <= 2 THEN d.nombre ELSE 0 END) AS dans_2pct,
    SUM(d.nombre) AS total,
    ROUND(SUM(CASE WHEN ABS(d.precision_pct) <= 1 THEN d.nombre ELSE 0 END) * 100.0 / SUM(d.nombre), 1) AS pct_dans_1pct
FROM distribution_precision_dosage d
JOIN medicaments m ON d.medicament_id = m.id
GROUP BY m.nom
ORDER BY total DESC;

-- ============================================================
-- 7. MONITORING TEMPÉRATURE
-- ============================================================

-- 7a. Statistiques de température
SELECT
    MIN(temperature) AS temp_min,
    MAX(temperature) AS temp_max,
    ROUND(AVG(temperature), 2) AS temp_moyenne,
    COUNT(*) AS nb_releves,
    MIN(date_heure) AS premier_releve,
    MAX(date_heure) AS dernier_releve
FROM temperatures;

-- 7b. Alertes température (hors plage 18-25°C)
SELECT
    date_heure,
    temperature,
    CASE
        WHEN temperature > 25 THEN 'ALERTE HAUTE'
        WHEN temperature < 18 THEN 'ALERTE BASSE'
        ELSE 'OK'
    END AS statut
FROM temperatures
WHERE temperature > 25 OR temperature < 18
ORDER BY date_heure;

-- ============================================================
-- 8. CONSOMMATION PAR SERVICE HOSPITALIER
-- ============================================================

-- 8a. Consommation par service
SELECT
    s.nom AS service,
    COUNT(*) AS nb_medicaments,
    SUM(um.nb_preparations) AS total_preparations,
    ROUND(SUM(um.dose_totale), 1) AS dose_totale_mg
FROM utilisation_medicaments um
JOIN services s ON um.service_id = s.id
GROUP BY s.nom
ORDER BY total_preparations DESC;

-- 8b. Matrice service × médicament
SELECT
    s.nom AS service,
    m.nom AS medicament,
    um.nb_preparations,
    um.dose_totale || ' ' || um.unite_mesure AS dose
FROM utilisation_medicaments um
JOIN services s ON um.service_id = s.id
JOIN medicaments m ON um.medicament_id = m.id
ORDER BY s.nom, um.nb_preparations DESC;

-- ============================================================
-- 9. GESTION DES STOCKS (SIMULATION CONSOMMATION)
-- ============================================================

-- 9a. État des stocks avec dates d'expiration
SELECT
    m.nom AS medicament,
    sm.lot,
    sm.quantite_totale,
    sm.volume_total,
    sm.date_expiration,
    CASE
        WHEN sm.date_expiration < date('now') THEN 'EXPIRÉ'
        WHEN sm.date_expiration < date('now', '+30 days') THEN 'EXPIRE BIENTÔT'
        ELSE 'OK'
    END AS statut_expiration
FROM statistiques_medicaments sm
JOIN medicaments m ON sm.medicament_id = m.id
ORDER BY sm.date_expiration;

-- 9b. Consommation moyenne mensuelle vs stock
SELECT
    m.nom AS medicament,
    ROUND(AVG(monthly.nb_preps), 1) AS preps_mois_moyenne,
    ROUND(AVG(monthly.dose_mois), 1) AS dose_mois_moyenne_mg,
    cu.quantite AS flacons_en_stock
FROM (
    SELECT
        medicament_id,
        strftime('%Y-%m', date_fin) AS mois,
        COUNT(*) AS nb_preps,
        SUM(dosage_mg) AS dose_mois
    FROM preparations
    WHERE date_fin IS NOT NULL
    GROUP BY medicament_id, mois
) monthly
JOIN medicaments m ON monthly.medicament_id = m.id
LEFT JOIN composants_utilisation cu ON cu.medicament_id = m.id
GROUP BY m.nom, cu.quantite
ORDER BY preps_mois_moyenne DESC
LIMIT 15;

-- ============================================================
-- 10. PRODUCTIVITÉ DES OPÉRATEURS
-- ============================================================

-- 10a. Résumé par opérateur
SELECT
    u.nom AS operateur,
    COUNT(*) AS nb_jours_travail,
    SUM(pu.nb_preparations) AS total_preparations,
    ROUND(AVG(pu.nb_preparations), 1) AS preps_par_jour,
    ROUND(AVG(pu.preparations_par_heure), 1) AS preps_par_heure_moy,
    MAX(pu.nb_preparations) AS record_jour
FROM productivite_utilisateurs pu
JOIN utilisateurs u ON pu.utilisateur_id = u.id
WHERE pu.nb_preparations > 0
GROUP BY u.nom
ORDER BY total_preparations DESC;

-- 10b. Évolution de la productivité dans le temps
SELECT
    strftime('%Y-%m', pu.date) AS mois,
    u.nom AS operateur,
    ROUND(AVG(pu.preparations_par_heure), 1) AS preps_par_heure_moy,
    SUM(pu.nb_preparations) AS total_preps
FROM productivite_utilisateurs pu
JOIN utilisateurs u ON pu.utilisateur_id = u.id
WHERE pu.nb_preparations > 0
GROUP BY mois, u.nom
ORDER BY mois, u.nom;

-- ============================================================
-- 11. ANALYSE DOSE-TEMPS PAR MOLÉCULE
-- ============================================================

-- 11a. Données brutes dose vs temps de production (pour scatter/regression)
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
ORDER BY m.nom, p.dosage_mg;

-- ============================================================
-- 12. ANALYSE DES SÉQUENCES DE PRODUCTION
-- ============================================================

-- 12a. Timeline avec estimation de l'heure de début
SELECT
    p.date_fin,
    date(p.date_fin) AS jour,
    m.nom AS molecule,
    p.dosage_mg,
    CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
    CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
    CAST(substr(p.temps_production,7,2) AS INTEGER) AS prod_sec,
    datetime(p.date_fin,
        '-' || (CAST(substr(p.temps_production,1,2) AS INTEGER)*3600 +
                CAST(substr(p.temps_production,4,2) AS INTEGER)*60 +
                CAST(substr(p.temps_production,7,2) AS INTEGER)) || ' seconds'
    ) AS date_debut_estimee
FROM preparations p
JOIN medicaments m ON p.medicament_id = m.id
WHERE p.temps_production IS NOT NULL AND p.temps_production <> ''
  AND p.date_fin IS NOT NULL
ORDER BY p.date_fin;

-- 12b. Mix de molécules (proportions pour simulation)
SELECT
    m.nom AS molecule,
    COUNT(*) AS nb,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM preparations p
JOIN medicaments m ON p.medicament_id = m.id
WHERE p.temps_production IS NOT NULL AND p.temps_production <> ''
GROUP BY m.nom
ORDER BY nb DESC;

-- 12c. Statistiques de volume journalier
SELECT
    date(date_fin) AS jour,
    CAST(strftime('%w', date_fin) AS INTEGER) AS jour_semaine,
    COUNT(*) AS nb_preparations,
    MIN(time(date_fin)) AS premiere,
    MAX(time(date_fin)) AS derniere,
    COUNT(DISTINCT medicament_id) AS nb_molecules
FROM preparations
WHERE date_fin IS NOT NULL
GROUP BY jour
ORDER BY jour;
