-- ============================================================
-- Base de données APOTECA - Robot de préparation de chimiothérapie
-- Schema SQL (compatible SQLite / PostgreSQL)
-- ============================================================

-- ========================
-- TABLES DE RÉFÉRENCE
-- ========================

-- Dispositifs (robots Apoteca)
CREATE TABLE IF NOT EXISTS dispositifs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom VARCHAR(100) NOT NULL UNIQUE
);

-- Utilisateurs du robot
CREATE TABLE IF NOT EXISTS utilisateurs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom VARCHAR(200) NOT NULL UNIQUE
);

-- Médicaments
CREATE TABLE IF NOT EXISTS medicaments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom_complet VARCHAR(500) NOT NULL UNIQUE,
    nom VARCHAR(200),
    concentration VARCHAR(50),
    fabricant VARCHAR(100)
);

-- Services hospitaliers (wards)
CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom VARCHAR(300) NOT NULL UNIQUE
);

-- Conteneurs finaux (poches, seringues)
CREATE TABLE IF NOT EXISTS conteneurs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom VARCHAR(300) NOT NULL UNIQUE
);

-- ========================
-- TABLES PRINCIPALES
-- ========================

-- Préparations (Process Step Time) - table principale
-- Chaque ligne = une préparation de chimiothérapie
CREATE TABLE IF NOT EXISTS preparations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    external_id VARCHAR(50),
    date_fin DATETIME,
    dispositif_id INTEGER REFERENCES dispositifs(id),
    patient_nom VARCHAR(300),
    patient_code VARCHAR(50),
    medicament_id INTEGER REFERENCES medicaments(id),
    dosage_mg DECIMAL(10,2),
    dosage_brut VARCHAR(50),
    conteneur_id INTEGER REFERENCES conteneurs(id),
    temps_confirmation VARCHAR(20),
    temps_queue VARCHAR(20),
    temps_production VARCHAR(20),
    temps_final_check VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_preparations_job_id ON preparations(job_id);
CREATE INDEX IF NOT EXISTS idx_preparations_date_fin ON preparations(date_fin);
CREATE INDEX IF NOT EXISTS idx_preparations_medicament ON preparations(medicament_id);
CREATE INDEX IF NOT EXISTS idx_preparations_patient_code ON preparations(patient_code);

-- Erreurs (Error Opportunity Rate)
CREATE TABLE IF NOT EXISTS erreurs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_heure DATETIME,
    dispositif_id INTEGER REFERENCES dispositifs(id),
    message VARCHAR(500),
    description VARCHAR(500),
    utilisateur_id INTEGER REFERENCES utilisateurs(id)
);

CREATE INDEX IF NOT EXISTS idx_erreurs_date ON erreurs(date_heure);

-- Températures
CREATE TABLE IF NOT EXISTS temperatures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_heure DATETIME,
    dispositif_id INTEGER REFERENCES dispositifs(id),
    temperature DECIMAL(5,2)
);

CREATE INDEX IF NOT EXISTS idx_temperatures_date ON temperatures(date_heure);

-- Tâches de nettoyage
CREATE TABLE IF NOT EXISTS taches_nettoyage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    debut DATETIME,
    fin DATETIME,
    dispositif_id INTEGER REFERENCES dispositifs(id),
    utilisateur_id INTEGER REFERENCES utilisateurs(id),
    type_nettoyage VARCHAR(50),
    commentaire TEXT
);

-- ========================
-- TABLES DE REPORTING
-- ========================

-- Activité utilisateurs (résumé)
CREATE TABLE IF NOT EXISTS activite_utilisateurs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    utilisateur_id INTEGER REFERENCES utilisateurs(id),
    dispositif_id INTEGER REFERENCES dispositifs(id),
    nb_preparations INTEGER,
    temps_total VARCHAR(20)
);

-- Productivité utilisateurs (par jour)
CREATE TABLE IF NOT EXISTS productivite_utilisateurs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    utilisateur_id INTEGER REFERENCES utilisateurs(id),
    nb_preparations INTEGER,
    preparations_par_heure DECIMAL(5,2),
    heures VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_productivite_date ON productivite_utilisateurs(date);
CREATE INDEX IF NOT EXISTS idx_productivite_utilisateur ON productivite_utilisateurs(utilisateur_id);

-- Performance journalière
CREATE TABLE IF NOT EXISTS performance_journaliere (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    dispositif_id INTEGER REFERENCES dispositifs(id),
    nb_preparations INTEGER,
    nb_pass INTEGER,
    nb_fail_dosage INTEGER,
    nb_fail_technique INTEGER,
    nb_reconstitutions INTEGER,
    debut DATETIME,
    fin DATETIME,
    nb_flacons INTEGER,
    temps_moyen_prep_sec INTEGER,
    pass_par_heure DECIMAL(5,1),
    temps_allumage VARCHAR(20),
    temps_utilisation VARCHAR(20),
    taux_utilisation VARCHAR(10)
);

-- Utilisation médicaments par service
CREATE TABLE IF NOT EXISTS utilisation_medicaments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_id INTEGER REFERENCES services(id),
    medicament_id INTEGER REFERENCES medicaments(id),
    nb_preparations INTEGER,
    dose_totale DECIMAL(10,2),
    unite_mesure VARCHAR(20)
);

-- Composants utilisation
CREATE TABLE IF NOT EXISTS composants_utilisation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    medicament_id INTEGER REFERENCES medicaments(id),
    quantite INTEGER
);

-- Statistiques médicaments (lots et volumes)
CREATE TABLE IF NOT EXISTS statistiques_medicaments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source VARCHAR(200),
    medicament_id INTEGER REFERENCES medicaments(id),
    quantite_totale VARCHAR(50),
    volume_total VARCHAR(50),
    lot VARCHAR(50),
    date_expiration DATE,
    dosage VARCHAR(50),
    volume VARCHAR(50)
);

-- Distribution de la précision de dosage (histogramme)
CREATE TABLE IF NOT EXISTS distribution_precision_dosage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    medicament_id INTEGER REFERENCES medicaments(id),
    precision_pct INTEGER,
    nombre INTEGER
);

-- ========================
-- VUES UTILES
-- ========================

-- Vue : préparations avec noms complets
CREATE VIEW IF NOT EXISTS v_preparations AS
SELECT
    p.job_id,
    p.external_id,
    p.date_fin,
    d.nom AS dispositif,
    p.patient_nom,
    p.patient_code,
    m.nom_complet AS medicament,
    p.dosage_brut,
    p.dosage_mg,
    c.nom AS conteneur_final,
    p.temps_confirmation,
    p.temps_queue,
    p.temps_production,
    p.temps_final_check
FROM preparations p
LEFT JOIN dispositifs d ON p.dispositif_id = d.id
LEFT JOIN medicaments m ON p.medicament_id = m.id
LEFT JOIN conteneurs c ON p.conteneur_id = c.id;

-- Vue : productivité avec noms d'utilisateurs
CREATE VIEW IF NOT EXISTS v_productivite AS
SELECT
    pu.date,
    u.nom AS utilisateur,
    pu.nb_preparations,
    pu.preparations_par_heure,
    pu.heures
FROM productivite_utilisateurs pu
LEFT JOIN utilisateurs u ON pu.utilisateur_id = u.id;

-- Vue : erreurs avec détails
CREATE VIEW IF NOT EXISTS v_erreurs AS
SELECT
    e.date_heure,
    d.nom AS dispositif,
    e.message,
    e.description,
    u.nom AS utilisateur
FROM erreurs e
LEFT JOIN dispositifs d ON e.dispositif_id = d.id
LEFT JOIN utilisateurs u ON e.utilisateur_id = u.id;

-- Vue : utilisation médicaments par service
CREATE VIEW IF NOT EXISTS v_utilisation_medicaments AS
SELECT
    s.nom AS service,
    m.nom_complet AS medicament,
    um.nb_preparations,
    um.dose_totale,
    um.unite_mesure
FROM utilisation_medicaments um
LEFT JOIN services s ON um.service_id = s.id
LEFT JOIN medicaments m ON um.medicament_id = m.id;
