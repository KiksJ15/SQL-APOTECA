#!/usr/bin/env python3
"""
Script d'import des fichiers CSV Apoteca vers une base SQLite.

Les CSV Apoteca ont un format spécial où les colonnes alternent
entre labels et valeurs (ex: "Utilisateur", "Dupont", "Dispositif", "Apoteca 1").
Ce script gère ce format et importe les données dans une base relationnelle.

Usage:
    python scripts/import_data.py [--db chemin_base.db] [--data dossier_csv]
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
from pathlib import Path


def get_or_create(cursor, table, nom_col, value):
    """Récupère l'ID d'un enregistrement ou le crée s'il n'existe pas."""
    if not value or value.strip() == "":
        return None
    value = value.strip()
    cursor.execute(f"SELECT id FROM {table} WHERE {nom_col} = ?", (value,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(f"INSERT INTO {table} ({nom_col}) VALUES (?)", (value,))
    return cursor.lastrowid


def get_or_create_medicament(cursor, nom_complet):
    """Crée ou récupère un médicament en parsant le nom complet."""
    if not nom_complet or nom_complet.strip() == "":
        return None
    nom_complet = nom_complet.strip()
    cursor.execute("SELECT id FROM medicaments WHERE nom_complet = ?", (nom_complet,))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Parser le nom: "CARBOPLATINE 10mg/ml (ACCORD)"
    nom = nom_complet
    concentration = None
    fabricant = None

    match = re.match(r'^(.+?)\s+(\d+\S*(?:mg|g|ml)\S*)\s+\((.+)\)$', nom_complet)
    if match:
        nom = match.group(1).strip()
        concentration = match.group(2).strip()
        fabricant = match.group(3).strip()
    else:
        match2 = re.match(r'^(.+?)\s+\((.+)\)$', nom_complet)
        if match2:
            nom = match2.group(1).strip()
            fabricant = match2.group(2).strip()

    cursor.execute(
        "INSERT INTO medicaments (nom_complet, nom, concentration, fabricant) VALUES (?, ?, ?, ?)",
        (nom_complet, nom, concentration, fabricant)
    )
    return cursor.lastrowid


def parse_decimal_fr(value):
    """Convertit un nombre au format français (virgule décimale) en float."""
    if not value:
        return None
    # Nettoyer les espaces insécables et normaux
    cleaned = value.replace('\xa0', '').replace(' ', '').replace('\u202f', '')
    # Enlever les unités
    cleaned = re.sub(r'[a-zA-Z°%/]+$', '', cleaned).strip()
    if not cleaned:
        return None
    # Remplacer la virgule décimale
    cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date_fr(value):
    """Convertit une date FR (dd/mm/yyyy HH:MM:SS) en format ISO."""
    if not value or value.strip() == "":
        return None
    value = value.strip()
    # Format: dd/mm/yyyy HH:MM:SS ou dd/mm/yyyy HH:MM
    match = re.match(r'(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2}(?::\d{2})?)', value)
    if match:
        d, m, y, t = match.groups()
        if len(t) == 5:
            t += ":00"
        return f"{y}-{m}-{d} {t}"
    # Format: dd/mm/yyyy
    match = re.match(r'(\d{2})/(\d{2})/(\d{4})$', value)
    if match:
        d, m, y = match.groups()
        return f"{y}-{m}-{d}"
    return value


def read_csv_file(filepath):
    """Lit un fichier CSV et retourne les lignes."""
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                reader = csv.reader(f)
                rows = list(reader)
                return rows
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Impossible de lire {filepath} avec les encodages testés")


def import_activite_utilisateurs(cursor, data_dir):
    """Importe Activité utilisateurs.csv"""
    filepath = os.path.join(data_dir, "Activité utilisateurs.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:  # skip header
        if len(row) < 8 or not row[1].strip():
            continue
        # Format: label, value, label, value, label, value, label, value
        utilisateur = row[1].strip()
        dispositif = row[3].strip()
        nb_preparations = int(row[5]) if row[5].strip().isdigit() else 0
        temps_total = row[7].strip()

        utilisateur_id = get_or_create(cursor, "utilisateurs", "nom", utilisateur)
        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)

        cursor.execute(
            """INSERT INTO activite_utilisateurs
               (utilisateur_id, dispositif_id, nb_preparations, temps_total)
               VALUES (?, ?, ?, ?)""",
            (utilisateur_id, dispositif_id, nb_preparations, temps_total)
        )
        count += 1
    return count


def import_process_step_time(cursor, data_dir):
    """Importe Process Step Time.csv ou Process Step Time_cleaned.csv"""
    filepath = os.path.join(data_dir, "Process Step Time.csv")
    cleaned_filepath = os.path.join(data_dir, "Process Step Time_cleaned.csv")
    if os.path.exists(filepath):
        use_cleaned = False
    elif os.path.exists(cleaned_filepath):
        filepath = cleaned_filepath
        use_cleaned = True
    else:
        print(f"  SKIP: Process Step Time non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        # Cleaned = 25 colonnes (patient anonymisé), Original = 26 colonnes
        if use_cleaned:
            if len(row) < 25 or not row[1].strip():
                continue
        else:
            if len(row) < 26 or not row[1].strip():
                continue

        job_id_str = row[1].strip()
        if not job_id_str.isdigit():
            continue
        job_id = int(job_id_str)
        external_id = row[3].strip()
        date_fin = parse_date_fr(row[5])
        dispositif = row[7].strip()

        if use_cleaned:
            # Cleaned: 25 cols - col 8 = "Patient" (label anonymisé)
            patient = row[8].strip()
            patient_code = row[10].strip()
            medicament = row[12].strip()
            dosage_brut = row[14].strip()
            conteneur = row[16].strip()
            confirmation = row[18].strip()
            queue = row[20].strip()
            production = row[22].strip()
            final_check = row[24].strip()
        else:
            # Original: 26 cols
            patient = row[9].strip()
            patient_code = row[11].strip()
            medicament = row[13].strip()
            dosage_brut = row[15].strip()
            conteneur = row[17].strip()
            confirmation = row[19].strip()
            queue = row[21].strip()
            production = row[23].strip()
            final_check = row[25].strip()

        dosage_mg = parse_decimal_fr(dosage_brut)
        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)
        medicament_id = get_or_create_medicament(cursor, medicament)
        conteneur_id = get_or_create(cursor, "conteneurs", "nom", conteneur)

        cursor.execute(
            """INSERT INTO preparations
               (job_id, external_id, date_fin, dispositif_id, patient_nom,
                patient_code, medicament_id, dosage_mg, dosage_brut,
                conteneur_id, temps_confirmation, temps_queue,
                temps_production, temps_final_check)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, external_id, date_fin, dispositif_id, patient,
             patient_code, medicament_id, dosage_mg, dosage_brut,
             conteneur_id, confirmation, queue, production, final_check)
        )
        count += 1
    return count


def import_erreurs(cursor, data_dir):
    """Importe Error Opportunity Rate.csv"""
    filepath = os.path.join(data_dir, "Error Opportunity Rate.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 11 or not row[2].strip():
            continue
        # 11 colonnes: Type, Date(label), datetime, Device(label), device, Message(label), msg, Description(label), desc, Utilisateur(label), user
        date_heure = parse_date_fr(row[2].strip())
        dispositif = row[4].strip()
        message = row[6].strip()
        description = row[8].strip()
        utilisateur = row[10].strip()

        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)
        utilisateur_id = get_or_create(cursor, "utilisateurs", "nom", utilisateur)

        cursor.execute(
            """INSERT INTO erreurs
               (date_heure, dispositif_id, message, description, utilisateur_id)
               VALUES (?, ?, ?, ?, ?)""",
            (date_heure, dispositif_id, message, description, utilisateur_id)
        )
        count += 1
    return count


def import_temperatures(cursor, data_dir):
    """Importe Temperatures.csv"""
    filepath = os.path.join(data_dir, "Temperatures.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 6 or not row[1].strip():
            continue
        date_heure = parse_date_fr(row[1].strip())
        dispositif = row[3].strip()
        temp_val = parse_decimal_fr(row[5])

        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)

        cursor.execute(
            """INSERT INTO temperatures (date_heure, dispositif_id, temperature)
               VALUES (?, ?, ?)""",
            (date_heure, dispositif_id, temp_val)
        )
        count += 1
    return count


def import_taches_nettoyage(cursor, data_dir):
    """Importe Tâche Propre.csv"""
    filepath = os.path.join(data_dir, "Tâche Propre.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 12 or not row[1].strip():
            continue
        debut = parse_date_fr(row[1].strip())
        fin = parse_date_fr(row[3].strip())
        dispositif = row[5].strip()
        utilisateur = row[7].strip()
        clean_type = row[9].strip()
        commentaire = row[11].strip() if len(row) > 11 else ""

        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)
        utilisateur_id = get_or_create(cursor, "utilisateurs", "nom", utilisateur)

        cursor.execute(
            """INSERT INTO taches_nettoyage
               (debut, fin, dispositif_id, utilisateur_id, type_nettoyage, commentaire)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (debut, fin, dispositif_id, utilisateur_id, clean_type, commentaire)
        )
        count += 1
    return count


def import_productivite(cursor, data_dir):
    """Importe Productivité utilisateurs.csv"""
    filepath = os.path.join(data_dir, "Productivité utilisateurs.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 10 or not row[1].strip():
            continue
        date = parse_date_fr(row[1].strip())
        utilisateur = row[3].strip()
        nb_preparations = int(row[5]) if row[5].strip().isdigit() else 0
        prep_par_heure = parse_decimal_fr(row[7])
        heures = row[9].strip()

        utilisateur_id = get_or_create(cursor, "utilisateurs", "nom", utilisateur)

        cursor.execute(
            """INSERT INTO productivite_utilisateurs
               (date, utilisateur_id, nb_preparations, preparations_par_heure, heures)
               VALUES (?, ?, ?, ?, ?)""",
            (date, utilisateur_id, nb_preparations, prep_par_heure, heures)
        )
        count += 1
    return count


def import_performance(cursor, data_dir):
    """Importe Performance.csv"""
    filepath = os.path.join(data_dir, "Performance.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 30 or not row[1].strip():
            continue
        date = parse_date_fr(row[1].strip())
        dispositif = row[3].strip()
        nb_preparations = int(row[5]) if row[5].strip().isdigit() else 0
        nb_pass = int(row[7]) if row[7].strip().isdigit() else 0
        nb_fail_dosage = int(row[9]) if row[9].strip().isdigit() else 0
        nb_fail_technique = int(row[11]) if row[11].strip().isdigit() else 0
        nb_reconstitutions = int(row[13]) if row[13].strip().isdigit() else 0
        debut = parse_date_fr(row[15].strip())
        fin = parse_date_fr(row[17].strip())
        nb_flacons = int(row[19]) if row[19].strip().isdigit() else 0
        temps_moyen_str = row[21].strip()
        # Parse "280s" -> 280
        temps_moyen_sec = None
        match = re.match(r'(\d+)s?', temps_moyen_str)
        if match:
            temps_moyen_sec = int(match.group(1))
        pass_par_heure = parse_decimal_fr(row[23])
        temps_allumage = row[25].strip()
        temps_utilisation = row[27].strip()
        taux_utilisation = row[29].strip() if len(row) > 29 else ""

        dispositif_id = get_or_create(cursor, "dispositifs", "nom", dispositif)

        cursor.execute(
            """INSERT INTO performance_journaliere
               (date, dispositif_id, nb_preparations, nb_pass, nb_fail_dosage,
                nb_fail_technique, nb_reconstitutions, debut, fin, nb_flacons,
                temps_moyen_prep_sec, pass_par_heure, temps_allumage,
                temps_utilisation, taux_utilisation)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (date, dispositif_id, nb_preparations, nb_pass, nb_fail_dosage,
             nb_fail_technique, nb_reconstitutions, debut, fin, nb_flacons,
             temps_moyen_sec, pass_par_heure, temps_allumage,
             temps_utilisation, taux_utilisation)
        )
        count += 1
    return count


def import_utilisation_medicaments(cursor, data_dir):
    """Importe Utilisation médicaments.csv"""
    filepath = os.path.join(data_dir, "Utilisation médicaments.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    seen = set()
    for row in rows[1:]:
        if len(row) < 14 or not row[0].strip():
            continue
        service_nom = row[0].strip()
        medicament = row[9].strip()
        nb_preparations_str = row[11].strip()
        dose_str = row[12].strip()
        unite = row[13].strip()

        # Éviter les doublons (le CSV contient des lignes répétées)
        key = (service_nom, medicament, dose_str)
        if key in seen:
            continue
        seen.add(key)

        nb_preparations = int(nb_preparations_str) if nb_preparations_str.isdigit() else 0
        dose_totale = parse_decimal_fr(dose_str)

        service_id = get_or_create(cursor, "services", "nom", service_nom)
        medicament_id = get_or_create_medicament(cursor, medicament)

        cursor.execute(
            """INSERT INTO utilisation_medicaments
               (service_id, medicament_id, nb_preparations, dose_totale, unite_mesure)
               VALUES (?, ?, ?, ?, ?)""",
            (service_id, medicament_id, nb_preparations, dose_totale, unite)
        )
        count += 1
    return count


def import_composants(cursor, data_dir):
    """Importe Composants utilization.csv"""
    filepath = os.path.join(data_dir, "Composants utilization.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    seen = set()
    for row in rows[1:]:
        if len(row) < 7 or not row[0].strip():
            continue
        medicament = row[0].strip()
        quantite_str = row[3].strip()

        # Éviter les doublons
        key = (medicament, quantite_str)
        if key in seen:
            continue
        seen.add(key)

        quantite = int(quantite_str) if quantite_str.isdigit() else 0
        medicament_id = get_or_create_medicament(cursor, medicament)

        cursor.execute(
            """INSERT INTO composants_utilisation (medicament_id, quantite)
               VALUES (?, ?)""",
            (medicament_id, quantite)
        )
        count += 1
    return count


def import_statistiques_medicaments(cursor, data_dir):
    """Importe Statistiques médicaments.csv"""
    filepath = os.path.join(data_dir, "Statistiques médicaments.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 17 or not row[3].strip():
            continue
        source = row[0].strip()
        medicament = row[3].strip()
        quantite_totale = row[5].strip()
        volume_total = row[7].strip() if len(row) > 7 else ""
        # Les colonnes suivantes sont dans des groupes répétés par lot
        lot = row[10].strip() if len(row) > 10 else ""
        date_exp = parse_date_fr(row[12].strip()) if len(row) > 12 else None
        dosage = row[14].strip() if len(row) > 14 else ""
        volume = row[16].strip() if len(row) > 16 else ""

        medicament_id = get_or_create_medicament(cursor, medicament)

        cursor.execute(
            """INSERT INTO statistiques_medicaments
               (source, medicament_id, quantite_totale, volume_total,
                lot, date_expiration, dosage, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (source, medicament_id, quantite_totale, volume_total,
             lot, date_exp, dosage, volume)
        )
        count += 1
    return count


def import_distribution_precision(cursor, data_dir):
    """Importe Distribution précision dosage.csv"""
    filepath = os.path.join(data_dir, "Distribution précision dosage.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0
    for row in rows[1:]:
        if len(row) < 44 or not row[2].strip():
            continue
        medicament = row[2].strip()
        medicament_id = get_or_create_medicament(cursor, medicament)

        # Les colonnes 4-43 contiennent les paires (precision_pct, count)
        # -10, count, -9, count, ..., 0, count, ..., 10, count
        for i in range(4, 44, 2):
            try:
                pct = int(row[i])
                nombre = int(row[i + 1]) if row[i + 1].strip().isdigit() else 0
                if nombre > 0:
                    cursor.execute(
                        """INSERT INTO distribution_precision_dosage
                           (medicament_id, precision_pct, nombre)
                           VALUES (?, ?, ?)""",
                        (medicament_id, pct, nombre)
                    )
                    count += 1
            except (ValueError, IndexError):
                continue
    return count


def import_stats_utilisateurs_medicaments(cursor, data_dir):
    """Importe Statistiques utilisateurs par médicaments (1).csv

    Ce fichier a un format pivot avec 78918 colonnes d'en-tête mais seulement
    7 colonnes de données par ligne. On le convertit en format long.
    Colonnes: conteneur, dispositif, dosage, volume, job_id, date_heure
    Le job_id permet de lier aux préparations existantes.
    """
    # Chercher le fichier (nom avec accents variables)
    filepath = None
    for f in os.listdir(data_dir):
        if f.startswith("Statistiques utilisateurs par m") and f.endswith(".csv"):
            filepath = os.path.join(data_dir, f)
            break
    if not filepath:
        print(f"  SKIP: Statistiques utilisateurs par médicaments non trouvé")
        return 0

    rows = read_csv_file(filepath)
    count = 0

    # Aussi générer un CSV propre en format long
    output_path = os.path.join(data_dir, "Stats_utilisateurs_medicaments_long.csv")
    with open(output_path, 'w', newline='', encoding='utf-8') as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["conteneur", "dispositif", "dosage", "volume", "job_id", "date_heure"])

        for row in rows[1:]:
            if len(row) < 6 or not row[4].strip():
                continue
            conteneur = row[0].strip()
            dispositif = row[1].strip()
            dosage = row[2].strip().replace('\xa0', ' ')
            volume = row[3].strip()
            job_id_str = row[4].strip()
            date_heure = row[5].strip()

            if not job_id_str.isdigit():
                continue
            job_id = int(job_id_str)

            # Écrire dans le CSV propre
            writer.writerow([conteneur, dispositif, dosage, volume, job_id, date_heure])

            # Mettre à jour le conteneur dans la table preparations si manquant
            if conteneur:
                conteneur_id = get_or_create(cursor, "conteneurs", "nom", conteneur)
                cursor.execute(
                    """UPDATE preparations SET conteneur_id = ?
                       WHERE job_id = ? AND (conteneur_id IS NULL)""",
                    (conteneur_id, job_id)
                )

            count += 1

    print(f"    -> CSV format long généré: {output_path}")
    return count


def get_last_date(db_path):
    """Retourne la dernière date en base (format YYYY-MM-DD) ou None si vide."""
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Chercher la date max dans les tables principales
        dates = []
        for query in [
            "SELECT MAX(date(date_fin)) FROM preparations",
            "SELECT MAX(date(date_heure)) FROM erreurs",
            "SELECT MAX(date(date_heure)) FROM temperatures",
            "SELECT MAX(date) FROM productivite_utilisateurs",
            "SELECT MAX(date) FROM performance_journaliere",
        ]:
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                if row and row[0]:
                    dates.append(row[0])
            except Exception:
                continue
        conn.close()
        return max(dates) if dates else None
    except Exception:
        return None


def clear_data_since(cursor, since_date):
    """Supprime les donnees depuis une date (pour import incremental)."""
    print(f"Suppression des donnees depuis {since_date}...")
    # Tables avec colonne date
    date_deletes = [
        ("preparations", f"date(date_fin) >= '{since_date}'"),
        ("erreurs", f"date(date_heure) >= '{since_date}'"),
        ("temperatures", f"date(date_heure) >= '{since_date}'"),
        ("taches_nettoyage", f"date(debut) >= '{since_date}'"),
        ("productivite_utilisateurs", f"date >= '{since_date}'"),
        ("performance_journaliere", f"date >= '{since_date}'"),
    ]
    for table, condition in date_deletes:
        try:
            cursor.execute(f"DELETE FROM {table} WHERE {condition}")
            deleted = cursor.rowcount
            if deleted > 0:
                print(f"  {table}: {deleted} enregistrements supprimes")
        except Exception as e:
            print(f"  {table}: erreur suppression: {e}")

    # Tables sans date (toujours reimporter en entier)
    for table in [
        "utilisation_medicaments", "composants_utilisation",
        "statistiques_medicaments", "distribution_precision_dosage",
        "activite_utilisateurs",
    ]:
        cursor.execute(f"DELETE FROM {table}")


def main():
    parser = argparse.ArgumentParser(description="Import des CSV Apoteca vers SQLite")
    parser.add_argument("--db", default="apoteca.db", help="Chemin de la base SQLite (defaut: apoteca.db)")
    parser.add_argument("--data", default="data", help="Dossier contenant les CSV (defaut: data/)")
    parser.add_argument("--since", default=None,
                        help="Import incremental: ne reimporte que depuis cette date (YYYY-MM-DD). "
                             "Sans ce flag, reimport complet.")
    args = parser.parse_args()

    # Résoudre les chemins relatifs par rapport à la racine du projet
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    db_path = args.db if os.path.isabs(args.db) else os.path.join(project_root, args.db)
    data_dir = args.data if os.path.isabs(args.data) else os.path.join(project_root, args.data)
    schema_path = os.path.join(project_root, "sql", "schema.sql")

    if not os.path.isdir(data_dir):
        print(f"ERREUR: Dossier '{data_dir}' non trouve.")
        sys.exit(1)

    print(f"Base de donnees : {db_path}")
    print(f"Dossier CSV     : {data_dir}")
    if args.since:
        print(f"Mode incremental: depuis {args.since}")
    else:
        print(f"Mode complet: reimport total")
    print()

    # Creer la base et le schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")

    with open(schema_path, 'r') as f:
        cursor.executescript(f.read())

    if args.since:
        # Mode incremental : supprimer uniquement les donnees recentes
        clear_data_since(cursor, args.since)
    else:
        # Mode complet : vider toutes les tables
        tables_to_clear = [
            "distribution_precision_dosage", "statistiques_medicaments",
            "composants_utilisation", "utilisation_medicaments",
            "performance_journaliere", "productivite_utilisateurs",
            "taches_nettoyage", "temperatures", "erreurs", "preparations",
            "activite_utilisateurs",
        ]
        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")
        for table in ["conteneurs", "services", "medicaments", "utilisateurs", "dispositifs"]:
            cursor.execute(f"DELETE FROM {table}")
        print("Tables videes avant import.\n")

    # Import de chaque fichier
    importers = [
        ("Activité utilisateurs", import_activite_utilisateurs),
        ("Process Step Time", import_process_step_time),
        ("Erreurs (Error Opportunity Rate)", import_erreurs),
        ("Températures", import_temperatures),
        ("Tâches de nettoyage", import_taches_nettoyage),
        ("Productivité utilisateurs", import_productivite),
        ("Performance journalière", import_performance),
        ("Utilisation médicaments", import_utilisation_medicaments),
        ("Composants utilisation", import_composants),
        ("Statistiques médicaments", import_statistiques_medicaments),
        ("Distribution précision dosage", import_distribution_precision),
        ("Stats utilisateurs/medicaments (pivot->long)", import_stats_utilisateurs_medicaments),
    ]

    total = 0
    for name, importer in importers:
        try:
            count = importer(cursor, data_dir)
            print(f"  {name}: {count} enregistrements importés")
            total += count
        except Exception as e:
            print(f"  ERREUR {name}: {e}")
            conn.rollback()
            raise

    conn.commit()

    # Résumé
    print(f"\n{'='*50}")
    print(f"Import terminé: {total} enregistrements au total")
    print(f"\nTables de référence:")
    for table in ["dispositifs", "utilisateurs", "medicaments", "services", "conteneurs"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cursor.fetchone()[0]}")

    print(f"\nTables de données:")
    for table in ["preparations", "erreurs", "temperatures", "taches_nettoyage",
                   "productivite_utilisateurs", "performance_journaliere",
                   "utilisation_medicaments", "composants_utilisation",
                   "statistiques_medicaments", "distribution_precision_dosage",
                   "activite_utilisateurs"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cursor.fetchone()[0]}")

    conn.close()
    print(f"\nBase de données sauvegardée dans: {db_path}")


if __name__ == "__main__":
    main()
