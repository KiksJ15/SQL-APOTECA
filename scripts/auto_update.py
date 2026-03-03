#!/usr/bin/env python3
"""
Script d'automatisation du téléchargement des CSV Apoteca et mise à jour de la base.

Utilise Playwright (navigateur headless) pour :
1. Se connecter à l'interface web Apoteca
2. Télécharger tous les rapports CSV
3. Relancer l'import dans la base SQLite

Usage:
    # Premier lancement (mode visible pour vérifier) :
    python scripts/auto_update.py --headed

    # Lancement automatique (headless) :
    python scripts/auto_update.py

    # Découvrir la structure de la page :
    python scripts/auto_update.py --discover

Prérequis:
    pip install playwright python-dotenv
    playwright install chromium
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERREUR: Playwright non installé.")
    print("  pip install playwright python-dotenv")
    print("  playwright install chromium")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERREUR: python-dotenv non installé.")
    print("  pip install python-dotenv")
    sys.exit(1)

# Chemins
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
ENV_FILE = PROJECT_ROOT / ".env"
LOG_FILE = PROJECT_ROOT / "logs" / "auto_update.log"

# URL Apoteca
BASE_URL = "https://sxapplapo01.curie.net"
REPORTS_URL = f"{BASE_URL}/#!/app/reports"

# Liste des rapports CSV à télécharger
# Clé = nom du fichier attendu dans data/, Valeur = identifiant du rapport dans l'UI
REPORTS = {
    "Process Step Time.csv": "Process Step Time",
    "Error Opportunity Rate.csv": "Error Opportunity Rate",
    "Temperatures.csv": "Temperatures",
    "Performance.csv": "Performance",
    "Activité utilisateurs.csv": "Activité utilisateurs",
    "Composants utilization.csv": "Composants utilization",
    "Distribution précision dosage.csv": "Distribution précision dosage",
    "Utilisation médicaments.csv": "Utilisation médicaments",
    "Productivité utilisateurs.csv": "Productivité utilisateurs",
    "Statistiques médicaments.csv": "Statistiques médicaments",
    "Tâche Propre.csv": "Tâche Propre",
    "Statistiques utilisateurs par mèdicaments  (1).csv": "Statistiques utilisateurs par médicaments",
}


def log(msg):
    """Log avec timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    # Écrire dans le fichier log
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_credentials():
    """Charge les identifiants depuis le fichier .env."""
    load_dotenv(ENV_FILE)
    username = os.getenv("APOTECA_USERNAME")
    password = os.getenv("APOTECA_PASSWORD")
    if not username or not password:
        print(f"ERREUR: Identifiants manquants dans {ENV_FILE}")
        print("Créez un fichier .env avec :")
        print("  APOTECA_USERNAME=votre_login")
        print("  APOTECA_PASSWORD=votre_mot_de_passe")
        sys.exit(1)
    return username, password


def login(page, username, password):
    """Se connecte à l'interface Apoteca."""
    log("Navigation vers la page de login...")
    page.goto(BASE_URL, wait_until="networkidle", timeout=30000)

    # Attendre le formulaire de login
    # NOTE: Ajustez ces sélecteurs selon l'interface Apoteca réelle
    # Essayer plusieurs sélecteurs courants
    login_selectors = [
        'input[type="text"]',
        'input[name="username"]',
        'input[name="login"]',
        'input[id="username"]',
        '#username',
        'input[placeholder*="user" i]',
        'input[placeholder*="login" i]',
        'input[placeholder*="identifiant" i]',
    ]

    password_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        '#password',
    ]

    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Login")',
        'button:has-text("Connexion")',
        'button:has-text("Se connecter")',
        'button:has-text("OK")',
    ]

    # Trouver le champ username
    username_field = None
    for sel in login_selectors:
        try:
            el = page.wait_for_selector(sel, timeout=3000)
            if el and el.is_visible():
                username_field = el
                log(f"  Champ username trouvé: {sel}")
                break
        except Exception:
            continue

    if not username_field:
        log("ERREUR: Impossible de trouver le champ username")
        log("Lancez avec --discover pour voir la structure de la page")
        return False

    # Trouver le champ password
    password_field = None
    for sel in password_selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                password_field = el
                log(f"  Champ password trouvé: {sel}")
                break
        except Exception:
            continue

    if not password_field:
        log("ERREUR: Impossible de trouver le champ password")
        return False

    # Remplir et soumettre
    username_field.fill(username)
    password_field.fill(password)

    # Trouver le bouton submit
    for sel in submit_selectors:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                log(f"  Bouton submit trouvé: {sel}")
                btn.click()
                break
        except Exception:
            continue

    # Attendre la navigation post-login
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(2)

    log("Login effectué")
    return True


def download_reports(page, download_dir):
    """Navigue vers les rapports et télécharge chaque CSV."""
    log(f"Navigation vers les rapports: {REPORTS_URL}")
    page.goto(REPORTS_URL, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    downloaded = []

    for filename, report_name in REPORTS.items():
        try:
            log(f"  Téléchargement: {report_name}...")

            # Cliquer sur le rapport (chercher un lien/bouton avec ce texte)
            report_link = None
            for sel in [
                f'text="{report_name}"',
                f'a:has-text("{report_name}")',
                f'button:has-text("{report_name}")',
                f'div:has-text("{report_name}")',
                f'span:has-text("{report_name}")',
            ]:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        report_link = el
                        break
                except Exception:
                    continue

            if not report_link:
                log(f"    SKIP: Rapport '{report_name}' non trouvé sur la page")
                continue

            # Cliquer sur le rapport
            report_link.click()
            page.wait_for_load_state("networkidle", timeout=10000)
            time.sleep(2)

            # Chercher le bouton de téléchargement/export CSV
            export_btn = None
            for sel in [
                'button:has-text("Export")',
                'button:has-text("CSV")',
                'button:has-text("Download")',
                'button:has-text("Télécharger")',
                'a:has-text("Export")',
                'a:has-text("CSV")',
                '[class*="export"]',
                '[class*="download"]',
                '[title*="export" i]',
                '[title*="csv" i]',
            ]:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        export_btn = el
                        break
                except Exception:
                    continue

            if not export_btn:
                log(f"    SKIP: Bouton export non trouvé pour '{report_name}'")
                # Revenir à la liste des rapports
                page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
                time.sleep(2)
                continue

            # Télécharger
            with page.expect_download(timeout=30000) as download_info:
                export_btn.click()
            download = download_info.value

            # Sauvegarder dans le dossier temporaire
            dest = os.path.join(download_dir, filename)
            download.save_as(dest)
            downloaded.append(filename)
            log(f"    OK: {filename}")

            # Revenir à la liste des rapports
            page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
            time.sleep(2)

        except Exception as e:
            log(f"    ERREUR: {report_name} -> {e}")
            # Tenter de revenir à la page rapports
            try:
                page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
                time.sleep(2)
            except Exception:
                pass

    return downloaded


def copy_to_data(download_dir):
    """Copie les CSV téléchargés vers le dossier data/."""
    DATA_DIR.mkdir(exist_ok=True)
    count = 0
    for f in os.listdir(download_dir):
        if f.endswith(".csv"):
            src = os.path.join(download_dir, f)
            dest = DATA_DIR / f
            shutil.copy2(src, dest)
            count += 1
    log(f"{count} fichiers CSV copiés dans {DATA_DIR}")
    return count


def run_import():
    """Lance le script d'import de la base SQLite."""
    import_script = SCRIPT_DIR / "import_data.py"
    log("Lancement de l'import SQLite...")
    result = subprocess.run(
        [sys.executable, str(import_script)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode == 0:
        log("Import terminé avec succès")
        # Afficher le résumé (dernières lignes)
        for line in result.stdout.strip().split("\n")[-5:]:
            log(f"  {line}")
    else:
        log(f"ERREUR import: {result.stderr}")
    return result.returncode == 0


def discover_page(page):
    """Mode découverte : affiche la structure de la page pour configurer les sélecteurs."""
    log("=== MODE DÉCOUVERTE ===")
    log(f"Navigation vers {BASE_URL}...")
    page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    log("\n--- FORMULAIRES trouvés ---")
    inputs = page.query_selector_all("input")
    for inp in inputs:
        attrs = {
            "type": inp.get_attribute("type"),
            "name": inp.get_attribute("name"),
            "id": inp.get_attribute("id"),
            "placeholder": inp.get_attribute("placeholder"),
        }
        attrs = {k: v for k, v in attrs.items() if v}
        log(f"  <input {attrs}>")

    buttons = page.query_selector_all("button, input[type='submit']")
    for btn in buttons:
        text = btn.inner_text() if btn.inner_text() else btn.get_attribute("value")
        log(f"  <button> {text}")

    log("\n--- LIENS trouvés ---")
    links = page.query_selector_all("a")
    for link in links[:30]:
        text = link.inner_text().strip()
        href = link.get_attribute("href")
        if text:
            log(f"  <a href='{href}'> {text[:80]}")

    log("\nLe navigateur reste ouvert. Inspectez la page puis fermez-le.")
    log("Notez les sélecteurs CSS des éléments à cliquer.")
    input("Appuyez sur Entrée pour fermer...")


def main():
    parser = argparse.ArgumentParser(description="Téléchargement automatique des CSV Apoteca")
    parser.add_argument("--headed", action="store_true", help="Mode visible (pour debug)")
    parser.add_argument("--discover", action="store_true", help="Mode découverte de la page")
    args = parser.parse_args()

    log("=" * 60)
    log("Début de la mise à jour automatique")

    username, password = load_credentials()

    with sync_playwright() as p:
        # Utiliser Chrome installé sur le PC (pare-feu, certificats déjà configurés)
        chrome_path = os.getenv("CHROME_PATH")
        if not chrome_path:
            # Chemins par défaut selon l'OS
            if sys.platform == "win32":
                for path in [
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                ]:
                    if os.path.exists(path):
                        chrome_path = path
                        break
            elif sys.platform == "darwin":
                mac_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                if os.path.exists(mac_path):
                    chrome_path = mac_path

        launch_args = {
            "headless": not (args.headed or args.discover),
        }
        if chrome_path:
            launch_args["executable_path"] = chrome_path
            log(f"Utilisation de Chrome: {chrome_path}")
        else:
            log("Chrome non trouvé, utilisation de Chromium Playwright")

        browser = p.chromium.launch(**launch_args)
        context = browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True,  # Certificats internes Curie
        )
        page = context.new_page()

        if args.discover:
            discover_page(page)
            browser.close()
            return

        # 1. Login
        if not login(page, username, password):
            browser.close()
            log("ÉCHEC: Impossible de se connecter")
            sys.exit(1)

        # 2. Télécharger les rapports
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            downloaded = download_reports(page, tmp_dir)

            if not downloaded:
                log("ATTENTION: Aucun fichier téléchargé")
                browser.close()
                sys.exit(1)

            log(f"{len(downloaded)}/{len(REPORTS)} rapports téléchargés")

            # 3. Copier vers data/
            copy_to_data(tmp_dir)

        browser.close()

        # 4. Relancer l'import
        run_import()

    log("Mise à jour terminée")
    log("=" * 60)


if __name__ == "__main__":
    main()
