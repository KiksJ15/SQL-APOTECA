#!/usr/bin/env python3
"""
Script d'automatisation du téléchargement des CSV Apoteca et mise à jour de la base.

Utilise Playwright (navigateur headless) pour :
1. Se connecter à l'interface web Apoteca
2. Naviguer dans le Telerik ReportViewer
3. Télécharger tous les rapports en CSV
4. Relancer l'import dans la base SQLite

Usage:
    # Premier lancement (mode visible pour vérifier) :
    python scripts/auto_update.py --headed

    # Lancement automatique (headless) :
    python scripts/auto_update.py

    # Découvrir la structure de la page :
    python scripts/auto_update.py --discover

    # Changer la date de début (défaut: 01/01/2025) :
    python scripts/auto_update.py --date-from 01/01/2024

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
# Clé = nom du fichier attendu dans data/, Valeur = texte exact dans la sidebar
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
    "Statistiques utilisateurs par mèdicaments  (1).csv": "Statistiques utilisateurs par mèdicaments",
}


def log(msg):
    """Log avec timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
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

    # Sélecteurs identifiés via --discover
    try:
        page.wait_for_selector('#username', timeout=10000)
    except Exception:
        # Peut-être déjà connecté ?
        if "reports" in page.url or "app" in page.url:
            log("Déjà connecté (session active)")
            return True
        log("ERREUR: Page de login non trouvée")
        return False

    log("  Remplissage du formulaire de login...")
    page.fill('#username', username)
    page.fill('#password', password)
    page.click('button:has-text("Login")')

    # Attendre la navigation post-login
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(3)

    # Vérifier qu'on est connecté
    current_url = page.url
    if "login" in current_url.lower() or page.query_selector('#username'):
        log("ERREUR: Login échoué (mauvais identifiants ?)")
        return False

    log("Login effectué avec succès")
    return True


def set_date_range(page, date_from, date_to):
    """Configure la plage de dates dans le formulaire du rapport."""
    try:
        # Champs de date du Telerik ReportViewer
        # Le champ "De" (date début)
        de_input = page.query_selector('input[placeholder*="De" i]')
        if not de_input:
            # Chercher par label
            de_inputs = page.query_selector_all('input[type="text"]')
            # Les 2 premiers inputs texte sont souvent De et À
            if len(de_inputs) >= 2:
                de_input = de_inputs[0]
                a_input = de_inputs[1]
            else:
                log("    Champs de date non trouvés, on garde les défauts")
                return
        else:
            a_input = page.query_selector('input[placeholder*="À" i]')

        if de_input:
            de_input.triple_click()  # Sélectionner tout le texte
            de_input.fill(date_from)
            log(f"    Date De: {date_from}")

        if a_input:
            a_input.triple_click()
            a_input.fill(date_to)
            log(f"    Date À: {date_to}")

    except Exception as e:
        log(f"    Erreur dates: {e} (on continue avec les défauts)")


def download_single_report(page, report_name, filename, download_dir, date_from, date_to):
    """Télécharge un seul rapport CSV."""
    log(f"  📥 {report_name}...")

    # 1. Cliquer sur le rapport dans la sidebar
    # Les rapports sont dans la sidebar gauche, chercher le texte exact
    report_link = None
    try:
        # Chercher un élément cliquable avec le texte exact
        all_links = page.query_selector_all('a, div[ng-click], span[ng-click], li')
        for el in all_links:
            try:
                text = el.inner_text().strip()
                if text == report_name:
                    report_link = el
                    break
            except Exception:
                continue

        # Fallback: chercher par texte partiel
        if not report_link:
            report_link = page.query_selector(f'text="{report_name}"')

    except Exception:
        pass

    if not report_link:
        log(f"    ❌ Rapport '{report_name}' non trouvé dans la sidebar")
        return False

    report_link.click()
    time.sleep(2)

    # 2. Configurer les dates
    set_date_range(page, date_from, date_to)
    time.sleep(1)

    # 3. Cliquer sur Preview
    try:
        preview_btn = page.query_selector('button:has-text("Preview")')
        if not preview_btn:
            preview_btn = page.query_selector('input[value="Preview"]')
        if preview_btn:
            preview_btn.click()
            log("    Chargement du rapport...")
            # Attendre que le rapport se charge (le viewer Telerik)
            page.wait_for_load_state("networkidle", timeout=60000)
            time.sleep(5)  # Les gros rapports peuvent prendre du temps
        else:
            log("    Bouton Preview non trouvé, le rapport est peut-être déjà chargé")
    except Exception as e:
        log(f"    Erreur Preview: {e}")

    # 4. Cliquer sur le bouton Export (↓) dans la toolbar Telerik
    try:
        # Le bouton export dans Telerik ReportViewer
        export_btn = None
        for sel in [
            '[title="Export"]',
            '[title="Exporter"]',
            '.trv-report-viewer-export',
            'a[title*="Export" i]',
            '[data-command="telerik_ReportViewer_export"]',
            # L'icône flèche vers le bas dans la toolbar
            '.k-i-download',
            '.k-icon.k-i-download',
            # Sélecteur générique pour l'icône export Telerik
            '[class*="export"]',
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    export_btn = el
                    log(f"    Bouton export trouvé: {sel}")
                    break
            except Exception:
                continue

        # Si pas trouvé, chercher dans les boutons de la toolbar
        if not export_btn:
            toolbar_btns = page.query_selector_all('.trv-toolbar button, .trv-toolbar a, [class*="toolbar"] button, [class*="toolbar"] a')
            for btn in toolbar_btns:
                title = btn.get_attribute("title") or ""
                cls = btn.get_attribute("class") or ""
                if "export" in title.lower() or "download" in title.lower() or "export" in cls.lower():
                    export_btn = btn
                    log(f"    Bouton export trouvé dans toolbar: title='{title}'")
                    break

        if not export_btn:
            log(f"    ❌ Bouton export non trouvé")
            return False

        export_btn.click()
        time.sleep(1)

        # 5. Sélectionner CSV dans le menu déroulant
        csv_option = None
        for sel in [
            'text="CSV"',
            'a:has-text("CSV")',
            'li:has-text("CSV")',
            'span:has-text("CSV")',
            '[data-format="CSV"]',
            'option:has-text("CSV")',
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    csv_option = el
                    break
            except Exception:
                continue

        if not csv_option:
            log(f"    ❌ Option CSV non trouvée dans le menu export")
            return False

        # 6. Télécharger
        with page.expect_download(timeout=60000) as download_info:
            csv_option.click()
        download = download_info.value

        dest = os.path.join(download_dir, filename)
        download.save_as(dest)
        file_size = os.path.getsize(dest)
        log(f"    ✅ {filename} ({file_size:,} octets)")
        return True

    except Exception as e:
        log(f"    ❌ Erreur export: {e}")
        return False


def download_reports(page, download_dir, date_from, date_to):
    """Télécharge tous les rapports CSV."""
    log(f"Navigation vers les rapports: {REPORTS_URL}")
    page.goto(REPORTS_URL, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    downloaded = []

    for filename, report_name in REPORTS.items():
        try:
            success = download_single_report(
                page, report_name, filename, download_dir, date_from, date_to
            )
            if success:
                downloaded.append(filename)

            # Revenir à la page rapports pour le prochain
            page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
            time.sleep(2)

        except Exception as e:
            log(f"  ❌ Erreur globale {report_name}: {e}")
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

    log("\n--- PAGE DE LOGIN ---")
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

    log("\n--- LIENS ---")
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
    parser.add_argument("--date-from", default="01/01/2025",
                        help="Date de début au format dd/mm/yyyy (défaut: 01/01/2025)")
    parser.add_argument("--date-to", default=None,
                        help="Date de fin au format dd/mm/yyyy (défaut: aujourd'hui)")
    args = parser.parse_args()

    if not args.date_to:
        args.date_to = datetime.now().strftime("%d/%m/%Y")

    log("=" * 60)
    log("Début de la mise à jour automatique")
    log(f"Période: {args.date_from} → {args.date_to}")

    username, password = load_credentials()

    with sync_playwright() as p:
        # Utiliser Chrome installé sur le PC (pare-feu, certificats déjà configurés)
        chrome_path = os.getenv("CHROME_PATH")
        if not chrome_path:
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
            ignore_https_errors=True,
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
            downloaded = download_reports(page, tmp_dir, args.date_from, args.date_to)

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
