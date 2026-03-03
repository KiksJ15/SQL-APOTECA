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

# Date de depart par defaut pour l'import complet
DEFAULT_START_DATE = "01/01/2023"

# Liste des rapports CSV a telecharger
# Cle = nom du fichier attendu dans data/, Valeur = texte exact dans la sidebar
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

# Rapports lourds : date de depart limitee (temperatures = 1 mesure/minute)
# En mode incremental, on prend depuis la derniere date en base
# En mode full, on limite quand meme a HEAVY_REPORTS_DAYS jours
HEAVY_REPORTS = {"Temperatures.csv"}
HEAVY_REPORTS_DAYS = 7  # jours max pour les rapports lourds en mode full


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

    # Attendre la navigation post-login (arrive sur #!/app/labs)
    # Le login Angular peut prendre du temps à rediriger
    time.sleep(5)
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(3)

    # Vérifier qu'on est connecté en attendant que l'URL change
    max_wait = 15
    logged_in = False
    for i in range(max_wait):
        current_url = page.url
        if "app/labs" in current_url or "app/reports" in current_url or "app/dashboard" in current_url:
            logged_in = True
            break
        time.sleep(1)

    if not logged_in:
        # Dernier check : le champ username a disparu ?
        if not page.query_selector('#username:visible'):
            logged_in = True

    if not logged_in:
        log(f"ERREUR: Login échoué (URL actuelle: {page.url})")
        return False

    log(f"Login effectué avec succès (URL: {page.url})")

    # Après login, on est sur #!/app/labs — naviguer vers les rapports
    # Cliquer sur l'icône rapports (3e icône en haut à droite)
    log("Navigation vers la page rapports...")
    try:
        # Essayer de cliquer l'icône rapport en haut à droite
        report_icon = None
        for sel in [
            'a[ui-sref=".reports"]',
            'a[href="#!/app/reports"]',
            'a[href*="reports"]',
            '[ui-sref*="reports"]',
        ]:
            try:
                el = page.query_selector(sel)
                if el:
                    report_icon = el
                    log(f"  Icône rapports trouvée: {sel}")
                    break
            except Exception:
                continue

        if report_icon:
            report_icon.click()
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(3)
        else:
            # Fallback: navigation directe par URL
            log("  Icône non trouvée, navigation directe vers /reports")
            page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
            time.sleep(3)
    except Exception as e:
        log(f"  Navigation rapports via icône échouée: {e}")
        page.goto(REPORTS_URL, wait_until="networkidle", timeout=15000)
        time.sleep(3)

    log("Page rapports atteinte")
    return True


def set_date_range(page, date_from, date_to):
    """Configure la plage de dates dans le formulaire du rapport."""
    try:
        # Champs de date du Telerik ReportViewer
        # Le champ "De" (date début)
        # Chercher les champs de date (inputs texte dans le formulaire)
        date_inputs = page.query_selector_all('input[type="text"]')
        # Filtrer ceux qui contiennent une date (format dd/mm/yyyy)
        de_input = None
        a_input = None
        for inp in date_inputs:
            val = inp.input_value()
            if val and "/" in val:
                if de_input is None:
                    de_input = inp
                elif a_input is None:
                    a_input = inp

        if not de_input:
            log("    Champs de date non trouvés, on garde les défauts")
            return

        if de_input:
            de_input.click(click_count=3)  # Sélectionner tout le texte
            de_input.type(date_from)
            log(f"    Date De: {date_from}")

        if a_input:
            a_input.click(click_count=3)
            a_input.type(date_to)
            log(f"    Date À: {date_to}")

    except Exception as e:
        log(f"    Erreur dates: {e} (on continue avec les défauts)")


def download_single_report(page, report_name, filename, download_dir, date_from, date_to, is_full_mode=False):
    """Télécharge un seul rapport CSV."""
    # Pour les rapports lourds (temperatures), limiter la plage de dates
    if filename in HEAVY_REPORTS and is_full_mode:
        from datetime import timedelta
        heavy_start = (datetime.now() - timedelta(days=HEAVY_REPORTS_DAYS)).strftime("%d/%m/%Y")
        log(f"  📥 {report_name} (limite a {HEAVY_REPORTS_DAYS} jours)...")
        date_from = heavy_start
    else:
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


def download_reports(page, download_dir, date_from, date_to, is_full_mode=False):
    """Télécharge tous les rapports CSV."""
    log(f"Navigation vers les rapports: {REPORTS_URL}")
    page.goto(REPORTS_URL, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    downloaded = []

    for filename, report_name in REPORTS.items():
        try:
            success = download_single_report(
                page, report_name, filename, download_dir, date_from, date_to,
                is_full_mode=is_full_mode
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


def run_import(since_date=None):
    """Lance le script d'import de la base SQLite."""
    import_script = SCRIPT_DIR / "import_data.py"
    cmd = [sys.executable, str(import_script)]
    if since_date:
        cmd += ["--since", since_date]
        log(f"Lancement de l'import incremental (depuis {since_date})...")
    else:
        log("Lancement de l'import complet...")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode == 0:
        log("Import termine avec succes")
        for line in result.stdout.strip().split("\n")[-5:]:
            log(f"  {line}")
    else:
        log(f"ERREUR import: {result.stderr}")
    return result.returncode == 0


def get_last_date_from_db():
    """Recupere la derniere date en base pour l'import incremental."""
    # Importer la fonction depuis import_data.py
    sys.path.insert(0, str(SCRIPT_DIR))
    try:
        from import_data import get_last_date
        db_path = PROJECT_ROOT / "apoteca.db"
        return get_last_date(str(db_path))
    except Exception:
        return None
    finally:
        sys.path.pop(0)


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
    parser = argparse.ArgumentParser(description="Telechargement automatique des CSV Apoteca")
    parser.add_argument("--headed", action="store_true", help="Mode visible (pour debug)")
    parser.add_argument("--discover", action="store_true", help="Mode decouverte de la page")
    parser.add_argument("--full", action="store_true",
                        help="Import complet (depuis 01/01/2025). Sans ce flag, mode incremental.")
    parser.add_argument("--date-from", default=None,
                        help="Date de debut au format dd/mm/yyyy (defaut: auto)")
    parser.add_argument("--date-to", default=None,
                        help="Date de fin au format dd/mm/yyyy (defaut: aujourd'hui)")
    args = parser.parse_args()

    if not args.date_to:
        args.date_to = datetime.now().strftime("%d/%m/%Y")

    # Mode incremental : detecter la derniere date en base
    since_date = None  # pour import_data.py (format YYYY-MM-DD)
    if not args.full and not args.date_from:
        last_date = get_last_date_from_db()
        if last_date:
            # Convertir YYYY-MM-DD -> dd/mm/yyyy pour Apoteca
            parts = last_date.split("-")
            args.date_from = f"{parts[2]}/{parts[1]}/{parts[0]}"
            since_date = last_date
            log(f"Mode incremental: derniere date en base = {last_date}")
        else:
            args.date_from = DEFAULT_START_DATE
            log(f"Base vide, import complet depuis {DEFAULT_START_DATE}")
    elif args.full:
        args.date_from = args.date_from or DEFAULT_START_DATE
        log("Mode complet force (--full)")
    else:
        log(f"Date de debut manuelle: {args.date_from}")

    log("=" * 60)
    log("Debut de la mise a jour automatique")
    log(f"Periode: {args.date_from} -> {args.date_to}")

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
            downloaded = download_reports(page, tmp_dir, args.date_from, args.date_to,
                                         is_full_mode=args.full)

            if not downloaded:
                log("ATTENTION: Aucun fichier téléchargé")
                browser.close()
                sys.exit(1)

            log(f"{len(downloaded)}/{len(REPORTS)} rapports téléchargés")

            # 3. Copier vers data/
            copy_to_data(tmp_dir)

        browser.close()

        # 4. Relancer l'import (incremental si since_date)
        run_import(since_date=since_date)

    log("Mise a jour terminee")
    log("=" * 60)


if __name__ == "__main__":
    main()
