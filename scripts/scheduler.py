#!/usr/bin/env python3
"""
Planificateur simple pour auto_update.py
Tourne en boucle et lance la mise a jour toutes les heures entre 9h et 18h (lun-ven).

Usage:
    python scripts/scheduler.py

Laisser tourner dans un terminal a cote de Streamlit.
Ctrl+C pour arreter.
"""

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
AUTO_UPDATE = SCRIPT_DIR / "auto_update.py"
INTERVAL_MINUTES = 60  # toutes les 60 minutes
HOUR_START = 9
HOUR_END = 18
# Lundi=0, Mardi=1, ..., Vendredi=4, Samedi=5, Dimanche=6
DAYS_ACTIVE = {0, 1, 2, 3, 4}  # lun-ven


def is_work_time():
    """Verifie si on est en heures ouvrables (9h-18h, lun-ven)."""
    now = datetime.now()
    return now.weekday() in DAYS_ACTIVE and HOUR_START <= now.hour < HOUR_END


def run_update():
    """Lance auto_update.py."""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Lancement de la mise a jour...")
    result = subprocess.run(
        [sys.executable, str(AUTO_UPDATE)],
        cwd=str(SCRIPT_DIR.parent),
    )
    if result.returncode == 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Mise a jour terminee avec succes")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Erreur (code {result.returncode})")


def main():
    print("=" * 50)
    print("  Planificateur Apoteca")
    print(f"  Mise a jour toutes les {INTERVAL_MINUTES} min")
    print(f"  Horaires: {HOUR_START}h-{HOUR_END}h, lun-ven")
    print("  Ctrl+C pour arreter")
    print("=" * 50)

    while True:
        if is_work_time():
            run_update()
        else:
            now = datetime.now()
            print(f"[{now.strftime('%H:%M:%S')}] Hors horaires ({now.strftime('%A %H:%M')}), en attente...")

        # Attendre jusqu'au prochain cycle
        time.sleep(INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArret du planificateur.")
