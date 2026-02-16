"""
Script pour supprimer la colonne 'textbox24' du fichier Process Step Time.
Usage:
    python scripts/remove_column.py <chemin_du_fichier>

Supporte les formats CSV et Excel (.xlsx/.xls).
Le fichier nettoyé est sauvegardé avec le suffixe '_cleaned'.
"""

import sys
import os

def remove_column(filepath, column_name="textBox24"):
    ext = os.path.splitext(filepath)[1].lower()
    base = os.path.splitext(filepath)[0]

    if ext == ".csv":
        import csv
        output_path = f"{base}_cleaned.csv"
        with open(filepath, "r", encoding="utf-8-sig") as infile:
            reader = csv.DictReader(infile)
            if column_name not in reader.fieldnames:
                print(f"Colonne '{column_name}' non trouvee dans le fichier.")
                print(f"Colonnes disponibles : {reader.fieldnames}")
                sys.exit(1)
            fieldnames = [f for f in reader.fieldnames if f != column_name]
            with open(output_path, "w", encoding="utf-8", newline="") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in reader:
                    del row[column_name]
                    writer.writerow(row)

    elif ext in (".xlsx", ".xls"):
        try:
            import openpyxl
        except ImportError:
            print("Installation de openpyxl...")
            os.system(f"{sys.executable} -m pip install openpyxl")
            import openpyxl

        output_path = f"{base}_cleaned.xlsx"
        wb = openpyxl.load_workbook(filepath)
        ws = wb.active

        # Trouver l'index de la colonne
        header_row = [cell.value for cell in ws[1]]
        if column_name not in header_row:
            print(f"Colonne '{column_name}' non trouvee dans le fichier.")
            print(f"Colonnes disponibles : {header_row}")
            sys.exit(1)

        col_idx = header_row.index(column_name) + 1  # openpyxl est 1-indexed
        ws.delete_cols(col_idx)
        wb.save(output_path)

    else:
        print(f"Format non supporte : {ext}")
        sys.exit(1)

    print(f"Colonne '{column_name}' supprimee avec succes.")
    print(f"Fichier sauvegarde : {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/remove_column.py <chemin_du_fichier>")
        sys.exit(1)

    filepath = sys.argv[1]
    if not os.path.exists(filepath):
        print(f"Fichier non trouve : {filepath}")
        sys.exit(1)

    remove_column(filepath)
