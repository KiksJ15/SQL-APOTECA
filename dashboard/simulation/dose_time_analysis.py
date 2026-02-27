"""
Analyse de la relation dose-temps pour les preparations APOTECA.
Correle le dosage (mg) avec le temps de production du robot.
"""

import numpy as np
import pandas as pd
from scipy import stats


def compute_molecule_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les stats de temps de production par molecule.

    Args:
        df: DataFrame avec colonnes [molecule, dosage_mg, prod_sec]

    Returns:
        DataFrame: molecule, count, dose_moy, temps_moy, temps_median,
                   temps_min, temps_max, temps_std
    """
    result = df.groupby("molecule").agg(
        count=("prod_sec", "size"),
        dose_moy=("dosage_mg", "mean"),
        dose_min=("dosage_mg", "min"),
        dose_max=("dosage_mg", "max"),
        temps_moy=("prod_sec", "mean"),
        temps_median=("prod_sec", "median"),
        temps_min=("prod_sec", "min"),
        temps_max=("prod_sec", "max"),
        temps_std=("prod_sec", "std"),
    ).reset_index()

    # Arrondir
    for col in ["dose_moy", "dose_min", "dose_max", "temps_moy", "temps_median", "temps_std"]:
        result[col] = result[col].round(1)

    return result.sort_values("count", ascending=False).reset_index(drop=True)


def compute_dose_time_regression(df: pd.DataFrame, molecule: str) -> dict:
    """
    Regression lineaire dose -> temps de production pour une molecule.

    Args:
        df: DataFrame avec colonnes [molecule, dosage_mg, prod_sec]
        molecule: nom de la molecule a analyser

    Returns:
        dict avec slope, intercept, r_squared, p_value, std_err, n_samples
        ou None si pas assez de donnees.
    """
    sub = df[df["molecule"] == molecule].dropna(subset=["dosage_mg", "prod_sec"])
    if len(sub) < 10:
        return None

    x = sub["dosage_mg"].values
    y = sub["prod_sec"].values

    # Verifier qu'il y a de la variance dans x
    if np.std(x) < 1e-6:
        return None

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    return {
        "molecule": molecule,
        "slope": round(slope, 4),
        "intercept": round(intercept, 2),
        "r_squared": round(r_value ** 2, 4),
        "p_value": round(p_value, 6),
        "std_err": round(std_err, 4),
        "n_samples": len(sub),
    }


def compute_dose_time_heatmap(df: pd.DataFrame, n_bins: int = 5, min_count: int = 10) -> pd.DataFrame:
    """
    Cree une matrice molecule x plage_dose -> temps moyen de production.

    Args:
        df: DataFrame avec colonnes [molecule, dosage_mg, prod_sec]
        n_bins: nombre de bins de dose par molecule
        min_count: nombre minimum de preps pour inclure une molecule

    Returns:
        DataFrame pivot : index=molecule, columns=plage_dose, values=temps_moy_sec
    """
    # Filtrer molecules avec assez de donnees
    counts = df["molecule"].value_counts()
    top_molecules = counts[counts >= min_count].index.tolist()
    filtered = df[df["molecule"].isin(top_molecules)].copy()

    if filtered.empty:
        return pd.DataFrame()

    # Creer des bins de dose par molecule (quantiles)
    rows = []
    for mol in top_molecules:
        sub = filtered[filtered["molecule"] == mol].copy()
        if len(sub) < n_bins:
            continue
        try:
            sub["dose_bin"] = pd.qcut(sub["dosage_mg"], q=n_bins, duplicates="drop")
            group = sub.groupby("dose_bin", observed=True)["prod_sec"].mean()
            for bin_label, avg_time in group.items():
                rows.append({
                    "molecule": mol,
                    "plage_dose": str(bin_label),
                    "temps_moy_sec": round(avg_time, 1),
                })
        except (ValueError, TypeError):
            continue

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    return result.pivot(index="molecule", columns="plage_dose", values="temps_moy_sec")


def compute_correlation_table(df: pd.DataFrame, min_count: int = 10) -> pd.DataFrame:
    """
    Correlation de Pearson entre dose et temps de production par molecule.

    Args:
        df: DataFrame avec colonnes [molecule, dosage_mg, prod_sec]
        min_count: nombre minimum de preps par molecule

    Returns:
        DataFrame: molecule, correlation, p_value, n_samples
    """
    rows = []
    for mol, group in df.groupby("molecule"):
        sub = group.dropna(subset=["dosage_mg", "prod_sec"])
        if len(sub) < min_count:
            continue
        if np.std(sub["dosage_mg"].values) < 1e-6:
            continue

        r, p = stats.pearsonr(sub["dosage_mg"].values, sub["prod_sec"].values)
        rows.append({
            "molecule": mol,
            "correlation": round(r, 4),
            "p_value": round(p, 6),
            "n_samples": len(sub),
            "significatif": "Oui" if p < 0.05 else "Non",
        })

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values("correlation", key=abs, ascending=False).reset_index(drop=True)
