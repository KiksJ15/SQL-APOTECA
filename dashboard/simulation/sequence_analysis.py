"""
Analyse des sequences de production APOTECA.
Reconstruit les timelines, calcule les inter-arrivees, patterns de batch,
matrices de transition, et fitte des distributions statistiques.
"""

import numpy as np
import pandas as pd
from scipy import stats as sp_stats


def compute_inter_arrival_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les temps inter-arrivees entre preparations consecutives par jour.

    Args:
        df: DataFrame avec colonnes [date_fin, jour, molecule, prod_sec]
            date_fin doit etre en datetime.

    Returns:
        DataFrame original avec colonne ajoutee 'inter_arrival_sec'.
    """
    result = df.sort_values("date_fin").copy()
    result["date_fin_dt"] = pd.to_datetime(result["date_fin"])

    inter_arrivals = []
    for _, day_group in result.groupby("jour"):
        day_sorted = day_group.sort_values("date_fin_dt")
        diffs = day_sorted["date_fin_dt"].diff().dt.total_seconds()
        inter_arrivals.extend(diffs.tolist())

    result["inter_arrival_sec"] = inter_arrivals
    return result


def estimate_start_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estime l'heure de debut de production : date_fin - temps_production.

    Args:
        df: DataFrame avec colonnes [date_fin, prod_sec]

    Returns:
        DataFrame avec colonne ajoutee 'date_debut'.
    """
    result = df.copy()
    result["date_fin_dt"] = pd.to_datetime(result["date_fin"])
    result["date_debut"] = result["date_fin_dt"] - pd.to_timedelta(result["prod_sec"], unit="s")
    return result


def compute_batch_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifie les series consecutives de meme molecule par jour (batches).

    Args:
        df: DataFrame avec colonnes [date_fin, jour, molecule]

    Returns:
        DataFrame: jour, molecule, batch_size, batch_start, batch_end
    """
    result = df.sort_values("date_fin").copy()
    batches = []

    for jour, day_group in result.groupby("jour"):
        day_sorted = day_group.sort_values("date_fin")
        molecules = day_sorted["molecule"].tolist()
        times = day_sorted["date_fin"].tolist()

        if not molecules:
            continue

        current_mol = molecules[0]
        batch_start = times[0]
        batch_size = 1

        for i in range(1, len(molecules)):
            if molecules[i] == current_mol:
                batch_size += 1
            else:
                batches.append({
                    "jour": jour,
                    "molecule": current_mol,
                    "batch_size": batch_size,
                    "batch_start": batch_start,
                    "batch_end": times[i - 1],
                })
                current_mol = molecules[i]
                batch_start = times[i]
                batch_size = 1

        batches.append({
            "jour": jour,
            "molecule": current_mol,
            "batch_size": batch_size,
            "batch_start": batch_start,
            "batch_end": times[-1],
        })

    return pd.DataFrame(batches)


def compute_transition_matrix(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """
    Matrice de transition molecule-a-molecule (probabilites).
    P(suivant=B | courant=A) pour les top_n molecules.

    Args:
        df: DataFrame avec colonnes [date_fin, jour, molecule]
        top_n: nombre de molecules a inclure

    Returns:
        DataFrame pivot : index=molecule_courante, columns=molecule_suivante, values=probabilite
    """
    sorted_df = df.sort_values("date_fin").copy()

    # Top molecules par frequence
    top_mols = sorted_df["molecule"].value_counts().head(top_n).index.tolist()
    filtered = sorted_df[sorted_df["molecule"].isin(top_mols)]

    transitions = []
    for _, day_group in filtered.groupby("jour"):
        day_sorted = day_group.sort_values("date_fin")
        mols = day_sorted["molecule"].tolist()
        for i in range(len(mols) - 1):
            transitions.append({"from": mols[i], "to": mols[i + 1]})

    if not transitions:
        return pd.DataFrame()

    trans_df = pd.DataFrame(transitions)
    counts = trans_df.groupby(["from", "to"]).size().reset_index(name="count")
    totals = counts.groupby("from")["count"].transform("sum")
    counts["prob"] = (counts["count"] / totals).round(3)

    pivot = counts.pivot(index="from", columns="to", values="prob").fillna(0)
    return pivot


def compute_hourly_rhythm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nombre moyen de preparations par heure de la journee.

    Args:
        df: DataFrame avec colonnes [date_fin, jour]

    Returns:
        DataFrame: heure (8-18), nb_moyen, nb_std, nb_total
    """
    result = df.copy()
    result["heure"] = pd.to_datetime(result["date_fin"]).dt.hour

    # Compter par jour et heure
    per_day_hour = result.groupby(["jour", "heure"]).size().reset_index(name="nb")

    # Nombre de jours actifs
    n_jours = result["jour"].nunique()

    # Agreger par heure
    hourly = per_day_hour.groupby("heure").agg(
        nb_total=("nb", "sum"),
        nb_moyen=("nb", "mean"),
        nb_std=("nb", "std"),
    ).reset_index()

    hourly["nb_moyen"] = hourly["nb_moyen"].round(1)
    hourly["nb_std"] = hourly["nb_std"].fillna(0).round(1)

    return hourly


def fit_production_time_distribution(times: np.ndarray) -> dict:
    """
    Fitte plusieurs distributions (lognormale, gamma, normale) au temps de production.
    Selectionne la meilleure par test de Kolmogorov-Smirnov.

    Args:
        times: array de temps de production en secondes (>0)

    Returns:
        dict: name, params, ks_statistic, p_value, mean, std
    """
    times = times[times > 0]
    if len(times) < 10:
        return {"name": "insufficient_data", "params": {}, "ks_statistic": None, "p_value": None}

    best = None
    best_ks = float("inf")

    # Lognormale
    try:
        shape, loc, scale = sp_stats.lognorm.fit(times, floc=0)
        ks, p = sp_stats.kstest(times, "lognorm", args=(shape, loc, scale))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "lognormale",
                "params": {"shape": round(shape, 4), "loc": round(loc, 2), "scale": round(scale, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(times), 1),
                "std": round(np.std(times), 1),
            }
    except Exception:
        pass

    # Gamma
    try:
        a, loc, scale = sp_stats.gamma.fit(times, floc=0)
        ks, p = sp_stats.kstest(times, "gamma", args=(a, loc, scale))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "gamma",
                "params": {"shape": round(a, 4), "loc": round(loc, 2), "scale": round(scale, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(times), 1),
                "std": round(np.std(times), 1),
            }
    except Exception:
        pass

    # Normale
    try:
        mu, sigma = sp_stats.norm.fit(times)
        ks, p = sp_stats.kstest(times, "norm", args=(mu, sigma))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "normale",
                "params": {"mu": round(mu, 2), "sigma": round(sigma, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(times), 1),
                "std": round(np.std(times), 1),
            }
    except Exception:
        pass

    if best is None:
        return {"name": "fit_failed", "params": {}, "ks_statistic": None, "p_value": None}

    return best


def fit_inter_arrival_distribution(inter_arrivals: np.ndarray) -> dict:
    """
    Fitte une distribution exponentielle ou gamma aux inter-arrivees.

    Args:
        inter_arrivals: array de temps inter-arrivees en secondes (>0)

    Returns:
        dict: name, params, ks_statistic, p_value
    """
    ia = inter_arrivals[inter_arrivals > 0]
    if len(ia) < 10:
        return {"name": "insufficient_data", "params": {}}

    best = None
    best_ks = float("inf")

    # Exponentielle
    try:
        loc, scale = sp_stats.expon.fit(ia, floc=0)
        ks, p = sp_stats.kstest(ia, "expon", args=(loc, scale))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "exponentielle",
                "params": {"loc": round(loc, 2), "scale": round(scale, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(ia), 1),
            }
    except Exception:
        pass

    # Gamma
    try:
        a, loc, scale = sp_stats.gamma.fit(ia, floc=0)
        ks, p = sp_stats.kstest(ia, "gamma", args=(a, loc, scale))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "gamma",
                "params": {"shape": round(a, 4), "loc": round(loc, 2), "scale": round(scale, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(ia), 1),
            }
    except Exception:
        pass

    # Lognormale
    try:
        shape, loc, scale = sp_stats.lognorm.fit(ia, floc=0)
        ks, p = sp_stats.kstest(ia, "lognorm", args=(shape, loc, scale))
        if ks < best_ks:
            best_ks = ks
            best = {
                "name": "lognormale",
                "params": {"shape": round(shape, 4), "loc": round(loc, 2), "scale": round(scale, 2)},
                "ks_statistic": round(ks, 4),
                "p_value": round(p, 4),
                "mean": round(np.mean(ia), 1),
            }
    except Exception:
        pass

    if best is None:
        return {"name": "fit_failed", "params": {}}

    return best
