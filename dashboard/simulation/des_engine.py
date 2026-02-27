"""
Moteur de Simulation a Evenements Discrets (DES) pour le robot APOTECA.
Simule une journee de production avec distributions fittees sur l'historique.
Utilise uniquement numpy/scipy (pas de SimPy).
"""

import heapq
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

from .sequence_analysis import (
    fit_inter_arrival_distribution,
    fit_production_time_distribution,
)


@dataclass
class SimulationConfig:
    """Configuration pour un run de simulation."""
    n_preparations: int = 45
    molecule_mix: dict = field(default_factory=dict)  # {molecule: proportion}
    start_time_hour: float = 9.0  # 09:00
    n_robots: int = 1
    volume_factor: float = 1.0
    random_seed: Optional[int] = 42


@dataclass
class SimulatedPreparation:
    """Un evenement de preparation simule."""
    prep_id: int
    molecule: str
    dose_mg: float
    arrival_time: float    # secondes depuis minuit
    start_time: float      # secondes depuis minuit
    production_time: float  # secondes
    end_time: float        # secondes depuis minuit
    wait_time: float       # secondes d'attente en queue
    robot_id: int


class APOTECASimulator:
    """
    Simulateur DES avec approche priority-queue.

    Algorithme :
    1. Genere N demandes avec molecule/dose depuis le mix + distributions
    2. Genere les temps d'arrivee depuis la distribution inter-arrivees
    3. Pour chaque demande, assigne au premier robot disponible
    4. Le robot produit pendant une duree tiree de la distribution de la molecule
    5. Enregistre tous les temps
    """

    def __init__(
        self,
        production_time_params: dict,   # {molecule: {name, params}}
        dose_params: dict,              # {molecule: {mean, std, min, max}}
        inter_arrival_params: dict,     # {name, params}
        molecule_mix: dict,             # {molecule: proportion}
        batch_sizes: dict,              # {molecule: avg_batch_size}
    ):
        self.production_time_params = production_time_params
        self.dose_params = dose_params
        self.inter_arrival_params = inter_arrival_params
        self.default_molecule_mix = molecule_mix
        self.batch_sizes = batch_sizes

    @classmethod
    def from_historical_data(cls, df: pd.DataFrame) -> "APOTECASimulator":
        """
        Factory : fitte toutes les distributions a partir des donnees historiques.

        Args:
            df: DataFrame avec colonnes [molecule, dosage_mg, prod_sec, date_fin, jour]
        """
        # 1. Fit des temps de production par molecule
        production_time_params = {}
        for mol, group in df.groupby("molecule"):
            times = group["prod_sec"].dropna().values.astype(float)
            if len(times) >= 10:
                fit = fit_production_time_distribution(times)
                production_time_params[mol] = fit

        # 2. Stats de dose par molecule
        dose_params = {}
        for mol, group in df.groupby("molecule"):
            doses = group["dosage_mg"].dropna().values
            if len(doses) >= 5:
                dose_params[mol] = {
                    "mean": float(np.mean(doses)),
                    "std": max(float(np.std(doses)), 0.1),
                    "min": float(np.min(doses)),
                    "max": float(np.max(doses)),
                }

        # 3. Fit des inter-arrivees
        sorted_df = df.sort_values("date_fin").copy()
        sorted_df["date_fin_dt"] = pd.to_datetime(sorted_df["date_fin"])
        all_inter = []
        for _, day_group in sorted_df.groupby("jour"):
            day_sorted = day_group.sort_values("date_fin_dt")
            diffs = day_sorted["date_fin_dt"].diff().dt.total_seconds().dropna()
            all_inter.extend(diffs[diffs > 0].tolist())

        inter_arrival_params = fit_inter_arrival_distribution(np.array(all_inter))

        # 4. Mix de molecules (proportions historiques)
        counts = df["molecule"].value_counts()
        total = counts.sum()
        molecule_mix = {mol: count / total for mol, count in counts.items()}

        # 5. Tailles de batch moyennes
        batch_sizes = {}
        for _, day_group in sorted_df.groupby("jour"):
            day_sorted = day_group.sort_values("date_fin")
            mols = day_sorted["molecule"].tolist()
            if not mols:
                continue
            current = mols[0]
            size = 1
            for i in range(1, len(mols)):
                if mols[i] == current:
                    size += 1
                else:
                    batch_sizes.setdefault(current, []).append(size)
                    current = mols[i]
                    size = 1
            batch_sizes.setdefault(current, []).append(size)

        avg_batch_sizes = {mol: np.mean(sizes) for mol, sizes in batch_sizes.items()}

        return cls(
            production_time_params=production_time_params,
            dose_params=dose_params,
            inter_arrival_params=inter_arrival_params,
            molecule_mix=molecule_mix,
            batch_sizes=avg_batch_sizes,
        )

    def _sample_production_time(self, molecule: str, rng: np.random.Generator) -> float:
        """Tire un temps de production depuis la distribution fittee."""
        params = self.production_time_params.get(molecule)
        if params is None or params.get("name") in ("insufficient_data", "fit_failed"):
            # Fallback : distribution globale (moyenne de toutes les molecules)
            all_means = [p["mean"] for p in self.production_time_params.values()
                         if p.get("mean") is not None]
            mean = np.mean(all_means) if all_means else 300.0
            return max(60.0, rng.normal(mean, mean * 0.2))

        name = params["name"]
        p = params["params"]

        if name == "lognormale":
            val = rng.lognormal(
                mean=np.log(p["scale"]),
                sigma=p["shape"],
            )
        elif name == "gamma":
            val = rng.gamma(shape=p["shape"], scale=p["scale"])
        elif name == "normale":
            val = rng.normal(loc=p["mu"], scale=p["sigma"])
        else:
            val = rng.normal(params.get("mean", 300), params.get("std", 60))

        return max(60.0, val)  # minimum 1 minute

    def _sample_dose(self, molecule: str, rng: np.random.Generator) -> float:
        """Tire une dose depuis les stats historiques."""
        params = self.dose_params.get(molecule)
        if params is None:
            return 100.0

        dose = rng.normal(params["mean"], params["std"])
        return np.clip(dose, params["min"], params["max"])

    def _sample_inter_arrival(self, rng: np.random.Generator) -> float:
        """Tire un temps inter-arrivee."""
        params = self.inter_arrival_params
        name = params.get("name", "")
        p = params.get("params", {})

        if name == "exponentielle":
            return max(30.0, rng.exponential(scale=p.get("scale", 300)))
        elif name == "gamma":
            return max(30.0, rng.gamma(shape=p.get("shape", 2), scale=p.get("scale", 150)))
        elif name == "lognormale":
            return max(30.0, rng.lognormal(
                mean=np.log(p.get("scale", 300)),
                sigma=p.get("shape", 0.5),
            ))
        else:
            return max(30.0, rng.exponential(scale=300))

    def _generate_molecule_sequence(self, n: int, mix: dict, rng: np.random.Generator) -> list:
        """
        Genere une sequence de molecules respectant les patterns de batch.
        """
        molecules = list(mix.keys())
        weights = np.array([mix[m] for m in molecules])
        weights = weights / weights.sum()

        sequence = []
        remaining = n

        while remaining > 0:
            # Choisir une molecule selon le mix
            mol = rng.choice(molecules, p=weights)

            # Taille de batch
            avg_batch = self.batch_sizes.get(mol, 1.5)
            batch_size = max(1, int(rng.poisson(max(1, avg_batch - 1)) + 1))
            batch_size = min(batch_size, remaining)

            sequence.extend([mol] * batch_size)
            remaining -= batch_size

        return sequence[:n]

    def run(self, config: SimulationConfig) -> list:
        """
        Execute la simulation.

        Returns:
            Liste de SimulatedPreparation.
        """
        rng = np.random.default_rng(config.random_seed)
        n = int(config.n_preparations * config.volume_factor)
        mix = config.molecule_mix if config.molecule_mix else self.default_molecule_mix

        # Generer la sequence de molecules (avec batching)
        mol_sequence = self._generate_molecule_sequence(n, mix, rng)

        # Generer les temps d'arrivee
        start_sec = config.start_time_hour * 3600  # e.g. 09:00 = 32400s
        arrivals = [start_sec]
        for _ in range(1, n):
            delta = self._sample_inter_arrival(rng)
            arrivals.append(arrivals[-1] + delta)

        # Priority queue : (next_available_time, robot_id)
        robot_heap = [(start_sec, i) for i in range(config.n_robots)]
        heapq.heapify(robot_heap)

        results = []
        for i in range(n):
            molecule = mol_sequence[i]
            dose = self._sample_dose(molecule, rng)
            arrival = arrivals[i]
            prod_time = self._sample_production_time(molecule, rng)

            # Assigner au robot le plus tot disponible
            robot_available, robot_id = heapq.heappop(robot_heap)
            actual_start = max(arrival, robot_available)
            end_time = actual_start + prod_time
            wait_time = actual_start - arrival

            results.append(SimulatedPreparation(
                prep_id=i + 1,
                molecule=molecule,
                dose_mg=round(dose, 1),
                arrival_time=arrival,
                start_time=actual_start,
                production_time=round(prod_time, 1),
                end_time=end_time,
                wait_time=round(wait_time, 1),
                robot_id=robot_id,
            ))

            # Remettre le robot dans le heap
            heapq.heappush(robot_heap, (end_time, robot_id))

        return results

    @staticmethod
    def to_dataframe(results: list) -> pd.DataFrame:
        """Convertit les resultats en DataFrame."""
        records = []
        for r in results:
            records.append({
                "prep_id": r.prep_id,
                "molecule": r.molecule,
                "dose_mg": r.dose_mg,
                "arrival_time": r.arrival_time,
                "start_time": r.start_time,
                "production_time": r.production_time,
                "end_time": r.end_time,
                "wait_time": r.wait_time,
                "robot_id": r.robot_id,
            })
        df = pd.DataFrame(records)

        # Convertir secondes en heures lisibles
        for col in ["arrival_time", "start_time", "end_time"]:
            df[f"{col}_str"] = df[col].apply(_seconds_to_hms)

        return df

    @staticmethod
    def compute_metrics(sim_df: pd.DataFrame) -> dict:
        """Calcule les metriques de performance d'une simulation."""
        if sim_df.empty:
            return {}

        total_duration = sim_df["end_time"].max() - sim_df["start_time"].min()
        total_prod_time = sim_df["production_time"].sum()
        n_robots = sim_df["robot_id"].nunique()

        return {
            "n_preparations": len(sim_df),
            "duree_totale_min": round(total_duration / 60, 1),
            "debut": _seconds_to_hms(sim_df["start_time"].min()),
            "fin": _seconds_to_hms(sim_df["end_time"].max()),
            "temps_prod_moyen_sec": round(sim_df["production_time"].mean(), 1),
            "temps_attente_moyen_sec": round(sim_df["wait_time"].mean(), 1),
            "debit_preps_heure": round(len(sim_df) / (total_duration / 3600), 1) if total_duration > 0 else 0,
            "taux_utilisation_pct": round(total_prod_time / (total_duration * n_robots) * 100, 1) if total_duration > 0 else 0,
            "n_robots": n_robots,
            "n_molecules": sim_df["molecule"].nunique(),
        }

    @staticmethod
    def compare_with_historical(sim_metrics: dict, hist_df: pd.DataFrame) -> pd.DataFrame:
        """Compare les metriques simulees vs historiques."""
        # Metriques historiques moyennes par jour
        hist_df = hist_df.copy()
        hist_df["date_fin_dt"] = pd.to_datetime(hist_df["date_fin"])
        hist_df["jour"] = hist_df["date_fin_dt"].dt.date

        daily = hist_df.groupby("jour").agg(
            n_preps=("prod_sec", "size"),
            temps_prod_moyen=("prod_sec", "mean"),
            duree_totale=("date_fin_dt", lambda x: (x.max() - x.min()).total_seconds() / 60),
        )

        hist_metrics = {
            "n_preparations": round(daily["n_preps"].mean(), 1),
            "duree_totale_min": round(daily["duree_totale"].mean(), 1),
            "temps_prod_moyen_sec": round(daily["temps_prod_moyen"].mean(), 1),
            "debit_preps_heure": round(
                daily["n_preps"].mean() / (daily["duree_totale"].mean() / 60), 1
            ) if daily["duree_totale"].mean() > 0 else 0,
        }

        comparison = pd.DataFrame([
            {
                "Metrique": "Nombre de preparations",
                "Simulation": sim_metrics.get("n_preparations", "-"),
                "Historique (moy/jour)": hist_metrics["n_preparations"],
            },
            {
                "Metrique": "Duree totale (min)",
                "Simulation": sim_metrics.get("duree_totale_min", "-"),
                "Historique (moy/jour)": hist_metrics["duree_totale_min"],
            },
            {
                "Metrique": "Temps prod. moyen (sec)",
                "Simulation": sim_metrics.get("temps_prod_moyen_sec", "-"),
                "Historique (moy/jour)": hist_metrics["temps_prod_moyen_sec"],
            },
            {
                "Metrique": "Debit (preps/heure)",
                "Simulation": sim_metrics.get("debit_preps_heure", "-"),
                "Historique (moy/jour)": hist_metrics["debit_preps_heure"],
            },
            {
                "Metrique": "Taux utilisation (%)",
                "Simulation": sim_metrics.get("taux_utilisation_pct", "-"),
                "Historique (moy/jour)": "-",
            },
            {
                "Metrique": "Temps attente moyen (sec)",
                "Simulation": sim_metrics.get("temps_attente_moyen_sec", "-"),
                "Historique (moy/jour)": "-",
            },
        ])

        return comparison


def _seconds_to_hms(seconds: float) -> str:
    """Convertit des secondes depuis minuit en HH:MM:SS."""
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"
