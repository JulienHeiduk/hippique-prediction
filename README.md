# Hippique Prediction — PMU Trot Paper Trading

Système de prédiction et de paper trading pour les courses de trot PMU (Attelé & Monté).
Voir `FORMULA_TROT.md` pour le détail complet du modèle de sélection.

---

## Installation

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

---

## Dashboard

```bash
streamlit run src/dashboard/app.py
```

Ouvre le viewer dans le navigateur. Utiliser le sélecteur de date dans la barre latérale
pour naviguer entre les fiches HTML des jours disponibles.

---

## Scheduler automatique

Le scheduler tourne en arrière-plan et gère toute la journée sans intervention :

| Heure | Action |
|---|---|
| 08:30 | Scraping programme + génération des paris + HTML → push GitHub |
| 10:00 – 22:00 (toutes les heures) | Re-scraping cotes + refresh des paris + HTML → push GitHub |
| 22:30 | Scraping résultats + résolution des paris + HTML P&L → push GitHub |

Démarrage manuel (terminal bloquant) :

```bash
.venv\Scripts\python.exe -c "
from src.trading.scheduler import start_scheduler
start_scheduler()
"
```

Ou double-cliquer sur **`run_scheduler.bat`** à la racine du projet.

---

## Démarrage automatique sur Windows

### 1. Configurer git push sans mot de passe

Le scheduler pousse le fichier HTML sur GitHub après chaque mise à jour. Git doit
s'authentifier seul via le Gestionnaire d'informations d'identification Windows.

1. Sur GitHub : **Settings → Developer settings → Personal access tokens → Tokens (classic)**
   → **Generate new token** → cocher `repo` → copier le token (`ghp_xxxx…`)

2. Dans un terminal, une seule fois :

```bash
git config --global credential.helper manager
git push   # → entrer login GitHub + token comme mot de passe
```

Les prochains `git push` (y compris ceux du scheduler) seront silencieux.

### 2. Planificateur de tâches Windows

1. Ouvrir **Planificateur de tâches** (`taskschd.msc`)
2. **Créer une tâche de base…**
   - **Déclencheur** : *Au démarrage de l'ordinateur* (ou *À l'ouverture de session*)
   - **Action** : *Démarrer un programme* → sélectionner `run_scheduler.bat`
   - **Démarrer dans** : `C:\Users\julie\OneDrive\Bureau\hippique-prediction`
3. Cliquer **OK** → clic droit sur la tâche → **Exécuter** pour tester immédiatement.

---

## Constantes configurables

Fichier : `config/settings.py`

| Constante | Valeur par défaut | Rôle |
|---|---|---|
| `EV_THRESHOLD` | `1.0` | Seuil minimum d'EV pour parier |
| `KELLY_FRACTION` | `0.25` | Fraction Kelly (mise conservative) |
| `UNIT_STAKE` | `2.0 €` | Mise de base par pari |

---

## Architecture

```
scraper/   →   features/   →   model/   →   trading/   →   dashboard/
  PMU API       form_score      M4 Combined   kelly_stake    Streamlit
  DuckDB        odds_features   score_combined generate_bets  viewer
                jockey_win_rate backtest       resolve_bets
```

Voir `FORMULA_TROT.md` pour l'explication complète du modèle.
