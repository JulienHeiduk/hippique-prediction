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

## Usage quotidien

Toutes les commandes s'exécutent depuis la racine du projet avec l'interpréteur du venv.

### 1. Session du matin — scraping + prédictions + HTML

À lancer **manuellement le matin** (ou laisser le scheduler tourner à 09:00 et 11:30).
Scrape le programme PMU, calcule les paris EV+ et génère la fiche HTML.

```bash
.venv\Scripts\python.exe -c "
from src.trading.scheduler import run_morning_session
run_morning_session('20260228')   # remplacer par la date du jour YYYYMMDD
"
```

> **Astuce :** les cotes PMU sont publiées progressivement. Si la fiche HTML affiche des
> **⚠ cotes manquantes**, relancer cette commande 30–60 minutes plus tard pour obtenir
> des cotes complètes avant de parier.

La fiche HTML est enregistrée dans :
```
data/reports/bets_YYYYMMDD.html
```

---

### 2. Mise à jour en cours de journée — résultats partiels + cotes actualisées

Après que les premières courses ont couru, cette commande :
- re-scrape les résultats des courses terminées et les dernières cotes des courses à venir,
- résout les paris en attente qui ont un résultat,
- met à jour les prédictions pour les courses restantes,
- régénère la fiche HTML.

```bash
.venv\Scripts\python.exe -c "
from src.scraper import get_connection, run_pipeline
from src.trading.engine import resolve_bets, generate_bets
from src.trading.reporter import export_bets_html

date = '20260228'   # remplacer par la date du jour YYYYMMDD

run_pipeline(date)
conn = get_connection()
try:
    resolve_bets(conn, date)
    generate_bets(conn, date)
    export_bets_html(conn, date)
finally:
    conn.close()
"
```

---

### 3. Session du soir — résultats finaux + P&L

Une fois toutes les courses terminées (après 22:00), cette commande résout tous les
paris en attente et produit la fiche HTML finale avec les gains/pertes du jour.

```bash
.venv\Scripts\python.exe -c "
from src.trading.scheduler import run_evening_session
run_evening_session('20260228')   # remplacer par la date du jour YYYYMMDD
"
```

---

## Scheduler automatique

Pour lancer le pipeline en automatique tous les jours sans intervention manuelle :

```bash
.venv\Scripts\python.exe -c "
from src.trading.scheduler import start_scheduler
start_scheduler()
"
```

Horaires configurés :

| Heure | Action |
|---|---|
| 09:00 | Scraping programme + génération des paris + HTML |
| 11:30 | Re-scraping pour récupérer les cotes tardives + HTML final |
| 22:00 | Scraping résultats + résolution des paris + HTML P&L |

> Le scheduler est bloquant. Laisser tourner en arrière-plan ou dans un terminal dédié.

---

## Lire la fiche HTML

Ouvrir directement dans un navigateur :

```
data/reports/bets_YYYYMMDD.html
```

La fiche affiche pour chaque course sélectionnée :
- le badge **✓ Cotes complètes** (vert) ou **⚠ N cote(s) manquante(s)** (orange)
- les paris EV+ avec probabilité modèle, probabilité marché et ratio EV
- le statut (en attente / gagné / perdu) et le P&L une fois les résultats connus

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
  DuckDB        odds_features   score_combined generate_bets  (à venir)
                jockey_win_rate backtest       resolve_bets
```

Voir `FORMULA_TROT.md` pour l'explication complète du modèle.
