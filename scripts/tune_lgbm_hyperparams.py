"""LightGBM hyperparameter tuning via Optuna — 3-fold time-series CV.

Uses GPU (device_type='gpu' / OpenCL) for faster trial evaluation.
Optimises for WIN ROI using LightGBM LambdaRank.

Usage:
    python scripts/tune_lgbm_hyperparams.py [--n-trials 50] [--cpu]

Outputs:
    data/models/lgbm_params.json  — best hyperparameters
    Summary table printed to stdout
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import optuna
import pandas as pd
from loguru import logger

from config.settings import MODEL_DIR
from src.scraper import get_connection
from src.features.pipeline import compute_features
from src.model.backtest import backtest, BacktestReport
from src.model.lgbm import _prepare_X

LGBM_PARAMS_PATH = MODEL_DIR / "lgbm_params.json"
N_FOLDS = 3
MIN_TRAIN_DAYS = 30


def _train_with_params(df: pd.DataFrame, params: dict, device: str) -> object:
    """Train LGBMRanker with custom params + WIN label (2=1st, 1=top3, 0=rest)."""
    import lightgbm as lgb

    df = df.sort_values("race_id").copy()
    X = _prepare_X(df)
    y = df["finish_position"].apply(
        lambda p: 2 if p == 1 else (1 if p <= 3 else 0)
    ).values
    groups = df.groupby("race_id", sort=True).size().values

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        ndcg_eval_at=[2],
        device_type=device,
        random_state=42,
        verbose=-1,
        **params,
    )
    model.fit(X, y, group=groups)
    return model


def _cv_roi(
    df: pd.DataFrame,
    params: dict,
    device: str,
    n_folds: int = N_FOLDS,
    bet_type: str = "win",
) -> float:
    """Time-series k-fold CV — returns mean ROI across folds."""
    from src.model.lgbm import score_lgbm

    dates = sorted(df["date"].unique())
    n = len(dates)
    # Each fold: train on first (fold+1)/(n_folds+1) dates, test on next chunk
    fold_size = max(1, (n - MIN_TRAIN_DAYS) // n_folds)

    rois: list[float] = []
    for fold in range(n_folds):
        train_end_idx = MIN_TRAIN_DAYS + fold * fold_size
        test_end_idx = min(train_end_idx + fold_size, n)

        train_dates = dates[:train_end_idx]
        test_dates = dates[train_end_idx:test_end_idx]

        if not train_dates or not test_dates:
            continue

        train_df = df[df["date"].isin(train_dates)]
        test_df = df[df["date"].isin(test_dates)]

        if train_df.empty or test_df.empty:
            continue

        try:
            model = _train_with_params(train_df, params, device)
        except Exception as exc:
            logger.warning("Training failed (fold {}): {}", fold, exc)
            return -1.0  # penalise bad params

        scorer = lambda d, m=model: score_lgbm(d, m)
        report = backtest(test_df, scorer, model_name="optuna_cv", bet_type=bet_type)
        rois.append(report.roi)

    return sum(rois) / len(rois) if rois else -1.0


def make_objective(df: pd.DataFrame, device: str, bet_type: str):
    def objective(trial: optuna.Trial) -> float:
        params = {
            "num_leaves": trial.suggest_int("num_leaves", 15, 127),
            "n_estimators": trial.suggest_int("n_estimators", 100, 600),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 60),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        }
        roi = _cv_roi(df, params, device, bet_type=bet_type)
        return roi

    return objective


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=50)
    parser.add_argument("--cpu", action="store_true", help="Force CPU (disable GPU)")
    parser.add_argument("--bet-type", default="win", choices=["win"])
    args = parser.parse_args()

    device = "cpu" if args.cpu else "gpu"

    # Verify GPU availability
    if device == "gpu":
        try:
            import lightgbm as lgb
            import numpy as np
            X = np.random.rand(50, 5)
            y = [1] * 25 + [0] * 25
            ds = lgb.Dataset(X, y, group=[50])
            lgb.train({"objective": "lambdarank", "device_type": "gpu", "verbose": -1},
                      ds, num_boost_round=3)
            logger.info("GPU (OpenCL) confirmed available")
        except Exception as exc:
            logger.warning("GPU unavailable ({}), falling back to CPU", exc)
            device = "cpu"

    logger.info("Loading historical features...")
    conn = get_connection()
    try:
        df = compute_features(conn)
    finally:
        conn.close()

    logger.info(
        "Dataset: {} dates / {} races / {} runners | device={}",
        df["date"].nunique(), df["race_id"].nunique(), len(df), device,
    )

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize")

    logger.info(
        "Starting Optuna: {} trials, {}-fold CV, bet_type={}",
        args.n_trials, N_FOLDS, args.bet_type,
    )

    study.optimize(
        make_objective(df, device, args.bet_type),
        n_trials=args.n_trials,
        show_progress_bar=True,
    )

    best = study.best_params
    best_roi = study.best_value

    logger.info("Best ROI (CV): {:.1%}", best_roi)
    logger.info("Best params: {}", best)

    # Also record baseline (current fixed params)
    baseline_params = {
        "num_leaves": 31,
        "n_estimators": 300,
        "learning_rate": 0.05,
        "min_child_samples": 10,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.0,
        "reg_lambda": 0.0,
    }
    baseline_roi = _cv_roi(df, baseline_params, device, bet_type=args.bet_type)

    print("\n" + "=" * 60)
    print(f"OPTUNA RESULTS — {args.bet_type.upper()} bets, {args.n_trials} trials")
    print("=" * 60)
    print(f"Baseline (fixed params)  CV ROI: {baseline_roi:.1%}")
    print(f"Optuna best              CV ROI: {best_roi:.1%}")
    print(f"Improvement:             {best_roi - baseline_roi:+.1%}")
    print("\nBest hyperparameters:")
    for k, v in best.items():
        baseline_v = baseline_params.get(k, "N/A")
        print(f"  {k:<22} {v!r:>12}   (was {baseline_v!r})")
    print("=" * 60 + "\n")

    # Save best params
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    output = {
        "source": "optuna",
        "n_trials": args.n_trials,
        "bet_type": args.bet_type,
        "cv_roi": best_roi,
        "baseline_cv_roi": baseline_roi,
        **best,
    }
    with open(LGBM_PARAMS_PATH, "w") as f:
        json.dump(output, f, indent=2)
    logger.info("Best params saved -> {}", LGBM_PARAMS_PATH)


if __name__ == "__main__":
    main()
