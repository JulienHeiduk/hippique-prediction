"""LightGBM binary classifier for plat (lab variant) with extended features.

Uses the same model class as production (``train_lgbm_binary``) but adds the
features that ``compute_features`` already builds yet ``PLAT_FEATURES`` ignores:
at-track jockey/trainer win rates, recent average finish positions, km-time
history, distance specialisation, horse-jockey pair stats, days-since-last-win,
and handicap weight differential.

Medians are stashed on the model object so the harness never touches disk
(unlike the prod codepath, which writes to ``LGBM_PLAT_MEDIANS_PATH`` on
every walk-forward iteration).
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from src.model.lgbm import PLAT_FEATURES, _compute_medians, _prepare_X


PLAT_EXT_FEATURES: list[str] = PLAT_FEATURES + [
    "jockey_win_rate_at_track",
    "trainer_win_rate_at_track",
    "avg_position_last3",
    "avg_position_last5",
    "avg_km_time_hist",
    "best_km_time_hist",
    "horse_win_rate_at_distance",
    "horse_avg_position_at_distance",
    "horse_jockey_win_rate",
    "horse_jockey_n_races",
    "days_since_last_win",
    "handicap_distance",
]


def make_train(seed: int = 42):
    def _train(df: pd.DataFrame):
        import lightgbm as lgb

        if df.empty or "finish_position" not in df.columns:
            raise ValueError("df must contain finish_position for training")

        df = df.sort_values("race_id").copy()
        features = PLAT_EXT_FEATURES
        medians = _compute_medians(df, features=features)

        X = _prepare_X(df, medians=medians, features=features)
        y = (pd.to_numeric(df["finish_position"], errors="coerce") == 1).astype(int).values

        model = lgb.LGBMClassifier(
            objective="binary",
            n_estimators=300,
            num_leaves=31,
            learning_rate=0.05,
            min_child_samples=10,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=seed,
            verbose=-1,
        )
        model.fit(X, y)

        model._lab_medians = medians
        model._lab_features = features

        n_races = df["race_id"].nunique()
        logger.info(
            "LGBM ext (plat lab, seed={}) trained on {} races / {} runners ({} winners) "
            "with {} features",
            seed, n_races, len(df), int(y.sum()), len(features),
        )
        return model

    return _train


train = make_train(seed=42)


def score(df: pd.DataFrame, model) -> pd.Series:
    medians = model._lab_medians
    features = model._lab_features

    X = _prepare_X(df, medians=medians, features=features)
    raw = model.predict_proba(X)[:, 1]

    out = pd.Series(raw, index=df["runner_id"].values, dtype=float)

    if "race_id" not in df.columns:
        return out
    for _, group in df.groupby("race_id"):
        idx = group["runner_id"].values
        s = out.loc[idx]
        total = s.sum()
        if total > 0:
            out.loc[idx] = s / total

    return out
