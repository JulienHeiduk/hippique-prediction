"""CatBoost binary classifier for plat (lab variant).

Same numeric feature set as production (``FEATURES_BY_DISCIPLINE['plat']``)
plus two raw categoricals — ``hippodrome`` and ``jockey_name`` — fed
directly to CatBoost so it can use its native target-encoder for
high-cardinality strings rather than the simple per-string win-rate that
production currently exposes.

Trained medians and the numeric feature list are stashed on the model
object after training so :func:`score` can re-prepare X without touching
disk.
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from src.model.lgbm import FEATURES_BY_DISCIPLINE, _compute_medians, _prepare_X


_CAT_FEATURES: list[str] = ["hippodrome", "jockey_name"]


def _prepare_X_cat(
    df: pd.DataFrame,
    medians: dict,
    numeric_features: list[str],
) -> pd.DataFrame:
    X_num = _prepare_X(df, medians=medians, features=numeric_features)
    X_cat = df[_CAT_FEATURES].fillna("__missing__").astype(str).reset_index(drop=True)
    X_num = X_num.reset_index(drop=True)
    return pd.concat([X_num, X_cat], axis=1)


def make_train(seed: int = 42):
    """Return a ``train(df)`` closure bound to a fixed CatBoost random seed.

    Used by :mod:`src.lab.registry` to register multiple seeded variants
    side-by-side for robustness checks.
    """
    def _train(df: pd.DataFrame):
        from catboost import CatBoostClassifier

        if df.empty or "finish_position" not in df.columns:
            raise ValueError("df must contain finish_position for training")

        df = df.sort_values("race_id").copy()
        numeric_features = FEATURES_BY_DISCIPLINE["plat"]
        medians = _compute_medians(df, features=numeric_features)

        X = _prepare_X_cat(df, medians=medians, numeric_features=numeric_features)
        y = (pd.to_numeric(df["finish_position"], errors="coerce") == 1).astype(int).values

        model = CatBoostClassifier(
            iterations=200,
            depth=6,
            learning_rate=0.1,
            loss_function="Logloss",
            cat_features=_CAT_FEATURES,
            random_seed=seed,
            verbose=False,
            thread_count=-1,
        )
        model.fit(X, y)

        model._lab_medians = medians
        model._lab_numeric_features = numeric_features

        n_races = df["race_id"].nunique()
        logger.info(
            "CatBoost (plat lab, seed={}) trained on {} races / {} runners ({} winners)",
            seed, n_races, len(df), int(y.sum()),
        )
        return model

    return _train


# Default seed=42 builder (kept for the existing cat_plat registry entry).
train = make_train(seed=42)


def score(df: pd.DataFrame, model) -> pd.Series:
    medians = model._lab_medians
    numeric_features = model._lab_numeric_features

    X = _prepare_X_cat(df, medians=medians, numeric_features=numeric_features)
    raw = model.predict_proba(X)[:, 1]

    out = pd.Series(raw, index=df["runner_id"].values, dtype=float)

    # Per-race normalisation — mirrors src/model/lgbm.py:score_lgbm_binary
    # so the harness sees the same probability shape as production.
    if "race_id" not in df.columns:
        return out
    for _, group in df.groupby("race_id"):
        idx = group["runner_id"].values
        s = out.loc[idx]
        total = s.sum()
        if total > 0:
            out.loc[idx] = s / total

    return out
