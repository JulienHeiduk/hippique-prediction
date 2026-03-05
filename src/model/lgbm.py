"""LightGBM LambdaRank model for horse race ranking."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from config.settings import LGBM_MODEL_PATH

# Features used for training and inference (must exist in the features DataFrame)
FEATURES = [
    "form_score",
    "morning_implied_prob_norm",
    "odds_drift_pct",
    "jockey_win_rate",
    "distance_metres",
    "field_size",
    "morning_odds_rank",
]


def _prepare_X(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and fill feature matrix."""
    X = df.reindex(columns=FEATURES).copy()
    for col in FEATURES:
        X[col] = pd.to_numeric(X[col], errors="coerce")
        median = X[col].median()
        X[col] = X[col].fillna(median if pd.notna(median) else 0.0)
    return X


def train_lgbm(df: pd.DataFrame):
    """Train a LightGBM LambdaRank model on historical data.

    Args:
        df: Features DataFrame from compute_features() — must contain
            finish_position (not null) and race_id.

    Returns:
        Trained LGBMRanker instance.
    """
    import lightgbm as lgb

    if df.empty or "finish_position" not in df.columns:
        raise ValueError("df must contain finish_position for training")

    df = df.sort_values("race_id").copy()

    X = _prepare_X(df)

    # Relevance: 2 = winner, 1 = top-3, 0 = rest
    y = df["finish_position"].apply(
        lambda p: 2 if p == 1 else (1 if p <= 3 else 0)
    ).values

    # Group sizes (runners per race, in sorted race_id order)
    groups = df.groupby("race_id", sort=True).size().values

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        ndcg_eval_at=[1, 3],
        n_estimators=300,
        num_leaves=31,
        learning_rate=0.05,
        min_child_samples=10,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )
    model.fit(X, y, group=groups)

    logger.info(
        "LightGBM LambdaRank trained on {} races / {} runners",
        len(groups), len(df),
    )
    return model


def save_lgbm_model(model, path: Path = LGBM_MODEL_PATH) -> Path:
    """Save the trained model to disk (LightGBM native text format)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    booster = model.booster_ if hasattr(model, "booster_") else model
    booster.save_model(str(path))
    logger.info("LightGBM model saved → {}", path)
    return path


def load_lgbm_model(path: Path = LGBM_MODEL_PATH):
    """Load model from disk. Returns None if file not found."""
    import lightgbm as lgb

    if not path.exists():
        logger.warning("LightGBM model not found at {} — skipping", path)
        return None
    model = lgb.Booster(model_file=str(path))
    logger.info("LightGBM model loaded from {}", path)
    return model


def score_lgbm(df: pd.DataFrame, model=None) -> pd.Series:
    """Score runners with the LightGBM model.

    Same interface as score_combined: returns a Series indexed by runner_id.
    Higher score = model ranks the horse higher.
    Auto-loads the model from disk when model=None.
    Returns a zero Series if the model is unavailable.
    """
    if model is None:
        model = load_lgbm_model()

    if model is None:
        return pd.Series(0.0, index=df["runner_id"])

    X = _prepare_X(df)
    raw = model.predict(X)
    result = pd.Series(raw, index=df["runner_id"].values)

    # Shift per race so minimum score = 0 (LightGBM raw scores can be negative;
    # generate_bets requires total_score > 0 to compute model_prob)
    if "race_id" in df.columns:
        for _, group in df.groupby("race_id"):
            idx = group["runner_id"].values
            min_s = result[idx].min()
            if min_s < 0:
                result[idx] = result[idx] - min_s
    elif result.min() < 0:
        result = result - result.min()

    return result
