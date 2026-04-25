"""LightGBM LambdaRank model for horse race ranking."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

from config.settings import (
    LGBM_MODEL_PATH, LGBM_MEDIANS_PATH,
    LGBM_PLAT_MODEL_PATH, LGBM_PLAT_MEDIANS_PATH,
    LGBM_PLAT_PARAMS_PATH,
    MODEL_DIR,
)

# Optuna-tuned hyperparameters (written by scripts/tune_lgbm_hyperparams.py)
_LGBM_PARAMS_PATH = MODEL_DIR / "lgbm_params.json"

_PARAMS_PATHS: dict[str, Path] = {
    "trot": _LGBM_PARAMS_PATH,
    "plat": LGBM_PLAT_PARAMS_PATH,
}

# Fixed fallback hyperparameters (used when no tuned params file exists)
_DEFAULT_PARAMS: dict = {
    "n_estimators": 300,
    "num_leaves": 31,
    "learning_rate": 0.05,
    "min_child_samples": 10,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
}


def _load_lgbm_params(discipline: str = "trot") -> dict:
    """Return tuned params from disk if available, else fallback defaults."""
    import json
    params_path = _PARAMS_PATHS.get(discipline, _LGBM_PARAMS_PATH)
    if params_path.exists():
        with open(params_path) as f:
            stored = json.load(f)
        # Strip metadata keys, keep only LightGBM params
        lgbm_keys = {"n_estimators", "num_leaves", "learning_rate",
                     "min_child_samples", "subsample", "colsample_bytree",
                     "reg_alpha", "reg_lambda"}
        params = {k: v for k, v in stored.items() if k in lgbm_keys}
        logger.debug("Loaded tuned LGBM params ({}) from {}", discipline, params_path)
        return params
    return _DEFAULT_PARAMS.copy()

if TYPE_CHECKING:
    from src.model.backtest import BacktestReport

# Features used for training and inference (must exist in the features DataFrame)
# NOTE: odds_drift_pct and odds_rank_change were REMOVED because they are
# look-ahead features: in training they capture the full-day market movement
# (morning -> post-time), but at prediction time only partial drift is
# available. This caused a train/serve skew.
TROT_FEATURES = [
    # Form features
    "form_score",
    "win_rate_last5",
    "top3_rate_last5",
    "form_trend",
    "best_position_last5",
    "n_valid_runs",
    # Market features (available at prediction time)
    "morning_implied_prob_norm",
    "morning_odds_rank",
    "is_favorite",
    "field_entropy",
    # Runner features
    "jockey_win_rate",
    "trainer_win_rate",
    "draw_position",
    "handicap_distance",
    "deferre",
    "race_hour",
    # Race features
    "distance_metres",
    "field_size",
    # Horse history
    "horse_n_runs",
    "horse_win_rate",
    "horse_win_rate_at_track",
    "days_since_last_race",
]

PLAT_FEATURES = [
    # Form features
    "form_score",
    "win_rate_last5",
    "top3_rate_last5",
    "form_trend",
    "best_position_last5",
    "n_valid_runs",
    # Market features (available at prediction time)
    "morning_implied_prob_norm",
    "morning_odds_rank",
    "is_favorite",
    "field_entropy",
    # Runner features
    "jockey_win_rate",
    "trainer_win_rate",
    "draw_position",
    "weight_kg",
    "race_hour",
    # Race features
    "distance_metres",
    "field_size",
    # Horse history
    "horse_n_runs",
    "horse_win_rate",
    "horse_win_rate_at_track",
    "days_since_last_race",
]

# Backward-compat alias
FEATURES = TROT_FEATURES

FEATURES_BY_DISCIPLINE: dict[str, list[str]] = {
    "trot": TROT_FEATURES,
    "plat": PLAT_FEATURES,
}

_MODEL_PATHS: dict[str, Path] = {
    "trot": LGBM_MODEL_PATH,
    "plat": LGBM_PLAT_MODEL_PATH,
}

_MEDIANS_PATHS: dict[str, Path] = {
    "trot": LGBM_MEDIANS_PATH,
    "plat": LGBM_PLAT_MEDIANS_PATH,
}


def _compute_medians(df: pd.DataFrame, features: list[str] | None = None) -> dict[str, float]:
    """Compute median for each feature from a DataFrame (for NaN imputation)."""
    if features is None:
        features = TROT_FEATURES
    medians = {}
    for col in features:
        vals = pd.to_numeric(df.get(col, pd.Series(dtype=float)), errors="coerce")
        med = vals.median()
        medians[col] = float(med) if pd.notna(med) else 0.0
    return medians


def save_medians(medians: dict[str, float], path: Path = LGBM_MEDIANS_PATH) -> Path:
    """Save training-time medians to disk for consistent NaN imputation."""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(medians, f, indent=2)
    logger.info("Feature medians saved -> {}", path)
    return path


def load_medians(path: Path = LGBM_MEDIANS_PATH) -> dict[str, float] | None:
    """Load training-time medians from disk. Returns None if not found."""
    import json
    if not path.exists():
        logger.warning("Feature medians not found at {} — will use per-batch median", path)
        return None
    with open(path) as f:
        return json.load(f)


def _prepare_X(
    df: pd.DataFrame,
    medians: dict[str, float] | None = None,
    features: list[str] | None = None,
) -> pd.DataFrame:
    """Extract and fill feature matrix.

    Args:
        df: DataFrame containing at least the FEATURES columns.
        medians: Pre-computed medians from training data. When provided,
            NaN values are filled using these fixed medians instead of
            computing per-batch medians (which would cause train/serve skew).
        features: Feature list to use. Defaults to TROT_FEATURES.
    """
    if features is None:
        features = TROT_FEATURES
    X = df.reindex(columns=features).copy()
    for col in features:
        X[col] = pd.to_numeric(X[col], errors="coerce").astype(float)
        if medians is not None:
            fill_val = medians.get(col, 0.0)
        else:
            med = X[col].median()
            fill_val = float(med) if pd.notna(med) else 0.0
        X[col] = X[col].fillna(fill_val)
    return X


def train_lgbm(df: pd.DataFrame, discipline: str = "trot"):
    """Train a LightGBM LambdaRank model on historical data.

    Args:
        df: Features DataFrame from compute_features() — must contain
            finish_position (not null) and race_id.
        discipline: "trot" or "plat" — selects feature list and medians path.

    Returns:
        Trained LGBMRanker instance.
    """
    import lightgbm as lgb

    if df.empty or "finish_position" not in df.columns:
        raise ValueError("df must contain finish_position for training")

    features = FEATURES_BY_DISCIPLINE[discipline]
    medians_path = _MEDIANS_PATHS[discipline]

    df = df.sort_values("race_id").copy()

    # Compute and persist training-time medians for consistent NaN imputation
    medians = _compute_medians(df, features=features)
    save_medians(medians, path=medians_path)

    X = _prepare_X(df, medians=medians, features=features)

    # Relevance: 2 = winner, 1 = top-3, 0 = rest
    y = df["finish_position"].apply(
        lambda p: 2 if p == 1 else (1 if p <= 3 else 0)
    ).values

    # Group sizes (runners per race, in sorted race_id order)
    groups = df.groupby("race_id", sort=True).size().values

    params = _load_lgbm_params(discipline)
    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        ndcg_eval_at=[1, 3],
        random_state=42,
        verbose=-1,
        **params,
    )
    model.fit(X, y, group=groups)

    logger.info(
        "LightGBM LambdaRank ({}) trained on {} races / {} runners",
        discipline, len(groups), len(df),
    )
    return model



def train_lgbm_binary(df: pd.DataFrame, discipline: str = "plat"):
    """Train a binary win-classifier (objective='binary').

    Label: 1 if finish_position == 1 else 0. Unlike LambdaRank, the
    raw output is a calibrated win probability that can be compared
    to the market-implied probability for EV decisions.

    Args:
        df: Features DataFrame from compute_features() — must contain
            finish_position and race_id.
        discipline: "trot" or "plat" — selects feature list and medians path.

    Returns:
        Trained LGBMClassifier instance.
    """
    import lightgbm as lgb

    if df.empty or "finish_position" not in df.columns:
        raise ValueError("df must contain finish_position for training")

    features = FEATURES_BY_DISCIPLINE[discipline]
    medians_path = _MEDIANS_PATHS[discipline]

    df = df.sort_values("race_id").copy()

    medians = _compute_medians(df, features=features)
    save_medians(medians, path=medians_path)

    X = _prepare_X(df, medians=medians, features=features)
    y = (pd.to_numeric(df["finish_position"], errors="coerce") == 1).astype(int).values

    params = _load_lgbm_params(discipline)
    model = lgb.LGBMClassifier(
        objective="binary",
        random_state=42,
        verbose=-1,
        **params,
    )
    model.fit(X, y)

    n_races = df["race_id"].nunique()
    logger.info(
        "LightGBM binary classifier ({}) trained on {} races / {} runners ({} winners)",
        discipline, n_races, len(df), int(y.sum()),
    )
    return model


def score_lgbm_binary(
    df: pd.DataFrame,
    model=None,
    discipline: str = "plat",
    blend_w: float | None = None,
) -> pd.Series:
    """Score with binary classifier: per-race normalize then market-blend.

    Returns a Series indexed by runner_id whose values sum to 1 within
    each race, so engine.py / backtest.py can divide-by-total to recover
    the same probabilities (no behaviour change in the consumers).

    Args:
        blend_w: Weight on the model in the blend. None → use settings
            PLAT_MARKET_BLEND_W. 1.0 disables blending (pure model).
    """
    from src.model.calibration import blend_with_market
    from config.settings import PLAT_MARKET_BLEND_W

    if blend_w is None:
        blend_w = PLAT_MARKET_BLEND_W

    if model is None:
        model = load_lgbm_model(path=_MODEL_PATHS[discipline])

    if model is None:
        return pd.Series(0.0, index=df["runner_id"])

    features = FEATURES_BY_DISCIPLINE[discipline]
    medians_path = _MEDIANS_PATHS[discipline]
    medians = load_medians(path=medians_path)
    X = _prepare_X(df, medians=medians, features=features)

    if hasattr(model, "predict_proba"):
        raw = model.predict_proba(X)[:, 1]
    else:
        # Booster loaded from disk: with objective='binary', predict() returns
        # the sigmoid-applied probability of class 1 directly.
        raw = model.predict(X)

    out = pd.Series(raw, index=df["runner_id"].values, dtype=float)

    if "race_id" not in df.columns:
        return out

    for _, group in df.groupby("race_id"):
        idx = group["runner_id"].values
        s = out.loc[idx]
        total = s.sum()
        if total > 0:
            s = s / total
        if blend_w < 1.0 and "morning_implied_prob_norm" in group.columns:
            implied_vals = pd.to_numeric(
                group["morning_implied_prob_norm"], errors="coerce",
            ).fillna(1.0 / len(group))
            implied = pd.Series(implied_vals.values, index=idx, dtype=float)
            implied_total = implied.sum()
            if implied_total > 0:
                implied = implied / implied_total
            blended = blend_with_market(s, implied, w=blend_w)
            blended_total = blended.sum()
            if blended_total > 0:
                blended = blended / blended_total
            out.loc[idx] = blended
        else:
            out.loc[idx] = s

    return out


def train_for_discipline(df: pd.DataFrame, discipline: str):
    """Dispatch to the right trainer for *discipline*.

    Plat → binary classifier (calibrated probability).
    Trot → LambdaRank (working in production).
    """
    if discipline == "plat":
        return train_lgbm_binary(df, discipline=discipline)
    return train_lgbm(df, discipline=discipline)


def score_for_discipline(df: pd.DataFrame, model, discipline: str) -> pd.Series:
    """Dispatch to the right scorer for *discipline*."""
    if discipline == "plat":
        return score_lgbm_binary(df, model, discipline=discipline)
    return score_lgbm(df, model, discipline=discipline)


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


def score_lgbm(df: pd.DataFrame, model=None, discipline: str = "trot") -> pd.Series:
    """Score runners with the LightGBM model.

    Same interface as score_combined: returns a Series indexed by runner_id.
    Higher score = model ranks the horse higher.
    Auto-loads the model from disk when model=None.
    Returns a zero Series if the model is unavailable.

    Args:
        discipline: "trot" or "plat" — selects feature list and medians path.
    """
    if model is None:
        model = load_lgbm_model(path=_MODEL_PATHS[discipline])

    if model is None:
        return pd.Series(0.0, index=df["runner_id"])

    features = FEATURES_BY_DISCIPLINE[discipline]
    medians_path = _MEDIANS_PATHS[discipline]

    # Use training-time medians for NaN imputation (avoids train/serve skew)
    medians = load_medians(path=medians_path)
    X = _prepare_X(df, medians=medians, features=features)
    raw = model.predict(X, num_iteration=model.best_iteration if hasattr(model, "best_iteration") else None)
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


def backtest_lgbm_walkforward(
    df: pd.DataFrame,
    min_train_days: int = 30,
    bet_type: str = "win",
    ev_filter: bool = False,
    ev_threshold: float = 1.0,
    trainer_fn=None,
    model_name: str = "lgbm_walkforward",
    discipline: str = "trot",
) -> "BacktestReport":
    """Walk-forward backtest for LightGBM — no data leakage.

    For each test date (after the first min_train_days), trains the model
    exclusively on all races *before* that date, then scores the test-date
    runners. P&L is computed via the standard backtest() function.

    Args:
        df:             Full features DataFrame from compute_features().
        min_train_days: Minimum number of past days required before testing.
        bet_type:       "win" or "place".
        ev_filter:      Apply EV filter (model_prob > implied_prob).
        ev_threshold:   EV threshold to use when ev_filter=True.
        trainer_fn:     Function to train the model (defaults to train_lgbm).
        model_name:     Name for the BacktestReport.
        discipline:     "trot" or "plat".

    Returns:
        BacktestReport aggregating all out-of-sample test days.
    """
    from src.model.backtest import backtest, BacktestReport

    if trainer_fn is None:
        trainer_fn = lambda d: train_lgbm(d, discipline=discipline)

    dates = sorted(df["date"].unique())
    if len(dates) <= min_train_days:
        raise ValueError(
            f"Need more than {min_train_days} dates of data, got {len(dates)}"
        )

    test_dates = dates[min_train_days:]
    full_report = BacktestReport(model_name=model_name, bet_type=bet_type)

    logger.info(
        "Walk-forward backtest ({}): {} train warmup / {} test dates",
        model_name, min_train_days, len(test_dates),
    )

    for i, test_date in enumerate(test_dates):
        train_df = df[df["date"] < test_date]
        test_df  = df[df["date"] == test_date]

        if train_df.empty or test_df.empty:
            continue

        try:
            model = trainer_fn(train_df)
        except Exception as exc:
            logger.warning("Training failed for test_date={}: {}", test_date, exc)
            continue

        scorer = lambda d, m=model, disc=discipline: score_lgbm(d, m, discipline=disc)
        day_report = backtest(
            test_df, scorer,
            model_name=model_name,
            bet_type=bet_type,
            ev_filter=ev_filter,
            ev_threshold=ev_threshold,
        )
        full_report.bets.extend(day_report.bets)

        if (i + 1) % 10 == 0:
            logger.info(
                "  {}/{} test dates done — running ROI={:.1%}",
                i + 1, len(test_dates), full_report.roi,
            )

    logger.info(
        "Walk-forward done: {} bets | ROI={:.1%} | hit={:.1%} | P&L={:.2f}",
        len(full_report.bets), full_report.roi,
        full_report.hit_rate,
        sum(b.pnl for b in full_report.bets),
    )
    return full_report
