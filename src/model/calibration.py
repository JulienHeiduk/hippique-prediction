"""Probability calibration helpers."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


def blend_with_market(
    model_prob: pd.Series,
    implied_prob: pd.Series,
    w: float = 0.4,
) -> pd.Series:
    """Linear blend of model probability with market-implied probability.

    p_final = w * model_prob + (1 - w) * implied_prob

    Inputs must share the same index (typically runner_id within one race).
    The caller is responsible for re-normalising to sum to 1 per race.
    """
    if not 0.0 <= w <= 1.0:
        raise ValueError(f"w must be in [0, 1], got {w}")
    return w * model_prob + (1.0 - w) * implied_prob


class IsotonicProbCalibrator:
    """Isotonic regression on (model_prob, outcome) pairs.

    Use to correct over- or under-confident model probabilities. Fit on a
    held-out set where outcomes are known; apply at inference to remap raw
    model probabilities onto the empirical hit-rate curve. Monotonic by
    construction, so the relative ranking of runners within a race is
    preserved.
    """

    def __init__(self):
        from sklearn.isotonic import IsotonicRegression
        self.iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        self.fitted = False

    def fit(self, probs, outcomes) -> "IsotonicProbCalibrator":
        p = np.asarray(probs, dtype=float)
        y = np.asarray(outcomes, dtype=float)
        mask = np.isfinite(p) & np.isfinite(y)
        if mask.sum() < 20:
            raise ValueError(f"Not enough finite samples to fit isotonic: {mask.sum()}")
        self.iso.fit(p[mask], y[mask])
        self.fitted = True
        return self

    def transform(self, probs):
        arr = np.asarray(probs, dtype=float)
        if not self.fitted:
            return arr
        return self.iso.transform(np.clip(arr, 0.0, 1.0))

    def save(self, path: Path) -> Path:
        import joblib
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.iso, path)
        logger.info("Isotonic calibrator saved -> {}", path)
        return path

    @classmethod
    def load(cls, path: Path) -> "IsotonicProbCalibrator | None":
        import joblib
        path = Path(path)
        if not path.exists():
            logger.debug("Isotonic calibrator not found at {} -- skipping", path)
            return None
        c = cls()
        c.iso = joblib.load(path)
        c.fitted = True
        return c


def fit_calibrator_temporal_holdout(
    df: pd.DataFrame,
    score_fn,
    train_fn,
    calib_frac: float = 0.2,
) -> IsotonicProbCalibrator:
    """Fit an IsotonicProbCalibrator using a temporal holdout.

    Splits *df* into (older 1-calib_frac) train and (newer calib_frac) holdout
    by date. Trains a model on the train slice with *train_fn(train_df)*,
    scores the holdout with *score_fn(holdout_df, model)*, then fits the
    isotonic on those out-of-sample scores vs. the actual win outcomes.

    The caller is expected to refit the production model on the *full* df
    afterwards; the calibrator transfers reasonably to a slightly larger
    training set in practice.
    """
    if "date" not in df.columns or "finish_position" not in df.columns:
        raise ValueError("df must have date and finish_position columns")

    dates = sorted(df["date"].unique())
    if len(dates) < 10:
        raise ValueError(f"Need >=10 dates for calibrator, got {len(dates)}")

    split = int(len(dates) * (1.0 - calib_frac))
    split = max(1, min(split, len(dates) - 1))
    train_dates = set(dates[:split])
    calib_dates = set(dates[split:])

    train_df = df[df["date"].isin(train_dates)].copy()
    calib_df = df[df["date"].isin(calib_dates)].copy()
    if train_df.empty or calib_df.empty:
        raise ValueError("Empty train or calib slice for calibrator")

    model = train_fn(train_df)
    scores = score_fn(calib_df, model)  # Series indexed by runner_id

    p = calib_df["runner_id"].map(scores).fillna(0.0).values
    y = (
        pd.to_numeric(calib_df["finish_position"], errors="coerce") == 1
    ).astype(int).values

    cal = IsotonicProbCalibrator().fit(p, y)
    logger.info(
        "Isotonic calibrator fitted on {} runners ({} winners), train_dates={} calib_dates={}",
        len(p), int(y.sum()), len(train_dates), len(calib_dates),
    )
    return cal
