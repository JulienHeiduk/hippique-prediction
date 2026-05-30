"""Registry of lab variants — the unit of comparison for the walk-forward harness.

A :class:`Variant` bundles everything the harness needs to train + score a
candidate strategy: features, model, optional calibration, and the EV gate
to apply at bet time. The registry is a plain dict so adding an experiment
is a one-liner from anywhere in :mod:`src.lab`.

The ``prod_plat`` baseline mirrors the production pipeline exactly so every
comparison is anchored to what is currently shipping.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import duckdb
import pandas as pd

from src.features.pipeline import compute_features
from src.lab.models import cat_plat, lgbm_plat_ext
from src.model.lgbm import score_for_discipline, train_for_discipline


@dataclass
class Variant:
    name: str
    discipline: str
    feature_fn: Callable[[duckdb.DuckDBPyConnection], pd.DataFrame]
    train_fn:   Callable[[pd.DataFrame], Any]
    score_fn:   Callable[[pd.DataFrame, Any], pd.Series]
    use_calibrator: bool = True
    ev_threshold:   float = 1.5


VARIANTS: dict[str, Variant] = {
    "prod_plat": Variant(
        name="prod_plat",
        discipline="plat",
        feature_fn=lambda conn: compute_features(conn, discipline="plat"),
        train_fn=lambda df: train_for_discipline(df, discipline="plat"),
        score_fn=lambda df, m: score_for_discipline(df, m, discipline="plat"),
        use_calibrator=True,
        ev_threshold=1.5,
    ),
    "cat_plat": Variant(
        name="cat_plat",
        discipline="plat",
        feature_fn=lambda conn: compute_features(conn, discipline="plat"),
        train_fn=cat_plat.make_train(seed=42),
        score_fn=cat_plat.score,
        use_calibrator=True,
        ev_threshold=1.5,
    ),
    "cat_plat_s7": Variant(
        name="cat_plat_s7",
        discipline="plat",
        feature_fn=lambda conn: compute_features(conn, discipline="plat"),
        train_fn=cat_plat.make_train(seed=7),
        score_fn=cat_plat.score,
        use_calibrator=True,
        ev_threshold=1.5,
    ),
    "cat_plat_s13": Variant(
        name="cat_plat_s13",
        discipline="plat",
        feature_fn=lambda conn: compute_features(conn, discipline="plat"),
        train_fn=cat_plat.make_train(seed=13),
        score_fn=cat_plat.score,
        use_calibrator=True,
        ev_threshold=1.5,
    ),
    "lgbm_plat_ext": Variant(
        name="lgbm_plat_ext",
        discipline="plat",
        feature_fn=lambda conn: compute_features(conn, discipline="plat"),
        train_fn=lgbm_plat_ext.make_train(seed=42),
        score_fn=lgbm_plat_ext.score,
        use_calibrator=True,
        ev_threshold=1.5,
    ),
}


def get_variant(name: str) -> Variant:
    if name not in VARIANTS:
        raise KeyError(f"Unknown variant '{name}'. Available: {sorted(VARIANTS)}")
    return VARIANTS[name]
