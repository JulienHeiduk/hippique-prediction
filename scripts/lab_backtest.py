"""CLI: run the lab walk-forward harness over one or more registered variants.

Usage:
    .venv/Scripts/python.exe -m scripts.lab_backtest
    .venv/Scripts/python.exe -m scripts.lab_backtest --variants prod_plat,my_alt
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger  # noqa: F401  (loguru side-effect: configures stderr sink)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.lab.harness import walkforward
from src.lab.registry import VARIANTS, get_variant
from src.scraper import close_connection, get_connection


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--variants", type=str, default="prod_plat",
        help="Comma-separated list of variant names. Default: prod_plat (baseline).",
    )
    parser.add_argument("--min-train-days", type=int, default=30)
    args = parser.parse_args()

    names = [n.strip() for n in args.variants.split(",") if n.strip()]
    try:
        variants = [get_variant(n) for n in names]
    except KeyError as exc:
        print(f"ERROR: {exc}")
        print(f"Registered: {sorted(VARIANTS)}")
        return 1

    conn = get_connection()
    try:
        summary = walkforward(conn, variants, min_train_days=args.min_train_days)
    finally:
        close_connection()

    print()
    print("=== Lab walk-forward summary ===")

    def _fmt(x):
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x)

    print(summary.to_string(index=False, formatters={c: _fmt for c in summary.select_dtypes("number").columns}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
