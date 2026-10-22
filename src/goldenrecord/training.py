"""Refit match-scoring weights from labelled pairs (the steward learning loop).

A small L2-regularised logistic regression fitted with Newton's method on the feature
columns. It runs on the driver (labelled sets are hundreds to thousands of rows), needs
only numpy, and returns weights in the same shape as ``matching.weights`` in config so a
reviewer can diff and promote them.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from goldenrecord.features import FEATURE_COLUMNS


@dataclass(frozen=True)
class FittedWeights:
    intercept: float
    weights: dict[str, float]
    iterations: int
    log_loss: float


def fit_logistic(
    x: np.ndarray, y: np.ndarray, l2: float = 1.0, max_iter: int = 50, tol: float = 1e-8
) -> tuple[np.ndarray, int]:
    """Newton-Raphson for L2-regularised logistic regression. ``x`` must include a bias column."""
    n_features = x.shape[1]
    beta = np.zeros(n_features)
    penalty = np.eye(n_features) * l2
    penalty[0, 0] = 0.0  # do not regularise the intercept
    iteration = 0
    for iteration in range(1, max_iter + 1):  # noqa: B007 - iteration count is returned
        p = 1.0 / (1.0 + np.exp(-x @ beta))
        gradient = x.T @ (y - p) - penalty @ beta
        hessian = (x.T * (p * (1 - p))) @ x + penalty
        step = np.linalg.solve(hessian, gradient)
        beta = beta + step
        if float(np.max(np.abs(step))) < tol:
            break
    return beta, iteration


def fit_weights(rows: Sequence[dict[str, float | bool | None]], l2: float = 1.0) -> FittedWeights:
    """``rows`` are dicts with every feature column plus ``is_match``."""
    if len(rows) < 20:
        raise ValueError("Need at least 20 labelled pairs to refit weights")
    features = list(FEATURE_COLUMNS)
    x = np.array([[1.0] + [float(r.get(f) or 0.0) for f in features] for r in rows])
    y = np.array([1.0 if r["is_match"] else 0.0 for r in rows])
    beta, iterations = fit_logistic(x, y, l2=l2)
    p = np.clip(1.0 / (1.0 + np.exp(-x @ beta)), 1e-9, 1 - 1e-9)
    loss = float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))
    return FittedWeights(
        intercept=round(float(beta[0]), 4),
        weights={f: round(float(b), 4) for f, b in zip(features, beta[1:], strict=True)},
        iterations=iterations,
        log_loss=round(loss, 5),
    )
