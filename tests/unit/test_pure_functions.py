"""Pure-Python helpers (no Spark session needed)."""

from __future__ import annotations

import math
import random

import pytest

from goldenrecord.evaluation import EvalResult
from goldenrecord.features import FEATURE_COLUMNS, jaro_winkler
from goldenrecord.metrics import pct
from goldenrecord.quality import RuleResult
from goldenrecord.scoring import sigmoid
from goldenrecord.tokenise import hmac_token
from goldenrecord.training import fit_weights


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ("martha", "marhta", 0.961),  # textbook values
        ("dwayne", "duane", 0.840),
        ("dixon", "dicksonx", 0.813),
        ("priya", "priya", 1.0),
    ],
)
def test_jaro_winkler_reference_values(a: str, b: str, expected: float) -> None:
    result = jaro_winkler(a, b)
    assert result is not None
    assert result == pytest.approx(expected, abs=1e-3)


def test_jaro_winkler_missing_and_disjoint() -> None:
    assert jaro_winkler(None, "x") is None
    assert jaro_winkler("", "x") is None
    assert jaro_winkler("abc", "xyz") == 0.0


def test_hmac_token_is_consistent_and_keyed() -> None:
    a = hmac_token(b"k1", "priya@gmail.com")
    assert a == hmac_token(b"k1", "priya@gmail.com")
    assert a != hmac_token(b"k2", "priya@gmail.com")
    assert a is not None and len(a) == 64 and "priya" not in a
    assert hmac_token(b"k1", None) is None


def test_sigmoid_and_pct() -> None:
    assert sigmoid(0) == 0.5
    assert sigmoid(10) > 0.9999
    assert pct(1, 4) == 25.0
    assert pct(1, 0) == 0.0


def test_eval_result_metrics() -> None:
    r = EvalResult(labelled_pairs=10, true_matches=6, true_positives=5, false_positives=1, false_negatives=1)
    assert r.precision == pytest.approx(5 / 6)
    assert r.recall == pytest.approx(5 / 6)
    assert r.f1 == pytest.approx(5 / 6)
    assert EvalResult(0, 0, 0, 0, 0).precision == 1.0


def test_rule_result_threshold() -> None:
    assert RuleResult("DQ01", "x", 99, 100, 0.99, True).ok
    assert not RuleResult("DQ01", "x", 98, 100, 0.99, True).ok
    assert RuleResult("DQ01", "x", 0, 0, 1.0, True).ok  # empty table passes


def test_fit_weights_learns_the_signal() -> None:
    rng = random.Random(7)
    rows = []
    for _ in range(400):
        is_match = rng.random() < 0.5
        row: dict[str, float | bool | None] = dict.fromkeys(FEATURE_COLUMNS, 0.0)
        row["email_exact"] = 1.0 if is_match and rng.random() < 0.8 else 0.0
        row["last_name_sim"] = rng.uniform(0.5, 1.0) if is_match else rng.uniform(-1.0, 0.3)
        row["is_match"] = is_match
        rows.append(row)
    fitted = fit_weights(rows)
    assert fitted.weights["email_exact"] > 1.0
    assert fitted.weights["last_name_sim"] > 1.0
    assert math.isfinite(fitted.log_loss) and fitted.log_loss < 0.5


def test_fit_weights_needs_enough_labels() -> None:
    with pytest.raises(ValueError, match="at least 20"):
        fit_weights([{"is_match": True}] * 5)
