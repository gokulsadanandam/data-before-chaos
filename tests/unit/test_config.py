"""Configuration loading and validation."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from goldenrecord.config import ConfigError, load_settings
from goldenrecord.features import FEATURE_COLUMNS
from goldenrecord.schema import GOLDEN_ATTRIBUTES
from tests.conftest import CONFIG_DIR


def _copy_config(tmp_path: Path) -> Path:
    target = tmp_path / "config"
    shutil.copytree(CONFIG_DIR, target)
    return target


def _edit_yaml(path: Path, edit) -> None:  # type: ignore[no-untyped-def]
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    edit(data)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def test_repository_config_loads(settings) -> None:  # type: ignore[no-untyped-def]
    assert {s.name for s in settings.sources} == {"erp_a", "erp_b", "crm", "web", "pos_loyalty", "mobile"}
    assert settings.engagement is not None
    assert settings.master_key_prefix == "C-"
    assert [r.id for r in settings.dq_rules] == ["DQ01", "DQ02", "DQ03", "DQ04", "DQ05", "DQ06"]


def test_every_golden_attribute_has_a_survivorship_rule(settings) -> None:  # type: ignore[no-untyped-def]
    assert {r.attribute for r in settings.survivorship} == set(GOLDEN_ATTRIBUTES)


def test_every_weight_is_a_known_feature(settings) -> None:  # type: ignore[no-untyped-def]
    assert set(settings.matching.weights) <= set(FEATURE_COLUMNS)


def test_consent_uses_most_restrictive(settings) -> None:  # type: ignore[no-untyped-def]
    rules = {r.attribute: r.strategy for r in settings.survivorship}
    assert rules["consent_email"] == rules["consent_sms"] == rules["consent_whatsapp"] == "most_restrictive"


def test_invalid_bands_are_rejected(tmp_path: Path) -> None:
    cfg = _copy_config(tmp_path)
    _edit_yaml(cfg / "pipeline.yaml", lambda d: d["matching"]["bands"].update(high=0.7, medium=0.8))
    with pytest.raises(ConfigError, match="bands"):
        load_settings(cfg)


def test_unknown_mapping_attribute_is_rejected(tmp_path: Path) -> None:
    cfg = _copy_config(tmp_path)
    _edit_yaml(cfg / "sources.yaml", lambda d: d["sources"]["crm"]["mapping"].update(shoe_size="Shoe"))
    with pytest.raises(ConfigError):
        load_settings(cfg)


def test_unknown_survivorship_strategy_is_rejected(tmp_path: Path) -> None:
    cfg = _copy_config(tmp_path)
    _edit_yaml(cfg / "survivorship.yaml", lambda d: d["attributes"]["email"].update(strategy="coin_flip"))
    with pytest.raises(ConfigError):
        load_settings(cfg)


def test_missing_file_is_reported(tmp_path: Path) -> None:
    cfg = _copy_config(tmp_path)
    (cfg / "dq_rules.yaml").unlink()
    with pytest.raises(ConfigError, match=r"dq_rules\.yaml"):
        load_settings(cfg)
