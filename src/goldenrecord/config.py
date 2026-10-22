"""Typed configuration loading.

All tunable values live in YAML under ``config/``; reference lists live in CSV under
``config/reference/``. This module validates them once at start-up so a bad config fails
fast with a clear message instead of deep inside a Spark job.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from goldenrecord.schema import GOLDEN_ATTRIBUTES, SOURCE_ATTRIBUTES


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class OrderSpec:
    path: str
    id_column: str
    customer_column: str
    date_column: str
    amount_column: str
    channel: str


@dataclass(frozen=True)
class SourceSpec:
    name: str
    description: str
    format: str
    path: str
    options: dict[str, str]
    trust: int
    id_column: str
    updated_at_column: str | None
    mapping: dict[str, str]
    orders: OrderSpec | None = None


@dataclass(frozen=True)
class EngagementSpec:
    format: str
    path: str
    options: dict[str, str]


@dataclass(frozen=True)
class SurvivorshipRule:
    attribute: str
    strategy: str
    source: str | None = None
    fallback: str | None = None


@dataclass(frozen=True)
class DQRule:
    id: str
    name: str
    check: str
    threshold: float
    blocking: bool
    column: str | None = None
    columns: tuple[str, ...] = ()
    any_of: tuple[str, ...] = ()
    pattern: str | None = None
    allow_null: bool = False
    reference: str | None = None


@dataclass(frozen=True)
class ReferenceData:
    country_aliases: dict[str, str]
    valid_countries: frozenset[str]
    calling_codes: dict[str, str]
    email_domain_typos: dict[str, str]
    placeholder_email_locals: frozenset[str]
    placeholder_email_domains: frozenset[str]
    placeholder_phones: frozenset[str]
    placeholder_names: frozenset[str]
    nicknames: dict[str, str]


@dataclass(frozen=True)
class MatchingConfig:
    intercept: float
    weights: dict[str, float]
    min_candidate_score: float
    band_high: float
    band_medium: float
    spot_check_rate: float
    ai_enabled: bool
    ai_lower: float
    ai_upper: float
    ai_blend_weight: float
    ai_labels: tuple[str, str]
    auto_merge_requires_identifier: bool = True


@dataclass(frozen=True)
class Settings:
    config_dir: Path
    raw: dict[str, Any]
    entity: str
    master_key_prefix: str
    master_key_digits: int
    tables: dict[str, str]
    sources: tuple[SourceSpec, ...]
    engagement: EngagementSpec | None
    survivorship: tuple[SurvivorshipRule, ...]
    dq_rules: tuple[DQRule, ...]
    matching: MatchingConfig
    reference: ReferenceData
    extras: dict[str, Any] = field(default_factory=dict)

    def table(self, key: str) -> str:
        try:
            return self.tables[key]
        except KeyError as exc:  # pragma: no cover - guarded by validation
            raise ConfigError(f"Unknown table key '{key}' in config/pipeline.yaml") from exc

    def section(self, name: str) -> dict[str, Any]:
        value = self.raw.get(name, {})
        if not isinstance(value, dict):
            raise ConfigError(f"Section '{name}' must be a mapping")
        return value

    def source(self, name: str) -> SourceSpec:
        for spec in self.sources:
            if spec.name == name:
                return spec
        raise ConfigError(f"Unknown source '{name}'")

    @property
    def source_trust(self) -> dict[str, int]:
        return {s.name: s.trust for s in self.sources}


VALID_STRATEGIES = frozenset(
    {
        "most_trusted",
        "most_recent",
        "most_frequent",
        "longest_complete",
        "source_of_record",
        "most_restrictive",
    }
)
VALID_CHECKS = frozenset({"not_null", "regex", "unique", "in_set", "mandatory"})


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Missing config file: {path}")
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return data


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ConfigError(f"Missing reference file: {path}")
    with path.open(encoding="utf-8", newline="") as handle:
        return [{k.strip(): (v or "").strip() for k, v in row.items()} for row in csv.DictReader(handle)]


def load_reference(config_dir: Path) -> ReferenceData:
    ref = config_dir / "reference"
    countries = _read_csv(ref / "countries.csv")
    placeholders = _read_csv(ref / "placeholders.csv")

    def by_kind(kind: str) -> frozenset[str]:
        return frozenset(r["value"].lower() for r in placeholders if r["kind"] == kind)

    return ReferenceData(
        country_aliases={r["alias"].lower(): r["iso2"].upper() for r in countries},
        valid_countries=frozenset(r["iso2"].upper() for r in countries),
        calling_codes={r["iso2"].upper(): r["calling_code"] for r in _read_csv(ref / "calling_codes.csv")},
        email_domain_typos={
            r["typo"].lower(): r["correct"].lower() for r in _read_csv(ref / "email_domain_typos.csv")
        },
        placeholder_email_locals=by_kind("email_local"),
        placeholder_email_domains=by_kind("email_domain"),
        placeholder_phones=by_kind("phone"),
        placeholder_names=by_kind("name"),
        nicknames={r["nickname"].lower(): r["canonical"].lower() for r in _read_csv(ref / "nicknames.csv")},
    )


def _parse_sources(data: dict[str, Any]) -> tuple[tuple[SourceSpec, ...], EngagementSpec | None]:
    raw_sources = data.get("sources")
    if not isinstance(raw_sources, dict) or not raw_sources:
        raise ConfigError("config/sources.yaml must define at least one source under 'sources'")
    specs: list[SourceSpec] = []
    for name, body in raw_sources.items():
        mapping = dict(body.get("mapping") or {})
        unknown = set(mapping) - set(SOURCE_ATTRIBUTES)
        if unknown:
            raise ConfigError(f"Source '{name}' maps unknown attributes: {sorted(unknown)}")
        if not ({"full_name"} & set(mapping) or {"last_name"} & set(mapping)):
            raise ConfigError(f"Source '{name}' must map full_name or last_name")
        if body.get("format") not in {"csv", "json", "parquet"}:
            raise ConfigError(f"Source '{name}' has unsupported format {body.get('format')!r}")
        orders_body = body.get("orders")
        orders = OrderSpec(**orders_body) if orders_body else None
        specs.append(
            SourceSpec(
                name=name,
                description=str(body.get("description", "")),
                format=str(body["format"]),
                path=str(body["path"]),
                options={k: str(v) for k, v in (body.get("options") or {}).items()},
                trust=int(body.get("trust", 50)),
                id_column=str(body["id_column"]),
                updated_at_column=body.get("updated_at_column"),
                mapping=mapping,
                orders=orders,
            )
        )
    eng = data.get("engagement")
    engagement = (
        EngagementSpec(
            format=str(eng["format"]),
            path=str(eng["path"]),
            options={k: str(v) for k, v in (eng.get("options") or {}).items()},
        )
        if eng
        else None
    )
    return tuple(specs), engagement


def _parse_survivorship(data: dict[str, Any], source_names: set[str]) -> tuple[SurvivorshipRule, ...]:
    attrs = data.get("attributes") or {}
    rules: list[SurvivorshipRule] = []
    for attribute in GOLDEN_ATTRIBUTES:
        body = attrs.get(attribute)
        if body is None:
            raise ConfigError(f"survivorship.yaml has no rule for golden attribute '{attribute}'")
        strategy = body.get("strategy")
        if strategy not in VALID_STRATEGIES:
            raise ConfigError(f"Unknown survivorship strategy {strategy!r} for '{attribute}'")
        if strategy == "source_of_record" and body.get("source") not in source_names:
            raise ConfigError(f"'{attribute}' names unknown source {body.get('source')!r}")
        rules.append(
            SurvivorshipRule(
                attribute=attribute,
                strategy=strategy,
                source=body.get("source"),
                fallback=body.get("fallback"),
            )
        )
    return tuple(rules)


def _parse_dq_rules(data: dict[str, Any]) -> tuple[DQRule, ...]:
    rules: list[DQRule] = []
    for body in data.get("rules") or []:
        if body.get("check") not in VALID_CHECKS:
            raise ConfigError(f"DQ rule {body.get('id')} has unknown check {body.get('check')!r}")
        rules.append(
            DQRule(
                id=str(body["id"]),
                name=str(body["name"]),
                check=str(body["check"]),
                threshold=float(body["threshold"]),
                blocking=bool(body.get("blocking", False)),
                column=body.get("column"),
                columns=tuple(body.get("columns") or ()),
                any_of=tuple(body.get("any_of") or ()),
                pattern=body.get("pattern"),
                allow_null=bool(body.get("allow_null", False)),
                reference=body.get("reference"),
            )
        )
    if not rules:
        raise ConfigError("dq_rules.yaml must define at least one rule")
    return tuple(rules)


def _parse_matching(section: dict[str, Any]) -> MatchingConfig:
    bands = section.get("bands") or {}
    ai = section.get("ai") or {}
    high, medium = float(bands.get("high", 0.9)), float(bands.get("medium", 0.75))
    if not 0 < medium < high <= 1:
        raise ConfigError("matching.bands must satisfy 0 < medium < high <= 1")
    labels = tuple(ai.get("labels") or ("same person", "different person"))
    if len(labels) != 2:
        raise ConfigError("matching.ai.labels must contain exactly two labels (match first)")
    return MatchingConfig(
        intercept=float(section.get("intercept", -4.0)),
        weights={k: float(v) for k, v in (section.get("weights") or {}).items()},
        min_candidate_score=float(section.get("min_candidate_score", 0.4)),
        band_high=high,
        band_medium=medium,
        spot_check_rate=float(section.get("spot_check_rate", 0.1)),
        ai_enabled=bool(ai.get("enabled", True)),
        ai_lower=float(ai.get("lower", 0.5)),
        ai_upper=float(ai.get("upper", high)),
        ai_blend_weight=float(ai.get("blend_weight", 0.3)),
        ai_labels=(str(labels[0]), str(labels[1])),
        auto_merge_requires_identifier=bool(section.get("auto_merge_requires_identifier", True)),
    )


def load_settings(config_dir: str | Path) -> Settings:
    """Load and validate every configuration file under ``config_dir``."""
    root = Path(config_dir)
    pipeline = _read_yaml(root / "pipeline.yaml")
    sources, engagement = _parse_sources(_read_yaml(root / "sources.yaml"))
    survivorship = _parse_survivorship(_read_yaml(root / "survivorship.yaml"), {s.name for s in sources})
    dq_rules = _parse_dq_rules(_read_yaml(root / "dq_rules.yaml"))
    tables = pipeline.get("tables") or {}
    if not isinstance(tables, dict) or not tables:
        raise ConfigError("pipeline.yaml must define 'tables'")
    return Settings(
        config_dir=root,
        raw=pipeline,
        entity=str(pipeline.get("entity", "customer")),
        master_key_prefix=str(pipeline.get("master_key_prefix", "C-")),
        master_key_digits=int(pipeline.get("master_key_digits", 6)),
        tables={str(k): str(v) for k, v in tables.items()},
        sources=sources,
        engagement=engagement,
        survivorship=survivorship,
        dq_rules=dq_rules,
        matching=_parse_matching(pipeline.get("matching") or {}),
        reference=load_reference(root),
    )
