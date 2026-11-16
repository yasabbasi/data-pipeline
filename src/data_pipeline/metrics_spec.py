"""Load and validate metric definitions from metrics.yaml."""

import re
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from pydantic_core import ErrorDetails

from data_pipeline.paths import PROJECT_ROOT

METRICS_FILE = PROJECT_ROOT / "metrics.yaml"
SOURCES = ("sold_items", "product_views")
AGGS = ("sum", "count", "count_distinct", "ratio")
UNITS = ("usd", "count", "ratio")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class MetricSpecError(ValueError):
    """A metrics file that breaks the rules for metric definitions."""


class Metric(BaseModel):
    """One metric definition from metrics.yaml."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    name: str
    label: str
    description: str
    unit: str
    agg: str
    additive: bool
    source: str | None = None
    expr: str | None = None
    filter: str | None = None
    numerator: str | None = None
    denominator: str | None = None

    @property
    def formula(self) -> str:
        """Human-readable definition, published in the metric_definitions table."""
        if self.agg == "ratio":
            return f"{self.numerator} / NULLIF({self.denominator}, 0)"
        call = {
            "sum": f"SUM({self.expr})",
            "count": "COUNT(*)",
            "count_distinct": f"COUNT(DISTINCT {self.expr})",
        }[self.agg]
        where = f" WHERE {self.filter}" if self.filter else ""
        return f"{call} FROM {self.source}{where}"

    @model_validator(mode="after")
    def _check_fields_fit_the_aggregation(self) -> Self:
        if not _NAME.fullmatch(self.name):
            raise ValueError("name must be lowercase snake_case")
        if self.agg not in AGGS:
            raise ValueError(f"agg must be one of {AGGS}")
        if self.unit not in UNITS:
            raise ValueError(f"unit must be one of {UNITS}")
        if self.agg == "ratio":
            if self.source or self.expr or self.filter:
                raise ValueError("ratio metrics take only numerator and denominator")
            if self.additive:
                raise ValueError("ratio metrics are never additive")
            return self
        if self.numerator or self.denominator:
            raise ValueError("only ratio metrics take numerator/denominator")
        if self.source not in SOURCES:
            raise ValueError(f"source must be one of {SOURCES}")
        if (self.agg == "count") == bool(self.expr):
            raise ValueError("count takes no expr; sum and count_distinct require one")
        if self.agg == "count_distinct" and self.additive:
            raise ValueError("count_distinct metrics are not additive across slices")
        return self


def load_metrics(path: Path = METRICS_FILE) -> dict[str, Metric]:
    """Read the metric definitions in file order, keyed by metric name."""
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict) or not isinstance(raw.get("metrics"), dict):
        raise MetricSpecError(f"{path}: expected a top-level 'metrics' mapping")
    metrics: dict[str, Metric] = {}
    for name, fields in raw["metrics"].items():
        try:
            metrics[name] = Metric(name=name, **fields)
        except ValidationError as exc:
            problems = "; ".join(_problem(error) for error in exc.errors())
            raise MetricSpecError(f"metric {name!r}: {problems}") from exc
        except TypeError as exc:
            raise MetricSpecError(f"metric {name!r}: {exc}") from exc
    for metric in metrics.values():
        _check_ratio_references(metric, metrics)
    return metrics


def _problem(error: ErrorDetails) -> str:
    field = ".".join(str(part) for part in error["loc"])
    match error["type"]:
        case "extra_forbidden":
            return f"unexpected keyword {field!r}"
        case "missing":
            return f"missing required keyword {field!r}"
        case "value_error":
            return str(error["ctx"]["error"])
        case _:
            return f"{field}: {error['msg']}"


def _check_ratio_references(metric: Metric, metrics: dict[str, Metric]) -> None:
    if metric.agg != "ratio":
        return
    for ref in (metric.numerator, metric.denominator):
        if ref is None or ref not in metrics or metrics[ref].agg == "ratio":
            raise MetricSpecError(f"metric {metric.name!r}: {ref!r} must name a non-ratio metric")
