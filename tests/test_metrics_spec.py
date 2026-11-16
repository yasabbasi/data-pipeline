from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from data_pipeline.metrics_spec import MetricSpecError, load_metrics

BASE: dict[str, dict[str, Any]] = {
    "units": {
        "label": "Units",
        "description": "d",
        "unit": "count",
        "agg": "count",
        "source": "sold_items",
        "additive": True,
    },
    "orders": {
        "label": "Orders",
        "description": "d",
        "unit": "count",
        "agg": "count_distinct",
        "source": "sold_items",
        "expr": "order_id",
        "additive": False,
    },
}


def write_spec(tmp_path: Path, metrics: dict[str, dict[str, Any]]) -> Path:
    path = tmp_path / "metrics.yaml"
    path.write_text(yaml.safe_dump({"metrics": metrics}))
    return path


def test_the_repo_metrics_file_loads() -> None:
    metrics = load_metrics()
    assert {"revenue", "orders", "units", "aov", "return_rate", "conversion_rate"} <= set(metrics)


@pytest.mark.parametrize(
    ("name", "formula"),
    [
        ("revenue", "SUM(sale_price) FROM sold_items"),
        ("returned_units", "COUNT(*) FROM sold_items WHERE returned_at IS NOT NULL"),
        ("aov", "revenue / NULLIF(orders, 0)"),
    ],
)
def test_formula_states_how_the_metric_is_computed(name: str, formula: str) -> None:
    assert load_metrics()[name].formula == formula


@pytest.mark.parametrize(
    ("name", "fields", "message"),
    [
        ("Bad-Name", {}, "snake_case"),
        ("units_sold\n", {}, "snake_case"),
        ("x", {"agg": "median"}, "agg must be"),
        ("x", {"unit": "eur"}, "unit must be"),
        ("x", {"source": "web_logs"}, "source must be"),
        ("x", {"agg": "sum"}, "require one"),
        ("x", {"expr": "id"}, "count takes no expr"),
        ("x", {"agg": "count_distinct", "expr": "id"}, "not additive"),
        ("x", {"colour": "red"}, "unexpected keyword"),
    ],
)
def test_an_invalid_metric_is_rejected(
    tmp_path: Path, name: str, fields: dict[str, Any], message: str
) -> None:
    spec = BASE | {name: BASE["units"] | fields}
    with pytest.raises(MetricSpecError, match=message):
        load_metrics(write_spec(tmp_path, spec))


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"numerator": "units", "denominator": "missing"}, "non-ratio metric"),
        ({"numerator": "units", "denominator": "orders", "additive": True}, "never additive"),
        ({"numerator": "units", "denominator": "orders", "source": "sold_items"}, "only numerator"),
    ],
)
def test_an_invalid_ratio_is_rejected(tmp_path: Path, fields: dict[str, Any], message: str) -> None:
    ratio = {"label": "R", "description": "d", "unit": "ratio", "agg": "ratio", "additive": False}
    with pytest.raises(MetricSpecError, match=message):
        load_metrics(write_spec(tmp_path, BASE | {"r": ratio | fields}))


def test_a_file_without_a_metrics_mapping_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "metrics.yaml"
    path.write_text("revenue: 1\n")
    with pytest.raises(MetricSpecError, match="top-level 'metrics'"):
        load_metrics(path)


def test_a_metric_without_a_required_field_is_rejected(tmp_path: Path) -> None:
    units = {key: value for key, value in BASE["units"].items() if key != "label"}
    with pytest.raises(MetricSpecError, match="metric 'units': missing required keyword 'label'"):
        load_metrics(write_spec(tmp_path, BASE | {"units": units}))


def test_a_field_of_the_wrong_type_is_rejected(tmp_path: Path) -> None:
    spec = BASE | {"units": BASE["units"] | {"additive": "yes"}}
    with pytest.raises(MetricSpecError, match="metric 'units': additive: .*boolean"):
        load_metrics(write_spec(tmp_path, spec))


def test_a_loaded_metric_cannot_be_changed() -> None:
    revenue = load_metrics()["revenue"]
    with pytest.raises(ValidationError, match="frozen"):
        revenue.label = "Sales"
