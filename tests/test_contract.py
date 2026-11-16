import json
from datetime import date
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from data_pipeline import contract
from data_pipeline.metrics_spec import load_metrics
from data_pipeline.paths import PROJECT_ROOT

SCHEMAS = (*contract.TABLES, contract.QUALITY_REPORT)
DIMENSIONS = {
    "daily_metrics": [
        "metric_date",
        "category",
        "distribution_center_id",
        "distribution_center_name",
    ],
    "daily_totals": ["metric_date"],
}


def daily_metrics_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "metric_date": "2026-01-05",
        "category": "Jeans",
        "distribution_center_id": 1,
        "distribution_center_name": "Northgate",
        "revenue": 120.5,
        "orders": 2,
        "units": 3,
        "returned_units": 1,
        "aov": 60.25,
        "return_rate": 1 / 3,
        "product_view_sessions": 4,
        "converted_sessions": 2,
        "conversion_rate": 0.5,
    }
    return row | overrides


def run_metadata_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "run_id": "r1",
        "contract_version": "v1",
        "pipeline_version": "0.1.0",
        "source": "fixtures",
        "generated_at": "2026-01-31T00:00:00Z",
        "data_min_date": str(date(2026, 1, 1)),
        "data_max_date": "2026-01-30",
        "restated_from": "2026-01-17",
        "quality_status": "pass",
    }
    return row | overrides


@pytest.mark.parametrize("name", SCHEMAS)
def test_every_schema_file_is_valid_draft_2020_12(name: str) -> None:
    Draft202012Validator.check_schema(contract.load_schema(name))


def test_the_current_version_has_a_pinned_checksum() -> None:
    pinned = json.loads(contract.CHECKSUMS_FILE.read_text())
    assert contract.CONTRACT_VERSION in pinned


def test_every_version_directory_is_pinned() -> None:
    pinned = json.loads(contract.CHECKSUMS_FILE.read_text())
    assert set(pinned) == {p.name for p in contract.CONTRACT_ROOT.iterdir() if p.is_dir()}


def test_a_published_version_still_matches_its_pinned_checksum() -> None:
    pinned = json.loads(contract.CHECKSUMS_FILE.read_text())
    for version, sha in pinned.items():
        assert contract.checksum(version) == sha, (
            f"contract/{version}/ changed. Published versions are frozen: "
            "put the change in a new version directory and pin its checksum."
        )


@pytest.mark.parametrize("table", contract.TABLES)
def test_every_table_column_is_required(table: str) -> None:
    schema = contract.load_schema(table)
    assert set(schema["required"]) == set(schema["properties"])


@pytest.mark.parametrize("table", contract.TABLES)
def test_every_table_column_has_a_duckdb_type(table: str) -> None:
    assert all(contract.columns(table).values())


@pytest.mark.parametrize("table", DIMENSIONS)
def test_metric_columns_match_metrics_yaml(table: str) -> None:
    metric_columns = [c for c in contract.columns(table) if c not in DIMENSIONS[table]]
    assert sorted(metric_columns) == sorted(load_metrics())


def test_the_contract_doc_names_every_table_and_column() -> None:
    doc = (PROJECT_ROOT / "docs" / "contract.md").read_text()
    for table in contract.TABLES:
        assert f"`{table}`" in doc
        for column in contract.columns(table):
            assert f"`{column}`" in doc, f"{table}.{column} missing from docs/contract.md"


def test_a_valid_row_has_no_violations() -> None:
    assert contract.validate("daily_metrics", [daily_metrics_row()]) == []


@pytest.mark.parametrize(
    ("overrides", "fragment"),
    [
        ({"revenue": -1.0}, "revenue"),
        ({"metric_date": "05/01/2026"}, "metric_date"),
        ({"return_rate": 1.5}, "return_rate"),
        ({"orders": 2.5}, "orders"),
        ({"category": None}, "category"),
        ({"surprise": 1}, "Additional properties"),
    ],
)
def test_an_invalid_row_is_reported(overrides: dict[str, Any], fragment: str) -> None:
    errors = contract.validate("daily_metrics", [daily_metrics_row(**overrides)])
    assert errors and fragment in errors[0]


def test_a_missing_column_is_reported() -> None:
    row = daily_metrics_row()
    del row["aov"]
    assert any(
        "'aov' is a required property" in e for e in contract.validate("daily_metrics", [row])
    )


def test_ratio_columns_accept_null() -> None:
    row = daily_metrics_row(orders=0, aov=None, units=0, return_rate=None)
    assert contract.validate("daily_metrics", [row]) == []


def test_metric_definitions_rows_built_from_metrics_yaml_are_valid() -> None:
    rows = [
        {
            "metric_name": m.name,
            "label": m.label,
            "description": m.description,
            "unit": m.unit,
            "aggregation": m.agg,
            "formula": m.formula,
            "additive": m.additive,
        }
        for m in load_metrics().values()
    ]
    assert contract.validate("metric_definitions", rows) == []


def test_a_passing_run_metadata_row_is_valid() -> None:
    assert contract.validate("run_metadata", [run_metadata_row()]) == []


def test_run_metadata_rejects_a_failed_quality_status() -> None:
    assert contract.validate("run_metadata", [run_metadata_row(quality_status="fail")])


def test_run_metadata_rejects_another_contract_version() -> None:
    assert contract.validate("run_metadata", [run_metadata_row(contract_version="v2")])
