import json
from datetime import date
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from data_pipeline import contract
from data_pipeline.metrics_spec import load_metrics
from data_pipeline.paths import PROJECT_ROOT

SCHEMAS = (*contract.TABLES, contract.QUALITY_REPORT)
V2_NEW_TABLES = ("daily_by_category", "daily_by_distribution_center")
TABLES_BY_VERSION = {"v1": contract.TABLES, "v2": (*contract.TABLES, *V2_NEW_TABLES)}
VERSIONED_SCHEMAS = [
    (version, name)
    for version, tables in TABLES_BY_VERSION.items()
    for name in (*tables, contract.QUALITY_REPORT)
]
VERSIONED_TABLES = [
    (version, table) for version, tables in TABLES_BY_VERSION.items() for table in tables
]
DIMENSIONS = {
    "daily_metrics": [
        "metric_date",
        "category",
        "distribution_center_id",
        "distribution_center_name",
    ],
    "daily_totals": ["metric_date"],
}
V2_DIMENSIONS = DIMENSIONS | {
    "daily_by_category": ["metric_date", "category"],
    "daily_by_distribution_center": [
        "metric_date",
        "distribution_center_id",
        "distribution_center_name",
    ],
}
V2_GRAINS = {
    "daily_by_category": ["metric_date", "category"],
    "daily_by_distribution_center": ["metric_date", "distribution_center_id"],
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


@pytest.mark.parametrize(("version", "name"), VERSIONED_SCHEMAS)
def test_every_schema_file_is_valid_draft_2020_12(version: str, name: str) -> None:
    Draft202012Validator.check_schema(contract.load_schema(name, version))


def test_the_pipeline_still_publishes_v1() -> None:
    assert contract.CONTRACT_VERSION == "v1"


@pytest.mark.parametrize("name", SCHEMAS)
def test_v2_keeps_each_v1_schema_with_only_the_version_changed(name: str) -> None:
    v1 = json.dumps(contract.load_schema(name, "v1"))
    expected = v1.replace(":contract:v1:", ":contract:v2:").replace(
        '"const": "v1"', '"const": "v2"'
    )
    assert json.dumps(contract.load_schema(name, "v2")) == expected


@pytest.mark.parametrize("table", V2_NEW_TABLES)
def test_v2_slice_tables_have_their_grain(table: str) -> None:
    assert contract.load_schema(table, "v2")["x-grain"] == V2_GRAINS[table]


@pytest.mark.parametrize("table", V2_NEW_TABLES)
def test_v2_slice_tables_type_metrics_as_daily_metrics(table: str) -> None:
    daily = contract.load_schema("daily_metrics", "v2")["properties"]
    properties = contract.load_schema(table, "v2")["properties"]
    for metric in load_metrics():
        for key in ("type", "minimum", "maximum", "x-duckdb-type"):
            assert properties[metric].get(key) == daily[metric].get(key), f"{metric}.{key}"


@pytest.mark.parametrize("table", V2_NEW_TABLES)
@pytest.mark.parametrize("metric", ["orders", "product_view_sessions", "converted_sessions"])
def test_v2_distinct_counts_are_described_as_not_additive(table: str, metric: str) -> None:
    description = contract.load_schema(table, "v2")["properties"][metric]["description"]
    assert "Distinct" in description or "distinct" in description
    assert "Not additive across" in description


def test_v2_run_metadata_requires_contract_version_v2() -> None:
    row = run_metadata_row(contract_version="v2")
    assert contract.validate("run_metadata", [row], "v2") == []
    assert contract.validate("run_metadata", [run_metadata_row()], "v2")


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


@pytest.mark.parametrize(("version", "table"), VERSIONED_TABLES)
def test_every_table_column_is_required(version: str, table: str) -> None:
    schema = contract.load_schema(table, version)
    assert set(schema["required"]) == set(schema["properties"])


@pytest.mark.parametrize(("version", "table"), VERSIONED_TABLES)
def test_every_table_column_has_a_duckdb_type(version: str, table: str) -> None:
    assert all(contract.columns(table, version).values())


@pytest.mark.parametrize(
    ("version", "table"),
    [("v1", t) for t in DIMENSIONS] + [("v2", t) for t in V2_DIMENSIONS],
)
def test_metric_columns_match_metrics_yaml(version: str, table: str) -> None:
    dimensions = V2_DIMENSIONS[table]
    metric_columns = [c for c in contract.columns(table, version) if c not in dimensions]
    assert sorted(metric_columns) == sorted(load_metrics())


@pytest.mark.parametrize(("version", "table"), VERSIONED_TABLES)
def test_the_contract_doc_names_every_table_and_column(version: str, table: str) -> None:
    doc = (PROJECT_ROOT / "docs" / "contract.md").read_text()
    assert f"`{table}`" in doc
    for column in contract.columns(table, version):
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
