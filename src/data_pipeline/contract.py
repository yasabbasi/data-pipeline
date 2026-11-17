"""The versioned output contract: JSON Schema files under contract/<version>/."""

import hashlib
import json
from collections.abc import Iterable
from functools import cache
from typing import Any

from jsonschema import Draft202012Validator

from data_pipeline.paths import PROJECT_ROOT

CONTRACT_VERSION = "v2"
CONTRACT_ROOT = PROJECT_ROOT / "contract"
CHECKSUMS_FILE = CONTRACT_ROOT / "checksums.json"
TABLES = (
    "daily_metrics",
    "daily_totals",
    "metric_definitions",
    "run_metadata",
    "daily_by_category",
    "daily_by_distribution_center",
)
QUALITY_REPORT = "quality_report"


@cache
def load_schema(name: str, version: str = CONTRACT_VERSION) -> dict[str, Any]:
    """Return the JSON Schema of one table or of the quality report."""
    path = CONTRACT_ROOT / version / f"{name.replace('_', '-')}.schema.json"
    schema: dict[str, Any] = json.loads(path.read_text())
    return schema


def columns(table: str, version: str = CONTRACT_VERSION) -> dict[str, str]:
    """Return each column's DuckDB type by column name, in contract order."""
    properties = load_schema(table, version)["properties"]
    return {name: spec["x-duckdb-type"] for name, spec in properties.items()}


def validate(
    name: str, records: Iterable[dict[str, Any]], version: str = CONTRACT_VERSION
) -> list[str]:
    """Return the violations found in records, empty when all are valid; dates are ISO strings."""
    validator = Draft202012Validator(
        load_schema(name, version), format_checker=Draft202012Validator.FORMAT_CHECKER
    )
    errors: list[str] = []
    for i, record in enumerate(records):
        for error in validator.iter_errors(record):
            where = "/".join(str(part) for part in error.absolute_path) or "<record>"
            errors.append(f"{name}[{i}] {where}: {error.message}")
    return errors


def checksum(version: str = CONTRACT_VERSION) -> str:
    """Return the SHA-256 of a version's schema files, by sorted name and exact bytes."""
    digest = hashlib.sha256()
    for path in sorted((CONTRACT_ROOT / version).glob("*.schema.json")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()
