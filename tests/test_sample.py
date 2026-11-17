import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from data_pipeline import contract
from data_pipeline.publish import TYPE_ALIASES
from data_pipeline.sample import RUN_ID, SAMPLE_DIR, SAMPLE_FILES, build_sample

REPORT_FILE = "quality_report.json"
SAMPLES = SAMPLE_DIR.parent
PINNED: dict[str, dict[str, str]] = json.loads((SAMPLES / "checksums.json").read_text())


@pytest.fixture(scope="module")
def rebuilt(tmp_path_factory: pytest.TempPathFactory) -> Path:
    dest = tmp_path_factory.mktemp("sample")
    build_sample(dest)
    return dest


@pytest.fixture(scope="module")
def report() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((SAMPLE_DIR / REPORT_FILE).read_text())
    return loaded


def parquet_file(directory: Path, table: str) -> Path:
    return directory / "parquet" / f"{table}.parquet"


def differing_rows(table: str, committed: Path, fresh: Path) -> int:
    columns = ", ".join(contract.columns(table))
    with duckdb.connect() as con:
        count: int = con.execute(
            f"""
            with committed as (
                select {columns}
                from read_parquet($committed)
            ),
            fresh as (
                select {columns}
                from read_parquet($fresh)
            ),
            only_committed as (
                select {columns}
                from committed
                except all
                select {columns}
                from fresh
            ),
            only_fresh as (
                select {columns}
                from fresh
                except all
                select {columns}
                from committed
            ),
            differences as (
                select {columns}
                from only_committed
                union all
                select {columns}
                from only_fresh
            )
            select count(*)
            from differences
            """,
            {"committed": str(committed), "fresh": str(fresh)},
        ).fetchall()[0][0]
    return count


def records(table: str) -> list[dict[str, Any]]:
    # TIMESTAMPTZ is read as an ISO string, the shape the schema expects.
    names = list(contract.columns(table))
    select = ", ".join(
        f"strftime({name} at time zone 'UTC', '%Y-%m-%dT%H:%M:%SZ') as {name}"
        if typ == "TIMESTAMPTZ"
        else name
        for name, typ in contract.columns(table).items()
    )
    with duckdb.connect() as con:
        rows = con.execute(
            f"select {select} from read_parquet(?)", [str(parquet_file(SAMPLE_DIR, table))]
        ).fetchall()
    return [
        {n: v.isoformat() if isinstance(v, date) else v for n, v in zip(names, row, strict=True)}
        for row in rows
    ]


@pytest.mark.parametrize("name", SAMPLE_FILES)
def test_every_sample_file_is_committed(name: str) -> None:
    assert (SAMPLE_DIR / name).exists(), f"{name} missing: run `make sample`"


@pytest.mark.parametrize("table", contract.TABLES)
def test_committed_sample_tables_are_up_to_date(table: str, rebuilt: Path) -> None:
    committed, fresh = parquet_file(SAMPLE_DIR, table), parquet_file(rebuilt, table)
    assert differing_rows(table, committed, fresh) == 0, f"{table}: run `make sample`"


def test_committed_sample_quality_report_is_up_to_date(
    report: dict[str, Any], rebuilt: Path
) -> None:
    assert report == json.loads((rebuilt / REPORT_FILE).read_text()), "run `make sample`"


def test_the_sample_has_a_readme_for_its_version() -> None:
    assert f"contract {contract.CONTRACT_VERSION}" in (SAMPLE_DIR / "README.md").read_text()


def test_the_sample_run_metadata_has_the_sample_pipeline_version() -> None:
    assert [r["pipeline_version"] for r in records("run_metadata")] == ["2.0.0"]


def test_every_sample_version_is_pinned() -> None:
    assert set(PINNED) == {p.name for p in SAMPLES.iterdir() if p.is_dir()}


def test_the_current_version_pins_every_sample_file() -> None:
    assert set(PINNED[contract.CONTRACT_VERSION]) == set(SAMPLE_FILES)


@pytest.mark.parametrize("version", sorted(PINNED))
def test_a_published_sample_still_matches_its_pinned_checksums(version: str) -> None:
    actual = {
        name: hashlib.sha256((SAMPLES / version / name).read_bytes()).hexdigest()
        for name in PINNED[version]
    }
    assert actual == PINNED[version], (
        f"samples/{version}/ changed. Published samples are frozen: "
        "a change to the outputs is a new contract version with its own sample."
    )


@pytest.mark.parametrize("table", contract.TABLES)
def test_sample_tables_have_the_contract_columns_and_types_in_order(table: str) -> None:
    with duckdb.connect() as con:
        sample = con.read_parquet(str(parquet_file(SAMPLE_DIR, table)))
        actual = list(zip(sample.columns, map(str, sample.types), strict=True))
    expected = [(c, TYPE_ALIASES.get(t, t)) for c, t in contract.columns(table).items()]
    assert actual == expected


@pytest.mark.parametrize("table", contract.TABLES)
def test_sample_rows_match_the_contract(table: str) -> None:
    rows = records(table)
    assert rows
    assert contract.validate(table, rows) == []


def test_sample_quality_report_matches_the_contract(report: dict[str, Any]) -> None:
    assert contract.validate(contract.QUALITY_REPORT, [report]) == []


def test_sample_quality_report_is_a_passing_run_with_the_sample_run_id(
    report: dict[str, Any],
) -> None:
    assert report["run_id"] == RUN_ID
    assert report["status"] == "pass"
