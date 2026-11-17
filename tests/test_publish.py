import json
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pydantic import ValidationError

from data_pipeline import contract
from data_pipeline.ingest import connect, run_ingest
from data_pipeline.publish import PublishError, PublishResult, RunMetadata, build_and_publish
from data_pipeline.quality import QualityReport
from data_pipeline.readers import FixtureReader
from data_pipeline.sources.thelook import fixtures

AS_OF = fixtures.AS_OF


def published_run(output: Path) -> dict[str, Any]:
    names = [c for c in contract.columns("run_metadata") if c != "generated_at"]
    with duckdb.connect(str(output / "metrics.duckdb"), read_only=True) as out:
        row = out.execute(f"select {', '.join(names)} from run_metadata").fetchall()[0]
    return dict(zip(names, row, strict=True))


def published_status(output: Path) -> str:
    status: str = json.loads((output / "quality_report.json").read_text())["status"]
    return status


@pytest.fixture
def con(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with connect(tmp_path / "warehouse.duckdb") as con:
        run_ingest(con, FixtureReader(), AS_OF)
        yield con


@pytest.fixture
def output(tmp_path: Path) -> Path:
    return tmp_path / "output"


@pytest.fixture
def runs(tmp_path: Path) -> Path:
    return tmp_path / "runs"


@pytest.fixture
def result(con: duckdb.DuckDBPyConnection, output: Path, runs: Path) -> PublishResult:
    return build_and_publish(con, output, runs)


@pytest.fixture
def run_metadata(result: PublishResult) -> RunMetadata:
    assert result.run_metadata is not None
    return result.run_metadata


@pytest.fixture
def failed(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path, result: PublishResult
) -> PublishResult:
    con.execute("delete from raw_products")
    return build_and_publish(con, output, runs)


@pytest.fixture
def second(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path, result: PublishResult
) -> PublishResult:
    run_ingest(con, FixtureReader(), AS_OF + timedelta(days=1))
    return build_and_publish(con, output, runs)


def test_a_run_that_passes_quality_is_published(result: PublishResult) -> None:
    assert result.published and result.report.status == "pass"


def test_the_published_quality_report_is_the_runs_report(
    result: PublishResult, output: Path
) -> None:
    text = (output / "quality_report.json").read_text()
    assert QualityReport.model_validate_json(text) == result.report


def test_published_tables_have_the_contract_columns(result: PublishResult, output: Path) -> None:
    with duckdb.connect(str(output / "metrics.duckdb"), read_only=True) as out:
        for table in contract.TABLES:
            described = out.execute(f"describe {table}").fetchall()
            assert [c[0] for c in described] == list(contract.columns(table))


def test_every_parquet_file_has_the_rows_of_its_table(result: PublishResult, output: Path) -> None:
    with duckdb.connect() as out:
        for table in contract.TABLES:
            parquet = output / "parquet" / f"{table}.parquet"
            n_parquet = out.execute(
                "select count(*) from read_parquet(?)", [str(parquet)]
            ).fetchall()[0][0]
            assert n_parquet == result.row_counts[table] > 0


def test_published_run_metadata_names_the_run(result: PublishResult, output: Path) -> None:
    meta = published_run(output)
    assert meta["run_id"] == result.report.run_id
    assert (meta["source"], meta["quality_status"], meta["contract_version"]) == (
        "fixtures",
        "pass",
        "v2",
    )


def test_data_max_date_is_the_last_complete_day(result: PublishResult, output: Path) -> None:
    assert str(published_run(output)["data_max_date"]) == "2026-01-30"


def test_restated_from_is_the_lookback_before_as_of(result: PublishResult, output: Path) -> None:
    restated_from = str(AS_OF.date() - timedelta(days=14))
    assert str(published_run(output)["restated_from"]) == restated_from


def test_the_returned_run_metadata_matches_the_contract(run_metadata: RunMetadata) -> None:
    assert contract.validate("run_metadata", [run_metadata.model_dump()]) == []


def test_run_metadata_fields_are_the_contract_columns_in_order() -> None:
    assert list(RunMetadata.model_fields) == list(contract.columns("run_metadata"))


def test_run_metadata_cannot_be_changed(run_metadata: RunMetadata) -> None:
    with pytest.raises(ValidationError, match="frozen"):
        run_metadata.run_id = "another-run"


def test_a_run_that_fails_quality_is_not_published(failed: PublishResult) -> None:
    assert not failed.published and failed.report.status == "fail"


def test_a_failed_run_still_writes_its_report(failed: PublishResult) -> None:
    assert json.loads(failed.report_path.read_text())["status"] == "fail"


def test_a_failed_run_keeps_the_previous_set(
    result: PublishResult, failed: PublishResult, output: Path
) -> None:
    assert published_run(output)["run_id"] == result.report.run_id
    assert published_status(output) == "pass"


def test_republishing_replaces_the_set(
    result: PublishResult, second: PublishResult, output: Path
) -> None:
    assert second.report.run_id != result.report.run_id
    assert published_run(output)["run_id"] == second.report.run_id


def test_republishing_leaves_no_temp_dirs(second: PublishResult, tmp_path: Path) -> None:
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".output")] == []


def test_a_directory_with_other_files_is_refused_and_left_untouched(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path
) -> None:
    (output / "notes").mkdir(parents=True)
    (output / "notes" / "keep.txt").write_text("keep")
    with pytest.raises(PublishError, match="notes"):
        build_and_publish(con, output, runs)
    assert (output / "notes" / "keep.txt").read_text() == "keep"


def test_a_file_as_the_output_directory_is_refused(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path
) -> None:
    output.write_text("keep")
    with pytest.raises(PublishError, match="not a directory"):
        build_and_publish(con, output, runs)


def test_a_symlinked_output_directory_publishes_into_its_target(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path, tmp_path: Path
) -> None:
    output.mkdir()
    link = tmp_path / "link"
    link.symlink_to(output)
    build_and_publish(con, link, runs)
    assert link.is_symlink()
    assert published_status(output) == "pass"


def test_a_leftover_from_a_killed_publish_does_not_block_the_next_one(
    con: duckdb.DuckDBPyConnection, output: Path, runs: Path, result: PublishResult
) -> None:
    leftover = output.with_name(f".output.old-{result.report.run_id}")
    (leftover / "parquet").mkdir(parents=True)
    build_and_publish(con, output, runs)
    assert not leftover.exists()
