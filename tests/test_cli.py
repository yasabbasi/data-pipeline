import subprocess
from datetime import date
from importlib.metadata import version
from pathlib import Path
from typing import Any

import duckdb
import pytest

from data_pipeline import publish
from data_pipeline.cli import main
from data_pipeline.ingest import connect, raw_table


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


@pytest.fixture
def output(tmp_path: Path) -> Path:
    return tmp_path / "output"


@pytest.fixture
def check_args(warehouse: Path, tmp_path: Path) -> list[str]:
    return ["--db", str(warehouse), "--runs-dir", str(tmp_path / "runs")]


@pytest.fixture
def build_args(check_args: list[str], output: Path) -> list[str]:
    return [*check_args, "--output-dir", str(output)]


@pytest.fixture
def ingested(warehouse: Path) -> Path:
    assert main(["ingest", "--db", str(warehouse)]) == 0
    return warehouse


@pytest.fixture
def two_ingests(warehouse: Path, capsys: pytest.CaptureFixture[str]) -> str:
    assert main(["ingest", "--db", str(warehouse), "--as-of", "2026-01-21"]) == 0
    assert main(["ingest", "--db", str(warehouse), "--as-of", "2026-01-31T00:00:00Z"]) == 0
    return capsys.readouterr().out


@pytest.fixture
def two_runs(build_args: list[str], capsys: pytest.CaptureFixture[str]) -> str:
    assert main(["run", *build_args, "--as-of", "2026-01-21", "--full-refresh"]) == 0
    assert main(["run", *build_args, "--as-of", "2026-01-31"]) == 0
    return capsys.readouterr().out


@pytest.fixture
def published(build_args: list[str], output: Path, capsys: pytest.CaptureFixture[str]) -> Path:
    assert main(["run", *build_args]) == 0
    capsys.readouterr()
    return output


def restated_from(output: Path) -> date:
    with duckdb.connect(str(output / "metrics.duckdb"), read_only=True) as con:
        restated: date = con.execute("select restated_from from run_metadata").fetchall()[0][0]
    return restated


def shown_dates(output: Path, capsys: pytest.CaptureFixture[str]) -> list[str]:
    assert main(["show", "--output-dir", str(output), "--days", "30"]) == 0
    lines = capsys.readouterr().out.splitlines()
    return [line.split(" | ")[0].removeprefix("| ") for line in lines if line.startswith("| 20")]


def test_version_flag_prints_the_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_:
        main(["--version"])
    assert exit_.value.code == 0
    assert capsys.readouterr().out.strip() == f"data-pipeline {version('data-pipeline')}"


def test_console_script_runs() -> None:
    result = subprocess.run(["data-pipeline", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "usage: data-pipeline" in result.stdout


def test_no_command_prints_the_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    assert "usage: data-pipeline" in capsys.readouterr().out


def test_ingest_prints_the_as_of_time_in_utc(two_ingests: str) -> None:
    assert "as of 2026-01-31 00:00 UTC" in two_ingests


def test_a_later_ingest_reads_from_14_days_before_the_previous_run(two_ingests: str) -> None:
    assert "from 2026-01-07 00:00" in two_ingests


def test_ingest_before_the_last_run_exits_1_with_a_message(
    two_ingests: str, warehouse: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["ingest", "--db", str(warehouse), "--as-of", "2026-01-30"]) == 1
    assert "before the last run" in capsys.readouterr().err


def test_check_without_a_warehouse_exits_1(check_args: list[str]) -> None:
    assert main(["check", *check_args]) == 1


def test_check_passes_on_the_fixtures(
    ingested: Path, check_args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["check", *check_args]) == 0
    assert "Quality PASS" in capsys.readouterr().out


def test_check_exits_1_and_names_the_critical_check_that_failed(
    ingested: Path, check_args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with connect(ingested) as con:
        con.execute(f"delete from {raw_table('products')}")
    assert main(["check", *check_args]) == 1
    assert "FAIL [critical] products.volume.not_empty" in capsys.readouterr().out


def test_build_without_a_warehouse_exits_1(build_args: list[str]) -> None:
    assert main(["build", *build_args]) == 1


def test_build_publishes_the_outputs(
    ingested: Path, build_args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["build", *build_args]) == 0
    assert "Published to" in capsys.readouterr().out


def test_build_publishes_nothing_when_a_critical_check_fails(
    ingested: Path, build_args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with connect(ingested) as con:
        con.execute(f"delete from {raw_table('orders')}")
    assert main(["build", *build_args]) == 1
    assert "Nothing published" in capsys.readouterr().err


def test_a_run_without_a_complete_day_exits_1_with_a_message(
    build_args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", *build_args, "--as-of", "2026-01-01T12:00"]) == 1
    assert "error: no complete day of data before 2026-01-01\n" in capsys.readouterr().err


def test_outputs_that_break_the_contract_exit_1_with_a_message(
    ingested: Path,
    build_args: list[str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(*args: Any, **kwargs: Any) -> None:
        raise publish.PublishError("outputs do not match the contract:\ndaily_totals: not gap-free")

    monkeypatch.setattr(publish, "_validate", refuse)
    assert main(["build", *build_args]) == 1
    assert "error: outputs do not match the contract:" in capsys.readouterr().err


def test_run_prints_how_long_it_took(two_runs: str) -> None:
    assert "Finished in" in two_runs


def test_show_prints_the_published_date_range(
    two_runs: str, output: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["show", "--output-dir", str(output), "--days", "3"]) == 0
    assert "Data 2026-01-01 to 2026-01-30" in capsys.readouterr().out


def test_show_prints_as_many_days_as_asked_for(
    two_runs: str, output: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["show", "--output-dir", str(output), "--days", "3"]) == 0
    assert capsys.readouterr().out.count("| 2026-01-") == 3


def test_show_without_published_outputs_exits_1(tmp_path: Path) -> None:
    assert main(["show", "--output-dir", str(tmp_path / "missing")]) == 1


def test_show_marks_the_days_that_may_still_change(
    published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    restated = restated_from(published)
    marked = [d for d in shown_dates(published, capsys) if d.endswith(" *")]
    assert marked
    assert all(date.fromisoformat(d.removesuffix(" *")) >= restated for d in marked)


def test_show_leaves_settled_days_unmarked(
    published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    restated = restated_from(published)
    unmarked = [d for d in shown_dates(published, capsys) if not d.endswith(" *")]
    assert unmarked
    assert all(date.fromisoformat(d) < restated for d in unmarked)


def test_show_explains_the_mark_in_a_last_line(
    published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["show", "--output-dir", str(published), "--days", "30"]) == 0
    assert capsys.readouterr().out.splitlines()[-1] == "* may still change"


def test_show_has_no_marks_when_every_day_is_settled(
    published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with duckdb.connect(str(published / "metrics.duckdb")) as con:
        con.execute("update run_metadata set restated_from = data_max_date + 1")
    assert main(["show", "--output-dir", str(published), "--days", "30"]) == 0
    shown = capsys.readouterr().out
    assert " *" not in shown and "* may still change" not in shown
