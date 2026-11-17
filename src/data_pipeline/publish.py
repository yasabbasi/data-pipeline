"""Build the metrics from the latest ingest and publish the contract outputs as a set."""

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel, ConfigDict

from data_pipeline import contract, metrics_layer, quality
from data_pipeline.ingest import LOOKBACK, latest_run

DAILY_METRICS_SAMPLE = 1000  # rows schema-validated row by row; the rest are type-checked
TYPE_ALIASES = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}
SET_NAMES = {"metrics.duckdb", "parquet", "quality_report.json"}


class PublishError(Exception):
    """Outputs that do not match the contract, so nothing is published."""


class RunMetadata(BaseModel):
    """The one row of run_metadata; dates and times are strings in the contract's formats."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    contract_version: str
    pipeline_version: str
    source: str
    generated_at: str
    data_min_date: str
    data_max_date: str
    restated_from: str
    quality_status: str


@dataclass(frozen=True)
class PublishResult:
    """The outcome of one build; run_metadata is None when the run failed quality."""

    report: quality.QualityReport
    report_path: Path
    run_metadata: RunMetadata | None
    row_counts: dict[str, int]

    @property
    def published(self) -> bool:
        """Whether the outputs were published."""
        return self.run_metadata is not None


def build_and_publish(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    runs_dir: Path,
    *,
    generated_at: datetime | None = None,  # both are fixed only for the reproducible sample
    pipeline_version: str | None = None,
) -> PublishResult:
    """Check quality, rebuild the metrics and replace the published set; a failed run keeps it."""
    output_dir = output_dir.resolve()
    _refuse_other_files(output_dir)
    generated_at = generated_at or datetime.now(UTC)
    report = quality.run_checks(con, generated_at=generated_at)
    report_path = quality.write_report(report, runs_dir)
    if report.status == "fail":
        return PublishResult(report, report_path, None, {})

    run = latest_run(con)
    assert run is not None  # run_checks has already refused a warehouse without a run
    first, last = metrics_layer.build(con, run.as_of.date())
    run_metadata = RunMetadata(
        run_id=run.run_id,
        contract_version=contract.CONTRACT_VERSION,
        pipeline_version=pipeline_version or version("data-pipeline"),
        source=run.source,
        generated_at=quality.iso_utc(generated_at),
        data_min_date=first.isoformat(),
        data_max_date=last.isoformat(),
        restated_from=(run.as_of.date() - LOOKBACK).isoformat(),
        quality_status=report.status,
    )
    _create_run_metadata(con, run_metadata)
    _validate(con, run_metadata, first, last)
    row_counts: dict[str, int] = {
        table: _scalar(con, f"select count(*) from {table}") for table in contract.TABLES
    }
    _write_set(con, output_dir, report)
    return PublishResult(report, report_path, run_metadata, row_counts)


def _refuse_other_files(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    if not output_dir.is_dir():
        raise PublishError(f"{output_dir} is not a directory")
    others = sorted({path.name for path in output_dir.iterdir()} - SET_NAMES)
    if others:
        raise PublishError(
            f"{output_dir} holds files this pipeline did not publish ({', '.join(others[:5])}); "
            "publishing replaces the whole directory, so choose an empty one"
        )


def _create_run_metadata(con: duckdb.DuckDBPyConnection, run_metadata: RunMetadata) -> None:
    columns = contract.columns("run_metadata")
    row = run_metadata.model_dump()
    select = ", ".join(f"cast(? as {typ.lower()}) as {name}" for name, typ in columns.items())
    con.execute(
        f"create or replace table run_metadata as select {select}",
        [row[name] for name in columns],
    )


def _validate(
    con: duckdb.DuckDBPyConnection, run_metadata: RunMetadata, first: date, last: date
) -> None:
    # Rows are read by the contract's column names, so the columns are checked first.
    _raise_if_any(_column_errors(con))
    errors = _duplicate_errors(con)
    days = _scalar(con, "select count(*) from daily_totals")
    if days != (last - first).days + 1:
        errors.append(f"daily_totals has {days} rows for {first}..{last}: not gap-free")
    errors += contract.validate("run_metadata", [run_metadata.model_dump()])
    for table, limit in [
        ("metric_definitions", None),
        ("daily_totals", None),
        ("daily_metrics", DAILY_METRICS_SAMPLE),
        ("daily_by_category", DAILY_METRICS_SAMPLE),
        ("daily_by_distribution_center", DAILY_METRICS_SAMPLE),
    ]:
        errors += contract.validate(table, _records(con, table, limit))
    _raise_if_any(errors)


def _raise_if_any(errors: list[str]) -> None:
    if errors:
        raise PublishError("outputs do not match the contract:\n" + "\n".join(errors[:20]))


def _column_errors(con: duckdb.DuckDBPyConnection) -> list[str]:
    errors = []
    for table in contract.TABLES:
        expected = [(c, TYPE_ALIASES.get(t, t)) for c, t in contract.columns(table).items()]
        actual = [(r[0], r[1]) for r in con.execute(f"describe {table}").fetchall()]
        if actual != expected:
            errors.append(f"{table} columns {actual} != contract {expected}")
    return errors


def _duplicate_errors(con: duckdb.DuckDBPyConnection) -> list[str]:
    errors = []
    for table in contract.TABLES:
        grain = contract.load_schema(table)["x-grain"]
        if not grain:
            continue
        dupes = _scalar(con, f"select count(*) - count(distinct ({', '.join(grain)})) from {table}")
        if dupes:
            errors.append(f"{table}: {dupes} duplicate rows for grain {grain}")
    return errors


# Rows as JSON-like dicts (dates as ISO strings), the shape the JSON Schemas describe.
def _records(con: duckdb.DuckDBPyConnection, table: str, limit: int | None) -> list[dict[str, Any]]:
    names = list(contract.columns(table))
    rows = con.execute(f"select {', '.join(names)} from {table} limit ?", [limit]).fetchall()
    return [
        {n: v.isoformat() if isinstance(v, date) else v for n, v in zip(names, row, strict=True)}
        for row in rows
    ]


def _write_set(
    con: duckdb.DuckDBPyConnection, output_dir: Path, report: quality.QualityReport
) -> None:
    # The set is written next to the output directory and renamed into place, so readers
    # never see a half-written set.
    tmp = output_dir.with_name(f".{output_dir.name}.tmp-{report.run_id}")
    old = output_dir.with_name(f".{output_dir.name}.old-{report.run_id}")
    for leftover in (tmp, old):
        shutil.rmtree(leftover, ignore_errors=True)
    (tmp / "parquet").mkdir(parents=True)
    con.execute("attach ? as published", [str(tmp / "metrics.duckdb")])
    try:
        for table in contract.TABLES:
            columns = ", ".join(contract.columns(table))
            con.execute(
                f"""
                create table published.main.{table} as
                select {columns}
                from {table}
                """
            )
            parquet = tmp / "parquet" / f"{table}.parquet"
            con.execute(f"copy {table} to ? (format parquet)", [str(parquet)])
    finally:
        con.execute("detach published")
    (tmp / "quality_report.json").write_text(report.to_json())
    if output_dir.exists():
        output_dir.rename(old)
    tmp.rename(output_dir)
    shutil.rmtree(old, ignore_errors=True)


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchall()[0][0]
