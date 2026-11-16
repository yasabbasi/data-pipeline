"""Quality checks over the raw tables: quarantine bad rows, report, fail on critical checks."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict

from data_pipeline import contract
from data_pipeline.ingest import Run, latest_run, raw_table
from data_pipeline.sources.thelook.tables import SOURCE_TABLES

MAX_QUARANTINE_RATE = 0.01
FRESHNESS = timedelta(days=2)  # newest order vs. the run's as_of (the source refreshes daily)
_REASON_SEPARATOR = ", "
_FROZEN = ConfigDict(frozen=True, extra="forbid")


class QualityError(Exception):
    """Checks that cannot run, or a report that does not match the contract."""


@dataclass(frozen=True)
class RowCheck:
    """A row-level check: rows of the raw table for which bad_if is true are quarantined."""

    name: str
    table: str
    check_type: str
    bad_if: str  # SQL predicate over the raw table's columns and the columns its joins add
    description: str
    params: tuple[Any, ...] = ()  # values for the ? placeholders in bad_if, in order
    ctes: tuple[str, ...] = ()  # "<name> as (select ...)", without placeholders
    joins: tuple[str, ...] = ()  # join clauses on those CTEs that add no rows


class TableCounts(BaseModel):
    """One table's row counts in the quality report."""

    model_config = _FROZEN

    table: str
    rows_ingested: int
    rows_quarantined: int


class CheckResult(BaseModel):
    """One check's outcome in the quality report; failed_rows is 0 for table-level checks."""

    model_config = _FROZEN

    name: str
    table: str
    check_type: str
    severity: Literal["critical", "warning"]
    status: Literal["pass", "fail"]
    failed_rows: int
    message: str


class QualityReport(BaseModel):
    """The quality report of one run, as written to quality_report.json."""

    model_config = _FROZEN

    contract_version: str
    run_id: str
    generated_at: str
    source: str
    status: Literal["pass", "warn", "fail"]
    tables: tuple[TableCounts, ...]
    checks: tuple[CheckResult, ...]

    def to_json(self) -> str:
        """Return the exact text of quality_report.json."""
        return json.dumps(self.model_dump(mode="json"), indent=2) + "\n"


def run_checks(
    con: duckdb.DuckDBPyConnection,
    *,
    max_quarantine_rate: float = MAX_QUARANTINE_RATE,
    freshness: timedelta = FRESHNESS,
    generated_at: datetime | None = None,  # fixed only for the reproducible sample
) -> QualityReport:
    """Check the latest ingested data, rebuild clean_/quarantine_ tables, return the report."""
    # Imported here because checks.py imports RowCheck from this module.
    from data_pipeline.sources.thelook import checks

    run = latest_run(con)
    if run is None:
        raise QualityError("nothing ingested yet: run `data-pipeline ingest` first")
    row_checks = checks.row_checks()
    tables: list[TableCounts] = []
    results: list[CheckResult] = []
    con.execute("begin transaction")
    try:
        for name in checks.TABLE_ORDER:
            mine = [c for c in row_checks if c.table == name]
            counts, table_results = _check_table(con, name, mine, max_quarantine_rate)
            tables.append(counts)
            results += table_results
        results.append(
            _freshness(con, checks.FRESHNESS_TABLE, checks.FRESHNESS_COLUMN, run, freshness)
        )
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    return _report(run, generated_at or datetime.now(UTC), tables, results)


def iso_utc(ts: datetime) -> str:
    """Format a timezone-aware time as the contract's UTC date-time string."""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_report(report: QualityReport, runs_dir: Path) -> Path:
    """Write the report to <runs_dir>/<run_id>/quality_report.json and return that path."""
    path = runs_dir / report.run_id / "quality_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_json())
    return path


def _check_table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    checks: Sequence[RowCheck],
    max_quarantine_rate: float,
) -> tuple[TableCounts, list[CheckResult]]:
    failed = _quarantine(con, name, checks)
    ingested: int = _scalar(con, f"select count(*) from {raw_table(name)}")
    quarantined: int = _scalar(con, f"select count(*) from quarantine_{name}")
    results = [
        _result(c.name, name, c.check_type, "warning", n == 0, n, f"{n} row(s): {c.description}")
        for c, n in zip(checks, failed, strict=True)
    ]
    rate = quarantined / ingested if ingested else 0.0
    results += [
        _result(
            f"{name}.volume.not_empty",
            name,
            "volume",
            "critical",
            ingested > 0,
            0,
            f"{ingested} row(s) ingested",
        ),
        _result(
            f"{name}.quarantine_rate",
            name,
            "quarantine_rate",
            "critical",
            rate <= max_quarantine_rate,
            0,
            f"{rate:.2%} of rows quarantined (limit {max_quarantine_rate:.0%})",
        ),
    ]
    counts = TableCounts(table=name, rows_ingested=ingested, rows_quarantined=quarantined)
    return counts, results


def _quarantine(
    con: duckdb.DuckDBPyConnection, table: str, checks: Sequence[RowCheck]
) -> list[int]:
    flagged = _flagged(table, checks)
    params = [p for c in checks for p in c.params]
    columns = ", ".join(SOURCE_TABLES[table].columns)
    failed = [f"failed_{i}" for i in range(len(checks))]
    any_failed = " or ".join(failed)
    reasons = ", ".join(f"case when {f} then ? end" for f in failed)
    con.execute(
        f"""
        create or replace table quarantine_{table} as
        {flagged}
        select {columns}, concat_ws(?, {reasons}) as failed_checks
        from flagged
        where {any_failed}
        order by raw_row
        """,
        [*params, _REASON_SEPARATOR, *(c.name for c in checks)],
    )
    con.execute(
        f"""
        create or replace table clean_{table} as
        {flagged}
        select {columns}
        from flagged
        where not ({any_failed})
        order by raw_row
        """,
        params,
    )
    counts = con.execute(
        f"""
        {flagged}
        select {", ".join(f"count(*) filter (where {f})" for f in failed)}
        from flagged
        """,
        params,
    ).fetchall()[0]
    return list(counts)


def _flagged(table: str, checks: Sequence[RowCheck]) -> str:
    columns = list(SOURCE_TABLES[table].columns)
    flags = [f"coalesce(({c.bad_if}), false) as failed_{i}" for i, c in enumerate(checks)]
    ctes = [cte for c in checks for cte in c.ctes]
    joins = [join for c in checks for join in c.joins]
    # The joins do not keep the raw table's row order, so the row number is carried along and
    # the clean and quarantine tables are written in that order, the same on every run.
    rows = f"""
        flagged as (
            select rowid as raw_row, {", ".join(columns + flags)}
            from {raw_table(table)}
            {" ".join(joins)}
        )
        """
    return "with " + ", ".join([*ctes, rows])


def _freshness(
    con: duckdb.DuckDBPyConnection, table: str, column: str, run: Run, limit: timedelta
) -> CheckResult:
    as_of = run.as_of.replace(tzinfo=None)  # naive UTC, like the warehouse columns
    newest: datetime | None = _scalar(con, f"select max({column}) from clean_{table}")
    message = (
        f"newest order {newest:%Y-%m-%d %H:%M}, as_of {as_of:%Y-%m-%d %H:%M} UTC "
        f"(limit {limit.days} days)"
        if newest
        else "no orders"
    )
    fresh = newest is not None and newest >= as_of - limit
    return _result(f"{table}.{column}.freshness", table, "freshness", "critical", fresh, 0, message)


def _report(
    run: Run,
    generated_at: datetime,
    tables: Sequence[TableCounts],
    results: Sequence[CheckResult],
) -> QualityReport:
    failed = {r.severity for r in results if r.status == "fail"}
    status: Literal["pass", "warn", "fail"] = (
        "fail" if "critical" in failed else "warn" if failed else "pass"
    )
    report = QualityReport(
        contract_version=contract.CONTRACT_VERSION,
        run_id=run.run_id,
        generated_at=iso_utc(generated_at),
        source=run.source,
        status=status,
        tables=tuple(tables),
        checks=tuple(results),
    )
    if errors := contract.validate(contract.QUALITY_REPORT, [report.model_dump(mode="json")]):
        raise QualityError(f"report does not match the contract: {errors}")
    return report


def _result(
    name: str,
    table: str,
    check_type: str,
    severity: Literal["critical", "warning"],
    passed: bool,
    failed_rows: int,
    message: str,
) -> CheckResult:
    return CheckResult(
        name=name,
        table=table,
        check_type=check_type,
        severity=severity,
        status="pass" if passed else "fail",
        failed_rows=failed_rows,
        message=message,
    )


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchall()[0][0]
