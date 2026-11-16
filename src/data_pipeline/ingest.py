"""Incremental ingestion of the source tables into a local DuckDB working database."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from data_pipeline.readers import Reader
from data_pipeline.sources.thelook.tables import SOURCE_TABLES, SourceTable

LOOKBACK = timedelta(days=14)  # returns arrive within 10.7 days of the order (live profile)
DUCKDB_TYPES = {
    "INT64": "bigint",
    "STRING": "varchar",
    "FLOAT64": "double",
    "TIMESTAMP": "timestamp",
}
_CREATE_INGEST_LOG = """
create table if not exists ingest_log (
    run_id varchar, source varchar, as_of timestamp, table_name varchar,
    window_start timestamp, rows_read bigint, ingested_at timestamp
)
"""
_CREATE_HISTORY_CHECKS = """
create table if not exists history_checks (
    run_id varchar, table_name varchar, before timestamp,
    source_rows bigint, source_key_sum hugeint,
    warehouse_rows bigint, warehouse_key_sum hugeint, checked_at timestamp
)
"""


class IngestError(Exception):
    """An ingest run that would mix sources, go back in time or mix versions of history."""


@dataclass(frozen=True)
class TableResult:
    """What one run read for one table; no window_start means everything before window_end."""

    table: str
    window_start: datetime | None
    window_end: datetime
    rows_read: int
    rows_total: int


@dataclass(frozen=True)
class Run:
    """One ingest run as recorded in ingest_log; as_of is UTC and timezone-aware."""

    run_id: str
    source: str
    as_of: datetime


def raw_table(name: str) -> str:
    """Return the name of the warehouse table that holds a source table's raw rows."""
    return f"raw_{name}"


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    """Open the working database, creating its raw and log tables if they are missing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    for table in SOURCE_TABLES.values():
        # No key constraints on purpose: rows with missing or duplicate keys are kept so the
        # quality checks can report and quarantine them.
        cols = ", ".join(f"{c} {DUCKDB_TYPES[t]}" for c, t in table.columns.items())
        con.execute(f"create table if not exists {raw_table(table.name)} ({cols})")
    con.execute(_CREATE_INGEST_LOG)
    con.execute(_CREATE_HISTORY_CHECKS)
    return con


def run_ingest(
    con: duckdb.DuckDBPyConnection,
    reader: Reader,
    as_of: datetime,
    *,
    full_refresh: bool = False,
    run_id: str | None = None,
) -> list[TableResult]:
    """Read the source as of as_of and replace each table's window in one transaction."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    as_of = as_of.astimezone(UTC)
    run_id = run_id or uuid.uuid4().hex
    start = None if full_refresh else _window_start(con, reader.name, as_of)
    if start is not None:
        _check_history(con, reader, start, as_of, run_id)

    # Every read finishes before any raw table changes, so a failed read changes nothing.
    batches = {
        name: reader.read(table, start if table.watermark else None, as_of)
        for name, table in SOURCE_TABLES.items()
    }

    con.execute("begin transaction")
    try:
        if full_refresh:
            con.execute("delete from ingest_log")
        results = _write_batches(con, reader.name, run_id, batches, start, as_of)
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    return results


def latest_run(con: duckdb.DuckDBPyConnection) -> Run | None:
    """Return the run with the latest as_of, or None when nothing has been ingested."""
    row = con.execute(
        """
        select run_id, source, as_of
        from ingest_log
        order by as_of desc, ingested_at desc
        limit 1
        """
    ).fetchone()
    return Run(row[0], row[1], row[2].replace(tzinfo=UTC)) if row else None


def _window_start(con: duckdb.DuckDBPyConnection, source: str, as_of: datetime) -> datetime | None:
    previous = latest_run(con)
    if previous is None:
        return None
    if previous.source != source:
        raise IngestError(
            f"the warehouse holds {previous.source} data; use another --db or --full-refresh"
        )
    if as_of < previous.as_of:
        raise IngestError(f"as_of {as_of:%Y-%m-%d %H:%M} is before the last run ({previous.as_of})")
    # Counted from the previous run's as_of, never from the newest created_at seen: the source
    # stamps order items and purchase events up to 4 days ahead, and those rows must still be
    # picked up once their time comes (docs/source-profile.md).
    return previous.as_of - LOOKBACK


def _check_history(
    con: duckdb.DuckDBPyConnection, reader: Reader, before: datetime, as_of: datetime, run_id: str
) -> None:
    changed = []
    now = _naive(datetime.now(UTC))
    for table in SOURCE_TABLES.values():
        if not table.watermark:
            continue
        source = reader.history(table, before, as_of)
        warehouse = con.execute(
            f"""
            select count(*), coalesce(sum({table.primary_key}), 0)
            from {raw_table(table.name)}
            where {table.watermark} < ?
            """,
            [_naive(before)],
        ).fetchall()[0]
        con.execute(
            """
            insert into history_checks (
                run_id, table_name, before, source_rows, source_key_sum,
                warehouse_rows, warehouse_key_sum, checked_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [run_id, table.name, _naive(before), *source, *warehouse, now],
        )
        if tuple(source) != tuple(warehouse):
            changed.append(
                f"{table.name} ({warehouse[0]:,} rows here, {source[0]:,} in the source)"
            )
    if changed:
        raise IngestError(
            f"source history before {before:%Y-%m-%d} changed since the last run: "
            f"{'; '.join(changed)}. Rerun with --full-refresh."
        )


def _write_batches(
    con: duckdb.DuckDBPyConnection,
    source: str,
    run_id: str,
    batches: dict[str, pa.Table],
    start: datetime | None,
    as_of: datetime,
) -> list[TableResult]:
    results = []
    now = _naive(datetime.now(UTC))
    for name, batch in batches.items():
        table = SOURCE_TABLES[name]
        window_start = start if table.watermark else None
        _replace_window(con, table, batch, window_start, as_of)
        con.execute(
            """
            insert into ingest_log (
                run_id, source, as_of, table_name, window_start, rows_read, ingested_at
            )
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            [run_id, source, _naive(as_of), name, _naive(window_start), batch.num_rows, now],
        )
        total = con.execute(f"select count(*) from {raw_table(name)}").fetchall()[0][0]
        results.append(TableResult(name, window_start, as_of, batch.num_rows, total))
    return results


def _replace_window(
    con: duckdb.DuckDBPyConnection,
    table: SourceTable,
    batch: pa.Table,
    start: datetime | None,
    end: datetime,
) -> None:
    raw = raw_table(table.name)
    if start is None:  # full read: the batch is the whole table up to `end`
        con.execute(f"delete from {raw}")
    else:
        con.execute(
            f"delete from {raw} where {table.watermark} >= ? and {table.watermark} < ?",
            [_naive(start), _naive(end)],
        )
    cols = ", ".join(table.columns)
    con.register("batch", batch)
    try:
        con.execute(f"insert into {raw} ({cols}) select {cols} from batch")
    finally:
        con.unregister("batch")


# DuckDB columns hold UTC without a time zone.
def _naive(ts: datetime | None) -> datetime | None:
    return ts.astimezone(UTC).replace(tzinfo=None) if ts else None
