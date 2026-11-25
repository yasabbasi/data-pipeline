"""Source rows as pyarrow tables, from synthetic fixtures or live BigQuery (read-only)."""

import os
from datetime import UTC, datetime
from typing import Any, Protocol

import pyarrow as pa
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from data_pipeline.sources.thelook import fixtures
from data_pipeline.sources.thelook.tables import DATASET, SourceTable

ARROW_TYPES = {
    "INT64": pa.int64(),
    "STRING": pa.string(),
    "FLOAT64": pa.float64(),
    "TIMESTAMP": pa.timestamp("us"),  # UTC, no time zone
}
DEFAULT_MAX_BYTES_BILLED = 1_000_000_000  # a whole live run processed 281 MB on 2026-11-16


def arrow_schema(table: SourceTable) -> pa.Schema:
    """Return the pyarrow schema every reader gives for a source table."""
    return pa.schema([(col, ARROW_TYPES[typ]) for col, typ in table.columns.items()])


class Reader(Protocol):
    """A source of rows with exactly the columns in tables.py, in that order."""

    name: str  # "fixtures" or "bigquery"; recorded with every run

    def read(self, table: SourceTable, start: datetime | None, end: datetime) -> pa.Table:
        """Return the rows with a watermark in [start, end), or the whole table if it has none."""
        ...

    def history(self, table: SourceTable, before: datetime, end: datetime) -> tuple[int, int]:
        """Return (row count, sum of primary keys) for rows whose watermark is before `before`."""
        ...


class FixtureReader:
    """Serves fixtures.generate() as the source looked at time `end`."""

    name = "fixtures"

    def __init__(self, **generate_kwargs: Any) -> None:
        self._kwargs = generate_kwargs
        self._snapshots: dict[datetime, dict[str, fixtures.Rows]] = {}

    def read(self, table: SourceTable, start: datetime | None, end: datetime) -> pa.Table:
        rows = self._snapshot(end)[table.name]
        if table.watermark:
            wm = table.watermark
            rows = [r for r in rows if (start is None or r[wm] >= start) and r[wm] < end]
        return pa.Table.from_pylist([_naive_utc(r) for r in rows], schema=arrow_schema(table))

    def history(self, table: SourceTable, before: datetime, end: datetime) -> tuple[int, int]:
        wm = _watermark(table)
        rows = [r for r in self._snapshot(end)[table.name] if r[wm] < before]
        return len(rows), sum(r[table.primary_key] for r in rows)

    def _snapshot(self, end: datetime) -> dict[str, fixtures.Rows]:
        if end not in self._snapshots:
            self._snapshots = {end: fixtures.generate(as_of=end, **self._kwargs)}
        return self._snapshots[end]


class BigQueryReader:
    """Reads the live dataset with parameterized queries, billed to GCP_PROJECT or the default."""

    name = "bigquery"

    def __init__(self, client: bigquery.Client | None = None) -> None:
        self._max_bytes_billed = _max_bytes_billed()
        self._client = client or bigquery.Client(project=os.environ.get("GCP_PROJECT"))
        self.bytes_processed = 0  # summed over this reader's queries, for cost notes
        self.bytes_billed = 0

    def read(self, table: SourceTable, start: datetime | None, end: datetime) -> pa.Table:
        rows = self._query(*build_query(table, start, end))
        # Downloaded over the REST API, so no BigQuery Storage API permission is needed.
        result = rows.to_arrow(create_bqstorage_client=False)
        return result.select(list(table.columns)).cast(arrow_schema(table))

    def history(self, table: SourceTable, before: datetime, end: datetime) -> tuple[int, int]:
        sql = (
            f"select count(*) as n, coalesce(sum({table.primary_key}), 0) as key_sum\n"
            f"from `{DATASET}.{table.name}`\n"
            f"where {_watermark(table)} < @before"
        )
        row = next(
            iter(self._query(sql, [bigquery.ScalarQueryParameter("before", "TIMESTAMP", before)]))
        )
        return row["n"], row["key_sum"]

    def _query(self, sql: str, params: list[bigquery.ScalarQueryParameter]) -> RowIterator:
        config = bigquery.QueryJobConfig(
            query_parameters=params, maximum_bytes_billed=self._max_bytes_billed
        )
        job = self._client.query(sql, job_config=config)
        rows = job.result()  # waits for the job, which fills in its byte statistics
        self.bytes_processed += job.total_bytes_processed or 0
        self.bytes_billed += job.total_bytes_billed or 0
        return rows


def build_query(
    table: SourceTable, start: datetime | None, end: datetime
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    """Return the query for one table's window and its parameters."""
    sql = f"select {', '.join(table.columns)}\nfrom `{DATASET}.{table.name}`"
    if not table.watermark:
        return sql, []
    conditions = [f"{table.watermark} < @end"]
    params = [bigquery.ScalarQueryParameter("end", "TIMESTAMP", end)]
    if start is not None:
        conditions.insert(0, f"{table.watermark} >= @start")
        params.insert(0, bigquery.ScalarQueryParameter("start", "TIMESTAMP", start))
    return f"{sql}\nwhere {' and '.join(conditions)}", params


def _max_bytes_billed() -> int:
    raw = os.environ.get("BQ_MAX_BYTES_BILLED") or str(DEFAULT_MAX_BYTES_BILLED)
    if not raw.isdecimal() or int(raw) <= 0:
        raise SystemExit(f"error: BQ_MAX_BYTES_BILLED must be a positive whole number, not {raw!r}")
    return int(raw)


def _watermark(table: SourceTable) -> str:
    if table.watermark is None:
        raise ValueError(f"{table.name} has no watermark column")
    return table.watermark


def _naive_utc(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v.astimezone(UTC).replace(tzinfo=None) if isinstance(v, datetime) else v
        for k, v in row.items()
    }
