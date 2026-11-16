from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from data_pipeline.ingest import IngestError, connect, raw_table, run_ingest
from data_pipeline.readers import FixtureReader
from data_pipeline.sources.thelook import fixtures
from data_pipeline.sources.thelook.tables import SOURCE_TABLES, SourceTable

AS_OF = datetime(2026, 1, 31, tzinfo=UTC)
EARLIER = AS_OF - timedelta(days=5)


class BigQueryNamedReader(FixtureReader):
    name = "bigquery"


class BrokenEventsReader(FixtureReader):
    def read(self, table: SourceTable, start: datetime | None, end: datetime) -> pa.Table:
        if table.name == "events":
            raise RuntimeError("network down")
        return super().read(table, start, end)


def snapshot(con: duckdb.DuckDBPyConnection) -> dict[str, list[tuple[Any, ...]]]:
    return {
        name: con.execute(
            f"select {', '.join(t.columns)} from {raw_table(name)} order by {t.primary_key}"
        ).fetchall()
        for name, t in SOURCE_TABLES.items()
    }


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    value: int = con.execute(sql, params).fetchall()[0][0]
    return value


def count(con: duckdb.DuckDBPyConnection, name: str) -> int:
    return scalar(con, f"select count(*) from {raw_table(name)}")


def returned_items(con: duckdb.DuckDBPyConnection) -> int:
    sql = f"select count(*) from {raw_table('order_items')} where status = ?"
    return scalar(con, sql, ["Returned"])


@pytest.fixture
def con(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with connect(tmp_path / "raw.duckdb") as con:
        yield con


@pytest.fixture(scope="module")
def source() -> dict[str, fixtures.Rows]:
    return fixtures.generate(as_of=AS_OF)


def test_a_first_run_loads_every_row_dated_before_as_of(
    con: duckdb.DuckDBPyConnection, source: dict[str, fixtures.Rows]
) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    assert count(con, "orders") == len(source["orders"])
    assert count(con, "products") == len(source["products"])


def test_a_first_run_defers_rows_dated_after_as_of(
    con: duckdb.DuckDBPyConnection, source: dict[str, fixtures.Rows]
) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    future_items = [i for i in source["order_items"] if i["created_at"] >= AS_OF]
    future_events = [e for e in source["events"] if e["created_at"] >= AS_OF]
    assert future_items and future_events
    assert count(con, "order_items") == len(source["order_items"]) - len(future_items)
    assert count(con, "events") == len(source["events"]) - len(future_events)


def test_a_rerun_with_the_same_as_of_changes_nothing(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    before = snapshot(con)
    run_ingest(con, FixtureReader(), AS_OF)
    assert snapshot(con) == before


def test_a_first_run_reads_every_table_whole(con: duckdb.DuckDBPyConnection) -> None:
    first = run_ingest(con, FixtureReader(), EARLIER)
    assert all(r.window_start is None for r in first)


def test_a_later_run_rereads_the_lookback_window(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    second = run_ingest(con, FixtureReader(), AS_OF)
    windows = {r.table: r.window_start for r in second}
    assert windows["order_items"] == EARLIER - timedelta(days=14)


def test_a_later_run_replaces_small_tables_whole(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    second = run_ingest(con, FixtureReader(), AS_OF)
    windows = {r.table: r.window_start for r in second}
    assert windows["products"] is None


def test_a_later_run_picks_up_status_updates_inside_the_lookback(
    con: duckdb.DuckDBPyConnection,
) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    returned_before = returned_items(con)
    run_ingest(con, FixtureReader(), AS_OF)
    assert returned_items(con) > returned_before


def test_a_later_run_picks_up_rows_created_since_the_last_run(
    con: duckdb.DuckDBPyConnection,
) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    items_before = count(con, "order_items")
    run_ingest(con, FixtureReader(), AS_OF)
    assert count(con, "order_items") > items_before


def test_daily_incremental_runs_match_one_full_backfill(tmp_path: Path) -> None:
    reader = FixtureReader()
    final = AS_OF + timedelta(days=12)  # past the last returns and future-dated items
    with connect(tmp_path / "incremental.duckdb") as inc:
        day = AS_OF - timedelta(days=21)
        while day <= final:
            run_ingest(inc, reader, day)
            day += timedelta(days=1)
        incremental = snapshot(inc)
    with connect(tmp_path / "full.duckdb") as full:
        run_ingest(full, reader, final)
        assert snapshot(full) == incremental
    assert all(incremental[name] for name in SOURCE_TABLES)


def test_a_run_dated_before_the_last_run_is_refused(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    with pytest.raises(IngestError, match="before the last run"):
        run_ingest(con, FixtureReader(), AS_OF - timedelta(days=1))


def test_a_run_from_another_source_is_refused(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    with pytest.raises(IngestError, match="holds fixtures data"):
        run_ingest(con, BigQueryNamedReader(), AS_OF)


def test_a_full_refresh_accepts_another_source_and_an_earlier_as_of(
    con: duckdb.DuckDBPyConnection,
) -> None:
    run_ingest(con, FixtureReader(), AS_OF)
    results = run_ingest(con, BigQueryNamedReader(), AS_OF - timedelta(days=1), full_refresh=True)
    assert all(r.window_start is None for r in results)


def test_incremental_runs_record_matching_history_checks(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    run_ingest(con, FixtureReader(), AS_OF)
    checks = con.execute(
        """
        select table_name, source_rows, source_key_sum, warehouse_rows, warehouse_key_sum
        from history_checks
        """
    ).fetchall()
    assert {c[0] for c in checks} == {"orders", "order_items", "users", "events"}
    assert all(c[1:3] == c[3:5] for c in checks)
    assert any(c[1] > 0 for c in checks)


def test_regenerated_source_history_is_refused_before_anything_changes(
    con: duckdb.DuckDBPyConnection,
) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    before = snapshot(con)
    regenerated = FixtureReader(seed=8)  # same shape, different history, like thelook's refresh
    with pytest.raises(IngestError, match="history before .* changed.*--full-refresh"):
        run_ingest(con, regenerated, AS_OF)
    assert snapshot(con) == before


def test_a_full_refresh_recovers_from_regenerated_source_history(
    con: duckdb.DuckDBPyConnection,
) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    regenerated = FixtureReader(seed=8)
    with pytest.raises(IngestError):
        run_ingest(con, regenerated, AS_OF)
    results = run_ingest(con, regenerated, AS_OF, full_refresh=True)
    assert all(r.window_start is None for r in results)


def test_a_failed_read_leaves_the_warehouse_untouched(con: duckdb.DuckDBPyConnection) -> None:
    run_ingest(con, FixtureReader(), EARLIER)
    before = snapshot(con)
    log_rows = scalar(con, "select count(*) from ingest_log")
    with pytest.raises(RuntimeError):
        run_ingest(con, BrokenEventsReader(), AS_OF)
    assert snapshot(con) == before
    assert scalar(con, "select count(*) from ingest_log") == log_rows


def test_an_as_of_without_a_time_zone_is_rejected(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(ValueError):
        run_ingest(con, FixtureReader(), datetime(2026, 1, 31))
