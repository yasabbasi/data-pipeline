from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Self, cast

import pyarrow as pa
import pytest
from google.cloud import bigquery

from data_pipeline.readers import BigQueryReader, FixtureReader, arrow_schema, build_query
from data_pipeline.sources.thelook.tables import DATASET, SOURCE_TABLES

START = datetime(2026, 1, 17, tzinfo=UTC)
END = datetime(2026, 1, 31, tzinfo=UTC)


class FakeClient:
    """Stands in for bigquery.Client, its query jobs and their row iterators."""

    total_bytes_processed = 1_500_000
    total_bytes_billed = 10_485_760  # BigQuery bills at least 10 MB per query

    def __init__(
        self, result: pa.Table | None = None, rows: list[dict[str, int]] | None = None
    ) -> None:
        self.result_table = result
        self.rows = rows or []
        self.calls: list[tuple[str, list[bigquery.ScalarQueryParameter]]] = []
        self.job_configs: list[bigquery.QueryJobConfig] = []

    def query(self, sql: str, job_config: bigquery.QueryJobConfig) -> Self:
        self.calls.append((sql, job_config.query_parameters))
        self.job_configs.append(job_config)
        return self

    def result(self) -> Self:
        return self

    def to_arrow(self, create_bqstorage_client: bool) -> pa.Table | None:
        assert create_bqstorage_client is False
        return self.result_table

    def __iter__(self) -> Iterator[dict[str, int]]:
        return iter(self.rows)


def reader_for(client: FakeClient) -> BigQueryReader:
    return BigQueryReader(client=cast(bigquery.Client, client))


@pytest.fixture
def history_client() -> FakeClient:
    return FakeClient(rows=[{"n": 42, "key_sum": 903}])


@pytest.fixture
def users_client() -> FakeClient:
    # Columns out of order and timestamps with a time zone, like BigQuery's arrow output.
    return FakeClient(
        pa.table(
            {
                "created_at": pa.array(
                    [datetime(2026, 1, 5, 23, 30, tzinfo=UTC)], pa.timestamp("us", "UTC")
                ),
                "traffic_source": ["Search"],
                "country": ["Japan"],
                "id": pa.array([7], pa.int64()),
            }
        )
    )


def test_a_window_query_selects_the_source_columns_of_the_table() -> None:
    sql, _ = build_query(SOURCE_TABLES["order_items"], START, END)
    assert sql.startswith(f"select {', '.join(SOURCE_TABLES['order_items'].columns)}\n")
    assert f"from `{DATASET}.order_items`" in sql


def test_a_window_query_binds_both_bounds_as_parameters() -> None:
    sql, params = build_query(SOURCE_TABLES["order_items"], START, END)
    assert sql.endswith("where created_at >= @start and created_at < @end")
    assert [(p.name, p.type_, p.value) for p in params] == [
        ("start", "TIMESTAMP", START),
        ("end", "TIMESTAMP", END),
    ]


def test_a_first_run_query_has_no_lower_bound() -> None:
    sql, params = build_query(SOURCE_TABLES["events"], None, END)
    assert sql.endswith("where created_at < @end")
    assert [p.name for p in params] == ["end"]


def test_a_table_without_a_watermark_is_queried_whole() -> None:
    sql, params = build_query(SOURCE_TABLES["products"], START, END)
    assert "where" not in sql
    assert params == []


def test_bigquery_history_returns_the_row_count_and_key_sum(history_client: FakeClient) -> None:
    reader = reader_for(history_client)
    assert reader.history(SOURCE_TABLES["orders"], START, END) == (42, 903)


def test_bigquery_history_query_counts_rows_before_a_bound_timestamp(
    history_client: FakeClient,
) -> None:
    reader_for(history_client).history(SOURCE_TABLES["orders"], START, END)
    sql, params = history_client.calls[0]
    assert "count(*) as n, coalesce(sum(order_id), 0) as key_sum" in sql
    assert sql.endswith("where created_at < @before")
    assert [(p.name, p.value) for p in params] == [("before", START)]


def test_bigquery_reader_adds_up_the_bytes_of_its_queries(history_client: FakeClient) -> None:
    reader = reader_for(history_client)
    reader.history(SOURCE_TABLES["orders"], START, END)
    assert (reader.bytes_processed, reader.bytes_billed) == (1_500_000, 10_485_760)


def test_bigquery_queries_bill_at_most_a_billion_bytes_by_default(
    users_client: FakeClient, history_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BQ_MAX_BYTES_BILLED", raising=False)
    reader_for(users_client).read(SOURCE_TABLES["users"], START, END)
    reader_for(history_client).history(SOURCE_TABLES["orders"], START, END)
    configs = users_client.job_configs + history_client.job_configs
    assert [c.maximum_bytes_billed for c in configs] == [1_000_000_000, 1_000_000_000]


def test_bigquery_byte_limit_comes_from_the_environment(
    history_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BQ_MAX_BYTES_BILLED", "500000000")
    reader_for(history_client).history(SOURCE_TABLES["orders"], START, END)
    assert history_client.job_configs[0].maximum_bytes_billed == 500_000_000


@pytest.mark.parametrize("value", ["0", "-5", "1e9", "1.5", "lots"])
def test_a_bad_bigquery_byte_limit_stops_the_run_naming_the_variable(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BQ_MAX_BYTES_BILLED", value)
    with pytest.raises(SystemExit, match="BQ_MAX_BYTES_BILLED"):
        reader_for(FakeClient())


def test_bigquery_reader_returns_the_source_schema_with_naive_utc_timestamps(
    users_client: FakeClient,
) -> None:
    table = SOURCE_TABLES["users"]
    out = reader_for(users_client).read(table, START, END)
    assert out.schema == arrow_schema(table)
    assert out.to_pylist() == [
        {
            "id": 7,
            "country": "Japan",
            "traffic_source": "Search",
            "created_at": datetime(2026, 1, 5, 23, 30),
        }
    ]


def test_bigquery_reader_queries_the_window_it_was_asked_for(users_client: FakeClient) -> None:
    reader_for(users_client).read(SOURCE_TABLES["users"], START, END)
    assert "created_at >= @start" in users_client.calls[0][0]


def test_fixture_reader_returns_the_source_schema() -> None:
    table = SOURCE_TABLES["orders"]
    assert FixtureReader().read(table, START, END).schema == arrow_schema(table)


def test_fixture_reader_returns_only_rows_inside_the_window() -> None:
    out = FixtureReader().read(SOURCE_TABLES["orders"], START, END)
    created = out.column("created_at").to_pylist()
    assert created
    assert all(START.replace(tzinfo=None) <= c < END.replace(tzinfo=None) for c in created)


def test_history_of_a_table_without_a_watermark_is_an_error() -> None:
    with pytest.raises(ValueError, match="products has no watermark column"):
        FixtureReader().history(SOURCE_TABLES["products"], START, END)
