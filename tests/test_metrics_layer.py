from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from data_pipeline import contract
from data_pipeline.ingest import connect, run_ingest
from data_pipeline.metrics_layer import MetricsError, build
from data_pipeline.metrics_spec import load_metrics
from data_pipeline.quality import QualityReport, run_checks
from data_pipeline.readers import FixtureReader
from data_pipeline.sources.thelook import fixtures
from data_pipeline.sources.thelook.checks import PRODUCT_ID_FROM_URI
from data_pipeline.sources.thelook.views import load_views

AS_OF = fixtures.AS_OF
GAP = date(2026, 1, 15)
SLICE_TABLES = ("daily_by_category", "daily_by_distribution_center")
COUNTS = ("orders", "units", "returned_units", "product_view_sessions", "converted_sessions")


@dataclass
class Sales:
    revenue: float = 0.0
    orders: set[int] = field(default_factory=set)
    units: int = 0
    returned_units: int = 0


def rows(
    con: duckdb.DuckDBPyConnection, table: str, where: str = "true", params: list[Any] | None = None
) -> list[dict[str, Any]]:
    names = list(contract.columns(table))
    found = con.execute(f"select {', '.join(names)} from {table} where {where}", params).fetchall()
    return [dict(zip(names, row, strict=True)) for row in found]


# Daily totals recomputed in plain Python from the fixture source: an independent oracle.
def expected_totals() -> dict[date, dict[str, float]]:
    source = fixtures.generate(as_of=AS_OF)
    placed = {o["order_id"]: o["created_at"].date() for o in source["orders"]}
    sales: defaultdict[date, Sales] = defaultdict(Sales)
    for item in source["order_items"]:
        day = placed[item["order_id"]]
        if item["created_at"] < AS_OF and item["status"] != "Cancelled" and day < AS_OF.date():
            sales[day].revenue += item["sale_price"]
            sales[day].orders.add(item["order_id"])
            sales[day].units += 1
            sales[day].returned_units += item["returned_at"] is not None
    events = [e for e in source["events"] if e["created_at"] < AS_OF]
    purchased = {e["session_id"] for e in events if e["event_type"] == "purchase"}
    views: defaultdict[date, set[str]] = defaultdict(set)
    for event in events:
        if event["event_type"] == "product" and event["created_at"].date() < AS_OF.date():
            views[event["created_at"].date()].add(event["session_id"])
    return {
        day: {
            "revenue": round(sales[day].revenue, 2),
            "orders": len(sales[day].orders),
            "units": sales[day].units,
            "returned_units": sales[day].returned_units,
            "product_view_sessions": len(views[day]),
            "converted_sessions": len(views[day] & purchased),
        }
        for day in set(sales) | set(views)
    }


def expected_orders_by_category() -> dict[tuple[date, str], set[int]]:
    source = fixtures.generate(as_of=AS_OF)
    placed = {o["order_id"]: o["created_at"].date() for o in source["orders"]}
    category = {p["id"]: p["category"] for p in source["products"]}
    orders: defaultdict[tuple[date, str], set[int]] = defaultdict(set)
    for item in source["order_items"]:
        day = placed[item["order_id"]]
        if item["created_at"] < AS_OF and item["status"] != "Cancelled" and day < AS_OF.date():
            orders[(day, category[item["product_id"]])].add(item["order_id"])
    return dict(orders)


def slice_table_sums(con: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    build(con, AS_OF.date())
    found = con.execute(
        f"""
        with slice_sums as (
            select
                metric_date,
                sum(revenue) as revenue,
                sum(units) as units,
                sum(returned_units) as returned_units,
                max(orders) as max_orders,
                count(*) as n_slices
            from {table}
            group by metric_date
        )
        select
            t.revenue, t.units, t.returned_units, t.orders,
            s.revenue, s.units, s.returned_units, s.max_orders, s.n_slices
        from daily_totals as t
        inner join slice_sums as s
            on t.metric_date = s.metric_date
        """
    ).fetchall()
    keys = [
        *("total_revenue", "total_units", "total_returned", "total_orders"),
        *("revenue", "units", "returned_units", "max_orders", "n_slices"),
    ]
    return [dict(zip(keys, row, strict=True)) for row in found]


@pytest.fixture
def con(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with connect(tmp_path / "warehouse.duckdb") as con:
        run_ingest(con, FixtureReader(), AS_OF)
        run_checks(con)
        yield con


@pytest.fixture(scope="module")
def expected() -> dict[date, dict[str, float]]:
    return expected_totals()


@pytest.fixture
def totals(con: duckdb.DuckDBPyConnection) -> dict[date, dict[str, Any]]:
    build(con, AS_OF.date())
    return {row["metric_date"]: row for row in rows(con, "daily_totals")}


@pytest.fixture
def slice_sums(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    build(con, AS_OF.date())
    names = ["revenue", "units", "returned_units", "orders"]
    found = con.execute(
        """
        with slice_sums as (
            select
                metric_date,
                sum(revenue) as revenue,
                sum(units) as units,
                sum(returned_units) as returned_units,
                sum(orders) as orders,
                count(*) as n_slices
            from daily_metrics
            group by metric_date
        )
        select
            t.revenue, t.units, t.returned_units, t.orders,
            s.revenue, s.units, s.returned_units, s.orders, s.n_slices
        from daily_totals as t
        inner join slice_sums as s
            on t.metric_date = s.metric_date
        """
    ).fetchall()
    keys = [*(f"total_{n}" for n in names), *(f"sliced_{n}" for n in names), "n_slices"]
    return [dict(zip(keys, row, strict=True)) for row in found]


@pytest.fixture
def gap_report(con: duckdb.DuckDBPyConnection) -> QualityReport:
    con.execute(
        """
        delete from raw_order_items
        using raw_orders
        where raw_order_items.order_id = raw_orders.order_id
            and cast(raw_orders.created_at as date) = ?
        """,
        [GAP],
    )
    con.execute("delete from raw_orders where cast(created_at as date) = ?", [GAP])
    con.execute("delete from raw_events where cast(created_at as date) = ?", [GAP])
    return run_checks(con)


@pytest.fixture
def gap_con(con: duckdb.DuckDBPyConnection, gap_report: QualityReport) -> duckdb.DuckDBPyConnection:
    build(con, AS_OF.date())
    return con


def test_build_returns_the_first_and_last_day_with_data(
    con: duckdb.DuckDBPyConnection, expected: dict[date, dict[str, float]]
) -> None:
    assert build(con, AS_OF.date()) == (min(expected), max(expected))


def test_daily_totals_has_a_row_for_every_day_with_activity(
    totals: dict[date, dict[str, Any]], expected: dict[date, dict[str, float]]
) -> None:
    assert totals.keys() == expected.keys()


def test_daily_totals_counts_match_an_independent_computation(
    totals: dict[date, dict[str, Any]], expected: dict[date, dict[str, float]]
) -> None:
    for day, exp in expected.items():
        for metric in COUNTS:
            assert totals[day][metric] == exp[metric], (day, metric)


def test_daily_totals_revenue_matches_an_independent_computation(
    totals: dict[date, dict[str, Any]], expected: dict[date, dict[str, float]]
) -> None:
    for day, exp in expected.items():
        assert totals[day]["revenue"] == pytest.approx(exp["revenue"], abs=0.005), day


def test_aov_is_revenue_divided_by_orders_in_cents(
    totals: dict[date, dict[str, Any]], expected: dict[date, dict[str, float]]
) -> None:
    for day, exp in expected.items():
        aov = round(exp["revenue"] / exp["orders"], 2)
        assert totals[day]["aov"] == pytest.approx(aov, abs=0.01), day


def test_conversion_rate_is_converted_sessions_divided_by_product_view_sessions(
    totals: dict[date, dict[str, Any]], expected: dict[date, dict[str, float]]
) -> None:
    for day, exp in expected.items():
        rate = exp["converted_sessions"] / exp["product_view_sessions"]
        assert totals[day]["conversion_rate"] == pytest.approx(rate), day


def test_conversion_rate_is_not_integer_division(totals: dict[date, dict[str, Any]]) -> None:
    assert any(0 < row["conversion_rate"] < 1 for row in totals.values())


def test_units_and_returned_units_in_daily_metrics_sum_to_daily_totals(
    slice_sums: list[dict[str, Any]],
) -> None:
    assert slice_sums
    for row in slice_sums:
        assert row["sliced_units"] == row["total_units"]
        assert row["sliced_returned_units"] == row["total_returned_units"]


def test_revenue_in_daily_metrics_sums_to_daily_totals_within_cent_rounding(
    slice_sums: list[dict[str, Any]],
) -> None:
    assert slice_sums
    for row in slice_sums:
        tolerance = 0.005 * row["n_slices"]
        assert row["sliced_revenue"] == pytest.approx(row["total_revenue"], abs=tolerance)


def test_orders_are_not_additive_because_an_order_can_span_slices(
    slice_sums: list[dict[str, Any]],
) -> None:
    assert slice_sums
    for row in slice_sums:
        assert row["sliced_orders"] >= row["total_orders"]


@pytest.mark.parametrize("table", SLICE_TABLES)
def test_additive_metrics_in_a_slice_table_sum_to_daily_totals(
    con: duckdb.DuckDBPyConnection, table: str
) -> None:
    sums = slice_table_sums(con, table)
    assert sums
    for row in sums:
        assert (row["units"], row["returned_units"]) == (row["total_units"], row["total_returned"])
        tolerance = 0.005 * row["n_slices"]
        assert row["revenue"] == pytest.approx(row["total_revenue"], abs=tolerance)


@pytest.mark.parametrize("table", SLICE_TABLES)
def test_no_slice_has_more_orders_than_its_day(con: duckdb.DuckDBPyConnection, table: str) -> None:
    sums = slice_table_sums(con, table)
    assert sums
    for row in sums:
        assert row["max_orders"] <= row["total_orders"]


def test_an_order_with_items_in_two_categories_counts_once_in_each(
    con: duckdb.DuckDBPyConnection,
) -> None:
    build(con, AS_OF.date())
    expected = expected_orders_by_category()
    by_category = {
        (r["metric_date"], r["category"]): r["orders"]
        for r in rows(con, SLICE_TABLES[0], "orders > 0")
    }
    assert by_category == {key: len(orders) for key, orders in expected.items()}
    categories_by_order: defaultdict[int, set[str]] = defaultdict(set)
    for (_, category), orders in expected.items():
        for order in orders:
            categories_by_order[order].add(category)
    assert any(len(categories) >= 2 for categories in categories_by_order.values())


def test_a_day_without_source_rows_passes_the_quality_checks(gap_report: QualityReport) -> None:
    assert gap_report.status == "pass"


def test_a_day_without_activity_is_zero_in_daily_totals(gap_con: duckdb.DuckDBPyConnection) -> None:
    row = rows(gap_con, "daily_totals", "metric_date = ?", [GAP])[0]
    assert (row["revenue"], row["orders"], row["units"], row["product_view_sessions"]) == (
        0,
        0,
        0,
        0,
    )


def test_a_day_without_activity_has_null_ratios(gap_con: duckdb.DuckDBPyConnection) -> None:
    row = rows(gap_con, "daily_totals", "metric_date = ?", [GAP])[0]
    assert row["aov"] is None and row["return_rate"] is None and row["conversion_rate"] is None


def test_a_day_without_activity_has_no_rows_in_daily_metrics(
    gap_con: duckdb.DuckDBPyConnection,
) -> None:
    assert rows(gap_con, "daily_metrics", "metric_date = ?", [GAP]) == []


def test_only_complete_days_are_built(tmp_path: Path) -> None:
    as_of = AS_OF - timedelta(hours=12)  # 2026-01-30 12:00: Jan 30 is not complete yet
    with connect(tmp_path / "warehouse.duckdb") as con:
        run_ingest(con, FixtureReader(), as_of)
        run_checks(con)
        _, last = build(con, as_of.date())
    assert last == date(2026, 1, 29)


def test_metric_definitions_are_in_metrics_yaml_order(con: duckdb.DuckDBPyConnection) -> None:
    build(con, AS_OF.date())
    names = [row["metric_name"] for row in rows(con, "metric_definitions")]
    assert names == list(load_metrics())


def test_a_metric_definition_carries_its_formula_unit_and_additivity(
    con: duckdb.DuckDBPyConnection,
) -> None:
    build(con, AS_OF.date())
    revenue = rows(con, "metric_definitions")[0]
    formula = load_metrics()["revenue"].formula
    assert revenue["formula"] == formula == "SUM(sale_price) FROM sold_items"
    assert revenue["additive"] is True and revenue["unit"] == "usd"


def test_metrics_must_match_the_contract_columns(con: duckdb.DuckDBPyConnection) -> None:
    metrics = load_metrics()
    del metrics["conversion_rate"]
    with pytest.raises(MetricsError, match="differs from contract"):
        build(con, AS_OF.date(), metrics)


def test_the_product_views_sql_reads_the_product_id_as_the_quality_check_does() -> None:
    assert PRODUCT_ID_FROM_URI in load_views()["product_views"]
