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
from data_pipeline.quality import CheckResult, QualityError, QualityReport, run_checks, write_report
from data_pipeline.readers import FixtureReader
from data_pipeline.sources.thelook import fixtures
from data_pipeline.sources.thelook.checks import row_checks
from data_pipeline.sources.thelook.tables import SOURCE_TABLES


def by_name(report: QualityReport) -> dict[str, CheckResult]:
    return {c.name: c for c in report.checks}


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    value: int = con.execute(sql, params).fetchall()[0][0]
    return value


def count(
    con: duckdb.DuckDBPyConnection, table: str, where: str = "true", params: list[Any] | None = None
) -> int:
    return scalar(con, f"select count(*) from {table} where {where}", params)


def duplicate_row(con: duckdb.DuckDBPyConnection, table: str, key: int) -> None:
    source = SOURCE_TABLES[table]
    cols = ", ".join(source.columns)
    con.execute(
        f"""
        insert into raw_{table} ({cols})
        select {cols}
        from raw_{table}
        where {source.primary_key} = ?
        """,
        [key],
    )


@pytest.fixture
def con(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with connect(tmp_path / "warehouse.duckdb") as con:
        run_ingest(con, FixtureReader(), fixtures.AS_OF)
        yield con


@pytest.fixture
def clean_report(con: duckdb.DuckDBPyConnection) -> QualityReport:
    return run_checks(con)


@pytest.fixture
def bad_rows_report(con: duckdb.DuckDBPyConnection) -> QualityReport:
    first_item = scalar(con, "select min(id) from raw_order_items")
    con.execute("update raw_order_items set sale_price = ? where id = ?", [-1, first_item])
    duplicate_row(con, "order_items", scalar(con, "select max(id) from raw_order_items"))
    first_view = scalar(con, "select min(id) from raw_events where event_type = ?", ["product"])
    con.execute("update raw_events set uri = ? where id = ?", ["/product/abc", first_view])
    return run_checks(con)


def test_clean_fixtures_pass_every_check(clean_report: QualityReport) -> None:
    assert clean_report.status == "pass"
    assert [c.name for c in clean_report.checks if c.status == "fail"] == []


def test_clean_fixtures_quarantine_nothing(
    con: duckdb.DuckDBPyConnection, clean_report: QualityReport
) -> None:
    for t in clean_report.tables:
        assert t.rows_ingested > 0 and t.rows_quarantined == 0
        assert count(con, f"clean_{t.table}") == t.rows_ingested


def test_the_report_matches_the_contract(clean_report: QualityReport) -> None:
    assert contract.validate("quality_report", [clean_report.model_dump(mode="json")]) == []


def test_the_report_lists_its_keys_in_contract_order(clean_report: QualityReport) -> None:
    schema = contract.load_schema("quality_report")["properties"]
    report = json.loads(clean_report.to_json())
    assert list(report) == list(schema)
    assert list(report["tables"][0]) == list(schema["tables"]["items"]["properties"])
    assert list(report["checks"][0]) == list(schema["checks"]["items"]["properties"])


def test_the_report_cannot_be_changed(clean_report: QualityReport) -> None:
    with pytest.raises(ValidationError):
        clean_report.status = "fail"


def test_check_names_in_a_report_are_unique(clean_report: QualityReport) -> None:
    names = [c.name for c in clean_report.checks]
    assert len(names) == len(set(names))


def test_every_check_has_a_unique_name() -> None:
    names = [c.name for c in row_checks()]
    assert len(names) == len(set(names))


def test_a_run_with_bad_rows_warns(bad_rows_report: QualityReport) -> None:
    assert bad_rows_report.status == "warn"


def test_bad_rows_are_counted_for_each_check_they_fail(bad_rows_report: QualityReport) -> None:
    checks = by_name(bad_rows_report)
    assert checks["order_items.sale_price.positive"].failed_rows == 1
    assert checks["order_items.id.unique"].failed_rows == 2  # both copies
    assert checks["events.uri.product_format"].failed_rows == 1


def test_a_malformed_product_uri_is_not_also_a_missing_product(
    bad_rows_report: QualityReport,
) -> None:
    assert by_name(bad_rows_report)["events.product_id.referential"].failed_rows == 0


def test_quarantined_rows_name_the_checks_they_failed(
    con: duckdb.DuckDBPyConnection, bad_rows_report: QualityReport
) -> None:
    rows = con.execute("select failed_checks from quarantine_order_items").fetchall()
    reasons = sorted(r[0] for r in rows)
    assert reasons == ["order_items.id.unique"] * 2 + ["order_items.sale_price.positive"]


def test_quarantined_rows_are_left_out_of_the_clean_table(
    con: duckdb.DuckDBPyConnection, bad_rows_report: QualityReport
) -> None:
    items = next(t for t in bad_rows_report.tables if t.table == "order_items")
    assert items.rows_quarantined == 3
    assert count(con, "clean_order_items") == items.rows_ingested - 3


def test_children_of_a_quarantined_row_are_quarantined_too(
    con: duckdb.DuckDBPyConnection,
) -> None:
    order_id = scalar(con, "select order_id from raw_order_items limit 1")
    n_items = count(con, "raw_order_items", "order_id = ?", [order_id])
    con.execute("update raw_orders set status = ? where order_id = ?", ["Teleported", order_id])
    checks = by_name(run_checks(con))
    assert checks["orders.status.accepted_values"].failed_rows == 1
    assert checks["order_items.order_id.referential"].failed_rows == n_items
    assert count(con, "clean_order_items", "order_id = ?", [order_id]) == 0


def test_a_missing_reference_fails_only_the_not_null_check(
    con: duckdb.DuckDBPyConnection,
) -> None:
    first_item = scalar(con, "select min(id) from raw_order_items")
    con.execute("update raw_order_items set order_id = null where id = ?", [first_item])
    checks = by_name(run_checks(con))
    assert checks["order_items.order_id.not_null"].failed_rows == 1
    assert checks["order_items.order_id.referential"].failed_rows == 0


def test_rows_without_a_key_are_not_duplicates_of_each_other(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute("update raw_users set id = null where id <= ?", [2])
    checks = by_name(run_checks(con))
    assert checks["users.id.not_null"].failed_rows == 2
    assert checks["users.id.unique"].failed_rows == 0


def test_an_empty_table_fails_the_run(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("delete from raw_distribution_centers")
    report = run_checks(con)
    assert report.status == "fail"
    assert by_name(report)["distribution_centers.volume.not_empty"].status == "fail"


def test_every_child_of_an_empty_table_is_quarantined(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("delete from raw_distribution_centers")
    checks = by_name(run_checks(con))
    n_products = count(con, "raw_products")
    assert checks["products.distribution_center_id.referential"].failed_rows == n_products
    assert checks["products.quarantine_rate"].status == "fail"


def test_too_many_quarantined_rows_fail_the_run(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("update raw_events set event_type = ? where id % 20 = 0", ["teleport"])  # ~5%
    report = run_checks(con)
    checks = by_name(report)
    assert report.status == "fail"
    assert checks["events.event_type.accepted_values"].severity == "warning"
    assert checks["events.quarantine_rate"].status == "fail"


def test_a_stale_source_fails_freshness(tmp_path: Path) -> None:
    with connect(tmp_path / "warehouse.duckdb") as con:
        run_ingest(con, FixtureReader(), fixtures.AS_OF + timedelta(days=5))
        report = run_checks(con)
    assert report.status == "fail"
    assert by_name(report)["orders.created_at.freshness"].status == "fail"


def test_checks_need_an_ingested_run(tmp_path: Path) -> None:
    with connect(tmp_path / "empty.duckdb") as con, pytest.raises(QualityError):
        run_checks(con)


def test_the_report_is_written_under_its_run_id(
    clean_report: QualityReport, tmp_path: Path
) -> None:
    path = write_report(clean_report, tmp_path / "runs")
    assert path == tmp_path / "runs" / clean_report.run_id / "quality_report.json"


def test_the_written_report_holds_the_whole_report(
    clean_report: QualityReport, tmp_path: Path
) -> None:
    path = write_report(clean_report, tmp_path / "runs")
    assert QualityReport.model_validate_json(path.read_text()) == clean_report
