"""Daily metrics from the clean tables, generated from metrics.yaml."""

from datetime import date

import duckdb

from data_pipeline import contract
from data_pipeline.metrics_spec import SOURCES, Metric, load_metrics
from data_pipeline.sources.thelook.views import SLICE, load_views

_GRAINS = {
    "daily_metrics": ("metric_date", *SLICE),
    "daily_totals": ("metric_date",),
}
_LINE = "\n            "
_EVERY_DAY = _LINE.join(
    [
        "select cast(d as date) as metric_date",
        "from generate_series(",
        "    cast($first_date as date), cast($last_date as date), interval 1 day",
        ") as days(d)",
    ]
)


class MetricsError(Exception):
    """Metrics that cannot be built: no complete day, or metrics.yaml differs from the contract."""


def build(
    con: duckdb.DuckDBPyConnection, as_of_date: date, metrics: dict[str, Metric] | None = None
) -> tuple[date, date]:
    """Create daily_metrics, daily_totals and metric_definitions; return the date range."""
    metrics = metrics or load_metrics()
    _check_metrics_match_contract(metrics)
    for name, select in load_views().items():
        con.execute(f"create or replace view {name} as\n{select}")
    first, last = _complete_days(con, as_of_date)
    con.execute(_metrics_sql("daily_metrics", metrics), {"as_of_date": as_of_date})
    con.execute(
        _metrics_sql("daily_totals", metrics, grain_rows=_EVERY_DAY),
        {"as_of_date": as_of_date, "first_date": first, "last_date": last},
    )
    _metric_definitions(con, metrics)
    return first, last


def _complete_days(con: duckdb.DuckDBPyConnection, as_of_date: date) -> tuple[date, date]:
    days = f"{_LINE}union all{_LINE}".join(
        f"select metric_date{_LINE}from {source}{_LINE}where metric_date < $as_of_date"
        for source in SOURCES
    )
    first, last = con.execute(
        f"""
        with complete_days as (
            {days}
        )
        select min(metric_date), max(metric_date)
        from complete_days
        """,
        {"as_of_date": as_of_date},
    ).fetchall()[0]
    if first is None:
        raise MetricsError(f"no complete day of data before {as_of_date}")
    return first, last


def _metrics_sql(table: str, metrics: dict[str, Metric], grain_rows: str | None = None) -> str:
    keys = ", ".join(_GRAINS[table])
    aggregates = ",\n        ".join(_aggregate_cte(source, keys, metrics) for source in SOURCES)
    # Without grain_rows the table is sparse: one row per grain value with activity.
    grain_rows = grain_rows or f"{_LINE}union{_LINE}".join(
        f"select {keys}{_LINE}from {source}_agg" for source in SOURCES
    )
    joins = _LINE.join(f"left join {source}_agg using ({keys})" for source in SOURCES)
    values = ", ".join(
        f"coalesce({m.name}, 0) as {m.name}" for m in metrics.values() if m.agg != "ratio"
    )
    columns = ", ".join(
        f"cast({_column_expr(name, metrics)} as {typ.lower()}) as {name}"
        for name, typ in contract.columns(table).items()
    )
    return f"""
        create or replace table {table} as
        with {aggregates},
        grain as (
            {grain_rows}
        ),
        filled as (
            select {keys}, {values}
            from grain
            {joins}
        )
        select {columns}
        from filled
        order by {", ".join(contract.load_schema(table)["x-grain"])}
        """


def _aggregate_cte(source: str, keys: str, metrics: dict[str, Metric]) -> str:
    aggregates = ", ".join(
        f"{_aggregate(m)} as {m.name}" for m in metrics.values() if m.source == source
    )
    return f"""{source}_agg as (
            select {keys}, {aggregates}
            from {source}
            where metric_date < $as_of_date
            group by {keys}
        )"""


def _aggregate(m: Metric) -> str:
    where = f" filter (where {m.filter})" if m.filter else ""
    if m.agg == "sum":
        return f"sum({m.expr}){where}"
    if m.agg == "count":
        return f"count(*){where}"
    return f"count(distinct {m.expr}){where}"


def _column_expr(name: str, metrics: dict[str, Metric]) -> str:
    m = metrics.get(name)
    if m is None:  # a grain column
        return name
    expr = f"{m.numerator} / nullif({m.denominator}, 0)" if m.agg == "ratio" else name
    return f"round({expr}, 2)" if m.unit == "usd" else expr


def _check_metrics_match_contract(metrics: dict[str, Metric]) -> None:
    for table, grain in _GRAINS.items():
        expected = [c for c in contract.columns(table) if c not in grain]
        if list(metrics) != expected:
            raise MetricsError(
                f"metrics.yaml {list(metrics)} differs from contract {table} columns {expected}"
            )


def _metric_definitions(con: duckdb.DuckDBPyConnection, metrics: dict[str, Metric]) -> None:
    columns = contract.columns("metric_definitions")
    con.execute(
        "create or replace table metric_definitions "
        f"({', '.join(f'{c} {t.lower()}' for c, t in columns.items())})"
    )
    con.executemany(
        f"""
        insert into metric_definitions ({", ".join(columns)})
        values ({", ".join("?" * len(columns))})
        """,
        [
            [m.name, m.label, m.description, m.unit, m.agg, m.formula, m.additive]
            for m in metrics.values()
        ],
    )
