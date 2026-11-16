"""Command line entry point: `data-pipeline <command>`."""

import argparse
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING

from data_pipeline.paths import OUTPUT_DIR, RUNS_DIR, WAREHOUSE_DB

if TYPE_CHECKING:
    from data_pipeline.quality import QualityReport

_PUBLISHED_RUN = """
select
    run_id, source, data_min_date, data_max_date, restated_from, quality_status,
    strftime(generated_at at time zone 'UTC', '%Y-%m-%d %H:%M') as generated_at
from run_metadata
"""
_RECENT_TOTALS = """
select
    metric_date, revenue, orders, units, returned_units, aov, return_rate, conversion_rate
from daily_totals
order by metric_date desc
limit ?
"""


def build_parser() -> argparse.ArgumentParser:
    """Return the parser for every command and its options."""
    parser = argparse.ArgumentParser(
        prog="data-pipeline",
        description="Ingest thelook_ecommerce, check quality, and build daily metrics.",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version('data-pipeline')}"
    )
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="Ingest, then build and publish (the whole pipeline)")
    _ingest_args(run)
    _build_args(run, db=False)
    ingest = sub.add_parser("ingest", help="Incrementally load the source tables into DuckDB")
    _ingest_args(ingest)
    check = sub.add_parser("check", help="Run data-quality checks on the latest ingested data")
    _build_args(check, output=False)
    build = sub.add_parser(
        "build", help="Check quality, build the metrics, and publish the contract outputs"
    )
    _build_args(build)
    show = sub.add_parser("show", help="Print the published run and its latest daily totals")
    _output_arg(show)
    show.add_argument("--days", type=int, default=7, help="how many recent days (default 7)")
    return parser


def _ingest_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--source",
        choices=["fixtures", "bigquery"],
        default="fixtures",
        help="synthetic fixtures (default, offline) or live BigQuery (needs gcloud ADC)",
    )
    p.add_argument(
        "--as-of",
        type=_utc_datetime,
        default=None,
        help="read the source as of this UTC time (ISO 8601; default: now for bigquery, "
        "the end of the fixture window for fixtures)",
    )
    p.add_argument(
        "--full-refresh", action="store_true", help="ignore earlier runs and reload everything"
    )
    p.add_argument("--db", type=Path, default=WAREHOUSE_DB, help="working DuckDB file")


def _build_args(p: argparse.ArgumentParser, output: bool = True, db: bool = True) -> None:
    if db:  # `run` already has --db from its ingest arguments
        p.add_argument("--db", type=Path, default=WAREHOUSE_DB, help="working DuckDB file")
    p.add_argument(
        "--runs-dir", type=Path, default=RUNS_DIR, help="where <run_id>/quality_report.json goes"
    )
    if output:
        _output_arg(p)


def _output_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.environ.get("DATA_OUTPUT_DIR") or OUTPUT_DIR),
        help="published outputs (default: $DATA_OUTPUT_DIR or data/output)",
    )


def main(argv: list[str] | None = None) -> int:
    """Run one command and return its exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    commands: dict[str, Callable[[argparse.Namespace], int]] = {
        "run": _run,
        "ingest": _ingest,
        "check": _check,
        "build": _build,
        "show": _show,
    }
    if args.command in commands:
        return commands[args.command](args)
    parser.print_help()
    return 0


# The modules behind each command are imported inside it, so --version and --help stay fast.


def _run(args: argparse.Namespace) -> int:
    started = time.perf_counter()
    status = _ingest(args) or _build(args)
    print(f"Finished in {time.perf_counter() - started:.1f}s")
    return status


def _ingest(args: argparse.Namespace) -> int:
    from data_pipeline import ingest, readers
    from data_pipeline.sources.thelook import fixtures

    reader: readers.Reader
    if args.source == "bigquery":
        reader = readers.BigQueryReader()
        as_of = args.as_of or datetime.now(UTC).replace(microsecond=0)
    else:
        reader = readers.FixtureReader()
        as_of = args.as_of or fixtures.AS_OF
    with ingest.connect(args.db) as con:
        try:
            results = ingest.run_ingest(con, reader, as_of, full_refresh=args.full_refresh)
        except ingest.IngestError as err:
            print(f"error: {err}", file=sys.stderr)
            return 1
    print(f"Ingested {args.source} as of {as_of:%Y-%m-%d %H:%M} UTC into {args.db}")
    for r in results:
        window = f"from {r.window_start:%Y-%m-%d %H:%M}" if r.window_start else "full"
        print(f"  {r.table:<22} {window:<22} read {r.rows_read:>9,}  total {r.rows_total:>9,}")
    if isinstance(reader, readers.BigQueryReader):
        print(
            f"  BigQuery: {reader.bytes_processed / 1e6:,.0f} MB processed, "
            f"{reader.bytes_billed / 1e6:,.0f} MB billed"
        )
    return 0


def _check(args: argparse.Namespace) -> int:
    from data_pipeline import ingest, quality

    if not args.db.exists():
        print(f"error: {args.db} not found: run `data-pipeline ingest` first", file=sys.stderr)
        return 1
    with ingest.connect(args.db) as con:
        try:
            report = quality.run_checks(con)
        except quality.QualityError as err:
            print(f"error: {err}", file=sys.stderr)
            return 1
    _print_quality(report, quality.write_report(report, args.runs_dir))
    return 1 if report.status == "fail" else 0


def _build(args: argparse.Namespace) -> int:
    from data_pipeline import ingest, metrics_layer, publish, quality

    if not args.db.exists():
        print(f"error: {args.db} not found: run `data-pipeline ingest` first", file=sys.stderr)
        return 1
    with ingest.connect(args.db) as con:
        try:
            result = publish.build_and_publish(con, args.output_dir, args.runs_dir)
        except (quality.QualityError, metrics_layer.MetricsError, publish.PublishError) as err:
            print(f"error: {err}", file=sys.stderr)
            return 1
    _print_quality(result.report, result.report_path)
    meta = result.run_metadata
    if meta is None:
        print("Nothing published: a critical quality check failed.", file=sys.stderr)
        return 1
    print(
        f"Published to {args.output_dir}: {meta.data_min_date} to {meta.data_max_date}"
        f" (restatable from {meta.restated_from})"
    )
    for table, rows in result.row_counts.items():
        print(f"  {table:<22} {rows:>9,} rows")
    return 0


def _show(args: argparse.Namespace) -> int:
    import duckdb

    db = args.output_dir / "metrics.duckdb"
    if not db.exists():
        print(f"error: {db} not found: run `data-pipeline build` first", file=sys.stderr)
        return 1
    with duckdb.connect(str(db), read_only=True) as con:
        run_id, source, first, last, restated, status, generated = con.execute(
            _PUBLISHED_RUN
        ).fetchall()[0]
        rows = con.execute(_RECENT_TOTALS, [args.days]).fetchall()
    print(f"Run {run_id} ({source}), published {generated} UTC, quality {status}")
    print(f"Data {first} to {last}; rows from {restated} on may still change\n")
    print("| Date | Revenue | Orders | Units | Returned | AOV | Return rate | Conversion |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for d, revenue, orders, units, returned, aov, return_rate, conversion in reversed(rows):
        print(
            f"| {d}{' *' if d >= restated else ''} | {revenue:,.2f} | {orders:,} | {units:,} "
            f"| {returned:,} | {_num(aov)} | {_pct(return_rate)} | {_pct(conversion)} |"
        )
    if any(row[0] >= restated for row in rows):
        print("\n* may still change")
    return 0


def _num(value: float | None) -> str:
    return "–" if value is None else f"{value:,.2f}"


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:.1%}"


def _print_quality(report: QualityReport, path: Path) -> None:
    print(f"Quality {report.status.upper()} for run {report.run_id} ({report.source})")
    for t in report.tables:
        print(
            f"  {t.table:<22} ingested {t.rows_ingested:>9,}  quarantined {t.rows_quarantined:>7,}"
        )
    for c in report.checks:
        if c.status == "fail":
            print(f"  FAIL [{c.severity}] {c.name}: {c.message}")
    print(f"Report: {path}")


def _utc_datetime(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


if __name__ == "__main__":
    sys.exit(main())
