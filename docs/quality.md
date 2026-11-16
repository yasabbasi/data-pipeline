# Data quality

`data-pipeline check` runs after every ingest. It checks the latest ingested data,
splits each `raw_<table>` into `clean_<table>` (what the metrics layer reads) and
`quarantine_<table>`, and writes `data/runs/<run_id>/quality_report.json`, which follows
[`contract/v1/quality-report.schema.json`](../contract/v1/quality-report.schema.json).
The engine is [`quality.py`](../src/data_pipeline/quality.py); the checks for the thelook
tables live in [`checks.py`](../src/data_pipeline/sources/thelook/checks.py).

## Policy

- **Bad rows are quarantined, never dropped.** Every row that fails a row-level check
  moves to `quarantine_<table>`, with a `failed_checks` column naming the checks it failed.
  Row-level checks have severity `warning`: the run continues with status `warn`.
- **Quarantine cascades.** Referential checks compare against the parent's *clean* table,
  so if an order is quarantined, its items are too. Otherwise they would silently vanish
  from joins in the metrics layer.
- **Critical checks fail the run.** A failed run publishes nothing (exit code 1). Each table
  has two critical checks, and there is one freshness check:
  - **Volume:** the table is not empty.
  - **Quarantine rate:** no more than 1% of the table's rows are quarantined. A few bad
    rows are expected; many mean the source or the pipeline is broken.
  - **Freshness:** the newest order is at most 2 days older than the run's `as_of`. The
    source refreshes daily.

## Row-level checks

| Check type | What fails | Tables |
|---|---|---|
| `not_null` | Keys and the columns the metrics use are NULL | all |
| `unique` | Primary key appears more than once (all copies are quarantined) | all |
| `accepted_values` | Unknown order/item status or event type | orders, order_items, events |
| `range` | `sale_price <= 0` | order_items |
| `chronology` | Lifecycle out of order (`shipped < created` for orders; `delivered < shipped`; `returned < delivered`) | orders, order_items |
| `consistency` | `returned_at` set without status `Returned`, or the reverse | order_items |
| `format` | A product event's `uri` is not `/product/<id>` | events |
| `referential` | Distribution center, order or product missing or quarantined | products, order_items, events |

An item's `created_at` is deliberately **not** checked against its `shipped_at`. The live
source stamps items from 4 hours before to 4 days after their order, so 30% of shipped
items would fail (see [source-profile.md](source-profile.md)). Sales are dated by the
order instead.

## Inspecting a run

```sql
-- DuckDB, against data/warehouse/warehouse.duckdb
select
    failed_checks,
    count(*) as n_rows
from quarantine_order_items
group by failed_checks;
```
