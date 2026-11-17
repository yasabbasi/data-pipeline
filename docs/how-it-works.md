# How it works

One run is `data-pipeline run`: ingest, then build. Build checks quality, builds the metrics
and publishes. Each step is also a command of its own (`ingest`, `check`, `build`, `show`).
This page explains the choices behind each step. The code is in
[`src/data_pipeline/`](../src/data_pipeline/).

## Ingestion

[`ingest.py`](../src/data_pipeline/ingest.py) loads the source tables into a working DuckDB
file, `data/warehouse/warehouse.duckdb`, as `raw_<table>`.

- **As of.** Each run reads the source as of one UTC time, `as_of`. The default is now for
  BigQuery and the end of the fixture window for fixtures. Rows dated at or after `as_of`
  are left for a later run.
- **Window.** The first run, and any run with `--full-refresh`, reads everything before
  `as_of`. A later run reads tables that have a watermark column (`created_at`) from the
  previous run's `as_of` minus 14 days up to its own `as_of`, and replaces that window in
  the raw table. The two small tables without a watermark, `products` and
  `distribution_centers`, are replaced whole on every run.
- **Why 14 days.** Rows change after they first appear. Statuses update, returns arrive
  within 10.7 days of the order, and items and purchase events can be stamped up to 4 days
  after it. Re-reading 14 days picks all of that up.
- **Why the window starts from the previous `as_of`.** It never starts from the newest
  `created_at` in the warehouse. The live source stamps order items and purchase events up
  to 4 days ahead. A window that started from the newest timestamp would skip rows that
  only become readable once their time comes. See [source-profile.md](source-profile.md).
- **No key constraints.** The raw tables have no primary keys or not-null columns. Rows with
  missing or duplicate keys are kept so the quality checks can report and quarantine them.
- **One transaction.** Every read finishes before any raw table changes, so a failed read
  changes nothing. All raw-table writes of a run, and its rows in `ingest_log`, are one transaction.
- **Refused runs.** An incremental run stops if the warehouse holds data from another
  source, or if its `as_of` is before the last run's. `--full-refresh` accepts both.

### The history check

An incremental run does not re-read rows older than its window, so it assumes they have not
changed. Before it downloads any rows it checks that. For each table with a watermark it
asks the source for the row count and the sum of primary keys of the rows before the
window, and compares them with the warehouse. Each comparison is saved in `history_checks`.
If a table differs, the run stops and asks for `--full-refresh`. Mixing two versions of
history would publish wrong numbers.

thelook regenerates its whole history on every daily refresh. Row counts and even the
earliest timestamps change from one day to the next. An incremental live run would fail the
history check as soon as the source refreshed, so `make run` always passes `--full-refresh`.
The incremental path runs on the fixtures, where history is stable; `make demo` shows it.

## Sources and readers

[`tables.py`](../src/data_pipeline/sources/thelook/tables.py) lists the six source tables
with their columns, primary key and watermark. The fixture generator, both readers, the raw
tables and the quality checks all use this list, so they cannot drift apart.

Only the listed columns are ingested. `users` keeps `id`, `country`, `traffic_source` and
`created_at`. Names, emails, addresses and the other location columns are left out because
no metric needs them.

[`readers.py`](../src/data_pipeline/readers.py) has two readers. Both return a pyarrow table
with exactly the listed columns, in order, with timestamps as UTC without a time zone. For a
table with a watermark they return the rows in `[start, end)`; other tables come back whole.

- **`FixtureReader`** (`--source fixtures`, the default) serves the synthetic fixtures as
  they looked at `end`. Tests, `make demo` and `make sample` use it. It needs no network.
- **`BigQueryReader`** (`--source bigquery`) reads `bigquery-public-data.thelook_ecommerce`
  with read-only queries. Window bounds are bound as query parameters.
  - **Credentials** are Application Default Credentials
    (`gcloud auth application-default login`). There are no key files.
  - **Billing project** is `GCP_PROJECT`, or the default project of those credentials when
    it is unset.
  - **Download** goes over the BigQuery REST API, so no BigQuery Storage API permission is
    needed. It is the slowest step of a live run.
  - **Cost** is printed after each ingest as megabytes processed and billed.

## Fixtures

[`fixtures.py`](../src/data_pipeline/sources/thelook/fixtures.py) generates synthetic data
shaped like thelook. It holds no real thelook rows. The default is 30 days from 2026-01-01
with 60 users, 24 products and 3 distribution centers.

- **Determinism.** The same seed always gives the same universe of users, orders and
  sessions. Ids and random draws are consumed for hidden orders too, and event ids follow
  time order over the whole universe. Nothing shifts when `as_of` changes. The order of the
  random draws in `generate` must stay as it is.
- **What `as_of` hides.** `as_of` only decides what is visible. Orders placed after it are
  left out with their items and purchase sessions. Browse events after it are left out.
  Order lifecycles (shipped, delivered, returned) are cut off at it, and the status is the
  last stage reached. Generating again with a later `as_of` shows late updates to earlier
  orders. That is what incremental ingestion has to handle.
- **Live quirks, reproduced on purpose.**
  - An item's `created_at` is offset from its order's: 55% of items up to 4 hours before,
    the rest up to 4 days after. An item can fall on another day than its order, before its
    own `shipped_at`, or after `as_of`.
  - Every order item has its own purchase session. The purchase event carries the item's
    `created_at`, so it can come before the cart event or days later.
  - Returns land within about 10.5 days of the order.

## Quality

[quality.md](quality.md) has the policy and the list of checks. In short: row-level checks
move bad rows to `quarantine_<table>`, the rest go to `clean_<table>`, and a failed critical
check stops the run. What the code adds:

- **Two parts.** [`quality.py`](../src/data_pipeline/quality.py) is a generic engine. The
  thelook checks are data in [`checks.py`](../src/data_pipeline/sources/thelook/checks.py).
- **Parents first.** Tables are checked in an order that puts parents before children, so
  a referential check can read its parent's clean table.
- **Missing values.** A check whose condition is NULL passes. Missing values are caught by
  the not-null checks only.
- **Rebuilt every time.** Every check run rebuilds all clean and quarantine tables in one
  transaction, in the raw table's row order.
- **Status.** `fail` if any critical check fails, `warn` if only row-level checks fail,
  otherwise `pass`. The limits are 1% quarantined per table and 2 days of staleness.
- **Report.** The report is validated against the contract's schema before it is written.

## Metrics layer

[`metrics_layer.py`](../src/data_pipeline/metrics_layer.py) generates its SQL from
[`metrics.yaml`](../metrics.yaml). The metric definitions are in [contract.md](contract.md).

- **Two source views.** `sold_items` ([`sold-items.sql`](../sql/views/sold-items.sql)) is
  the order items that were not cancelled, dated by their order's `created_at`.
  `product_views` ([`product-views.sql`](../sql/views/product-views.sql)) is the product
  events, flagged when the same session has a purchase event. Both read the clean tables.
- **One aggregate per non-ratio metric.** Each is a `sum`, `count(*)` or `count(distinct)`
  over its view, with an optional filter, grouped by the table's grain.
- **Ratios from those aggregates.** A ratio is numerator over denominator, NULL when the
  denominator is 0. Dollar values are rounded to cents.
- **Complete UTC days only.** Rows dated on or after the date of the run's `as_of` are left
  out, so the current, partial day is never published. A run with no complete day fails.
- **Four tables.** `daily_metrics` has a row only where a slice had activity. `daily_totals`
  has every day from the first to the last, with zeros on days without activity. Its orders
  and sessions are counted over the whole day, not summed from slices. `daily_by_category`
  and `daily_by_distribution_center` are sparse like `daily_metrics`, with orders and
  sessions counted distinct within their category or distribution center.
- **The inner joins drop nothing.** The views join items to orders, products and
  distribution centers with inner joins. The quality checks have already quarantined every
  row whose parent is missing or quarantined, so each clean row finds its parents.
- **Same order as the contract.** The build stops if the metrics in `metrics.yaml` are not
  the contract's metric columns, in the same order.

## Publishing

[`publish.py`](../src/data_pipeline/publish.py) publishes the outputs as a set.

1. **Checks first.** The quality checks run and the report goes to
   `data/runs/<run_id>/quality_report.json`. If a critical check failed, the run ends here
   with exit code 1.
2. **Build.** The metrics layer is rebuilt in the working database and `run_metadata` gets
   its one row. `restated_from` is the date of `as_of` minus 14 days.
3. **Validate against the contract.** Column names and types must equal the schemas in
   [`contract/v2/`](../contract/v2/), the version the pipeline publishes. No table may have
   two rows for one grain value. `daily_totals` must have no gaps. Rows are validated
   against the JSON Schemas: all of them for the small tables, the first 1,000 for
   `daily_metrics`, `daily_by_category` and `daily_by_distribution_center`.
4. **Write and replace.** `metrics.duckdb`, `parquet/<table>.parquet` and
   `quality_report.json` are written to a temporary directory next to the output directory.
   It is then renamed into place and the old set is deleted. Readers never see a
   half-written set. The swap is two renames, so for a moment the directory does not exist.

The output directory is `--output-dir`, then `DATA_OUTPUT_DIR`, then `data/output/`. Publishing
replaces the whole directory, so a run stops before it starts when the directory holds anything
the pipeline did not publish.

A run that fails a critical check or the validation leaves:

- the previous published set, untouched;
- its quality report under `data/runs/<run_id>/`;
- the ingested rows and the rebuilt clean and quarantine tables in the working database;
- after a crash while writing, possibly a hidden `.<output>.tmp-<run_id>` directory next to
  the output directory, which is safe to delete.

A run that fails during ingestion leaves the raw tables and `ingest_log` as they were. Its
history comparisons stay in `history_checks`.
