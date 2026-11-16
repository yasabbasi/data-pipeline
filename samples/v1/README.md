# Sample outputs, contract v1

The published outputs of data-pipeline for its **synthetic fixtures** (no real thelook
data): 30 days from 2026-01-01, as of 2026-01-31. Use them to test code that reads the
outputs (mcp-server, task-agent) without running the pipeline.

| File | Contents |
|---|---|
| `parquet/daily_metrics.parquet` | Metrics by day, category and distribution center |
| `parquet/daily_totals.parquet` | Metrics by day, gap-free |
| `parquet/metric_definitions.parquet` | One row per metric |
| `parquet/run_metadata.parquet` | The run that produced them |
| `quality_report.json` | Data-quality results for that run |

The tables follow [docs/contract.md](../../docs/contract.md). The sample is reproducible
(fixed seed, run id `sample-v1`, timestamps at 2026-01-31T00:00:00Z). Rebuild it with
`make sample`; `tests/test_sample.py` fails when this copy is out of date. The live
pipeline also writes `metrics.duckdb`; it is left out here because DuckDB's file format is
tied to the DuckDB version.
