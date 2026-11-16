# Data and clean-room note

## Source data

This pipeline reads `bigquery-public-data.thelook_ecommerce`. That is a synthetic e-commerce
dataset that Google publishes as a BigQuery public dataset. Google provides it "AS IS" and
refreshes it daily. It contains no real customers or transactions.

- This repository does **not** redistribute that dataset. Anyone running the live pipeline
  queries it from their own Google Cloud project, under Google's terms for public datasets.
- Pipeline outputs built from live data are written to the gitignored `data/` directory and
  are never committed.
- Test fixtures and the committed sample (`samples/`) come from this repo's own synthetic
  fixture generator, not from thelook rows.

## Clean room

All code, SQL, metric definitions, prompts, templates and documentation here were written
from scratch for this project. They use only public documentation for the tools involved
(BigQuery, DuckDB, uv, GitHub Actions). Nothing was copied, adapted or recreated from memory
from any employer's code, data, prompts, templates, documents or prior implementations.
