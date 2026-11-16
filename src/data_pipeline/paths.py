"""Where things live in a checkout. The pipeline runs from one, installed in editable mode."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_DB = DATA_DIR / "warehouse" / "warehouse.duckdb"
RUNS_DIR = DATA_DIR / "runs"
OUTPUT_DIR = DATA_DIR / "output"
