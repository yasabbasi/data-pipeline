"""Build the committed sample, samples/<version>/: the contract outputs for the fixtures."""

import shutil
import tempfile
from pathlib import Path

from data_pipeline import contract
from data_pipeline.ingest import connect, run_ingest
from data_pipeline.paths import PROJECT_ROOT
from data_pipeline.publish import PublishResult, build_and_publish
from data_pipeline.readers import FixtureReader
from data_pipeline.sources.thelook import fixtures

SAMPLE_DIR = PROJECT_ROOT / "samples" / contract.CONTRACT_VERSION
RUN_ID = f"sample-{contract.CONTRACT_VERSION}"
PIPELINE_VERSION = "2.0.0"
# No DuckDB file: its storage format is tied to the DuckDB version.
SAMPLE_FILES = [f"parquet/{t}.parquet" for t in contract.TABLES] + ["quality_report.json"]


def build_sample(dest: Path = SAMPLE_DIR) -> PublishResult:
    """Run the pipeline on the fixtures with a fixed run id, time and version; copy to dest."""
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        with connect(work / "warehouse.duckdb") as con:
            run_ingest(con, FixtureReader(), fixtures.AS_OF, run_id=RUN_ID)
            result = build_and_publish(
                con,
                work / "output",
                work / "runs",
                generated_at=fixtures.AS_OF,
                pipeline_version=PIPELINE_VERSION,
            )
        if not result.published:
            raise RuntimeError(f"fixtures failed quality checks: see {result.report_path}")
        for name in SAMPLE_FILES:
            target = dest / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(work / "output" / name, target)
    return result


if __name__ == "__main__":
    result = build_sample()
    print(f"Wrote {SAMPLE_DIR.relative_to(PROJECT_ROOT)}: {result.row_counts}")
