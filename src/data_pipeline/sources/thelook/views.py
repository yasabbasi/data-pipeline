"""Source views of the metrics layer over the clean thelook tables, kept in sql/views/."""

from data_pipeline.metrics_spec import SOURCES
from data_pipeline.paths import PROJECT_ROOT

VIEWS_DIR = PROJECT_ROOT / "sql" / "views"
SLICE = ("category", "distribution_center_id", "distribution_center_name")


def load_views() -> dict[str, str]:
    """Return the select statement of each metric source, by view name."""
    return {name: (VIEWS_DIR / f"{name.replace('_', '-')}.sql").read_text() for name in SOURCES}
