"""Source tables and columns ingested from bigquery-public-data.thelook_ecommerce."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

DATASET = "bigquery-public-data.thelook_ecommerce"


@dataclass(frozen=True)
class SourceTable:
    """One source table: column names to BigQuery types; no watermark means full refresh."""

    name: str
    columns: Mapping[str, str]
    primary_key: str
    watermark: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", MappingProxyType(dict(self.columns)))


SOURCE_TABLES: dict[str, SourceTable] = {
    t.name: t
    for t in [
        SourceTable(
            name="orders",
            columns={
                "order_id": "INT64",
                "user_id": "INT64",
                "status": "STRING",
                "created_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "num_of_item": "INT64",
            },
            primary_key="order_id",
            watermark="created_at",
        ),
        SourceTable(
            name="order_items",
            columns={
                "id": "INT64",
                "order_id": "INT64",
                "user_id": "INT64",
                "product_id": "INT64",
                "status": "STRING",
                "created_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "sale_price": "FLOAT64",
            },
            primary_key="id",
            watermark="created_at",
        ),
        SourceTable(
            name="products",
            columns={
                "id": "INT64",
                "cost": "FLOAT64",
                "category": "STRING",
                "name": "STRING",
                "brand": "STRING",
                "retail_price": "FLOAT64",
                "department": "STRING",
                "distribution_center_id": "INT64",
            },
            primary_key="id",
            watermark=None,
        ),
        SourceTable(
            name="users",
            columns={
                "id": "INT64",
                "country": "STRING",
                "traffic_source": "STRING",
                "created_at": "TIMESTAMP",
            },
            primary_key="id",
            watermark="created_at",
        ),
        SourceTable(
            name="events",
            columns={
                "id": "INT64",
                "user_id": "INT64",
                "sequence_number": "INT64",
                "session_id": "STRING",
                "created_at": "TIMESTAMP",
                "traffic_source": "STRING",
                "uri": "STRING",
                "event_type": "STRING",
            },
            primary_key="id",
            watermark="created_at",
        ),
        SourceTable(
            name="distribution_centers",
            columns={"id": "INT64", "name": "STRING"},
            primary_key="id",
            watermark=None,
        ),
    ]
}

# Values the fixtures emit and the metrics layer relies on; confirmed by the live profile.
ITEM_STATUSES = ("Processing", "Shipped", "Complete", "Returned", "Cancelled")
EVENT_TYPES = ("home", "department", "product", "cart", "purchase", "cancel")
PRODUCT_URI_PREFIX = "/product/"
