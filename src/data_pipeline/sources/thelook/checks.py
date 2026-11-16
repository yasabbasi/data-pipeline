"""Quality checks for the thelook tables: required columns, accepted values and references."""

from data_pipeline.ingest import raw_table
from data_pipeline.quality import RowCheck
from data_pipeline.sources.thelook.tables import (
    EVENT_TYPES,
    ITEM_STATUSES,
    PRODUCT_URI_PREFIX,
    SOURCE_TABLES,
    SourceTable,
)

# Parents first, so referential checks can read their clean tables.
TABLE_ORDER = ["distribution_centers", "products", "orders", "order_items", "users", "events"]
# Columns the metrics layer relies on. Watermark columns are never null: ingestion filters on them.
REQUIRED = {
    "distribution_centers": ["id", "name"],
    "products": ["id", "category", "distribution_center_id"],
    "orders": ["order_id", "status"],
    "order_items": ["id", "order_id", "product_id", "status", "sale_price"],
    "users": ["id"],
    "events": ["id", "session_id", "event_type"],
}
PRODUCT_ID_FROM_URI = (
    f"try_cast(regexp_extract(uri, '^{PRODUCT_URI_PREFIX}([0-9]+)$', 1) as bigint)"
)
FRESHNESS_TABLE = "orders"
FRESHNESS_COLUMN = "created_at"
RETURNED = "Returned"
PRODUCT_EVENT = "product"


def row_checks() -> list[RowCheck]:
    """Return every row-level check on the thelook tables, in report order."""
    return [
        *_not_null_checks(),
        *(_unique(table) for table in SOURCE_TABLES.values()),
        *_value_checks(),
        *_lifecycle_checks(),
        *_referential_checks(),
    ]


def _not_null_checks() -> list[RowCheck]:
    return [
        RowCheck(
            f"{table}.{column}.not_null",
            table,
            "not_null",
            f"{column} is null",
            f"{column} is NULL",
        )
        for table, columns in REQUIRED.items()
        for column in columns
    ]


def _unique(table: SourceTable) -> RowCheck:
    key = table.primary_key
    return RowCheck(
        f"{table.name}.{key}.unique",
        table.name,
        "unique",
        "duplicate_key is not null",
        f"{key} appears more than once (all copies quarantined)",
        ctes=(
            f"""
            duplicate_keys as (
                select {key} as duplicate_key
                from {raw_table(table.name)}
                group by {key}
                having count(*) > 1
            )
            """,
        ),
        joins=(f"left join duplicate_keys on {key} = duplicate_key",),
    )


def _value_checks() -> list[RowCheck]:
    return [
        RowCheck(
            "orders.status.accepted_values",
            "orders",
            "accepted_values",
            "status not in ?",
            "status is not a known status",
            (ITEM_STATUSES,),
        ),
        RowCheck(
            "order_items.status.accepted_values",
            "order_items",
            "accepted_values",
            "status not in ?",
            "status is not a known status",
            (ITEM_STATUSES,),
        ),
        RowCheck(
            "events.event_type.accepted_values",
            "events",
            "accepted_values",
            "event_type not in ?",
            "event_type is not a known event type",
            (EVENT_TYPES,),
        ),
        RowCheck(
            "order_items.sale_price.positive",
            "order_items",
            "range",
            "sale_price <= 0",
            "sale_price is zero or negative",
        ),
    ]


def _lifecycle_checks() -> list[RowCheck]:
    return [
        RowCheck(
            "orders.lifecycle.chronology",
            "orders",
            "chronology",
            "shipped_at < created_at or delivered_at < shipped_at or returned_at < delivered_at",
            "lifecycle timestamps are out of order",
        ),
        # Item created_at is not checked against shipped_at: the live source stamps items up
        # to 4 days after their order (docs/source-profile.md), so that is expected.
        RowCheck(
            "order_items.lifecycle.chronology",
            "order_items",
            "chronology",
            "delivered_at < shipped_at or returned_at < delivered_at",
            "lifecycle timestamps are out of order",
        ),
        RowCheck(
            "order_items.returned_at.matches_status",
            "order_items",
            "consistency",
            "(status = ?) <> (returned_at is not null)",
            "returned_at is set without status Returned, or the reverse",
            (RETURNED,),
        ),
        RowCheck(
            "events.uri.product_format",
            "events",
            "format",
            "event_type = ? and not coalesce(regexp_full_match(uri, ?), false)",
            "a product event's uri is not /product/<id>",
            (PRODUCT_EVENT, f"{PRODUCT_URI_PREFIX}[0-9]+"),
        ),
    ]


def _referential_checks() -> list[RowCheck]:
    return [
        _referential(
            "products",
            "distribution_center_id",
            "distribution_centers",
            "distribution center missing or quarantined",
        ),
        _referential("order_items", "order_id", "orders", "order missing or quarantined"),
        _referential("order_items", "product_id", "products", "product missing or quarantined"),
        _referential(
            "events",
            "product_id",
            "products",
            "a product event's product is missing or quarantined",
            value=PRODUCT_ID_FROM_URI,
        ),
    ]


def _referential(
    table: str, column: str, parent: str, description: str, *, value: str | None = None
) -> RowCheck:
    key = SOURCE_TABLES[parent].primary_key
    value = value or column
    return RowCheck(
        f"{table}.{column}.referential",
        table,
        "referential",
        # Same result as `value not in (select key from clean parent)`: when the parent has no
        # clean rows every row fails, even one without a value.
        f"{parent}_key is null and ({value} is not null or {parent}_key_count = 0)",
        description,
        ctes=(
            f"""
            {parent}_keys as (
                select distinct {key} as {parent}_key
                from clean_{parent}
                where {key} is not null
            )
            """,
            f"""
            {parent}_size as (
                select count(*) as {parent}_key_count
                from {parent}_keys
            )
            """,
        ),
        joins=(
            f"left join {parent}_keys on {value} = {parent}_key",
            f"cross join {parent}_size",
        ),
    )
