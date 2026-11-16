import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from data_pipeline.sources.thelook.fixtures import Rows, generate
from data_pipeline.sources.thelook.tables import (
    EVENT_TYPES,
    ITEM_STATUSES,
    PRODUCT_URI_PREFIX,
    SOURCE_TABLES,
)

AS_OF = datetime(2026, 1, 31, tzinfo=UTC)
Row = dict[str, Any]
DEFAULT_UNIVERSE_SHA256 = "226f38d524eea8fddb06d03561f2b1b08350578868660fc13101f63f4b6dc88b"


@pytest.fixture(scope="module")
def data() -> dict[str, Rows]:
    return generate()


@pytest.fixture(scope="module")
def early() -> dict[str, Rows]:
    return generate(as_of=AS_OF - timedelta(days=10))


def placed_at(data: dict[str, Rows]) -> dict[int, datetime]:
    return {o["order_id"]: o["created_at"] for o in data["orders"]}


def test_tables_have_exactly_the_source_columns(data: dict[str, Rows]) -> None:
    assert set(data) == set(SOURCE_TABLES)
    for name, table in SOURCE_TABLES.items():
        assert data[name], f"{name} is empty"
        for row in data[name]:
            assert set(row) == set(table.columns)


def test_the_same_seed_gives_the_same_data() -> None:
    assert generate(seed=3) == generate(seed=3)


def test_a_different_seed_gives_different_data() -> None:
    assert generate(seed=3) != generate(seed=4)


def test_the_default_universe_has_not_changed(data: dict[str, Rows]) -> None:
    dumped = json.dumps(data, default=str, sort_keys=True)
    assert hashlib.sha256(dumped.encode()).hexdigest() == DEFAULT_UNIVERSE_SHA256


def test_primary_keys_are_unique(data: dict[str, Rows]) -> None:
    for name, table in SOURCE_TABLES.items():
        keys = [row[table.primary_key] for row in data[name]]
        assert len(keys) == len(set(keys)), name


def test_every_foreign_key_points_at_a_visible_row(data: dict[str, Rows]) -> None:
    ids = {name: {r[SOURCE_TABLES[name].primary_key] for r in data[name]} for name in data}
    assert {i["order_id"] for i in data["order_items"]} <= ids["orders"]
    assert {i["product_id"] for i in data["order_items"]} <= ids["products"]
    assert {p["distribution_center_id"] for p in data["products"]} <= ids["distribution_centers"]
    assert {o["user_id"] for o in data["orders"]} <= ids["users"]


def test_num_of_item_equals_the_number_of_items_in_the_order(data: dict[str, Rows]) -> None:
    counts: dict[int, int] = {}
    for item in data["order_items"]:
        counts[item["order_id"]] = counts.get(item["order_id"], 0) + 1
    assert all(counts[o["order_id"]] == o["num_of_item"] for o in data["orders"])


def lifecycle(data: dict[str, Rows]) -> list[tuple[Row, list[datetime]]]:
    placed = placed_at(data)
    return [
        (
            row,
            [
                stamp
                for stamp in (
                    placed[row["order_id"]],
                    row["shipped_at"],
                    row["delivered_at"],
                    row["returned_at"],
                )
                if stamp is not None
            ],
        )
        for row in data["orders"] + data["order_items"]
    ]


def test_every_status_is_one_the_source_has(data: dict[str, Rows]) -> None:
    assert {row["status"] for row, _ in lifecycle(data)} <= set(ITEM_STATUSES)


def test_lifecycle_stamps_are_in_order_from_the_order(data: dict[str, Rows]) -> None:
    assert all(stamps == sorted(stamps) for _, stamps in lifecycle(data))


def test_no_lifecycle_stamp_is_after_as_of(data: dict[str, Rows]) -> None:
    assert all(stamp <= AS_OF for _, stamps in lifecycle(data) for stamp in stamps)


def test_only_returned_rows_have_a_return_time(data: dict[str, Rows]) -> None:
    assert all(
        (row["returned_at"] is not None) == (row["status"] == "Returned")
        for row, _ in lifecycle(data)
    )


def test_some_items_are_created_on_another_day_than_their_order(data: dict[str, Rows]) -> None:
    placed = placed_at(data)
    assert any(i["created_at"].date() != placed[i["order_id"]].date() for i in data["order_items"])


def test_some_items_are_created_before_their_order(data: dict[str, Rows]) -> None:
    placed = placed_at(data)
    assert any(i["created_at"] < placed[i["order_id"]] for i in data["order_items"])


def test_some_items_ship_before_they_are_created(data: dict[str, Rows]) -> None:
    items = data["order_items"]
    assert any(i["shipped_at"] and i["shipped_at"] < i["created_at"] for i in items)


def test_some_items_are_future_dated_yet_already_published(data: dict[str, Rows]) -> None:
    assert any(i["created_at"] > AS_OF for i in data["order_items"])


def test_item_created_at_is_within_four_hours_before_and_four_days_after_its_order(
    data: dict[str, Rows],
) -> None:
    placed = placed_at(data)
    offsets = [i["created_at"] - placed[i["order_id"]] for i in data["order_items"]]
    assert all(timedelta(hours=-4) <= o <= timedelta(hours=96) for o in offsets)


def test_only_purchase_events_are_future_dated(data: dict[str, Rows]) -> None:
    future = [e for e in data["events"] if e["created_at"] > AS_OF]
    assert future, "expected purchase events dated after as_of, like the live source"
    assert all(e["event_type"] == "purchase" for e in future)


def test_every_order_item_has_its_own_purchase_session(data: dict[str, Rows]) -> None:
    purchases = [e for e in data["events"] if e["event_type"] == "purchase"]
    assert len(purchases) == len(data["order_items"])
    assert len({e["session_id"] for e in purchases}) == len(purchases)


def test_every_session_views_a_product(data: dict[str, Rows]) -> None:
    by_session: dict[str, set[str]] = {}
    for e in data["events"]:
        by_session.setdefault(e["session_id"], set()).add(e["event_type"])
    assert all("product" in types for types in by_session.values())


def test_event_types_are_ones_the_source_has(data: dict[str, Rows]) -> None:
    assert {e["event_type"] for e in data["events"]} <= set(EVENT_TYPES)


def test_product_events_have_a_product_uri(data: dict[str, Rows]) -> None:
    product_events = [e for e in data["events"] if e["event_type"] == "product"]
    assert all(e["uri"].startswith(PRODUCT_URI_PREFIX) for e in product_events)


def test_event_ids_are_positive_integers(data: dict[str, Rows]) -> None:
    assert all(isinstance(e["id"], int) and e["id"] >= 1 for e in data["events"])


def test_some_sessions_are_guest_sessions(data: dict[str, Rows]) -> None:
    assert any(e["user_id"] is None for e in data["events"])


def test_event_times_are_utc(data: dict[str, Rows]) -> None:
    assert all(e["created_at"].tzinfo is UTC for e in data["events"])


def by_id(rows: Rows) -> dict[int, Row]:
    return {row["id"]: row for row in rows}


def test_a_later_as_of_adds_items(early: dict[str, Rows], data: dict[str, Rows]) -> None:
    assert by_id(early["order_items"]).keys() < by_id(data["order_items"]).keys()


def test_a_later_as_of_keeps_what_identifies_an_earlier_item(
    early: dict[str, Rows], data: dict[str, Rows]
) -> None:
    late = by_id(data["order_items"])
    fixed = ("order_id", "product_id", "created_at")
    assert all(
        [late[item_id][c] for c in fixed] == [item[c] for c in fixed]
        for item_id, item in by_id(early["order_items"]).items()
    )


def test_a_later_as_of_advances_the_status_of_some_earlier_items(
    early: dict[str, Rows], data: dict[str, Rows]
) -> None:
    late = by_id(data["order_items"])
    assert any(
        late[item_id]["status"] != item["status"]
        for item_id, item in by_id(early["order_items"]).items()
    )


def test_a_later_as_of_adds_events(early: dict[str, Rows], data: dict[str, Rows]) -> None:
    assert by_id(early["events"]).keys() < by_id(data["events"]).keys()


def test_a_later_as_of_leaves_earlier_events_unchanged(
    early: dict[str, Rows], data: dict[str, Rows]
) -> None:
    late = by_id(data["events"])
    assert all(late[event_id] == event for event_id, event in by_id(early["events"]).items())
