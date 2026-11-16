"""Deterministic synthetic data shaped like thelook_ecommerce, with no real thelook rows."""

import random
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, time, timedelta
from itertools import count
from typing import Any

from data_pipeline.sources.thelook.tables import SOURCE_TABLES

Rows = list[dict[str, Any]]
# Each event with its order's created_at, or None for a browse session. Purchase sessions are
# visible with their order, even when dated after as_of, like the live source.
_PlacedEvents = list[tuple[datetime | None, dict[str, Any]]]

DISTRIBUTION_CENTERS = ["Northgate", "Riverside", "Lakeview"]
CATEGORIES = ["Jeans", "Tops & Tees", "Outerwear & Coats", "Accessories"]
BRANDS = ["Acme", "Birch", "Cobalt", "Dune"]
COUNTRIES = ["United States", "Brasil", "Germany", "Japan"]
TRAFFIC_SOURCES = ["Search", "Organic", "Email", "Facebook", "Display"]
# The status an order reaches given enough time, with its weight.
FINAL_STATUSES = {
    "Complete": 0.55,
    "Returned": 0.12,
    "Cancelled": 0.13,
    "Shipped": 0.1,
    "Processing": 0.1,
}
# Item created_at minus its order's, in hours: 55% of items up to 4h before, the rest up to
# 4 days after. Fits the live profile (min -4h, median -0.45h, p99 +96h, 45% of items on
# another day than their order).
ITEM_BEFORE_SHARE = 0.55
ITEM_BEFORE_HOURS = (-4, 0)
ITEM_AFTER_HOURS = (0, 96)
START = date(2026, 1, 1)
DAYS = 30
AS_OF = datetime.combine(START + timedelta(days=DAYS), time(), UTC)


def generate(
    seed: int = 7,
    start: date = START,
    days: int = DAYS,
    as_of: datetime | None = None,
    n_users: int = 60,
    n_products: int = 24,
) -> dict[str, Rows]:
    """Return the source tables as they looked at as_of; a seed always gives the same universe."""
    rng = random.Random(seed)
    as_of = as_of or datetime.combine(start + timedelta(days=days), time(), UTC)
    day0 = datetime.combine(start, time(), UTC)
    tables: dict[str, Rows] = {name: [] for name in SOURCE_TABLES}
    tables["distribution_centers"] = [
        {"id": i, "name": name} for i, name in enumerate(DISTRIBUTION_CENTERS, start=1)
    ]
    tables["products"] = _products(rng, n_products)
    tables["users"] = [u for u in _users(rng, n_users, day0) if u["created_at"] <= as_of]
    prices = {p["id"]: p["retail_price"] for p in tables["products"]}
    events: _PlacedEvents = []
    order_ids, item_ids = count(1), count(1)
    for d in range(days):
        day = day0 + timedelta(days=d)
        for _ in range(rng.randint(3, 9)):
            order, items, sessions = _order(rng, day, as_of, n_users, prices, order_ids, item_ids)
            events += sessions
            # Ids and random draws are consumed for hidden orders too, keeping them stable.
            if order["created_at"] > as_of:
                continue
            tables["orders"].append(order)
            tables["order_items"] += items
        for _ in range(rng.randint(5, 12)):
            events += [(None, e) for e in _browse_session(rng, day, n_users, n_products)]
    tables["events"] = _visible_events(events, as_of)
    return tables


def _products(rng: random.Random, n_products: int) -> Rows:
    products: Rows = []
    for product_id in range(1, n_products + 1):
        price = round(rng.uniform(10, 150), 2)
        products.append(
            {
                "id": product_id,
                "cost": round(price * rng.uniform(0.4, 0.6), 2),
                "category": CATEGORIES[product_id % len(CATEGORIES)],
                "name": f"Product {product_id}",
                "brand": rng.choice(BRANDS),
                "retail_price": price,
                "department": rng.choice(["Women", "Men"]),
                "distribution_center_id": 1 + product_id % len(DISTRIBUTION_CENTERS),
            }
        )
    return products


def _users(rng: random.Random, n_users: int, day0: datetime) -> Rows:
    return [
        {
            "id": user_id,
            "country": rng.choice(COUNTRIES),
            "traffic_source": rng.choice(TRAFFIC_SOURCES),
            "created_at": day0 - timedelta(days=rng.randint(1, 365), seconds=rng.randint(0, 86399)),
        }
        for user_id in range(1, n_users + 1)
    ]


def _order(
    rng: random.Random,
    day: datetime,
    as_of: datetime,
    n_users: int,
    prices: Mapping[int, float],
    order_ids: Iterator[int],
    item_ids: Iterator[int],
) -> tuple[dict[str, Any], Rows, _PlacedEvents]:
    order_id = next(order_ids)
    user_id = rng.randint(1, n_users)
    created = day + timedelta(seconds=rng.randint(600, 86399))
    final = rng.choices(list(FINAL_STATUSES), weights=list(FINAL_STATUSES.values()))[0]
    lifecycle = _lifecycle(rng, created, final, as_of)
    items: Rows = []
    sessions: _PlacedEvents = []
    for _ in range(rng.randint(1, 4)):
        item, session = _order_item(rng, next(item_ids), order_id, user_id, created, prices)
        items.append(item | lifecycle)
        sessions += [(created, e) for e in session]
    order = {
        "order_id": order_id,
        "user_id": user_id,
        "created_at": created,
        "num_of_item": len(items),
        **lifecycle,
    }
    return order, items, sessions


def _order_item(
    rng: random.Random,
    item_id: int,
    order_id: int,
    user_id: int,
    placed: datetime,
    prices: Mapping[int, float],
) -> tuple[dict[str, Any], Rows]:
    product_id = rng.randint(1, len(prices))
    created = placed + _item_offset(rng)
    session = _purchase_session(rng, user_id, placed, created, product_id)
    item = {
        "id": item_id,
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "created_at": created,
        "sale_price": prices[product_id],
    }
    return item, session


def _lifecycle(
    rng: random.Random, created: datetime, final: str, as_of: datetime
) -> dict[str, Any]:
    # The draws do not depend on as_of; only the status and timestamps reported do.
    # Returns land within ~10.5 days of the order (live: max 10.7 days).
    shipped = created + timedelta(hours=rng.uniform(2, 48))
    delivered = shipped + timedelta(days=rng.uniform(1, 4))
    returned = delivered + timedelta(days=rng.uniform(0.5, 4.5))
    stages: dict[str, list[str]] = {
        "Processing": [],
        "Cancelled": [],
        "Shipped": ["shipped"],
        "Complete": ["shipped", "delivered"],
        "Returned": ["shipped", "delivered", "returned"],
    }
    at = {"shipped": shipped, "delivered": delivered, "returned": returned}
    reached = [s for s in stages[final] if at[s] <= as_of]
    if reached == stages[final]:
        status = final
    else:  # lifecycle not finished yet: report the last stage reached by as_of
        status = ["Processing", "Shipped", "Complete"][len(reached)]
    return {
        "status": status,
        "shipped_at": shipped if "shipped" in reached else None,
        "delivered_at": delivered if "delivered" in reached else None,
        "returned_at": returned if "returned" in reached else None,
    }


def _item_offset(rng: random.Random) -> timedelta:
    low, high = ITEM_BEFORE_HOURS if rng.random() < ITEM_BEFORE_SHARE else ITEM_AFTER_HOURS
    return timedelta(hours=rng.uniform(low, high))


def _purchase_session(
    rng: random.Random, user_id: int, placed: datetime, purchased: datetime, product_id: int
) -> Rows:
    # Views and cart lead up to the order; the purchase event carries the item's created_at,
    # so it can come before the cart or days later (live: only purchase events are future).
    steps = [("home", "/"), ("product", f"/product/{product_id}")]
    steps += [("cart", "/cart"), ("purchase", "/purchase")]
    start = placed - timedelta(minutes=(len(steps) - 1) * 2)
    return _session(rng, user_id, start, steps, end=purchased)


def _browse_session(rng: random.Random, day: datetime, n_users: int, n_products: int) -> Rows:
    user_id = rng.randint(1, n_users) if rng.random() < 0.5 else None
    started = day + timedelta(seconds=rng.randint(0, 86000))
    viewed = [rng.randint(1, n_products) for _ in range(rng.randint(1, 3))]
    steps = [("home", "/")] + [("product", f"/product/{pid}") for pid in viewed]
    if rng.random() < 0.3:
        steps.append(("cart", "/cart"))
    return _session(rng, user_id, started, steps)


def _session(
    rng: random.Random,
    user_id: int | None,
    start: datetime,
    steps: list[tuple[str, str]],
    end: datetime | None = None,
) -> Rows:
    session_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
    traffic = rng.choice(TRAFFIC_SOURCES)
    rows: Rows = []
    for seq, (event_type, uri) in enumerate(steps, start=1):
        at = end if (end and seq == len(steps)) else start + timedelta(minutes=2 * (seq - 1))
        rows.append(
            {
                "id": None,
                "user_id": user_id,
                "sequence_number": seq,
                "session_id": session_id,
                "created_at": at,
                "traffic_source": traffic,
                "uri": uri,
                "event_type": event_type,
            }
        )
    return rows


def _visible_events(events: _PlacedEvents, as_of: datetime) -> Rows:
    # Ids follow time order over the whole universe, so they don't change with as_of.
    ordered = sorted(
        events, key=lambda pe: (pe[1]["created_at"], pe[1]["session_id"], pe[1]["sequence_number"])
    )
    for event_id, (_, event) in enumerate(ordered, start=1):
        event["id"] = event_id
    return [
        e for placed, e in ordered if (placed if placed is not None else e["created_at"]) <= as_of
    ]
