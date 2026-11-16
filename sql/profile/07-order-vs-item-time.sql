-- Item vs. order timestamps, return lag from the order date, future rows, num_of_item consistency.
with items as (
    select
        i.shipped_at,
        i.created_at as item_created_at,
        o.shipped_at as order_shipped_at,
        o.created_at as order_created_at,
        timestamp_diff(i.created_at, o.created_at, minute) / 60 as offset_hours,
        timestamp_diff(i.returned_at, o.created_at, minute) / 1440 as return_days_from_order
    from `bigquery-public-data.thelook_ecommerce.order_items` as i
    inner join `bigquery-public-data.thelook_ecommerce.orders` as o
        on i.order_id = o.order_id
),

item_stats as (
    select
        min(offset_hours) as min_offset_h,
        approx_quantiles(offset_hours, 100)[offset(1)] as p01_offset_h,
        approx_quantiles(offset_hours, 100)[offset(50)] as p50_offset_h,
        approx_quantiles(offset_hours, 100)[offset(99)] as p99_offset_h,
        max(offset_hours) as max_offset_h,
        countif(shipped_at < order_created_at) as shipped_before_order,
        countif(shipped_at != order_shipped_at) as shipped_differs_from_order,
        approx_quantiles(return_days_from_order, 100)[offset(99)] as p99_return_days,
        max(return_days_from_order) as max_return_days,
        countif(return_days_from_order > 14) as returns_after_14d,
        countif(item_created_at > current_timestamp()) as future_items,
        countif(order_created_at > current_timestamp()) as future_orders_of_items
    from items
),

event_stats as (
    select
        countif(event_type = 'purchase') as future_purchase_events,
        countif(event_type != 'purchase') as future_other_events
    from `bigquery-public-data.thelook_ecommerce.events`
    where created_at > current_timestamp()
),

item_counts as (
    select
        order_id,
        count(*) as n_items
    from `bigquery-public-data.thelook_ecommerce.order_items`
    group by order_id
),

order_stats as (
    select countif(o.num_of_item != n.n_items) as num_of_item_mismatch
    from `bigquery-public-data.thelook_ecommerce.orders` as o
    inner join item_counts as n
        on o.order_id = n.order_id
)

select
    item_stats.min_offset_h,
    item_stats.p01_offset_h,
    item_stats.p50_offset_h,
    item_stats.p99_offset_h,
    item_stats.max_offset_h,
    item_stats.shipped_before_order,
    item_stats.shipped_differs_from_order,
    item_stats.p99_return_days,
    item_stats.max_return_days,
    item_stats.returns_after_14d,
    item_stats.future_items,
    item_stats.future_orders_of_items,
    event_stats.future_purchase_events,
    event_stats.future_other_events,
    order_stats.num_of_item_mismatch
from item_stats
cross join event_stats
cross join order_stats
