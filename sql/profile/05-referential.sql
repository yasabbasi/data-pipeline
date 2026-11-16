-- Orphan counts and item/order consistency.
with item_order_orphans as (
    select count(*) as items_without_order
    from `bigquery-public-data.thelook_ecommerce.order_items` as i
    left join `bigquery-public-data.thelook_ecommerce.orders` as o
        on i.order_id = o.order_id
    where o.order_id is null
),

item_product_orphans as (
    select count(*) as items_without_product
    from `bigquery-public-data.thelook_ecommerce.order_items` as i
    left join `bigquery-public-data.thelook_ecommerce.products` as p
        on i.product_id = p.id
    where p.id is null
),

product_center_orphans as (
    select count(*) as products_without_dc
    from `bigquery-public-data.thelook_ecommerce.products` as p
    left join `bigquery-public-data.thelook_ecommerce.distribution_centers` as d
        on p.distribution_center_id = d.id
    where d.id is null
),

order_user_orphans as (
    select count(*) as orders_without_user
    from `bigquery-public-data.thelook_ecommerce.orders` as o
    left join `bigquery-public-data.thelook_ecommerce.users` as u
        on o.user_id = u.id
    where u.id is null
),

item_date_mismatches as (
    select count(*) as items_dated_differently_from_order
    from `bigquery-public-data.thelook_ecommerce.order_items` as i
    inner join `bigquery-public-data.thelook_ecommerce.orders` as o
        on i.order_id = o.order_id
    where date(i.created_at) != date(o.created_at)
),

item_status_mismatches as (
    select count(*) as items_status_differs_from_order
    from `bigquery-public-data.thelook_ecommerce.order_items` as i
    inner join `bigquery-public-data.thelook_ecommerce.orders` as o
        on i.order_id = o.order_id
    where i.status != o.status
),

category_stats as (
    select
        count(distinct category) as n_categories,
        countif(category is null) as products_null_category
    from `bigquery-public-data.thelook_ecommerce.products`
)

select
    item_order_orphans.items_without_order,
    item_product_orphans.items_without_product,
    product_center_orphans.products_without_dc,
    order_user_orphans.orders_without_user,
    item_date_mismatches.items_dated_differently_from_order,
    item_status_mismatches.items_status_differs_from_order,
    category_stats.n_categories,
    category_stats.products_null_category
from item_order_orphans
cross join item_product_orphans
cross join product_center_orphans
cross join order_user_orphans
cross join item_date_mismatches
cross join item_status_mismatches
cross join category_stats
