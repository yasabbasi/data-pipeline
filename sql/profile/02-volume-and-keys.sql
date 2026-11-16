-- Row counts, primary-key uniqueness, created_at range (incl. future and NULL timestamps).
select
    'orders' as tbl,
    count(*) as n_rows,
    count(distinct order_id) as n_keys,
    min(created_at) as min_created,
    max(created_at) as max_created,
    countif(created_at > current_timestamp()) as n_future,
    countif(created_at is null) as n_null_created
from `bigquery-public-data.thelook_ecommerce.orders`
union all
select
    'order_items' as tbl,
    count(*) as n_rows,
    count(distinct id) as n_keys,
    min(created_at) as min_created,
    max(created_at) as max_created,
    countif(created_at > current_timestamp()) as n_future,
    countif(created_at is null) as n_null_created
from `bigquery-public-data.thelook_ecommerce.order_items`
union all
select
    'users' as tbl,
    count(*) as n_rows,
    count(distinct id) as n_keys,
    min(created_at) as min_created,
    max(created_at) as max_created,
    countif(created_at > current_timestamp()) as n_future,
    countif(created_at is null) as n_null_created
from `bigquery-public-data.thelook_ecommerce.users`
union all
select
    'events' as tbl,
    count(*) as n_rows,
    count(distinct id) as n_keys,
    min(created_at) as min_created,
    max(created_at) as max_created,
    countif(created_at > current_timestamp()) as n_future,
    countif(created_at is null) as n_null_created
from `bigquery-public-data.thelook_ecommerce.events`
union all
select
    'products' as tbl,
    count(*) as n_rows,
    count(distinct id) as n_keys,
    null as min_created,
    null as max_created,
    null as n_future,
    null as n_null_created
from `bigquery-public-data.thelook_ecommerce.products`
union all
select
    'distribution_centers' as tbl,
    count(*) as n_rows,
    count(distinct id) as n_keys,
    null as min_created,
    null as max_created,
    null as n_future,
    null as n_null_created
from `bigquery-public-data.thelook_ecommerce.distribution_centers`
order by tbl
