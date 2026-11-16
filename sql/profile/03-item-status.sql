-- Item statuses vs. which lifecycle timestamps are set, plus chronology violations.
select
    status,
    count(*) as n_items,
    countif(shipped_at is not null) as has_shipped,
    countif(delivered_at is not null) as has_delivered,
    countif(returned_at is not null) as has_returned,
    countif(shipped_at < created_at) as shipped_before_created,
    countif(delivered_at < shipped_at) as delivered_before_shipped,
    countif(returned_at < delivered_at) as returned_before_delivered,
    countif(sale_price is null or sale_price <= 0) as bad_price
from `bigquery-public-data.thelook_ecommerce.order_items`
group by status
order by n_items desc
