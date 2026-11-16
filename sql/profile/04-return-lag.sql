-- How long after the sale do returns arrive? Sizes the incremental lookback window.
select
    count(*) as n_returned,
    approx_quantiles(timestamp_diff(returned_at, created_at, hour) / 24, 100)[offset(50)]
        as p50_days,
    approx_quantiles(timestamp_diff(returned_at, created_at, hour) / 24, 100)[offset(90)]
        as p90_days,
    approx_quantiles(timestamp_diff(returned_at, created_at, hour) / 24, 100)[offset(99)]
        as p99_days,
    max(timestamp_diff(returned_at, created_at, hour) / 24) as max_days,
    countif(timestamp_diff(returned_at, created_at, hour) > 14 * 24) as n_after_14_days
from `bigquery-public-data.thelook_ecommerce.order_items`
where returned_at is not null
