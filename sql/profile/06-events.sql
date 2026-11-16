-- Event types, product-URI shape, and how purchases relate to product views in a session.
with sessions as (
    select
        session_id,
        logical_or(event_type = 'product') as viewed_product,
        logical_or(event_type = 'purchase') as purchased,
        count(distinct date(created_at)) as n_dates
    from `bigquery-public-data.thelook_ecommerce.events`
    group by session_id
)

select
    'event_type' as kind,
    event_type as `value`,
    count(*) as n
from `bigquery-public-data.thelook_ecommerce.events`
group by event_type
union all
select
    'product_uri_matches_pattern' as kind,
    cast(regexp_contains(uri, r'^/product/\d+$') as string) as `value`,
    count(*) as n
from `bigquery-public-data.thelook_ecommerce.events`
where event_type = 'product'
group by `value`
union all
select
    'events_null_user_id' as kind,
    cast(user_id is null as string) as `value`,
    count(*) as n
from `bigquery-public-data.thelook_ecommerce.events`
group by `value`
union all
select
    'session_viewed_product/purchased' as kind,
    format('%t/%t', viewed_product, purchased) as `value`,
    count(*) as n
from sessions
group by `value`
union all
select
    'session_spans_dates' as kind,
    cast(n_dates as string) as `value`,
    count(*) as n
from sessions
group by `value`
order by kind, `value`
