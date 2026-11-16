-- Grain: one row per product event, dated by its created_at (UTC); session_converted is true
-- when the same session has a purchase event.
-- The inner joins drop nothing: the quality checks quarantine rows whose parent is missing.
with purchase_sessions as (
    select distinct session_id
    from clean_events
    where event_type = 'purchase'
),

viewed_products as (
    select
        session_id,
        cast(created_at as date) as metric_date,
        try_cast(regexp_extract(uri, '^/product/([0-9]+)$', 1) as bigint) as product_id
    from clean_events
    where event_type = 'product'
)

select
    v.metric_date,
    p.category,
    p.distribution_center_id,
    dc.name as distribution_center_name,
    v.session_id,
    ps.session_id is not null as session_converted
from viewed_products as v
inner join clean_products as p
    on v.product_id = p.id
inner join clean_distribution_centers as dc
    on p.distribution_center_id = dc.id
left join purchase_sessions as ps
    on v.session_id = ps.session_id
