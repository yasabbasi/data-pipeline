-- Grain: one row per order item that was not cancelled, dated by its order's created_at (UTC).
-- The inner joins drop nothing: the quality checks quarantine rows whose parent is missing.
select
    p.category,
    p.distribution_center_id,
    dc.name as distribution_center_name,
    i.order_id,
    i.sale_price,
    i.returned_at,
    cast(o.created_at as date) as metric_date
from clean_order_items as i
inner join clean_orders as o
    on i.order_id = o.order_id
inner join clean_products as p
    on i.product_id = p.id
inner join clean_distribution_centers as dc
    on p.distribution_center_id = dc.id
where i.status != 'Cancelled'
