-- Column names and types of the source tables we ingest.
select
    table_name,
    column_name,
    data_type,
    is_nullable
-- BigQuery documents its INFORMATION_SCHEMA views in upper case; keep them that way.
from `bigquery-public-data.thelook_ecommerce.INFORMATION_SCHEMA.COLUMNS`
where table_name in ('orders', 'order_items', 'products', 'users', 'events', 'distribution_centers')
order by table_name, ordinal_position
