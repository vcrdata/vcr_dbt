with
stock_locations as (
    select * from {{ ref('stg_odoo__stock_location') }}
)
, stock_warehouses as (
    select * from {{ ref('stg_odoo__stock_warehouse') }}
)

select 
    sl.location_id
    , sl.location_name
    , sl.location_complete_name
    , sl.location_usage
    , sl.active
    , sl.barcode
    , sl.location_parent_id
    , sl.parent_path
    , sl.warehouse_id
    , sw.warehouse_code
    , sw.warehouse_name
    , sl.last_update
    , sl.create_date
from stock_locations sl 
left join stock_warehouses sw
on sl.warehouse_id = sw.warehouse_id 