with
stock_quant as (
    select * from  {{ ref('stg_odoo__stock_quant') }}
    where rank_loc = 1
)
, stock_locations as (
    select * from {{ ref('int_stock_location') }}
)

select 
    sq.stock_quant_id
    , sq.serial_id
    , sq.in_date
    , sq.last_update
    , sq.product_id
    , sq.quantity
    , sq.reserved_quantity
    , sq.location_id
    , sl.location_name
    , sl.location_complete_name
    , sl.location_usage
    , sl.warehouse_name
    , sl.warehouse_code
from stock_quant sq
left join stock_locations sl 
on sq.location_id = sl.location_id