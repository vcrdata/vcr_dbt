with 
stock_lots as (
    select * from {{ ref("stg_odoo__stock_prod_lot") }}
)
, stock_quant as (
    select * from {{ ref('int_serial_quant') }}
)
, lot_pricelist as (
    select * from {{ ref('stg_odoo__product_pricelist_item') }}
)

select 
    sl.*
    , (regexp_matches(sl.serial_name, '([^_]+)$'))[1] root_serial_name
    {# , slr.linked_serial_id #}
    , case 
        when sq.location_usage = 'internal' and sl.active = True then quantity
        else 0
    end as internal_quantity
    , sq.quantity
    , sq.reserved_quantity
    , sq.location_name
    , sq.location_complete_name
    , sq.location_usage
    , sq.warehouse_code
    , sq.warehouse_name
    , sq.in_date stock_in_date
    , lp.fixed_price
from stock_lots sl 
left join stock_quant sq 
on sl.serial_id = sq.serial_id
left join lot_pricelist lp 
on sl.serial_id = lp.serial_id