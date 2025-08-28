with 
stock_move as (
    SELECT * FROM {{ ref('stg_odoo__stock_move') }}
    where move_state <> 'cancel'
)
, stock_move_line as (
    SELECT * FROM {{ ref('stg_odoo__stock_move_line') }}
    where move_line_state <> 'cancel'
)
, stock_picking as (
    SELECT * FROM {{ ref('stg_odoo__stock_picking') }}
    where picking_state <> 'cancel'
)
, stock_picking_type as (
    SELECT * FROM {{ ref('stg_odoo__stock_picking_type') }}
)
, sale_orders as (
    select * from {{ ref('int_order') }}
)
, joined_tbl as (
    SELECT
        sml.move_line_id
        , sml.serial_id
        , sml.product_id
        , sml.qty_done
        , sml.location_id
        , sml.location_dest_id
        , sml.move_line_state
        
        , sm.move_id
        , sm.move_date
        , sm.partner_id
        , sm.move_name
        , sm.move_reference
        , sm.move_origin
        , sm.move_state
        
        , coalesce(sp.picking_id, 0) picking_id
        , sp.picking_date
        , sp.picking_date_done
        , sp.picking_state
        , spt.picking_type_name

        , case 
            when so.sale_order_id is not null then sp.sale_id
            else null 
        end  sale_order_id
        , case 
            when so.sale_order_id is not null then sm.sale_line_id
            else null 
        end  sale_order_line_id
        , sm.po_line_id

    FROM stock_move_line sml
    left join stock_move sm 
    on sml.move_id = sm.move_id
    left join stock_picking sp 
    on sml.picking_id = sp.picking_id
    left join stock_picking_type spt 
    on sp.picking_type_id = spt.picking_type_id
    left join sale_orders so 
    on so.sale_order_id = sp.sale_id
)

select 
    *
from joined_tbl
where 1=1