
with 
move_line_fact as (
    select 
        * 
    from {{ ref('dw_f_move_line') }}
)
, stock_locations as (
    select 
        * 
    from {{ ref('int_stock_location') }}
)
, all_serials as (
    select 
        serial_id 
        , serial_name
        , root_serial_name
        , serial_display_name
        , biz_product_code
        , default_code
    from {{ ref('dw_d_all_serial') }}
)

, final_data as (
    select 
        mlf.move_line_id
        , mlf.move_id
        , mlf.picking_id
        , mlf.move_state

        , mlf.serial_id
        , s.serial_name
        , s.root_serial_name
        , s.serial_display_name
        
        , mlf.product_id
        , s.biz_product_code
        , s.default_code

        , mlf.move_name
        , mlf.move_reference
        , mlf.move_origin
        , mlf.move_date

        , mlf.partner_id
        , mlf.sale_order_id
        , mlf.sale_order_line_id
        , mlf.po_line_id

        , mlf.location_id
        , l1.location_name
        , l1.location_complete_name
        , l1.warehouse_code

        , mlf.location_dest_id
        , l2.location_name location_dest_name
        , l2.location_complete_name location_dest_complete_name
        , l2.warehouse_code location_dest_warehouse_code

    from move_line_fact mlf 
    left join stock_locations l1 
    on mlf.location_id = l1.location_id

    left join stock_locations l2 
    on mlf.location_dest_id = l2.location_id

    left join all_serials s
    on mlf.serial_id = s.serial_id

    where 1=1
    {# and (l1.location_name = 'Trưng bày'
    or l2.location_name = 'Trưng bày'
    ) #}
    and mlf.serial_id is not null
    and mlf.move_state = 'done'
)

select 
    *
from final_data
