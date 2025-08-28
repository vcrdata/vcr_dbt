
with 
stone_serials as (
    select * from {{ ref('dw_d_stone_serial') }}
)
, jewelry_serials as (
    select * from {{ ref('dw_d_jewelry_serial') }}
)
, stone_products as (
    select * from {{ ref('dw_d_stone_product') }}
)
, move_line_metrics as (
    select * from {{ ref('rpt_move_line_metrics') }}
)
, sale_data_rpt as (
    select * from {{ ref('rpt_sale_order_data') }}
)
, po_rpt as (
    select * from {{ ref('rpt_all_purchase') }}
)
select 
    ss.serial_id
    , ss.serial_name
    , ss.root_serial_name
    , ss.serial_display_name
    , ss.product_id
    
    /*STONE PRODUCTS INFO*/
    , sp.default_code
    , sp.old_reference
    , sp.stone_type_code
    , sp.stone_type_name
    , sp.shape_code
    , sp.shape_name
    , sp.size_code
    , sp.size_name
    , sp.color_code
    , sp.color_name
    , sp.clarity_code
    , sp.clarity_name

    /*ATTRIBUTE OF STONE*/
    , ss.stone_weight
    , ss.stone_color
    , ss.stone_clarity
    , ss.stone_polish
    , ss.stone_symmetry
    , ss.stone_cut
    , ss.stone_measurement
    , ss.stone_fluorescence
    , ss.stone_shape
    , ss.stone_gia_report_number

    /*COST AND PRICE*/
    , ss.cost
    , ss.fixed_price

    , js.serial_id jewelry_serial_id
    , js.serial_name jewelry_serial_name
    , js.root_serial_name jewelry_root_serial_name
    , js.serial_display_name jewelry_serial_display_name
    
    , case
        when js.serial_id is null then ss.internal_quantity
        else js.internal_quantity
    end as internal_quantity
    , case
        when js.serial_id is null then ss.location_name 
        else js.location_name 
    end as location_name
    , case
        when js.serial_id is null then ss.location_complete_name 
        else js.location_complete_name 
    end as location_complete_name
    , case
        when js.serial_id is null then ss.location_usage 
        else js.location_usage 
    end as location_usage
    , case
        when js.serial_id is null then ss.warehouse_code 
        else js.warehouse_code 
    end as warehouse_code
    , case
        when js.serial_id is null then ss.warehouse_name 
        else js.warehouse_name 
    end as warehouse_name

    /*product code info*/
    , js.default_code  as jewelry_default_code
    , js.biz_product_code as jewelry_biz_product_code  

    /*Move line metrics*/
    , mlm.input_date
    , mlm.display_date
    , mlm.destructed_date
    , mlm.sale_output_date
    , mlm.display_duration

    /*Order data*/
    , mlm.sale_order_line_id
    , sd.order_code
    , sd.order_date
    , sd.confirmation_date
    , sd.jewelry_price_unit
    , sd.jewelry_price_subtotal
    , sd.jewelry_total_discount
    , sd.stone_price_unit
    , sd.stone_price_subtotal
    , sd.stone_total_discount
    , sd.total_discount
    , sd.price_subtotal
    , sd.team_name

    /* Purchase data*/
    , po.po_line_id
    , po.po_code
    , po.po_date_order
    , po.price_subtotal po_price_subtotal
    , po.warehouse_code po_warehouse_code
    
from stone_serials ss 

left join stone_products sp 
on ss.product_id = sp.product_id

left join jewelry_serials js 
on ss.jewelry_serial_id = js.serial_id

left join move_line_metrics mlm
on ss.serial_id = mlm.serial_id

left join sale_data_rpt sd
on mlm.sale_order_line_id = sd.sale_order_line_id

left join po_rpt po
on ss.serial_id = po.serial_id