with 
rpt_serial_cycle_metrics as (
    select * from {{ ref('rpt_serial_cycle_metrics') }}
)
, jewelry_serials as (
    select * from {{ ref('dw_d_jewelry_serial') }}
)
, sale_data_rpt as (
    select * from {{ ref('rpt_sale_order_data') }}
)
, product_display_date as (
    select 
        product_id
        , min(display_date) as first_display_date
    from rpt_serial_cycle_metrics
    group by product_id
)
select
    m.root_serial_name 
    , m.serial_cycle
    , m.input_date
    , m.display_date
    , m.destructed_date
    , m.sale_output_date
    , m.sale_order_line_id
    , m.po_line_id
    , m.display_duration
    , m.num_serial_id
    , m.serial_ids
    , m.serial_names 
    , m.last_serial_id

    /*jewelry serial info*/
    , js.serial_name as last_serial_name
    , js.serial_display_name as last_serial_display_name
    , js.biz_product_code
    , js.product_note

    , js.categ_full_name

    , pd.first_display_date

    , js.internal_quantity
    , js.location_name
    , js.location_complete_name
    , js.warehouse_code
    , js.warehouse_name
    , js.price_segment
    , js.design_form
    , js.cost jewelry_cost
    , js.fixed_price jewelry_fixed_price
    , js.gross_price jewelry_gross_price
    , js.stone_root_serial_names
    , js.stone_cost 
    , js.stone_price
    , js.jewelry_size
    , js.jewelry_weight_mace
    , js.total_weight_gram
    , js.total_weight_mace 

    /*order info*/
    , sd.order_code
    , sd.order_date
    , sd.confirmation_date
    , sd.price_unit sale_order_price_unit
    , sd.price_subtotal sale_order_price_subtotal
    , sd.total_discount
    , sd.team_name

from rpt_serial_cycle_metrics m 
join jewelry_serials js 
on m.last_serial_id = js.serial_id

left join sale_data_rpt sd
on m.sale_order_line_id = sd.sale_order_line_id

left join product_display_date pd 
on pd.product_id = m.product_id
where 1=1
{# and js.internal_quantity > 0 #}