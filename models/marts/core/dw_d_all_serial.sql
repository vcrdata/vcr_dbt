{{ config(materialized='view') }}

with 
all_serials as (
    select 
        serial_id 
        , serial_name
        , root_serial_name
        , serial_display_name
        , product_id
        , biz_product_code
        , default_code
        , price_segment
        , 1 as jewelry_prd_flag
        , internal_quantity
        , cost jewelry_cost
        , fixed_price
        , gross_price
        , stone_cost
    from {{ ref('dw_d_jewelry_serial') }}

    union

    select
        serial_id 
        , serial_name
        , root_serial_name
        , serial_display_name
        , product_id
        , biz_product_code
        , default_code
        , null as price_segment
        , 0 as jewelry_prd_flag
        , internal_quantity
        , null as jewelry_cost
        , fixed_price
        , gross_price
        , cost stone_cost
    from {{ ref("dw_d_stone_serial") }}
)

select
    *
from all_serials