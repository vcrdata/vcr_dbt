{{ config(materialized='view') }}
with
jewelry_product_dim as (
    select * from {{ ref("dw_d_jewelry_product") }}
)

, stone_product_dim as (
    select * from {{ ref("dw_d_stone_product") }}
)

, union_products as (
    select 
        j.product_id
        , j.default_code
        , j.biz_product_code 
        , 1 as jewelry_prd_flag
        , j.price_segment
        , j.design_form
    from jewelry_product_dim j 
    union
    select 
        s.product_id
        , s.default_code
        , s.biz_product_code 
        , 0 as jewelry_prd_flag
        , null as price_segment
        , null as design_form
    from stone_product_dim s 
)

select 
    *
from union_products
