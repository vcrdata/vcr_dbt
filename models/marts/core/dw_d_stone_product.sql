{% set v_stone_categ_codes = var('stone_categ_codes') %}

with

int_products as (
    select * from {{ ref("int_product") }}
)

, filtered_jewelry_products as (
    select
        /*PRODUCT INFO*/
        ip.product_id
        , ip.active
        , ip.default_code
        , ip.last_update
        , ip.create_date
        

        /*CATEGORY INFO*/
        , ip.categ_id
        , ip.categ_code
        , ip.categ_name
        , ip.categ_full_name
        , ip.parent_path
        
        /*PRODUCT CODE INFO*/
        , coalesce(ip.old_reference, ip.default_code) as biz_product_code
        , ip.old_reference

        /*PRODUCTS INFO*/
        , ip.industry_stone_type_id stone_type_id 
        , ip.industry_stone_type_code stone_type_code
        , ip.industry_stone_type_name stone_type_name
        , ip.purity_shape_id shape_id
        , ip.purity_shape_code shape_code
        , ip.purity_shape_name shape_name
        , ip.variety_size_id size_id
        , ip.variety_size_code size_code
        , ip.variety_size_name size_name
        , ip.type_color_id color_id
        , ip.type_color_code color_code
        , ip.type_color_name color_name
        , ip.main_stone_clarity_id clarity_id
        , ip.main_stone_clarity_code clarity_code
        , ip.main_stone_clarity_name clarity_name

    from int_products ip 
    where 1=1
    and ip.categ_code in ('{{ v_stone_categ_codes | join('\', \'') }}')
    or ip.industry_stone_type_code in ('{{ v_stone_categ_codes | join('\', \'') }}')
)

select 
    *
from filtered_jewelry_products
