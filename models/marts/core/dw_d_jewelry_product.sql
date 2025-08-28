{% set v_stone_categ_codes = var('stone_categ_codes') %}

with

int_products as (
    select * from {{ ref("int_product") }}
)

, wedding_product_codes as (
    select * from {{ ref('stg_user_input__wedding_product_code') }}
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
        , ip.old_reference
        , ip.old_code_text
        , ip.product_note

        ,case 
            when ip.old_reference ~ '^[^-]+(-[^-]+){4}$' then concat(split_part(ip.old_reference, '-', 1), '-', split_part(ip.old_reference, '-', 2))
            else ip.old_reference
        end as biz_product_code
        ,case 
            when ip.old_reference ~ '^[^-]+(-[^-]+){4}$' then split_part(ip.old_reference, '-', 3)
            else null
        end as price_segment
        ,case 
            when ip.old_reference ~ '^[^-]+(-[^-]+){4}$' then split_part(ip.old_reference, '-', 4)
            else null
        end as base_price
        ,case 
            when ip.old_reference ~ '^[^-]+(-[^-]+){4}$' then split_part(ip.old_reference, '-', 5)
            else null
        end as design_form

        /*PRODUCTS INFO*/
        , ip.industry_stone_type_id industry_id 
        , ip.industry_stone_type_code industry_code
        , ip.industry_stone_type_name industry_name
        , ip.purity_shape_id purity_id
        , ip.purity_shape_code purity_code
        , ip.purity_shape_name purity_name
        , ip.variety_size_id variety_id
        , ip.variety_size_code variety_code
        , ip.variety_size_name variety_name
        , ip.type_color_id type_id
        , ip.type_color_code type_code
        , ip.type_color_name type_name
        , ip.main_stone_clarity_id main_stone_id
        , ip.main_stone_clarity_code main_stone_code
        , ip.main_stone_clarity_name main_stone_name
        , ip.side_stone_id
        , ip.side_stone_code
        , ip.side_stone_name

    from int_products ip 

    where 1=1
    and ip.categ_code not in ('{{ v_stone_categ_codes | join('\', \'') }}')
    and (
        ip.industry_stone_type_code not in ('{{ v_stone_categ_codes | join('\', \'') }}')
        or ip.industry_stone_type_code is null
    )
)

, mapping_wedding_products as (
    select
        jp.*
        , wpc.ca_cn_flag
        , wpc.cx_flag

    from filtered_jewelry_products jp 

    left join wedding_product_codes wpc
    on wpc.biz_product_code = jp.biz_product_code
)

select 
    *
from mapping_wedding_products
