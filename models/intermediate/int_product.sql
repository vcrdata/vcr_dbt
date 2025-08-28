{% set v_ignored_categ_codes = var('ignored_categ_codes') %}

with products as (
    select * from {{ ref('stg_odoo__product') }}
)

, product_templates as (
    select * from {{ ref('stg_odoo__product_template') }}
)

, product_categ as (
    select * from {{ ref("stg_odoo__product_category") }}
)

, industry_stone_type_codes as (
    select * from {{ ref("stg_odoo__product_industry_stone_type_code") }}
)

, purity_shape_codes as (
    select * from {{ ref("stg_odoo__product_purity_shape_code") }}
)

, variety_size_codes as (
    select * from {{ ref("stg_odoo__product_variety_size_code") }}
)

, type_color_codes as (
    select * from {{ ref("stg_odoo__product_type_color_code") }}
)

, main_stone_clarity_codes as (
    select * from {{ ref("stg_odoo__product_main_stone_clarity_code") }}
)

, side_stone_codes as (
    select * from {{ ref("stg_odoo__product_side_stone_code") }}
)

, joined_product_info as (
    select 
        /*PRODUCT INFO*/
        p.product_id
        , p.active
        , p.default_code
        , p.last_update
        , p.create_date
        

        /*CATEGORY INFO*/
        , pt.categ_id
        , pc.categ_code
        , pc.categ_name
        , pc.categ_full_name
        , pc.parent_path
        , pt.product_note

        /*PRODUCT CODE INFO*/
        , pt.old_reference
        , pt.old_code_text
        
        , pt.industry_stone_type_id 
        , isc.code industry_stone_type_code
        , isc.description industry_stone_type_name
        
        , pt.purity_shape_id
        , psc.code purity_shape_code
        , psc.description purity_shape_name
        
        , pt.variety_size_id
        , vsc.code variety_size_code
        , vsc.description variety_size_name
        
        , pt.type_color_id
        , tcc.code type_color_code
        , tcc.description type_color_name
        
        , pt.main_stone_clarity_id
        , mscc.code main_stone_clarity_code
        , mscc.description main_stone_clarity_name
        
        , pt.side_stone_id
        , ssc.code side_stone_code
        , ssc.description side_stone_name
        
    from products p

    left join product_templates pt 
    on p.product_tmpl_id = pt.product_tmpl_id

    left join product_categ pc 
    on pc.product_categ_id = pt.categ_id

    left join industry_stone_type_codes isc
    on isc.id = pt.industry_stone_type_id

    left join purity_shape_codes psc
    on psc.id = pt.purity_shape_id

    left join variety_size_codes vsc
    on vsc.id = pt.variety_size_id

    left join type_color_codes tcc
    on tcc.id = pt.type_color_id

    left join main_stone_clarity_codes mscc
    on mscc.id = pt.main_stone_clarity_id

    left join side_stone_codes ssc
    on ssc.id = pt.side_stone_id

)
, final_data as (
    select 
        *
    from joined_product_info
    where 1=1
    and categ_code not in ( '{{  v_ignored_categ_codes | join('\', \'') }}' )
)

select 
    *
from final_data