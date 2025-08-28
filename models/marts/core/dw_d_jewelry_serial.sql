with

int_serials as (
    select * from {{ ref('int_serial') }}
)

,int_serial_attributes as (
    select * from {{ ref('int_serial_attr') }}
)

,dw_d_jewelry_products as(
    select * from {{ ref("dw_d_jewelry_product") }}
)

, jewelry_sizes as (
    select * from {{ ref('stg_odoo__stock_prod_lot_size_code') }}
)

, jewelry_colors as (
    select * from {{ ref('stg_odoo__stock_prod_lot_color_code') }}
)

, jewelry_main_stones as (
    select * from {{ ref('stg_odoo__stock_prod_lot_main_stone_code') }}
)

, jewelry_side_stones as (
    select * from {{ ref('stg_odoo__stock_prod_lot_side_stone_code') }}
)

, serial_attribute_cols as (
    select 
        serial_id
        ,MAX(
            case 
                when attribute_name = 'TSV Size sản phẩm' 
                    and attribute_value ~ '^[-+]?[0-9]*\.?[0-9]+$' 
                    then CAST(attribute_value AS float)
                when attribute_name = 'Size sản phẩm' 
                    and attribute_value ~ '^[-+]?[0-9]*\.?[0-9]+$' 
                    then CAST(attribute_value AS float)
                else null 
            end
        ) as jewelry_size
        , max(case 
            when attribute_name = 'Trọng lượng vàng (chỉ)' then attribute_value
            when attribute_name = 'TSV Trọng lượng vàng (chỉ)' then attribute_value
            else null
        end) jewelry_weight_mace
        , max(case 
            when attribute_name = 'Tổng trọng lượng (gram)' then attribute_value
            when attribute_name = 'TSV Tổng trọng lượng (gram)' then attribute_value
            else null
        end) total_weight_gram
        , max(case 
            when attribute_name = 'Tổng trọng lượng (chỉ)' then attribute_value
            else null
        end) total_weight_mace
    from int_serial_attributes 
    group by serial_id
)
, attached_stone_serials as (
    select 
        s.jewelry_serial_id
        , string_agg(cast(s.serial_id as varchar), ';') stone_serial_ids
        , string_agg(s.root_serial_name, ';') stone_root_serial_names
        , string_agg(s.serial_name, ';') stone_serial_names
        , string_agg(s.default_code, ';') stone_default_codes
        , count(1) stone_quant
        , sum(s.cost) stone_cost
        , sum(s.fixed_price) stone_price
    from {{ ref('dw_d_stone_serial') }} s 
    where 1=1
    and s.jewelry_serial_id is not null 
    group by s.jewelry_serial_id
)

, joined_jewelry_serials as (
    select  
        s.serial_id
        , s.serial_name
        , s.root_serial_name
        , s.serial_display_name
        , s.product_id
        , s.serial_parent_id
        , s.last_update
        , s.create_date
        , s.product_uom_id
        , s.active 
        , s.jewelry_side_stone_id
        , s.jewelry_main_stone_id
        , s.jewelry_size_id
        , s.jewelry_color_id
        /*Attributes*/
        , sac.jewelry_size
        , sac.jewelry_weight_mace
        , sac.total_weight_gram
        , sac.total_weight_mace 
        , jc.color_code
        , jc.color_name
        , js.size_code
        , js.size_name
        , jms.main_stone_code
        , jms.main_stone_name
        , jss.side_stone_code
        , jss.side_stone_name
        
        /*quant info*/
        , s.internal_quantity
        , s.location_name
        , s.location_complete_name
        , s.location_usage
        , s.warehouse_code
        , s.warehouse_name

        /*jewelry product basic*/
        , jp.default_code
        , jp.old_reference
        ,
        case
            when jp.ca_cn_flag = 1 
                and sac.jewelry_size >= 12 then REPLACE(jp.biz_product_code, 'NC', 'CA')
            when jp.ca_cn_flag = 1 
                and sac.jewelry_size < 12 then REPLACE(jp.biz_product_code, 'NC', 'CN')
            when jp.cx_flag = 1 then 'CX' || right(jp.biz_product_code, 5)
            else jp.biz_product_code
        end biz_product_code
        , jp.categ_code
        , jp.categ_full_name
        , jp.price_segment
        , jp.design_form
        , jp.base_price
        , jp.product_note

        /*price info */
        , s.cost
        , s.gross_price
        , s.fixed_price

        /*stone price for attachment jewelry*/
        , ass.stone_serial_ids
        , ass.stone_root_serial_names
        , ass.stone_serial_names
        , ass.stone_default_codes
        , ass.stone_quant
        , ass.stone_cost
        , ass.stone_price        

    from int_serials s 
    
    /*only keep the jewelry product serials*/
    join dw_d_jewelry_products jp 
    on s.product_id = jp.product_id

    join serial_attribute_cols sac
    on sac.serial_id = s.serial_id

    left join jewelry_sizes js
    on s.jewelry_size_id = js.size_id
    
    left join jewelry_colors jc 
    on s.jewelry_color_id = jc.color_id
    
    left join jewelry_main_stones jms 
    on s.jewelry_main_stone_id = jms.main_stone_id
    
    left join jewelry_side_stones jss 
    on s.jewelry_side_stone_id = jss.side_stone_id

    left join attached_stone_serials ass
    on s.serial_id = ass.jewelry_serial_id
)

select
    *
from joined_jewelry_serials