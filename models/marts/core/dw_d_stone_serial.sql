with

int_serials as (
    select * from {{ ref('int_serial') }}
)

,int_serial_attributes as (
    select * from {{ ref('int_serial_attr') }}
)

,dw_d_stone_products as(
    select * from {{ ref("dw_d_stone_product") }}
)

, stock_lot_rels as (
    select * from {{ ref("stg_odoo__stock_prod_lot_linked_rel") }}
)

, serial_attribute_cols as (
    select 
        serial_id
        , MAX(case when attribute_name = 'Trọng lượng đá (carat)' then trim(attribute_value) else null end) as stone_weight
        , MAX(case when attribute_name = 'Cấp độ màu sắc' then trim(attribute_value) else null end) as stone_color
        , MAX(case when attribute_name = 'Cấp độ tinh khiết' then trim(attribute_value) else null end) as stone_clarity
        , MAX(case when attribute_name = 'Cấp độ bóng' then trim(attribute_value) else null end) as stone_polish
        , MAX(case when attribute_name = 'Cấp độ đối xứng' then trim(attribute_value) else null end) as stone_symmetry
        , MAX(case when attribute_name = 'Cấp độ cắt mài' then trim(attribute_value) else null end) as stone_cut
        , MAX(case when attribute_name = 'Kích thước đá (mm)' then ltrim(ltrim(replace(attribute_value, ',', '.'), E'\t')) else null end) as stone_measurement
        , MAX(case when attribute_name = 'Cấp độ huỳnh quang' then trim(attribute_value) else null end) as stone_fluorescence
        , MAX(case when attribute_name = 'Hình dáng đá chính' then trim(attribute_value) else null end) as stone_shape
        , MAX(case 
            when attribute_name = 'Mã số đá-GIA' then trim(attribute_value)
            when attribute_name = 'Mã số đá-VCR' then trim(attribute_value) 
        end) as stone_gia_report_number
    from int_serial_attributes 
    group by serial_id
)

,joined_stone_serials as (
    select  
        s.serial_id
        , s.serial_name
        , s.root_serial_name
        , s.serial_display_name
        , s.product_id
        , s.serial_parent_id
        , s.last_update
        , s.create_date
        , s.cost
        , s.gross_price
        , s.fixed_price
        , s.product_uom_id
        , s.active
        , slr.jewelry_serial_id

        /*stone serial attributes*/
        , sac.stone_weight
        , sac.stone_color
        , sac.stone_clarity
        , sac.stone_polish
        , sac.stone_symmetry
        , sac.stone_cut
        , sac.stone_measurement
        , sac.stone_fluorescence
        , sac.stone_shape
        , sac.stone_gia_report_number

        /*quant info*/
        , s.internal_quantity
        , s.location_name
        , s.location_complete_name
        , s.location_usage
        , s.warehouse_code
        , s.warehouse_name

        /*product info*/
        , sp.default_code
        , sp.old_reference
        , sp.biz_product_code
    from int_serials s 
    
    /*only keep the jewelry product serials*/
    join dw_d_stone_products sp 
    on s.product_id = sp.product_id

    join serial_attribute_cols sac
    on sac.serial_id = s.serial_id

    left join stock_lot_rels slr 
    on s.serial_id = slr.stone_serial_id
)

select
    *
from joined_stone_serials