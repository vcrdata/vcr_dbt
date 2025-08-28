with 

attributes as (
    select * from {{ ref('stg_odoo__attr') }}
)


, attribute_values as (
    select * from {{ ref('stg_odoo__attr_value') }}
)


, attribute_serial_lines as (
    select * from {{ ref('stg_odoo__attr_line') }}
)

select 
    asl.serial_id
    , asl.attribute_id
    , asl.attribute_value_id
    , av.attribute_value
    , a.attribute_name
from attribute_serial_lines asl  
left join attribute_values av 
on asl.attribute_value_id = av.value_id

left join attributes a 
on asl.attribute_id = a.attribute_id

where asl.serial_id is not null