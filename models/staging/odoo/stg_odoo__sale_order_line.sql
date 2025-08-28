{{ config(materialized='ephemeral') }}

WITH 
source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_sale_order_line') }} AS odoo_sale_order_line
)
, renamed AS (
  SELECT
    "id" sale_order_line_id
    , "state"
    , "discount"
    , "order_id" sale_order_id
    , "route_id"
    , "order_type"
    , "discount_amount"
    , "price_subtotal"
    , "price_unit"
    , "product_id"
    , "product_uom_qty"
    , "product_uom"
    , "salesman_id"
    , "invoice_status"
    , "write_date" + interval '7 hours' last_update
    , "create_date" + interval '7 hours' 
  FROM source
)
select
  *
from renamed
where 1=1