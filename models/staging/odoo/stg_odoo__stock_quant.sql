WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_quant') }} AS odoo_stock_quant
)
, renamed AS (
  SELECT
    "id" stock_quant_id,
    "notes",
    "lot_id" serial_id,
    "in_date" + interval '7 hours' AS in_date,
    "quantity",
    "product_id",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "location_id",
    "reserved_quantity",
    "product_category_id",
    row_number() over (partition by lot_id order by id desc) rank_loc 
  FROM source
)
SELECT
  *
FROM renamed