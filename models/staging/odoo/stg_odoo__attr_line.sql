WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_v_ace_product_attribute_line') }} AS odoo_ace_product_attribute_line
)
, renamed AS (
  SELECT
    "id" attribute_line_id,
    "lot_id" serial_id,
    "value_id" attribute_value_id,
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "attribute_id",
    "move_line_id"
  FROM source
)
SELECT
  *
FROM renamed