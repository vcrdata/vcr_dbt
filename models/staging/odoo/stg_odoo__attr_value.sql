WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_attribute_value') }} AS odoo_product_attribute_value
)
, renamed AS (
  SELECT
    "id" value_id,
    "name" attribute_value,
    "type",
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "attribute_id"
  FROM source
)
SELECT
  *
FROM renamed