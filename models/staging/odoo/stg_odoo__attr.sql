WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_attribute') }} AS odoo_product_attribute
)
, renamed AS (
  SELECT
    "id" attribute_id,
    "hide",
    "name" attribute_name,
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "attribute_type"
  FROM source
)
SELECT
  *
FROM renamed