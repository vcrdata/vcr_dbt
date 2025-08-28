WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_product') }} AS odoo_product_product
)

, renamed AS (
  SELECT
    "id" product_id,
    "active",
    "write_date" + interval '7 hours' last_update,
    "create_date" + interval '7 hours' create_date,
    "default_code",
    "product_tmpl_id"
  FROM source
)

SELECT
    *
FROM renamed