WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_category') }} AS odoo_product_category
)

, renamed AS (
  SELECT
    "id" product_categ_id,
    "code" categ_code,
    "name" categ_name,
    "parent_id",
    "write_uid",
    "create_uid",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "parent_path",
    "complete_name" categ_full_name
  FROM source
)

SELECT
  *
FROM renamed