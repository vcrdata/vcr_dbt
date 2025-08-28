WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_warehouse') }} AS odoo_stock_warehouse
), renamed AS (
  SELECT
    "id" warehouse_id,
    "code" warehouse_code,
    "name" warehouse_name,
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed