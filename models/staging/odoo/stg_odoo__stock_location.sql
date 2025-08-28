WITH 
source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_location') }} AS odoo_stock_location
)
, renamed AS (
  SELECT
    "id" location_id,
    "name" location_name,
    "usage" location_usage,
    "active",
    "barcode",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "location_id" location_parent_id,
    "parent_path",
    "warehouse_id",
    "complete_name" location_complete_name
  FROM source
)
SELECT
  *
FROM renamed