WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_jewelry_color') }} AS odoo_jewelry_color
)
, renamed AS (
  SELECT
    "id" color_id,
    "name" color_code,
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description" color_name
  FROM source
)
SELECT
  *
FROM renamed