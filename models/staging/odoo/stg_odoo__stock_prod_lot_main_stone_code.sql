WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_center_stone') }} AS odoo_center_stone
)
, renamed AS (
  SELECT
    "id" main_stone_id,
    "name" main_stone_code,
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description" main_stone_name
  FROM source
)
SELECT
  *
FROM renamed