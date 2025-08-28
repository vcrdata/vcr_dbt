WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_main_stone') }} AS odoo_main_stone
)

, renamed AS (
  SELECT
    "id",
    "name" code,
    "active",
    "write_uid",
    "create_uid",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description"
  FROM source
)

SELECT
  *
FROM renamed