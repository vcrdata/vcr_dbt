WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_jewelry_types') }} AS odoo_jewelry_types
)

, renamed AS (
  SELECT
    "id",
    "name" code,
    "active",
    "write_uid",
    "create_uid",
    "write_date" + interval '7 hours' last_update,
    "create_date" + interval '7 hours' create_date,
    "description"
  FROM source
)
SELECT
  *
FROM renamed