WITH 
source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_jewelry_size') }} AS odoo_jewelry_size
)
, renamed AS (
  SELECT
    "id" size_id,
    "name" size_code,
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description" size_name
  FROM source
)
SELECT
  *
FROM renamed