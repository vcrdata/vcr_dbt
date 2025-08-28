WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_utm_source') }} AS odoo_utm_source
)
, renamed AS (
  SELECT
    "id" source_id
    , "name" source_name
    , "active"
    , "write_date" last_update
    , "create_date"
  FROM source
)
SELECT
  *
FROM renamed