WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_membership_level') }} AS odoo_membership_level
)
, renamed AS (
  SELECT
    "id" membership_id
    , "code" membership_code 
    , "name" membership_name
    , "point"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
    , "referring_percent"
  FROM source
)
SELECT
  *
FROM renamed