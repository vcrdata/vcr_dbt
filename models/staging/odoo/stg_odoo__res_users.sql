WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_res_users') }} AS odoo_res_users
)
, renamed AS (
  SELECT
    "id" user_id
    , "active"
    , "partner_id"
    , "write_date" + interval '7 hours' write_date
    , "create_date" + interval '7 hours' last_update
    , "sale_team_id"
  FROM source
)
SELECT
  *
FROM renamed