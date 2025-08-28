WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_crm_team') }} AS odoo_crm_team
)
, renamed AS (
  SELECT
    "id" team_id
    , "name" team_name
    , "active"
    , "team_type"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
    , "shop_warehouse_id"
  FROM source
)
SELECT
  *
FROM renamed