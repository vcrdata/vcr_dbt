WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_crm_lead_line') }} AS odoo_crm_lead_line
)
, renamed AS (
  SELECT
    "id" lead_line_id
    , "name"
    , "lead_id"
    , "product_id"
    , "price_unit"
    , "product_qty"
    , "planned_revenue"
    , "expected_revenue"
    , "create_date" + interval '7 hours' AS create_date
    , "write_date" + interval '7 hours' AS last_update
  FROM source
)
SELECT
  *
FROM renamed