WITH 
source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_res_partner') }} AS odoo_res_partner
)
, renamed AS (
  SELECT
    "id" partner_id
    , "ref" partner_ref
    , "name" partner_name
    , "type" partner_type
    , "email"
    , "phone"
    , "active"
    , "gender"
    , "mobile"
    , "street"
    , "street2"
    , "team_id"
    , "user_id"
    , "website"
    , "birthday"
    , "customer" customer_flg
    , "employee" employee_flg
    , "supplier" supplier_flg
    , "is_company"
    , "function" partner_function
    , "ward_id"
    , "district_id"
    , "state_id"
    , "country_id"
    , "membership_id"
    , "loyalty_points"
    , "vietnam_full_address"
    , "commercial_partner_id"
    , "commercial_company_name"
    , "write_date" + interval '7 hours' last_update
    , "create_date" + interval '7 hours' create_date
  FROM source
)
SELECT
  *
FROM renamed