WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_ace_sale_promotion') }} AS odoo_ace_sale_promotion
)
, renamed AS (
  SELECT
    "id" promotion_id
    , "name" promotion_name
    , "type" promotion_type
    , "active"
    , "discount"
    , "apply_coupon"
    , "rule_date_from" + interval '7 hours' AS rule_date_from
    , "rule_date_to" + interval '7 hours' AS rule_date_to
    , "discount_type"
    , "allocation_type"
    , "gift_product_id"
    , "rule_min_quantity"
    , "validity_duration"
    , "maximum_use_number"
    , "rule_minimum_amount"
    , "rule_partners_domain"
    , "rule_minimum_amount_tax_inclusion"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed