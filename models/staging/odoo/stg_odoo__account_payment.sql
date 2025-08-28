WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_account_payment') }} AS odoo_account_payment
)
, renamed AS (
  SELECT
    "id" payment_id
    , "name" payment_name
    , "notes"
    , "state" payment_state
    , "amount" payment_amount
    , "order_id" sale_order_id
    , "partner_id"
    , "move_name"
    , "journal_id"
    , "currency_id"
    , "partner_type"
    , "payment_date"
    , "payment_type"
    , "amount_signed"
    , "communication"
    , "payment_difference_handling"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed