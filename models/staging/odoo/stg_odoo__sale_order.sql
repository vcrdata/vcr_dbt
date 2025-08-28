WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_sale_order') }} AS odoo_sale_order
)
, renamed AS (
  SELECT
    "id" sale_order_id
    , "name" order_code
    , "note" order_note
    , "state" order_state
    , "origin"
    , "team_id"
    , "user_id"
    , "medium_id"
    , "source_id"
    , "campaign_id"
    , "confirmation_date" + interval '7 hours' confirmation_date
    , "commitment_date" + interval '7 hours' commitment_date
    , "effective_date" + interval '7 hours' effective_date
    , "validity_date" + interval '7 hours' validity_date
    , "date_order" + interval '7 hours' order_date
    , "partner_id"
    , "points_won"
    , "amount_untaxed"
    , "amount_tax"
    , "amount_total" order_amt_total
    , "points_total"
    , "warehouse_id"
    , "discount_rate" discount_small_change
    , "amount_discount" order_amt_discount
    , "invoice_status"
    , "opportunity_id"
    , "picking_policy"
    , "run_loyalty_reward"
    , "analytic_account_id"
    , "deposit_cancel_info"
    , "procurement_group_id"
    , "create_date" + interval '7 hours' create_date
    , "write_date" + interval '7 hours' last_update
  FROM source
)
SELECT
  *
FROM renamed