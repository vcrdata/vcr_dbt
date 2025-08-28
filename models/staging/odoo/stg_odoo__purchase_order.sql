WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_purchase_order') }} AS odoo_purchase_order
)
, renamed AS (
  SELECT
    "id" po_id
    , "name" po_code
    , "type" po_type 
    , "notes"
    , "state" po_state
    , "origin" po_origin
    , "partner_id"
    , "amount_untaxed" po_amount_untaxed
    , "amount_tax" po_amount_tax
    , "amount_total" po_amount_total
    , "date_order" + interval '7 hours' po_date_order 
    , "date_approve" + interval '7 hours' po_date_approve
    , "date_planned" + interval '7 hours' date_planned
    , "invoice_count"
    , "picking_count"
    , "invoice_status"
    , "requisition_id"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' create_date
  FROM source
)
SELECT
  *
FROM renamed