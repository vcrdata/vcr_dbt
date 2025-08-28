WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_purchase_order_line') }} AS odoo_purchase_order_line
)
, renamed AS (
  SELECT
    "id" po_line_id
    , "lot_name"
    , "order_id" po_id
    , "price_tax"
    , "price_unit"
    , "product_id"
    , "sale_price"
    , "product_qty"
    , "product_uom"
    , "qty_invoiced"
    , "qty_received"
    , "price_total"
    , "price_subtotal"
    , "product_uom_qty"
    , "sale_order_line" sale_order_line_id
    , "repurchase_percent"
    , "write_date" + interval '7 hours' last_update
    , "create_date" + interval '7 hours' create_date
  FROM source
)
SELECT
  *
FROM renamed