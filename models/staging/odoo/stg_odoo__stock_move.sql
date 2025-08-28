WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_move') }} AS odoo_stock_move
)
, renamed AS (
  SELECT
    "id" move_id
    , "date" move_date
    , "name" move_name
    , "note"
    , "state" move_state
    , "value"
    , "origin" move_origin
    , "real_qty"
    , "propagate"
    , "reference" move_reference
    , "to_refund"
    , "partner_id"
    , "picking_id"
    , "price_unit"
    , "product_id"
    , "product_qty"
    , "product_uom"
    , "inventory_id"
    , "date_expected" + interval '7 hours' AS date_expected
    , "remaining_qty"
    , "procure_method"
    , "picking_type_id"
    , "product_uom_qty"
    , "remaining_value"
    , "warehouse_id"
    , "location_id"
    , "location_dest_id"
    , "sale_line_id"
    , "purchase_line_id" po_line_id
    , "origin_returned_move_id"
    , "created_purchase_line_id"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed