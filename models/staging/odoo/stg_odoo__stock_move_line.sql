WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_move_line') }} AS odoo_stock_move_line
)
, renamed AS (
  SELECT
    "id" move_line_id
    , "cost"
    , "date" + interval '7 hours' AS move_line_date
    , "notes"
    , "state" move_line_state 
    , "lot_id" serial_id
    , "move_id"
    , "lot_name"
    , "lot_name_copy"
    , "qty_done"
    , "reference" move_line_reference
    , "picking_id"
    , "product_qty"
    , "product_id"
    , "product_uom_id"
    , "cabochon_id"
    , "center_stone_id"
    , "jewelry_size_id"
    , "product_uom_qty"
    , "jewelry_color_id"
    , "location_id"
    , "location_dest_id"
    , "create_date" + interval '7 hours' AS create_date
    , "write_date" + interval '7 hours' AS last_update
  FROM source
)
SELECT
  *
FROM renamed