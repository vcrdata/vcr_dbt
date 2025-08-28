WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_picking') }} AS odoo_stock_picking
)
, renamed AS (
  SELECT
    "id" picking_id
    , "date" + interval '7 hours' AS picking_date
    , "scheduled_date" + interval '7 hours' AS picking_scheduled_date
    , "date_done" + interval '7 hours' AS picking_date_done
    , "name" picking_name
    , "note"
    , "state" picking_state
    , "origin" picking_origin
    , "priority"
    , "move_type"
    , "is_locked"
    , "is_approve"
    , "partner_id"
    , "sale_id"
    , "backorder_id"
    , "scan_barcode"
    , "picking_type_id"
    , "repair_order_id"
    , "location_id"
    , "location_dest_id"
    , "wh_in_transfer_id"
    , "create_date" + interval '7 hours' AS create_date
    , "write_date" + interval '7 hours' AS last_update
  FROM source
)
SELECT
  *
FROM renamed