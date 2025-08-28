WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_picking_type') }} AS odoo_stock_picking_type
)
, renamed AS (
  SELECT
    "id" picking_type_id
    , "code"
    , "name" picking_type_name
    , "active"
    , "barcode"
    , "is_related"
    , "sequence_id"
    , "warehouse_id"
    , "show_reserved"
    , "show_operations"
    , "use_create_lots"
    , "show_entire_packs"
    , "use_existing_lots"
    , "return_picking_type_id"
    , "default_location_src_id"
    , "default_location_dest_id"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed