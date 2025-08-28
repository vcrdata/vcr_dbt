WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_production_lot') }}
)

, renamed AS (
  SELECT
    "id" serial_id,
    "name" serial_name,
    "notes",
    "active",
    "old_name",
    "parent_id" serial_parent_id,
    "product_id",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "cost",
    "gross_price",
    "display_name" serial_display_name,
    "product_uom_id",
    "cabochon_id" jewelry_side_stone_id,
    "center_stone_id" jewelry_main_stone_id,
    "jewelry_size_id",
    "jewelry_color_id"
  FROM source
)

SELECT
  *
FROM renamed