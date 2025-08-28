WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_stock_production_lot_linked_rel') }}
)
, renamed AS (
  SELECT
    "lot_id" jewelry_serial_id,
    "linked_lot_id" stone_serial_id
  FROM source
)
SELECT
  *
FROM renamed