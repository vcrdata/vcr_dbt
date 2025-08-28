WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_pricelist_item') }} AS odoo_product_pricelist_item
)
, renamed AS (
  SELECT
    "id" pricelist_item_id
    , "pricelist_id"
    , "base"
    , "applied_on"
    , "product_tmpl_id"
    , "lot_id" serial_id
    , "categ_id"
    , "date_start" + interval '7 hours' AS date_start
    , "date_end" + interval '7 hours' AS date_end
    , "product_id"
    , "fixed_price"
    , "price_discount"
    , "compute_price"
    , "min_quantity"
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed