{{ config(materialized='ephemeral') }}
WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_sale_order_promotion_rel') }} AS odoo_sale_order_promotion_rel
)
, renamed AS (
  SELECT
    "order_id" sale_order_id,
    "promotion_id"
  FROM source
)
SELECT
  *
FROM renamed