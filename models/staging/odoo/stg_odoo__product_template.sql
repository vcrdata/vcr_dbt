WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_product_template') }} AS odoo_product_template
)

, renamed AS (
  SELECT
    "id" product_tmpl_id,
    "name" code,
    "type" product_type,
    "active",
    "uom_id",
    "sale_ok",
    "categ_id",
    "group_id",
    "sequence",
    "uom_po_id",
    "write_date" + interval '7 hours' last_update,
    "create_date" + interval '7 hours' create_date,
    "default_code",
    "old_reference",
    "jewelry_industry_id" industry_stone_type_id,
    "jewelry_purity_id" purity_shape_id,
    "jewelry_varieties_id" variety_size_id,
    "jewelry_types_id" type_color_id,
    "main_stone_id" main_stone_clarity_id,
    "side_stone_id" side_stone_id,
    "old_code_text",
    "description" product_note
  FROM source
)

SELECT
  *
FROM renamed