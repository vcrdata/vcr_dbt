WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_cabochon') }} AS odoo_cabochon
), renamed AS (
  SELECT
    "id" side_stone_id,
    "name" side_stone_code,
    "active",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description" side_stone_name
  FROM source
)
SELECT
  *
FROM renamed