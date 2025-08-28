WITH source AS (
  SELECT
    *
  FROM {{ source('user_input', 'user_input_wedding_product_code') }} AS user_input_wedding_product_code
)
, renamed AS (
  SELECT
    "biz_product_code",
    "ca_cn_flag",
    "cx_flag"
  FROM source
)
SELECT
  *
FROM renamed