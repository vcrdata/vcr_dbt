WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_survey_label') }} AS odoo_survey_label
), renamed AS (
  SELECT
    "id" label_id,
    "value" label_value,
    "quizz_mark",
    "write_date" + interval '7 hours' AS write_date,
    "create_date" + interval '7 hours' AS last_update,
    "question_id",
    "question_id_2"
  FROM source
)
SELECT
  *
FROM renamed