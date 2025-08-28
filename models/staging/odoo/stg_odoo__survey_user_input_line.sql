WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_survey_user_input_line') }} AS odoo_survey_user_input_line
), renamed AS (
  SELECT
    "id" survey_answer_id,
    "skipped",
    "order_id",
    "stage_id",
    "survey_id",
    "partner_id",
    "quizz_mark",
    "value_date" + interval '7 hours' AS value_date,
    "value_text",
    "write_date" + interval '7 hours' AS last_update,
    "answer_type",
    "create_date" + interval '7 hours' AS create_date,
    "date_create" + interval '7 hours' AS date_create,
    "question_id",
    "value_number",
    "user_input_id",
    "opportunity_id",
    "value_free_text",
    "value_suggested",
    "value_suggested_row"
  FROM source
)
SELECT
  *
FROM renamed
where 1=1