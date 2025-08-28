WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_survey_question') }} AS odoo_survey_question
), renamed AS (
  SELECT
    "id" question_id,
    "type" question_type,
    "page_id",
    "question",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description",
    "display_mode",
    "matrix_subtype",
    "comments_allowed",
    "comments_message",
    "constr_error_msg",
    "constr_mandatory",
    "validation_email",
    "validation_max_date" + interval '7 hours' AS validation_max_date,
    "validation_min_date" + interval '7 hours' AS validation_min_date,
    "validation_required",
    "validation_error_msg",
    "validation_length_max",
    "validation_length_min",
    "comment_count_as_answer",
    "validation_max_float_value",
    "validation_min_float_value"
  FROM source
)
SELECT
  *
FROM renamed