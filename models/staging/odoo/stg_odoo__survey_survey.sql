WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_survey_survey') }} AS odoo_survey_survey
), renamed AS (
  SELECT
    "id" survey_id,
    "color",
    "title",
    "active",
    "stage_id",
    "quizz_mode",
    "write_date" + interval '7 hours' AS last_update,
    "create_date" + interval '7 hours' AS create_date,
    "description",
    "email_template_id",
    "thank_you_message",
    "users_can_go_back",
    "message_main_attachment_id"
  FROM source
)
SELECT
  *
FROM renamed