WITH source AS (
  SELECT
    *
  FROM {{ source('odoo', 'odoo_crm_lead') }} AS odoo_crm_lead
)
, renamed AS (
  SELECT
    "id" lead_id
    , "name" lead_name
    , "display_name" lead_display_name
    , "type" lead_type
    , "active"
    , "team_id"
    , "user_id"
    , "term_id"
    , "visitor"
    , "stage_id"
    , "state_id"
    , "source_id"
    , "medium_id"
    , "parent_id"
    , "partner_id"
    , "campaign_id"
    , "probability"
    , "description" note
    , "contact_name"
    , "partner_name"
    , "customer_type"
    , "lost_reason"
    , "purchase_reason"
    , "won_status"
    , "date_action_last" + interval '7 hours' AS date_action_last
    , "planned_revenue"
    , "expected_revenue"
    , "date_last_stage_update" + interval '7 hours' AS date_last_stage_update
    , "outcome_date" + interval '7 hours' AS outcome_date
    , "date_open" + interval '7 hours' AS date_open
    , "date_closed" + interval '7 hours' AS date_closed
    , "date_deadline" + interval '7 hours' AS date_deadline
    , CASE
        WHEN description ~ '^[^/]*/[^/]*/[^/]*/[^/]*$' THEN 
            (regexp_match(description, '([^/]*)/([^/]*)/([^/]*)/([^/]*)'))[1]
        ELSE NULL
    END AS campaign_code
    , CASE
        WHEN description ~ '^[^/]*/[^/]*/[^/]*/[^/]*$' THEN 
            (regexp_match(description, '([^/]*)/([^/]*)/([^/]*)/([^/]*)'))[2]
        ELSE NULL
    END AS marketer_code
    , CASE
        WHEN description ~ '^[^/]*/[^/]*/[^/]*/[^/]*$' THEN 
            (regexp_match(description, '([^/]*)/([^/]*)/([^/]*)/([^/]*)'))[3]
        ELSE NULL
    END AS page_employee_code
    , CASE
        WHEN description ~ '^[^/]*/[^/]*/[^/]*/[^/]*$' THEN 
            (regexp_match(description, '([^/]*)/([^/]*)/([^/]*)/([^/]*)'))[4]
        ELSE NULL
    END AS online_sale_consultant_code
    , CASE
		    WHEN description ~ '^[^/]+/[^/]+/[^/]+/[^/]+$' THEN 'marketing'
		    ELSE 'sale'
    END AS department_source
    , "write_date" + interval '7 hours' AS last_update
    , "create_date" + interval '7 hours' AS create_date
  FROM source
)
SELECT
  *
FROM renamed