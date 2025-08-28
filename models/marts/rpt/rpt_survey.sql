with 
survey as (
    select 
        *
    from {{ ref('stg_odoo__survey_survey') }}
), survey_user_input_line as (
    select 
        *
    from {{ ref('stg_odoo__survey_user_input_line') }}
), survey_question as (
    select 
        *
    from {{ ref('stg_odoo__survey_question') }}
), survey_label as (
    select 
        *
    from {{ ref('stg_odoo__survey_label') }}
) 
, all_partners as (
    select 
        *
    from {{ ref('dw_d_partner') }}
)
, price_ranges_prd_answers as (
    select 
        ss.survey_id
        , ss.stage_id 
        , suil.opportunity_id 
        , suil.question_id 
        , suil.user_input_id
        , suil.partner_id 
        , ap.partner_ref
        , ap.partner_name
        , ap.mobile
        , ap.gender
        , ss.title 
        , suil.date_create
        , sq.question 
        , suil.answer_type 
        , s2.label_value 
        , suil.value_free_text 
        , suil.value_text 
        , suil.value_date
        , suil.value_number 
        , max(case when sl.label_value = 'Dưới 5 triệu' then 1 else 0 end) as under_5_million
        , max(case when sl.label_value = 'Từ 5 triệu - 15 triệu' then 1 else 0 end) as from_5_to_15_million
        , max(case when sl.label_value = 'Từ 15 triệu - 30 triệu' then 1 else 0 end) as from_15_to_30_million
        , max(case when sl.label_value = 'Từ 30 triệu - 50 triệu' then 1 else 0 end) as from_30_to_50_million
        , max(case when sl.label_value = 'Từ 50 triệu - 100 triệu' then 1 else 0 end) as from_50_to_100_million
        , max(case when sl.label_value = 'Trên 100 triệu' then 1 else 0 end) as over_100_million
    from survey ss
    left join survey_user_input_line suil 
    on ss.survey_id = suil.survey_id 
    left join survey_question sq 
    on suil.question_id = sq.question_id 
    left join survey_label sl
    on suil.value_suggested_row = sl.label_id
    left join survey_label s2 
    on suil.value_suggested = s2.label_id 
    left join all_partners ap
    on suil.partner_id = ap.partner_id 
    where 1=1
    and suil.question_id = 230
    group by 
        ss.survey_id
        , ss.stage_id 
        , suil.opportunity_id 
        , suil.question_id 
        , suil.user_input_id
        , suil.partner_id 
        , ap.partner_ref
        , ap.partner_name
        , ap.mobile
        , ap.gender
        , ss.title 
        , suil.date_create
        , sq.question 
        , suil.answer_type 
        , s2.label_value
        , suil.value_free_text 
        , suil.value_text 
        , suil.value_date
        , suil.value_number 
)
, normal_answers as (
    select 
        ss.survey_id
        , ss.stage_id 
        , suil.opportunity_id 
        , suil.question_id 
        , suil.user_input_id
        , suil.partner_id 
        , ap.partner_ref
        , ap.partner_name
        , ap.mobile
        , ap.gender
        , ss.title 
        , suil.date_create
        , sq.question 
        , suil.answer_type 
        , sl.label_value 
        , suil.value_free_text 
        , suil.value_text 
        , suil.value_date
        , suil.value_number 
    from survey ss
    left join survey_user_input_line suil 
    on ss.survey_id = suil.survey_id 
    left join survey_question sq 
    on suil.question_id = sq.question_id 
    left join survey_label sl 
    on suil.value_suggested = sl.label_id 
    left join all_partners ap
    on suil.partner_id = ap.partner_id 
    where 1=1
    and suil.question_id != 230
)
select 
    *
    , null as under_5_million
    , null as from_5_to_15_million
    , null as from_15_to_30_million
    , null as from_30_to_50_million
    , null as from_50_to_100_million
    , null as over_100_million 
from normal_answers a2
union 
select
    *
from price_ranges_prd_answers a1 