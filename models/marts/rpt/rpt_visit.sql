with lead_data as (
    select 
        *
    from {{ ref('stg_odoo__crm_lead') }}
)
, lead_tag as (
    select
        r.lead_id
        , r.tag_id
        , t.tag_name 
    from {{ ref('stg_odoo__crm_lead_tag_rel') }} r
    left join {{ ref('stg_odoo__crm_lead_tag') }} as t
    on r.tag_id = t.id
)
, customer_data as (
    select * from {{ ref('dw_d_partner') }}
)
, lost_reason as (
    select * from {{ ref('stg_odoo__crm_lost_reason') }}
)
, all_products as (
    select * from {{ ref('dw_d_all_product') }}
)
, crm_team as (
    select * from {{ ref('stg_odoo__crm_team') }}
)
, crm_source as (
    select * from {{ ref('stg_odoo__utm_source') }}
)
, crm_medium as (
    select * from {{ ref('stg_odoo__utm_medium') }}
)
, salemans as (
    select * from {{ ref('int_saleman') }}
)
, sale_order_lines as (
    select
        s.opportunity_id
        , min(s.order_date) as order_date
        , string_agg(distinct s.order_code, ',') as order_codes
        , string_agg(distinct p.biz_product_code, ',') as biz_product_codes
        , string_agg(distinct left(p.biz_product_code, 2), ',') as biz_product_categs
        , string_agg(distinct s.promotion_name, ';') as promotion_names
    from {{ ref('dw_f_sale_order_line') }} s 
    left join all_products p 
    on s.product_id = p.product_id
    group by s.opportunity_id
)
select
    l.lead_id
    , l.lead_name
    , l.lead_display_name
    , l.lead_type
    , l.active
    , l.team_id
    , l.user_id
    , l.term_id
    , l.visitor
    , l.stage_id
    , l.state_id
    , l.source_id
    , l.medium_id
    , l.parent_id
    , l.partner_id
    , l.campaign_id
    , l.probability
    , l.contact_name
    , l.customer_type
    
    , l.purchase_reason
    , l.won_status
    , l.date_action_last
    , l.planned_revenue
    , l.expected_revenue
    , l.date_last_stage_update
    , l.outcome_date
    , l.date_open
    , l.date_closed
    , l.date_deadline

    , l.last_update
    , l.create_date

    -- parsing lead note
    , l.note
    , l.campaign_code 
    , l.marketer_code
    , l.page_employee_code
    , l.online_sale_consultant_code
    , l.department_source

    -- tag name
    , t.tag_name

    -- lost reason data
    , l.lost_reason
    , lr.lost_reason_name

    -- customer data
    , c.partner_ref
    , c.partner_name
    , c.state_name
    , c.gender
    , c.partner_age_grp
    , c.membership_name
    , c.mobile
    , c.first_confirmation_date
    , c.first_order_date
    , case 
        when TO_CHAR(s.order_date, 'YYYY') = TO_CHAR(c.first_order_date, 'YYYY') then 'KH MOI'
        when TO_CHAR(s.order_date, 'YYYY') <> TO_CHAR(c.first_order_date, 'YYYY') then 'KH CU'
        when TO_CHAR(l.create_date, 'YYYY') = TO_CHAR(c.first_order_date, 'YYYY') then 'KH MOI'
        when TO_CHAR(l.create_date, 'YYYY') <> TO_CHAR(c.first_order_date, 'YYYY') then 'KH CU'
        when c.first_order_date is null then 'KH MOI'
        else ''
    end cust_type

    -- sale order lines data
    , s.order_codes
    , s.order_date
    , s.biz_product_codes
    , s.biz_product_categs
    , s.promotion_names
    , ct.team_name
    , sm.partner_name as saleman_name
    , cs.source_name
    , cm.medium_name
    
from lead_data l
left join lead_tag t
on l.lead_id = t.lead_id
left join customer_data c
on l.partner_id = c.partner_id
left join lost_reason lr
on l.lost_reason = lr.lost_reason_id
left join sale_order_lines s
on l.lead_id = s.opportunity_id
left join crm_team ct
on l.team_id = ct.team_id
left join salemans sm
on l.user_id = sm.user_id
left join crm_source cs
on l.source_id = cs.source_id
left join crm_medium cm
on l.medium_id = cm.medium_id

where 1=1