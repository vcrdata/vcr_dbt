with 
crm_lead as (
    select * from {{ ref('stg_odoo__crm_lead') }}
)
, crm_lead_line as (
    select * from {{ ref('stg_odoo__crm_lead_line') }}
)
select 
    l.lead_id
    , ll.lead_line_id
    , l.lead_display_name
    , l.team_id
    , l.visitor
    , l.medium_id
    , l.source_id
    , l.partner_id
    , l.note
    , l.lost_reason
    , l.purchase_reason
    , l.won_status
    , l.outcome_date
    , l.create_date as lead_create_date 
    , ll.price_unit
    , ll.product_qty
    , ll.planned_revenue
    , ll.product_id
from crm_lead l
join crm_lead_line ll
on l.lead_id = ll.lead_id
