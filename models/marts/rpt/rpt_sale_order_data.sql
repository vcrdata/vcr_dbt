with 
sale_line_fact as (
    select * from {{ ref('dw_f_sale_order_line') }}
)
, partner_dim as (
    select * from {{ ref('dw_d_partner') }}
)
, utm_source as (
    select * from {{ ref('stg_odoo__utm_source') }}
)
, stg_lead as (
    select * from {{ ref('stg_odoo__crm_lead') }}
)
, all_serials as (
    select * from {{ ref('dw_d_all_serial') }}
)
, all_products as (
    select * from {{ ref('dw_d_all_product') }}
)
, crm_team as (
    select * from {{ ref('stg_odoo__crm_team') }}
)
, salemans as (
    select * from {{ ref('int_saleman') }}
)

select 
    slf.sale_order_line_id
    , slf.sale_order_id
    
    /*order info*/
    , slf.order_code
    , slf.order_type
    , slf.order_state 
    , slf.order_date
    , slf.confirmation_date
    , slf.order_note

    /*utm info*/
    , l.source_id
    , s.source_name
    , ct.team_name
    , sm.partner_name as saleman

    /*product info*/
    , slf.serial_id
    , ps.serial_name
    , slf.product_id
    , coalesce(ps.default_code, ap.default_code) product_default_code
    , coalesce(ps.biz_product_code, ap.biz_product_code) biz_product_code
    , coalesce(ps.price_segment, ap.price_segment) price_segment
    , ps.jewelry_cost
    , ps.stone_cost

    /*order line info*/
    , slf.price_unit
    , slf.price_subtotal
    , slf.price_unit - slf.price_subtotal as total_discount
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 1) or ( ps.jewelry_prd_flag = 1 ) 
            then slf.price_unit
        else 0
    end jewelry_price_unit
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 1) or ( ps.jewelry_prd_flag = 1 ) 
            then slf.price_subtotal
        else 0
    end jewelry_price_subtotal
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 1) or ( ps.jewelry_prd_flag = 1 ) 
            then slf.price_unit - slf.price_subtotal
        else 0
    end jewelry_total_discount
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 0) or ( ps.jewelry_prd_flag = 0 ) 
            then slf.price_unit
        else 0
    end stone_price_unit
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 0) or ( ps.jewelry_prd_flag = 0 ) 
            then slf.price_subtotal
        else 0
    end stone_price_subtotal
    , case 
        when (slf.serial_id is null and ap.jewelry_prd_flag = 0) or ( ps.jewelry_prd_flag = 0 )
            then slf.price_unit - slf.price_subtotal
        else 0
    end stone_total_discount
    , slf.order_amt_total
    , slf.order_deposit_amt
    , slf.deposit_amt
    , slf.price_subtotal - slf.deposit_amt as remaining_amt

    /*partner info*/
    , slf.partner_id
    , p.partner_ref
    , p.partner_name
    , p.gender
    , p.email
    , p.mobile
    , p.birthday
    , p.partner_age_grp
    , p.membership_name
    , p.first_confirmation_date
    , case 
        when slf.confirmation_date is null and p.first_confirmation_date is not null then 'KH Cũ'
        when extract(year from p.first_confirmation_date) <> extract(year from slf.confirmation_date) then 'KH Cũ'
        else 'KH Mới'
    end as partner_type

    /*promotion info*/
    , slf.promotion_id
    , slf.promotion_name

    /*lead info*/
    , slf.opportunity_id
    , l.note as lead_note
    , l.campaign_code
    , l.marketer_code
    , l.page_employee_code
    , l.online_sale_consultant_code
    , l.lead_name

from sale_line_fact slf

left join partner_dim p 
on slf.partner_id = p.partner_id

left join stg_lead l
on l.lead_id = slf.opportunity_id

left join utm_source s
on l.source_id = s.source_id

left join all_serials ps
on ps.serial_id = slf.serial_id

left join all_products ap
on ap.product_id = slf.product_id

left join crm_team ct
on slf.team_id = ct.team_id

left join salemans sm
on slf.user_id = sm.user_id

