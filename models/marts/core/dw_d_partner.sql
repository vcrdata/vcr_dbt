with partner_data as (
    select * from {{ ref('stg_odoo__res_partner') }}
)
, membership_level as (
    select * from {{ ref('stg_odoo__membership_level') }}
)
, order_data as (
    select 
        partner_id
        , min(confirmation_date) first_confirmation_date
        , min(order_date) first_order_date
    from {{ ref('int_order') }}
    group by partner_id
)
, res_country_state as (
    select * from {{ ref('stg_odoo__res_country_state') }}
)
select
    p.partner_id
    , p.partner_ref
    , p.partner_name
    , p.partner_type
    , p.email
    , p.phone
    , p.active
    , p.gender
    , p.mobile
    , p.street
    , p.street2
    , p.team_id
    , p.user_id
    , p.website
    , p.birthday
    , CASE
        WHEN date_part('year', age(p.birthday)) < 25 THEN '24'
        WHEN date_part('year', age(p.birthday)) < 34 THEN '25-34'
        WHEN date_part('year', age(p.birthday)) < 45 THEN '35-44'
        WHEN date_part('year', age(p.birthday)) >= 45 THEN '45'
        ELSE ''
    END partner_age_grp
    , p.customer_flg
    , p.employee_flg
    , p.supplier_flg
    , p.is_company
    , p.partner_function
    , p.ward_id
    , p.district_id
    , p.state_id
    , rcs.state_name
    , p.country_id
    , p.membership_id
    , m.membership_name
    , p.loyalty_points
    , p.vietnam_full_address
    , p.commercial_partner_id
    , p.commercial_company_name
    , o.first_confirmation_date
    , o.first_order_date
    , p.last_update
    , p.create_date
from partner_data p
left join membership_level m 
on p.membership_id = m.membership_id
left join order_data o 
on p.partner_id = o.partner_id
left join res_country_state rcs
on p.state_id = rcs.state_id