with
users as (
    select * from {{ ref('stg_odoo__res_users') }}
)
, parters as (
    select * from {{ ref('dw_d_partner') }}
)


select 
    u.user_id
    , u.sale_team_id
    , p.partner_ref
    , p.partner_name
from users u 
left join parters p
on u.partner_id = p.partner_id