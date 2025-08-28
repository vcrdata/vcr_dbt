with
sale_orders as (
    select * from {{ ref('int_order') }}
)
, sale_order_lines as (
    select * from {{ ref('stg_odoo__sale_order_line') }}
)
, move_line_fact as (
    select 
        *
        , row_number() over (
            partition by sale_order_line_id
            order by move_line_id desc
        ) rn
    from {{ ref('dw_f_move_line') }}
    where sale_order_line_id is not null
)
, promotion_dim as (
    select * from {{ ref('stg_odoo__sale_promotion') }}
)
, promotion_rels as (
    select 
        sale_order_id
        , string_agg(s.promotion_id::text, ',') as promotion_id
        , string_agg(promotion_name, ',') as promotion_name
    from {{ ref('stg_odoo__sale_order_promotion_rel') }} s
    left join promotion_dim pd
    on s.promotion_id = pd.promotion_id
    where pd.promotion_id is not null
    group by sale_order_id
)
select
    sol.sale_order_line_id
    , sol.sale_order_id
    , so.order_code
    , sol.product_id
    , sol.order_type
    , sol.discount
    , sol.discount_amount
    , sol.price_unit
    , sol.price_subtotal
    , so.num_payment
    , so.order_deposit_amt
    , so.order_amt_total
    , round(
        so.order_deposit_amt * 
        (sol.price_subtotal / nullif(so.order_amt_total, 0)))::bigint as deposit_amt
    , so.order_state
    , so.team_id
    , so.user_id
    , so.partner_id
    , so.medium_id
    , so.source_id
    , so.campaign_id
    , so.confirmation_date
    , so.order_date
    , so.warehouse_id
    , so.opportunity_id
    , so.invoice_status
    , so.order_note

    , ml.serial_id
    , pr.promotion_id
    , pr.promotion_name

    , so.create_date
    , so.last_update
from sale_order_lines sol 
join sale_orders so
on sol.sale_order_id = so.sale_order_id

left join move_line_fact ml
on ml.sale_order_line_id = sol.sale_order_line_id
and ml.rn = 1

left join promotion_rels pr
on so.sale_order_id = pr.sale_order_id

where 1=1

order by so.sale_order_id desc