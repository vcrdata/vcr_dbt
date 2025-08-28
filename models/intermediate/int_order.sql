with
sale_orders as (
    select * from {{ ref('stg_odoo__sale_order') }}
    where order_state != 'cancel'
)
, payments as (
    select
        sale_order_id
        , count(1) num_payment
        , sum(payment_amount) order_deposit_amt
    from {{ ref('stg_odoo__account_payment') }}
    where sale_order_id is not null 
    and payment_state not in ('draft', 'cancelled')
    group by sale_order_id
)

select 
    o.*
    , p.num_payment
    , p.order_deposit_amt
from sale_orders o 
left join payments p 
on o.sale_order_id = p.sale_order_id

where 1=1