WITH
purchase_orders as (
    SELECT * FROM {{ ref('stg_odoo__purchase_order') }}
)
, purchase_order_lines as (
    SELECT * FROM {{ ref('stg_odoo__purchase_order_line') }}
)
, move_line_fact as (
    select * from {{ ref('dw_f_move_line') }}
) 
, joined_tbls as (
    select
        po.po_id
        ,po.po_code
        , po.po_type
        , po.po_state
        , po.po_origin
        , po.partner_id
        , mlf.serial_id
        , po.po_date_order
        , po.po_date_approve
        , po.invoice_status
        , pol.po_line_id
        , pol.price_tax
        , pol.price_total
        , pol.price_subtotal
        , pol.price_unit
        , pol.sale_price
        , pol.repurchase_percent
        , pol.sale_order_line_id
        , mlf.location_dest_id
        , po.create_date
        , po.last_update
    from purchase_order_lines pol 
    join purchase_orders po 
    on pol.po_id = po.po_id
    join move_line_fact mlf
    on pol.po_line_id = mlf.po_line_id
    where 1=1
    and po.po_state <> 'cancel'
)
select
    *
from joined_tbls