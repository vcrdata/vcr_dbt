with
all_serials as (
    select 
        *
    from {{ ref('dw_d_all_serial') }}
)
, po_line_fact as (
    select
        *
    from {{ ref('dw_f_po_line') }}
)
, all_partners as (
    select 
        *
    from {{ ref('dw_d_partner') }}
)
, stock_locations as (
    select 
        * 
    from {{ ref('int_stock_location') }}
)
, joined_all_tbls as (
    select
        plf.po_line_id
        , plf.po_id
        , plf.po_code
        , plf.po_type
        , plf.po_state
        , plf.po_origin
        , plf.partner_id
        , pa.partner_name
        , pa.partner_ref
        , plf.serial_id
        , s.serial_name
        , s.root_serial_name
        , s.serial_display_name
        , s.biz_product_code
        , s.default_code
        , plf.po_date_order
        , plf.po_date_approve
        , plf.invoice_status
        , plf.price_tax
        , plf.price_total
        , plf.price_subtotal
        , plf.price_unit
        , plf.sale_price
        , plf.repurchase_percent
        , plf.sale_order_line_id
        , plf.location_dest_id
        , l.location_name
        , l.location_complete_name
        , l.warehouse_code
        , plf.create_date
        , plf.last_update 
    from po_line_fact plf 
    left join all_serials s
    on plf.serial_id = s.serial_id

    left join all_partners pa
    on plf.partner_id = pa.partner_id

    left join stock_locations l 
    on plf.location_dest_id = l.location_id
)
select
    *
from joined_all_tbls
