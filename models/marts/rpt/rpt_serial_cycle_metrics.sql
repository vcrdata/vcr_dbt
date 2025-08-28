with 
move_line_fact as (
    select * from {{ ref('dw_f_move_line') }}
    where serial_id is not null 
)
, all_serials as (
    select * from {{ ref('dw_d_all_serial') }}
)
, display_stocks as (
    select location_id from {{ ref('int_stock_location') }}
    where location_name = 'Trưng bày'
)
, joined_tbl as (
    select 
        s.serial_name
        , s.root_serial_name
        , s.internal_quantity
        , mlf.*
        , case 
            when mlf.sale_order_id is null  
                then lag(sale_order_line_id) over (
                    partition by s.root_serial_name
                    order by mlf.move_line_id --mlf.picking_id,
                )
        end last_sale_order_line_id
    from move_line_fact mlf
    join all_serials s
    on mlf.serial_id = s.serial_id
    where 1=1
    and move_line_state = 'done'
)
, cycle_tbl as (
    select 
        *
        , 1 + sum(
            case
                when last_sale_order_line_id is not null then 1
                else 0
            end 
        ) over (
            partition by root_serial_name
            order by picking_id, move_line_id
        ) serial_cycle
    from joined_tbl
)
, serial_dates as (
    select
        c.root_serial_name
        , c.serial_cycle
        , min(c.product_id) as product_id
        , min(c.move_date) as input_date
        , min(
            case
                when ds.location_id is not null then c.move_date
            end
        ) display_date
        , min(
            case
                when c.location_dest_id = 61 then c.move_date
            end
        ) destructed_date
        , max(
            case
                when c.sale_order_line_id is not null then c.move_date 
            end 
        ) sale_output_date
        , max(c.sale_order_line_id) sale_order_line_id
        , max(c.po_line_id) po_line_id
        , max(internal_quantity) internal_quantity
        , count(distinct cast(c.serial_id as varchar)) num_serial_id
        , string_agg(distinct cast(c.serial_id as varchar), ',') serial_ids
        , string_agg(distinct c.serial_name, ',') serial_names
        , max(c.serial_id) last_serial_id
    from cycle_tbl c
    left join display_stocks ds
    on c.location_dest_id = ds.location_id
    group by c.root_serial_name, c.serial_cycle 
)
select 
    sd.root_serial_name 
    , sd.serial_cycle
    , sd.product_id
    , sd.input_date
    , sd.display_date
    , sd.destructed_date
    , sd.sale_output_date
    , sd.sale_order_line_id
    , sd.po_line_id
    , case 
    	when sd.sale_output_date is not null then EXTRACT(DAY from (sd.sale_output_date - sd.display_date))::integer 
    	when sd.destructed_date is not null then EXTRACT(DAY from (sd.destructed_date - sd.display_date))::integer
    	when sd.internal_quantity > 0 then EXTRACT(DAY from (CURRENT_DATE AT TIME zone 'Asia/Ho_Chi_Minh' - sd.display_date))::integer + 1
    	else null
    end  as display_duration
    , sd.num_serial_id
    , sd.serial_ids
    , sd.serial_names 
    , sd.last_serial_id
from serial_dates sd 
where 1=1