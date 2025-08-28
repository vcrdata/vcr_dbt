with 
move_line_fact as (
    select * from {{ ref('dw_f_move_line') }}
    where serial_id is not null 
)
, display_stocks as (
    select location_id from {{ ref('int_stock_location') }}
    where location_name = 'Trưng bày'
)
, all_serials as (
    select * from {{ ref('dw_d_all_serial') }}
)
, serial_dates as (
    select 
        mlf.serial_id
        , min(mlf.move_date) as input_date
        , min(
            case
                when ds.location_id is not null then mlf.move_date
            end
        ) display_date
        , min(
            case
                when mlf.location_dest_id = 61 then mlf.move_date
            end
        ) destructed_date
        , max(
            case
                when mlf.sale_order_line_id is not null then mlf.move_date 
            end 
        ) sale_output_date
        , max(mlf.sale_order_line_id) sale_order_line_id
        , max(mlf.po_line_id) po_line_id
    from move_line_fact mlf
    left join display_stocks ds
    on mlf.location_dest_id = ds.location_id
    group by mlf.serial_id
)
select 
    sd.serial_id 
    , sd.input_date
    , sd.display_date
    , sd.destructed_date
    , sd.sale_output_date
    , sd.sale_order_line_id
    , sd.po_line_id
    , case 
    	when sd.sale_output_date is not null then EXTRACT(DAY from (sd.sale_output_date - sd.display_date))::integer 
    	when sd.destructed_date is not null then EXTRACT(DAY from (sd.destructed_date - sd.display_date))::integer
    	when s.internal_quantity > 0 then EXTRACT(DAY from (CURRENT_DATE AT TIME zone 'Asia/Ho_Chi_Minh' - sd.display_date))::integer + 1
    	else null
    end  as display_duration
from serial_dates sd 
left join all_serials s
on sd.serial_id = s.serial_id