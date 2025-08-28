with source as (
        select * from {{ source('odoo', 'odoo_crm_lost_reason') }}
  ),
  renamed as (
      select
        id as lost_reason_id,
        "name" as lost_reason_name,
        active ,
        write_uid ,
        create_uid ,
        write_date + interval '7 hours' as last_update,
        create_date + interval '7 hours' as create_date
      from source
  )
  select * from renamed
    