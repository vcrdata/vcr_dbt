with source as (
        select * from {{ source('odoo', 'odoo_res_country_state') }}
  ),
  renamed as (
      select
        "id" state_id,
        "code",
        "name" state_name,
        "write_uid" ,
        "country_id",
        "create_uid",
        "write_date" + interval '7 hours' as last_update,
        "create_date" + interval '7 hours' as create_date
      from source
  )
  select * from renamed
    