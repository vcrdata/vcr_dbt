with source as (
  select * from {{ source('odoo', 'odoo_utm_medium') }}
),
renamed as (
    select
		"id" as medium_id,
		"name" as medium_name,
		"active",
		"write_uid",
		"create_uid",
		"write_date" + interval '7 hours' as last_update,
		"create_date" + interval '7 hours' as create_date
    from source
)
select * from renamed
    