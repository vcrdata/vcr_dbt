with source as (
    select * from {{ source('odoo', 'odoo_crm_lead_tag') }}
),
renamed as (
    select
        id
        , "name" tag_name
        , color
        , active
        , write_uid
        , create_uid
        , write_date + interval '7 hours' as last_update
        , create_date + interval '7 hours' as create_date
    from source
)
select * from renamed
    