with source as (
        select * from {{ source('odoo', 'odoo_crm_lead_tag_rel') }}
  ),
  renamed as (
      select
        lead_id
        , tag_id
      from source
  )
  select * from renamed
    