with source as (
  select * from {{ source('slack', 'fivetran_audit') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
