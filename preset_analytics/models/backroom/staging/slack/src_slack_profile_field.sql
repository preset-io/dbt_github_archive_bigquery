with source as (
  select * from {{ source('slack', 'profile_field') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
