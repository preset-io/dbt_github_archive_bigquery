with source as (
  select * from {{ source('slack', 'profile_field_option') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
