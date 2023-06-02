with source as (
  select * from {{ source('slack', 'users') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
