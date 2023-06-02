with source as (
  select * from {{ source('slack', 'channel') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
