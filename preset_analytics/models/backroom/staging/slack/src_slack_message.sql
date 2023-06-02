with source as (
  select * from {{ source('slack', 'message') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
