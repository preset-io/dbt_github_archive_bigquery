with source as (
  select * from {{ source('slack', 'message_block') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
