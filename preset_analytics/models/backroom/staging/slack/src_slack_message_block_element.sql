with source as (
  select * from {{ source('slack', 'message_block_element') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
