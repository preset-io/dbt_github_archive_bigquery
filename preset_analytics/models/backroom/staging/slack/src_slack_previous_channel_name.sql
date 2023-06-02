with source as (
  select * from {{ source('slack', 'previous_channel_name') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
