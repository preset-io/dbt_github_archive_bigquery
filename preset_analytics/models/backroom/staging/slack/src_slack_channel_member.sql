with source as (
  select * from {{ source('slack', 'channel_member') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
