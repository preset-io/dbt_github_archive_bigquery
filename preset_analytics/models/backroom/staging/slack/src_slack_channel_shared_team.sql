with source as (
  select * from {{ source('slack', 'channel_shared_team') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
