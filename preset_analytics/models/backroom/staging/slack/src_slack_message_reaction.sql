with source as (
  select * from {{ source('slack', 'message_reaction') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
