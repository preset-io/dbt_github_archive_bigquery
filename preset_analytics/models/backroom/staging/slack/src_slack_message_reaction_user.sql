with source as (
      select * from {{ source('slack', 'message_reaction_user') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
