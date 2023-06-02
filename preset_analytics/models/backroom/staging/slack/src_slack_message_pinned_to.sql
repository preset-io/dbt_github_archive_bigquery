with source as (
  select * from {{ source('slack', 'message_pinned_to') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
