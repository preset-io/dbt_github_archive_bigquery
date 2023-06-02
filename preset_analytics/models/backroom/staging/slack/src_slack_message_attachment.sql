with source as (
  select * from {{ source('slack', 'message_attachment') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
