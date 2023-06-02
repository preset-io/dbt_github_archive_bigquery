with source as (
  select * from {{ source('slack', 'message_file') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
