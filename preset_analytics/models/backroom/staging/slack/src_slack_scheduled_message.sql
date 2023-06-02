with source as (
  select * from {{ source('slack', 'scheduled_message') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
