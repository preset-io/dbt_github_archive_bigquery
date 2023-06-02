with source as (
  select * from {{ source('slack', 'pinned_message') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
