with source as (
  select * from {{ source('slack', 'bookmark') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
