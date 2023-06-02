with source as (
  select * from {{ source('slack', 'usergroup') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
