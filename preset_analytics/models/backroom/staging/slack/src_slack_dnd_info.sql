with source as (
  select * from {{ source('slack', 'dnd_info') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
