with source as (
  select * from {{ source('slack', 'usergroup_user') }}
),
renamed as (
  select
    *

  from source
)
select * from renamed
