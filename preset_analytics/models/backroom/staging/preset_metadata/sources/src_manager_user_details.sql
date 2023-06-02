SELECT
  ds,
  id,
  user_id,
  created_dttm,
  last_modified_dttm,
  creator_user_id,
  last_modified_user_id,
  company_name,
  company_category,
  company_size,
  use_description,
FROM {{ ref('src_manager_user_details_dedup') }}
WHERE ds >= {{ var("start_date") }}
