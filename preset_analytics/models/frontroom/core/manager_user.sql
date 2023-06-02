{{ config(materialized='table') }}
{% set results = run_query("SELECT list_id, manager_user_boolean_column_name FROM " ~ ref("seed_hs_list_pivot")) %}

SELECT
  *
FROM {{ ref('manager_user_history') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('manager_user_history') }})
