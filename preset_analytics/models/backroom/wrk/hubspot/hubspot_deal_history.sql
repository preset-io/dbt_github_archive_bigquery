{{ config(
    schema='history',
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

SELECT
  A.*,
  {{ team_attributes(alias="B", include_team_id=False, include_core=False, include_mau_rank=False) }}
FROM {{ ref('wrk_hubspot_deal_history') }} AS A
LEFT JOIN {{ ref('wrk_manager_team') }} AS B
  ON A.preset_team_id = B.team_id
    AND A.dt = B.ds
