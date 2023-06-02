{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('manager_invite_history') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('manager_invite_history') }})
