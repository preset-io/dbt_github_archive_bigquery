{{ config(materialized='table') }}

SELECT DISTINCT
    CONCAT(B.hs_contact_id, '_', A.team_id) AS pk,
    CAST(A.team_id AS STRING) AS team_id,
    CAST(B.hs_contact_id AS STRING) AS hs_contact_id,
FROM {{ ref('manager_team_membership_latest') }} AS A
INNER JOIN {{ ref('wrk_manager_user_latest') }} AS B
    ON A.user_id = B.user_id
WHERE B.hs_contact_id IS NOT NULL
    AND A.team_id IS NOT NULL
