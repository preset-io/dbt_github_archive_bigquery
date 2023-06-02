{{ config(materialized='table', tags='egaf') }}
{#
-- Daily
SELECT
    NULLIF(A.team_id, -1) AS team_id,
    A.first_event,
    DATE_DIFF(B.dt, A.first_event, DAY) AS lag,
    COUNT(*) AS users,
FROM {{ ref('wrk_egaf_first_event') }} AS A
INNER JOIN {{ ref('wrk_egaf_events') }} AS B
    ON A.entity_id = B.entity_id
    AND A.entity_type = B.entity_type
    AND NULLIF(A.team_id, -1) = B.team_id
-- records aggregated at the full team level
WHERE B.workspace_hash = 'ALL WORKSPACES'
GROUP BY 1, 2, 3
#}
-- Weekly
SELECT
    A.entity_type,
    A.team_id,
    A.first_event,
    DATE_DIFF(B.dt, A.first_event, WEEK) AS lag,
    COUNT(*) AS users,
FROM (
    SELECT
        NULLIF(team_id, -1) AS team_id,
        entity_id,
        entity_type,
        DATE_TRUNC(first_event, WEEK) AS first_event,
    FROM {{ ref('wrk_egaf_first_event') }}
    WHERE entity_type = 'user'
) AS A
INNER JOIN (
    SELECT DISTINCT
        entity_id,
        entity_type,
        team_id,
        DATE_TRUNC(dt, WEEK) AS dt,
    FROM {{ ref('wrk_egaf_events') }}
    WHERE entity_type = 'user'
        AND workspace_hash = 'ALL WORKSPACES'
) AS B
    ON
    A.entity_id = B.entity_id
    AND A.entity_type = B.entity_type
    AND A.team_id = B.team_id
GROUP BY 1, 2, 3, 4
