{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'dt', 'data_type': 'date'},
    tags=['egaf']
) }}


-- Big UNION ALL of all the entities we want to track
WITH all_entities AS (
    SELECT * FROM {{ ref('wrk_egaf_event_action') }}

    UNION ALL

    SELECT * FROM {{ ref('wrk_egaf_event_chart') }}

    UNION ALL

    SELECT * FROM {{ ref('wrk_egaf_event_dashboard') }}

    UNION ALL

    SELECT * FROM {{ ref('wrk_egaf_event_team') }}

    UNION ALL

    SELECT * FROM {{ ref('wrk_egaf_event_user') }}

),

all_entities_agg AS (
    SELECT
        entity_type,
        dt,
        team_id,
        workspace_hash,
        entity_id,
        is_example,
        COUNT(*) AS events,
    FROM all_entities
    GROUP BY 1, 2, 3, 4, 5, 6

    UNION DISTINCT

    SELECT
        entity_type,
        dt,
        team_id,
        'ALL WORKSPACES' AS workspace_hash,
        entity_id,
        is_example,
        COUNT(*) AS events,
    FROM all_entities
    GROUP BY 1, 2, 3, 4, 5, 6
)


SELECT
    entity_type,
    dt,
    team_id,
    COALESCE(workspace_hash, 'ALL WORKSPACES') AS workspace_hash,
    entity_id,
    is_example,
    events,
FROM all_entities_agg
