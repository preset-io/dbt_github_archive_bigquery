{{ config(
    materialized='incremental',
    unique_key=['entity_type', 'entity_id', 'team_id'],
    tags=['egaf']
) }}

WITH events_agg AS (
    SELECT
        entity_type,
        entity_id,
        COALESCE(team_id, -1) AS team_id,
        MIN(dt) AS first_event,
        MAX(dt) AS most_recent_event,
    FROM {{ ref('wrk_egaf_events') }}
    -- records aggregated at the full team level
    WHERE workspace_hash = 'ALL WORKSPACES'
    {% if is_incremental() %}
    AND {{ generate_incremental_statement(this) }}
    {% endif %}
    GROUP BY
        entity_type,
        entity_id,
        team_id

    {% if is_incremental() %}
    UNION ALL

    SELECT
        entity_type,
        entity_id,
        team_id,
        first_event,
        most_recent_event,
    FROM {{ this }}
    {% endif %}
)

SELECT
    entity_type,
    entity_id,
    team_id,
    MIN(first_event) AS first_event,
    MAX(most_recent_event) AS most_recent_event,
FROM events_agg
GROUP BY
    entity_type,
    entity_id,
    team_id
