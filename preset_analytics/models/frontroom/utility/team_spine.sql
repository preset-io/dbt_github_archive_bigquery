{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    id AS team_id,
    MIN(DATE(created_dttm)) AS team_created_dttm
FROM {{ ref('src_manager_team') }}
GROUP BY 1
