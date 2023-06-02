{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}



-- Identifying teams made out of mostly Preset members
WITH team_counts AS (
    SELECT
        A.ds,
        B.team_id,
        SUM(CASE WHEN A.is_preset THEN 1 ELSE 0 END) AS preset_members,
        COUNT(*) AS team_size,
    FROM {{ ref('src_manager_preset_user') }} AS A
    INNER JOIN {{ ref('src_manager_team_membership') }} AS B
        ON A.user_id = B.user_id
            AND A.ds = B.ds
    WHERE A.ds >= {{ var("start_date") }}
    {% if is_incremental() %}
        AND {{ generate_incremental_statement(this, date_col='A.ds', this_date_col='ds') }}
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    ds,
    team_id,
    preset_members,
    team_size,
    preset_members / team_size AS preset_percentage,
FROM team_counts
WHERE preset_members > 0
    AND preset_members / team_size > 0.5
