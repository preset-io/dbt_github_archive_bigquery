{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

WITH date_spine AS (
    SELECT
        dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this, this_date_col='ds') }}
    {% endif %}
)

SELECT
    date_spine.dt AS ds,
    A.team_id,
    A.object_key,
    A.object_type,
    A.action,
    SUM(CAST(A.views AS BIGNUMERIC)) AS num_daily_views,
    ARRAY_AGG(DISTINCT A.manager_user_id IGNORE NULLS) AS daily_users_array,
    COUNT(DISTINCT A.manager_user_id) AS num_daily_users,
FROM date_spine
LEFT JOIN {{ ref('wrk_action_actor_accounting_raw') }} AS A
    ON date_spine.dt = A.dt
WHERE A.object_key IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
