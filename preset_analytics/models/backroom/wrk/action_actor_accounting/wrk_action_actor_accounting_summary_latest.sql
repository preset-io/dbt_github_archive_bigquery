{{ config(materialized='table') }}

SELECT
    ds,
    team_id,
    object_key,
    object_type,
    action,
    ltd_views,
    ltd_users,
    {% for i in var("day_counts") %}
        n{{ i }}d_views,
        n{{ i }}d_users,
    {% endfor %}
FROM {{ ref('wrk_action_actor_accounting_summary') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('wrk_action_actor_accounting_summary') }})
