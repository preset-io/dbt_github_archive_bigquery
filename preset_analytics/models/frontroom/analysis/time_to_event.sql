{{
  config(
    materialized='table'
  )
}}

{% set event_types = [
    {"event_name":'activated', "logic":'is_activated'},
    {"event_name":'non_ex_dbs', "logic":'non_example_database_count > 0'},
    {"event_name":'non_ex_visuals', "logic":'chart_noex_count > 0'},
    {"event_name":'non_ex_dashboards', "logic":'dashboard_noex_count > 0'},
    {"event_name":'invited_user', "logic":'invite_sent > 0'},
    {"event_name":'greater_than_one_registerd_users', "logic":'team_members > 1'},
    {"event_name":'dashboards_w_viewers', "logic":'da_dashboard > 0'},
    ]
%}


WITH
{% for event_type in event_types %}
    ranked_time_to_{{ event_type.event_name }} AS (
        SELECT
            team_id,
            DATE(created_dttm) AS created_dt,
            ds AS {{ event_type.event_name }}_dt,
            ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY ds) AS rn,
        FROM {{ ref('manager_team_history') }}
        WHERE {{ event_type.logic }}
    ),

    time_to_{{ event_type.event_name }} AS (
        SELECT
            team_id,
            created_dt,
            {{ event_type.event_name }}_dt,
            DATE_DIFF({{ event_type.event_name }}_dt, created_dt, DAY) AS days_to_{{ event_type.event_name }},
        FROM ranked_time_to_{{ event_type.event_name }}
        WHERE rn = 1
    ),
{% endfor %}

joined_events AS (
    SELECT
        team.team_id,
        team.created_dttm,
        {% for event_type in event_types %}
        time_to_{{ event_type.event_name }}.days_to_{{ event_type.event_name }},
        {% endfor %}
    FROM {{ ref('manager_team_history') }} AS team
    {% for event_type in event_types %}
    LEFT JOIN time_to_{{ event_type.event_name }}
        ON team.team_id = time_to_{{ event_type.event_name }}.team_id
    {% endfor %}
    WHERE team.ds = (SELECT MAX(ds) FROM {{ ref('manager_team_history') }})
        AND team.team_id IS NOT NULL
        AND team.created_dttm IS NOT NULL
)

SELECT *
FROM joined_events
