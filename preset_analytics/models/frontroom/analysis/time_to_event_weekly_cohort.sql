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

SELECT
    DATE_TRUNC(created_dttm, week) AS week_cohort
    , COUNT(team_id) AS num_teams
    {% for event_type in event_types %}
    -- {{ event_type.event_name }}
    , COUNT(days_to_{{ event_type.event_name }}) AS num_teams_{{ event_type.event_name }}
    , SAFE_DIVIDE(COUNT(days_to_{{ event_type.event_name }}), COUNT(*)) AS ptc_{{ event_type.event_name }}
    , AVG(days_to_{{ event_type.event_name }}) AS avg_day_to_{{ event_type.event_name }}
    {% endfor %}
FROM {{ ref('time_to_event') }}
GROUP BY ROLLUP (week_cohort)
