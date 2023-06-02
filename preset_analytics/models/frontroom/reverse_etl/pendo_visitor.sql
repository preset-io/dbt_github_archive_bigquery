WITH production_pendo_users AS (
  SELECT
    CAST(REGEXP_EXTRACT(visitor_id, r'([0-9]+)-production') AS INT64) AS user_id
    , visitor_id
    , visitor_id_hash
  FROM {{ ref('pendo__visitor') }}
  WHERE REGEXP_CONTAINS(visitor_id, '-production')
),

all_users AS (
  SELECT
    production_pendo_users.visitor_id,
    production_pendo_users.visitor_id_hash,
    user.user_id,
    user.intent_choice_evaluator AS is_evaluator,
    user.intent_choice_connector AS is_connector,
    user.intent_choice_builder AS is_builder,
    user.intent_choice_consumer AS is_consumer,
    user.intent_choice_other AS is_other,
    team.non_example_database_count AS non_example_database_by_team_count,
    COALESCE(user_events.workspace_hostname_first, team_events.workspace_hostname_first) AS workspace_hostname_first,
    COALESCE(user_events.workspace_hostname_latest, team_events.workspace_hostname_latest) AS workspace_hostname_latest,
    COALESCE(user_events.workspace_hostname_is_most_active, team_events.workspace_hostname_is_most_active) AS workspace_hostname_is_most_active,
  FROM production_pendo_users
  INNER JOIN {{ ref('manager_user') }} AS user
    ON production_pendo_users.user_id = user.user_id
  LEFT JOIN {{ ref('user_events_agg_denorm') }} AS user_events
    ON user.user_id = CAST(user_events.user_id AS INT64)
  LEFT JOIN {{ ref('user_team_events_agg') }} AS team_map
    ON user.user_id = CAST(team_map.user_id AS INT64)
      AND team_map.is_most_active
  LEFT JOIN {{ ref('manager_team') }} AS team
    ON team_map.team_hash = team.team_hash
  LEFT JOIN {{ ref('team_events_agg_denorm') }} AS team_events
    ON team.team_id = team_events.team_id
)

SELECT *
FROM all_users
