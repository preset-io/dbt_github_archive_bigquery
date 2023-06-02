WITH chameleon_users AS (
  SELECT
    user_id,
    chameleon_id,
  FROM {{ ref('wrk_chameleon_user_mapping') }}
)

SELECT
  chameleon_users.user_id,
  chameleon_users.chameleon_id,
  user.company_size,
  user.department,
  user.is_preset,
  user.role,
  user.intent_choice_evaluator AS onboarding_response_choice_evaluator,
  user.intent_choice_connector AS onboarding_response_choice_connector,
  user.intent_choice_builder AS onboarding_response_choice_builder,
  user.intent_choice_consumer AS onboarding_response_choice_consumer,
  user.intent_choice_other AS onboarding_response_choice_other,
  user.created_dttm AS user_created_dttm,
  team.non_example_database_count AS non_example_database_by_team_count,
  COALESCE(user_events.workspace_hostname_first, team_events.workspace_hostname_first) AS workspace_hostname_first,
  COALESCE(user_events.workspace_hostname_latest, team_events.workspace_hostname_latest) AS workspace_hostname_latest,
  COALESCE(user_events.workspace_hostname_is_most_active, team_events.workspace_hostname_is_most_active) AS workspace_hostname_is_most_active,
FROM chameleon_users
INNER JOIN {{ ref('manager_user') }} AS user
  ON chameleon_users.user_id = user.user_id
LEFT JOIN {{ ref('user_events_agg_denorm') }} AS user_events
  ON user.user_id = CAST(user_events.user_id AS INT64)
LEFT JOIN {{ ref('user_team_events_agg') }} AS team_map
  ON user.user_id = CAST(team_map.user_id AS INT64)
    AND team_map.is_most_active
LEFT JOIN {{ ref('manager_team') }} AS team
  ON team_map.team_hash = team.team_hash
LEFT JOIN {{ ref('team_events_agg_denorm') }} AS team_events
  ON team.team_id = team_events.team_id
