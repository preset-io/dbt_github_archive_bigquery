WITH V as (
  SELECT
    user_id,
    answers,
    CASE WHEN has_superset_experience THEN 'Yes'
         WHEN NOT has_superset_experience THEN 'No'
         ELSE 'Unknown'
         END AS retl_apache_superset_user,
    role,
    department,
    CASE WHEN intent_choice_evaluator THEN 'Evaluator;'
         END AS evaluator,
    CASE WHEN intent_choice_connector THEN 'Connector;'
         END AS connector,
    CASE WHEN intent_choice_builder THEN 'Builder;'
         END AS builder,
    CASE WHEN intent_choice_consumer THEN 'Consumer;'
         END AS consumer,
    CASE WHEN intent_choice_other IS NOT NULL THEN 'Other'
         END AS other,
    LEFT(intent_choice_other, 128) AS other_value
  FROM {{ ref( 'manager_user_onboarding_latest' ) }}
  )

SELECT
  PU.email,
  V.user_id,
  V.answers,
  V.retl_apache_superset_user,
  V.role AS retl_what_is_your_role,
  V.department AS retl_department_function,
  V.other_value AS other_preset_user_intent,
  {{ safe_concat(['V.evaluator', 'V.connector', 'V.builder', 'V.consumer', 'V.other']) }} AS intent
FROM V
INNER JOIN {{ ref( 'manager_preset_user_latest' ) }} as PU
  ON V.user_id = PU.user_id
WHERE PU.email IS NOT NULL
