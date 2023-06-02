{% set start_dt = '2019-01-01' %}

SELECT
    ds,
    id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    user_id,
    has_superset_experience,
    role,
    department,
    intent_choice_evaluator,
    intent_choice_connector,
    intent_choice_builder,
    intent_choice_consumer,
    intent_choice_other,
    answers,
FROM {{ ref('src_manager_user_onboarding_dedup') }}
WHERE ds >= {{ var("start_date") }}
