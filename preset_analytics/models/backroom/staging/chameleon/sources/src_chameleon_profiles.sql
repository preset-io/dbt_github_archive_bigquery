with source as (
      select * from {{ source('chameleon', 'profiles') }}
),
renamed as (
  select
    uid AS chameleon_uid,
    id AS chameleon_id,
    email,
    delivery_ids,
    browser_x,
    browser_tz,
    browser_l,
    browser_n,
    browser_k,
    user_created_dttm,
    last_seen_at,
    last_seen_session_count,
    onboarding_response_choice_evaluator,
    onboarding_response_choice_connector,
    onboarding_response_choice_builder,
    onboarding_response_choice_consumer,
    onboarding_response_choice_other,
    is_preset,
    percent,
    chameleon_tag_ids,
    chameleon_launchers_started,
    chameleon_admin,
    username_hash,
    role,
    department,
    company_id AS chameleon_company_id,
    company,
    company_size,
    created_at,
    updated_at,
    loaded_at,
  from source
)
select * from renamed
