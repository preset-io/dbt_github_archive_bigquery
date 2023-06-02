{% set reverse_trial_launch_date = '2022-11-16' %}
{% set trial_length = 14 %}

SELECT
    ds,
    id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    name AS team_hash,
    description,
    workspace_limit,
    title,
    deleted,
    auth_connection,
    recurly_plan_code,
    initial_signup_tier,
    tier,
    subscription_status,
    billing_frequency,
    billing_method,
    billing_status AS old_billing_status,
    new_billing_status,
    COALESCE(
        billing_status,
        CASE
            WHEN deleted THEN 'DELETED'
            ELSE CASE
                WHEN tier IN ('PARTNER', 'ENTERPRISE', 'INTERNAL') THEN 'ENTERPRISE'
                WHEN subscription_status = 'FREE' THEN 'FREE'
                WHEN subscription_status = 'PAID' THEN 'PAID'
                WHEN subscription_status = 'TRIAL' THEN
                    CASE WHEN created_dttm < '{{ reverse_trial_launch_date }}'
                        THEN 'PRE_REVERSE_'
                        ELSE ''
                    END
                    ||
                    CASE
                        WHEN new_billing_status IN ('DELINQUENT')
                            THEN 'TRIAL_EXPIRED'
                        WHEN new_billing_status NOT IN ('DELINQUENT') THEN 'TRIAL'
                    END
                  WHEN subscription_status = 'EXPIRED' AND new_billing_status = 'EXPIRED' THEN 'TRIAL_EXPIRED'
                ELSE 'INDETERMINATE'
            END
        END
        ) AS billing_status,
    is_hibernated,
FROM {{ ref('src_manager_team_dedup') }}
WHERE ds >= {{ var("start_date") }}
    -- Related to a spam incident on 2021-11-21
    AND title NOT LIKE 'ПОЛУЧИТЕ%'
