/*
Using manager_team to create a table at the `hs_company_id` atomicity.
In reality Hubspot companies may have zero or many teams associated,
but most have a single one. For those who have many, we select the
primary team via the is_primary_team flag in manager_team.
 */
{% set max_fields = [
    'team_id',
    'team_hash',
    'team_description',
    'workspace_limit',
    'team_name',
    'team_is_deleted',
    'team_billing_status',
    'team_auth_connection',
    'ds',
    'created_dttm',
    'team_creation_era',
    'last_modified_dttm',
    'creator_user_id',
    'last_modified_user_id',
    'is_preset',
    'contract_start_date',
    'contract_end_date',
    'email_domains',
    'first_visit',
    'most_recent_visit',
    'activation_score',
]%}
{% set sum_fields = [
    'dau',
    'wau',
    'mau',
    'da_dashboard',
    'wa_dashboard',
    'ma_dashboard',
    'da_dashboard_noex',
    'wa_dashboard_noex',
    'ma_dashboard_noex',
    'invite_pending',
    'invite_accepted',
    'invite_sent',
    'non_example_database_count',
    'visits_7d',
    'visits_28d',
    'seats',
    'mrr',
    'arr',
    'workspace_count',
    'dashboard_noex_count',
]%}
SELECT
    hs_company_id,
    ARRAY_AGG(team_id) AS array_team_id,
    -- associate the primary team with the company
    -- this is dedup'ed in manager team
    COALESCE(
        MIN(CASE WHEN is_primary_team_for_company then team_id END)
        , MAX(team_id)
    ) AS primary_team_id,
    {% for s in max_fields %}
    MAX({{ s }}) AS {{ s }},
    {% endfor %}
    {% for s in sum_fields %}
    SUM({{ s }}) AS {{ s }},
    {% endfor %}
    TO_JSON_STRING(ARRAY_CONCAT_AGG(connected_database_type_array)) AS connected_database_type_array,
FROM {{ ref('manager_team') }}
WHERE hs_company_id IS NOT NULL
GROUP BY 1
