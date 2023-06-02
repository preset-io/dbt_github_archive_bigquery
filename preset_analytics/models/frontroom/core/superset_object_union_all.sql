{%
set common_fields = [
    'is_example',
    'is_preset',
    'team_name',
    'team_billing_status',
    'team_id',
    'team_is_deleted',
    'workspace_title',
    'hs_company_name',
    'hs_company_state',
    'hs_deal_stage',
    'hs_company_customer_type',
    'hs_company_db_created',
    'hs_company_deal_pipeline_name',
    'hs_company_ce_status',
    'hs_company_owner_id',
    'hs_company_last_activity_date',
]
%}
{% set common_fields_str = common_fields | join(', \n    ') %}

SELECT
    created_dttm,
    dashboard_key AS object_key,
    'dashboard' AS object_type,
    dashboard_title AS label,
    {{ common_fields_str }}
FROM {{ ref('superset_dashboard') }}
----------------------------------------------------------------------------
UNION ALL

SELECT
    created_dttm,
    chart_key,
    'chart' AS object_type,
    chart_name,
    {{ common_fields_str }}
FROM {{ ref('superset_chart') }}
----------------------------------------------------------------------------
UNION ALL

SELECT
    created_dttm,
    database_connection_key,
    'database' AS object_type,
    database_connection_name,
    {{ common_fields_str }}
FROM {{ ref('superset_database_connection') }}
----------------------------------------------------------------------------
UNION ALL

SELECT
    created_dttm,
    saved_query_key,
    'saved_query' AS object_type,
    saved_query_label,
    {{ common_fields_str }}
FROM {{ ref('superset_saved_query') }}
