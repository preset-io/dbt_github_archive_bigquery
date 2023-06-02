{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'},
) }}

SELECT
    ds,
    id AS workspace_id,
    name AS workspace_hash,
    title AS workspace_title,
    hostname AS workspace_hostname,
    split(hostname, '.')[OFFSET(1)] AS workspace_region,
    description AS workspace_description,
    allow_public_dashboards,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    last_accessed_at,
    team_id,
    status AS workspace_status,
FROM {{ ref('src_manager_workspace') }}
{% if is_incremental() %}
WHERE {{ generate_incremental_statement(this, date_col='ds', this_date_col='ds') }}
{% endif %}
