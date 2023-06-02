{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

{{ preset_scrape_model_prep(
    source_table=source('production_preset_metadata', 'manager_team_membership'),
    dttm_column='ds',
    pk_fields=['user_id', 'team_id']
) }}
