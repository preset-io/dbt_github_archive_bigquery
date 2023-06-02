{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

{{ preset_scrape_model_prep(
    source_table=source('production_preset_metadata', 'superset_dashboard_chart'),
    dttm_column='ds',
    pk_fields=['dashboard_id', 'slice_id', 'workspace_id', 'team_id'],
    dedup_override=true,
    do_primary_key='ds, dashboard_id, slice_id, workspace_id, team_id',
    do_order_by_key='ds',
    do_except=false
) }}
