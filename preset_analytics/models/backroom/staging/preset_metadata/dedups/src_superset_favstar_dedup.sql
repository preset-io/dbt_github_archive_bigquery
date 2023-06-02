{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

{{ preset_scrape_model_prep(
    source_table=source('production_preset_metadata', 'superset_favstar'),
    dttm_column='ds',
    pk_fields=['id', 'workspace_id'],
    dedup_override=true,
    do_primary_key='workspace_id, id, ds',
    do_order_by_key='dttm'
) }}
