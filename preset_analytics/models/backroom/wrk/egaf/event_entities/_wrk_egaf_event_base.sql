{{ config(materialized='ephemeral') }}

{% set process_to_dt = latest_dt(ref('superset_event_log'), "dt", previous_day=False) %}

SELECT *
FROM {{ ref('superset_event_log') }}
WHERE dt <= DATE("{{ process_to_dt }}")
{% if is_incremental() %}
    AND {{ generate_incremental_statement(this) }}
{% endif %}
