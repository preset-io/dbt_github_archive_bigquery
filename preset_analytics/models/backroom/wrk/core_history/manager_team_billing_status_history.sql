{{ config(
    materialized='table',
    schema='preset_metadata'
) }}

{% set partition_start_dt = '2017-01-01' %} -- arbitrary date in the past. needed to query partitioned table.

WITH status_leads AS (
    SELECT
        id,
        ds,
        billing_status,
        LAG(billing_status) OVER (PARTITION BY id ORDER BY ds) AS billing_status_previous,
    FROM {{ ref('src_manager_team') }}
    WHERE ds > '{{ partition_start_dt }}'
)

SELECT
    id,
    billing_status,
    ds AS effective_from,
    LAG(ds) OVER (PARTITION BY id ORDER BY ds DESC) AS effective_to,
    ROW_NUMBER() OVER (PARTITION BY id, billing_status ORDER BY ds) AS num_status_type_to_dt,
    ROW_NUMBER() OVER (PARTITION BY id, billing_status ORDER BY ds) = 1 AS is_first_for_status,
    ROW_NUMBER() OVER (PARTITION BY id, billing_status ORDER BY ds DESC) = 1 AS is_latest_for_status,
FROM status_leads
WHERE billing_status != billing_status_previous
    OR billing_status_previous IS NULL
