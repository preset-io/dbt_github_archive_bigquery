{{ config(
    materialized='incremental',
    full_refresh = var("super_full_refresh_use_with_care"),
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}


WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

SELECT
  date_spine.dt,
  pv.context_ip AS ip_address,
  CURRENT_TIMESTAMP() AS preprocessed_at, -- for use in process selection
  MIN(pv.received_at) AS first_received_at,
FROM date_spine
LEFT JOIN {{ ref('wrk_segment_unified_page_views') }} AS pv
  ON date_spine.dt = DATE(pv.received_at)
WHERE pv.context_ip IS NOT NULL
GROUP BY 1, 2, 3
