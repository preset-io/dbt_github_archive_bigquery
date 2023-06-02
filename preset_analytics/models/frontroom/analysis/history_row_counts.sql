{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

{# all history tables of note in the history schema #}
{% set tbls = [
    {'name': 'hubspot_deal_history', 'date_col': 'dt'},
    {'name': 'manager_invite_history', 'date_col': 'ds'},
    {'name': 'manager_team_history', 'date_col': 'ds'},
    {'name': 'manager_user_history', 'date_col': 'ds'},
    {'name': 'manager_workspace_history', 'date_col': 'ds'},
    {'name': 'superset_chart_history', 'date_col': 'dt'},
    {'name': 'superset_dashboard_chart_history', 'date_col': 'dt'},
    {'name': 'superset_dashboard_history', 'date_col': 'dt'},
    {'name': 'superset_database_connection_history', 'date_col': 'dt'},
    {'name': 'superset_dataset_history', 'date_col': 'dt'},
    {'name': 'superset_report_schedule_history', 'date_col': 'dt'},
    {'name': 'superset_saved_query_history', 'date_col': 'dt'}
  ]
%}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
),

daily_counts AS (
  {% for tbl in tbls %}
    SELECT
      date_spine.dt,
      "{{ tbl.name }}" AS tbl_name,
      COUNT(*) AS num_records
    FROM date_spine
    LEFT JOIN {{ ref(tbl.name) }} AS t
      ON date_spine.dt = t.{{ tbl.date_col }}
    GROUP BY 1,2

    {% if not loop.last %}
    UNION ALL
    {% endif %}

  {% endfor %}
)

SELECT
  dt,
  tbl_name,
  num_records
FROM daily_counts
