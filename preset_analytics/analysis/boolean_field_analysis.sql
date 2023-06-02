
{% set datasets = [
  'core',
  'core_analysis',
  'core_community_tracker',
  'core_external',
  'core_github',
  'core_github_preset',
  'core_google_sheets',
  'core_growth',
  'core_history',
  'core_hubspot',
  'core_hubspot_fivetran',
  'core_hubspot_managed_package',
  'core_int_pendo',
  'core_meta',
  'core_pendo',
  'core_preset_metadata',
  'core_recurly',
  'core_reverse_etl',
  'core_seeds',
  'core_segment_managed_package',
  'core_shortcut',
  'core_stg_pendo',
  'core_superset_events',
  'core_wrk',
  'production_gatsby_marketing_website'
  ]
%}

with cols as (
  {% for dataset in datasets %}
  select
    column_name
    , table_schema || '.' || table_name as full_table_path
    , data_type
  from preset-cloud-dbt.{{ dataset }}.INFORMATION_SCHEMA.COLUMNS

  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

, string_to_bool_cols as (
  select * from cols
  where column_name like '%_is_%'
    and data_type != 'BOOL'
)

select *
from string_to_bool_cols
