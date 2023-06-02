{{
  config(
    alias='ticket',
    materialized='view'
  )
}}



{%- set cols = dbtplyr.get_column_names( source('fivetran_hubspot', 'ticket') ) -%}
{%- set cols_property = dbtplyr.starts_with('property_', cols) %}
{%- set cols_reg = dbtplyr.not_contains('property_', cols) %}

SELECT
  {{ dbtplyr.across(cols_reg, "{{var}} as {{var}}") }},
  {{ rem_across(cols_property, 'property_') }}


FROM
  {{ source('fivetran_hubspot', 'ticket') }}
