{#
    Given a source table and a DATETIME or DATE column, this macro returns
    a DATE `YYYY-MM-DD` pointing the the most recent available day.

    For streaming tables that come in impredictable batches, the previous_day
    flag when on will very likely point to the latest FULL day as we can
    safely assume that if we have received event for a given day, the
    previous one is complete.
#}
{%- macro latest_dt(source_table, dttm_column="ds", previous_day=False) -%}
	{%- set max_partition = none -%}
    {%- if execute -%}
        {%- call statement('max_partition_date_query', fetch_result=True) -%}
          SELECT CAST(MAX({{ dttm_column }}) AS DATE) AS max_partition_date
          FROM  {{ source_table }}
          WHERE {{ dttm_column }} >= CAST('1970-01-01' AS DATE)
        {%- endcall -%}
	    {%- set result = load_result('max_partition_date_query') -%}
        {% if result %}
	        {%- set max_partition = result['data'][0][0] -%}
        {%- endif -%}
    {%- endif -%}
    {%- if max_partition -%}
        {%- if previous_day-%}
	        {%- set max_partition = max_partition - modules.datetime.timedelta(1) -%}
            {{- max_partition.strftime('%Y-%m-%d') -}}
        {%- else -%}
            {{- max_partition.strftime('%Y-%m-%d') -}}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}
