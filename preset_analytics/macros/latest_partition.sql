{#
    Given a source table that is partitionned, this macro returns
    a query pointing to the latest partition

    Note that this could be improved to better support different column
    types with proper casting. We assume a DATETIME column here.

    Also note that we may need to wait on a more directed signal from the
    previous ETL phase that insures that ALL workspaces have been synced
#}
{% macro latest_partition(source_table, dttm_column="ds", previous_day=False) %}
    {%- set max_partition = latest_dt(source_table, dttm_column, previous_day) -%}

	SELECT * FROM {{ source_table }}
    {% if max_partition -%}
        WHERE {{ dttm_column }} = '{{ max_partition }}'
    {% else -%}
        WHERE {{ dttm_column }} > '1970-01-01'
    {% endif %}
{% endmacro %}
