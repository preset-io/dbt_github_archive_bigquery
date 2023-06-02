{% macro run_audit_helper() %}
    {%- set columns_to_compare=adapter.get_columns_in_relation(ref('wrk_egaf_summary'))  -%}

    {% set old_etl_relation_query %}
        select *, {{ dbt_utils.surrogate_key(['team_id','entity_type','is_example','dt','status_7d','status_28d','ga_previous_status_7d','ga_previous_status_28d','l7','l28']) }} as pk
        from (select * except (team_id), coalesce(team_id, -1) as team_id from {{ ref('wrk_egaf_summary') }})
        where is_example is not null
    {% endset %}

    {% set new_etl_relation_query %}
        select *, {{ dbt_utils.surrogate_key(['team_id','entity_type','is_example','dt','status_7d','status_28d','ga_previous_status_7d','ga_previous_status_28d','l7','l28']) }} as pk
        from `core_wrk_pr285`.`wrk_egaf_summary`
    {% endset %}

    {% if execute %}
        {% for column in columns_to_compare %}
            {{ log('Comparing column "' ~ column.name ~'"', info=True) }}

            {% set audit_query = audit_helper.compare_column_values(
                a_query=old_etl_relation_query,
                b_query=new_etl_relation_query,
                primary_key="pk",
                column_to_compare=column.name
            ) %}

            {% set audit_results = run_query(audit_query) %}
            {% do audit_results.print_table() %}
            {{ log("", info=True) }}

        {% endfor %}
    {% endif %}
{% endmacro %}
