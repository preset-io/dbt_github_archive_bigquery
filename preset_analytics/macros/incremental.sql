{% macro generate_incremental_statement(model, date_col='dt', this_date_col='dt', inc_dt_range_strategy=var('inc_dt_range_strategy', "date_offset") ) %}

    {#  This macro generates the dates to be replaced in incremental models.

        Strategy options are the following:
            date_offset (default)
                generates a date range based on inc_dt_offset var
            range_select
                user provided range of dates to replace
            list_select
                user provided list of dates to replace
            catchup
                all dates greater than or equal to the max of the currently model

        Set inc_verbose to True to print logs to CLI
    #}

    {% set defaults = {
        'inc_dt_range_strategy': "date_offset",
        'inc_dt_offset_days': 2,
        'inc_dt_list_select': [],
        'inc_dt_range_start_dt': 'DATE_ADD(DATE("' ~ run_started_at.strftime("%Y-%m-%d") ~ '"), INTERVAL -7 DAY)',
        'inc_dt_range_end_dt': 'DATE_ADD(DATE("' ~ run_started_at.strftime("%Y-%m-%d") ~ '"), INTERVAL -1 DAY)'
        }
    %}

    {% set configs = {
        'inc_dt_range_strategy': var('inc_dt_range_strategy', defaults['inc_dt_range_strategy']),
        'inc_dt_offset_days': var('inc_dt_offset_days', defaults['inc_dt_offset_days']),
        'inc_dt_list_select': var('inc_dt_list_select', defaults['inc_dt_list_select']),
        'inc_dt_range_start_dt': var('inc_dt_range_start_dt', defaults['inc_dt_range_start_dt']),
        'inc_dt_range_end_dt': var('inc_dt_range_end_dt', defaults['inc_dt_range_end_dt'])
        }
    %}

    {{ log('running '~model~' with '~configs['inc_dt_range_strategy']~" incremental strategy", info=var('inc_verbose', False)) }}

    {%- if configs['inc_dt_range_strategy'] == 'catchup' -%}
        {{date_col}} >= (SELECT MAX({{this_date_col}}) FROM {{ this }})
    {%- else -%}
        {%- set partitions_to_replace = generate_partitions_to_replace(configs) -%}
        {{date_col}} IN ({{ partitions_to_replace | join(',') }})
    {%- endif -%}
{% endmacro %}


{# macro to generate incremental partitions to replace #}
{% macro generate_partitions_to_replace(configs) %}
    {% set partitions_to_replace = [] %}

    {%- if configs['inc_dt_range_strategy'] == "range_select" -%}
        {% if execute %}
            {% set date_range_query %}
                SELECT dt
                FROM {{ref('date_spine')}}
                WHERE dt BETWEEN {{ configs['inc_dt_range_start_dt'] }} AND {{ configs['inc_dt_range_end_dt'] }}
            {% endset %}
            {% set results = run_query(date_range_query) %}

            {%- for dt in results.columns[0].values() -%}
                {{ partitions_to_replace.append( '"' + (dt)|string + '"' ) }}
            {%- endfor -%}
        {% endif %}

    {%- elif configs['inc_dt_range_strategy'] == "list_select"  -%}
       {% set partitions_to_replace = configs['inc_dt_list_select'] %}

    {%- else -%}
        {%- for n in range(configs['inc_dt_offset_days']) -%}
            {{ partitions_to_replace.append( 'date_sub(date(' + configs['inc_dt_range_end_dt'] + '), interval ' + (n)|string + ' day)' ) }}
        {%- endfor -%}]
    {%- endif -%}

    {{ return(partitions_to_replace) }}
{% endmacro %}

{% macro infer_strategy(configs, defaults) %}

    {# check if strategy is explicity set#}
    {% if configs['inc_dt_range_strategy'] != defaults['inc_dt_range_strategy'] %}

    {# check if dt list is set #}
    {% elif configs['inc_dt_list_select']|length > 0 %}
        {% do configs.update({'inc_dt_range_strategy': 'list_select'}) %}

    {# check if date range is set #}
    {% elif configs['inc_dt_range_start_dt'] != defaults['inc_dt_range_start_dt'] or configs['inc_dt_range_end_dt'] != defaults['inc_dt_range_end_dt'] %}
        {% do configs.update({'inc_dt_range_strategy': 'range_select'}) %}

    {# use default if other not able to be inferred #}
    {% else %}
    {% endif %}

    {{ return(configs) }}
{% endmacro %}

{% macro warn_on_full_refresh_attempt(this) %}
    {% if is_incremental() and not var("super_full_refresh_use_with_care") %}
        {{ log("WARNING: you are attempting a full-refresh on a protected table: " ~ this, info=True) }}
        {{ log("If you would like to run a full refresh, run with: --vars '{super_full_refresh_use_with_care: True} .", info=True) }}
    {% endif %}
{% endmacro %}
