{% macro switch(input, mapping, direction='left') %}
    {# macro to switch based on a list of tuple mappings.
        input - field to switch
        mapping - list of mappings
        direction - whether to apply the mappings from left to right or right to left
    #}

    {# reverse the direction of the mapping #}
    {%- if direction == 'right' -%}
        {%- set mapping_r = [] -%}
        {%- for left, right in mapping -%}
            {{ mapping_r.append([right, left]) | default("", True)}}
        {%- endfor -%}
        {%- set mapping = mapping_r -%}
    {%- endif -%}

    CASE {{ input }}
        {% for left, right in mapping %}
        WHEN {{ left }} THEN {{ right }}
        {% endfor %}
    END
{% endmacro %}

{% macro safe_concat(field_list) %}
  {# Takes an input list and generates a concat() statement with each argument in the list safe_casted to a string and wrapped in an ifnull() #}
  concat({% for f in field_list %}
    ifnull(safe_cast({{ f }} as string), '')
    {% if not loop.last %}, {% endif %}
  {% endfor %})
{% endmacro %}

{% macro array_from_string(field_name, remove_quotes=True, select_distinct=True) %}
    {# Takes an array string and produces an array #}
    ARRAY(
        SELECT
        {% if select_distinct %}
        DISTINCT
        {% endif %}
        *
        FROM UNNEST(
            SPLIT(
                SUBSTR(
                    {% if remove_quotes %}
                    REPLACE(
                    {% endif %}
                    TRIM({{field_name}})
                    {% if remove_quotes %}
                    , "'", "")
                    {% endif %}
                    ,
                    2,
                    LENGTH(
                        {% if remove_quotes %}
                        REPLACE(
                        {% endif %}
                        TRIM({{field_name}})
                        {% if remove_quotes %}
                        , "'", "")
                        {% endif %}
                    ) - 2
                )
            )
        )
    )
{% endmacro %}
