{#
    Creates a query that `UNION ALL` a list of table_names.
    `table_prefix` will be used as prefix to the table name in the list
    `table_suffix` will be used as suffix to the table name in the list
    `xpaths` is a list of tuples that each contain:
        - the column name to extract from
        - the xpath extraction string
        - the alias for the new extracted column
#}

{% macro airbyte_github_union_all(table_suffix, xpaths=[]) %}
    {%- for repo in var('github_repos') %}
        SELECT
        '{{ repo.repo}}' AS repo,
        {%- for col, xpath, alias in xpaths %}
            JSON_EXTRACT_SCALAR({{ col }}, '{{ xpath }}') AS {{ alias}},
        {%- endfor %}
        *,
        FROM `preset-cloud-analytics`.community_data_base.github_{{ repo.table_str }}_{{ table_suffix }}
        {%- if not loop.last %}
            UNION ALL
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}
