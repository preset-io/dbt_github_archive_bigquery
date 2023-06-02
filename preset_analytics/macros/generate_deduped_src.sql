{% macro generate_deduped_src(model, primary_key, order_by_key) %}
    WITH src AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ primary_key }}
                ORDER BY {{ order_by_key }} DESC
            ) AS rn,
        FROM {{ model }}
    )

    SELECT * EXCEPT(rn)
    FROM src
    WHERE rn = 1

{% endmacro %}
